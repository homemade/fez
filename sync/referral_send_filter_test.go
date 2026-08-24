package sync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/tidwall/gjson"
)

// stubReferralFilter is a test double for [ReferralSendFilter]. Each
// call to AllowSend consults the deny/error maps keyed on profileID.
// Every allowed batch records the release invocation so tests can pin
// the on-total-failure rollback path.
type stubReferralFilter struct {
	mu             sync.Mutex
	deny           map[string]bool
	errOnProfile   map[string]error
	allowed        []string // profiles AllowSend approved (order of call)
	released       []string // profiles whose release was invoked
	releaseErr     map[string]error
	seenProfileIDs []string
}

func (f *stubReferralFilter) AllowSend(ctx context.Context, profileID string) (bool, func(context.Context) error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seenProfileIDs = append(f.seenProfileIDs, profileID)
	if err := f.errOnProfile[profileID]; err != nil {
		return false, nil, err
	}
	if f.deny[profileID] {
		return false, nil, nil
	}
	f.allowed = append(f.allowed, profileID)
	pid := profileID
	release := func(context.Context) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.released = append(f.released, pid)
		return f.releaseErr[pid]
	}
	return true, release, nil
}

func newFilterTestService(t *testing.T, messagesServerURL, raiselyServerURL string, filter ReferralSendFilter) *Service {
	t.Helper()
	sc := &SyncContext{Campaign: "test-campaign"}
	sc.Config.API.Keys.Raisely = "k"
	sc.Config.API.Endpoints.Raisely = raiselyServerURL
	sc.Config.API.Endpoints.RaiselyMessages = messagesServerURL
	return &Service{
		sc:                 sc,
		fetcher:            &RaiselyFetcherAndUpdater{SyncContext: sc},
		referralSendFilter: filter,
	}
}

func threeEntryBatch() ReferralBatch {
	return ReferralBatch{
		Messages: []RaiselyCustomMessageRequest{
			{Source: "campaign:c1", User: map[string]interface{}{"email": "a@example.com"}},
			{Source: "campaign:c1", User: map[string]interface{}{"email": "b@example.com"}},
			{Source: "campaign:c1", User: map[string]interface{}{"email": "c@example.com"}},
		},
		EntryIndices:   []int{0, 1, 2},
		ProfileID:      "profile-xyz",
		ReferralsField: "private.invitations",
		ReferralsJSON: `[` +
			`{"email":"a@example.com"},` +
			`{"email":"b@example.com"},` +
			`{"email":"c@example.com"}` +
			`]`,
	}
}

// TestProcessReferrals_NilFilter_HistoricalBehaviour pins that a nil
// ReferralSendFilter leaves ProcessReferrals unchanged — every message
// is attempted and every entry is written back on success.
func TestProcessReferrals_NilFilter_HistoricalBehaviour(t *testing.T) {
	sendCount := 0
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		sendCount++
		w.WriteHeader(http.StatusOK)
	})

	var writeBack []byte
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBack = readAll(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	svc := newFilterTestService(t, messagesServer.URL, raiselyAPI.URL, nil)
	batch := threeEntryBatch()

	if err := svc.ProcessReferrals([]ReferralBatch{batch}, t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sendCount != 3 {
		t.Errorf("expected 3 sends with nil filter, got %d", sendCount)
	}
	body := gjson.ParseBytes(writeBack)
	for i := 0; i < 3; i++ {
		if !body.Get("data.private.invitations." + strconv.Itoa(i) + ".processedAt").Exists() {
			t.Errorf("entry %d should be written back with nil filter", i)
		}
	}
}

// TestProcessReferrals_FilterDenies_ShortCircuitsBatch pins the core
// concurrent-race behaviour: a filter denial on the profile short-
// circuits the whole batch — no sends, no write-back — so all
// unprocessed entries stay processedAt-empty for the next-trigger
// retry once the reservation window clears.
func TestProcessReferrals_FilterDenies_ShortCircuitsBatch(t *testing.T) {
	sendCount := 0
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		sendCount++
		w.WriteHeader(http.StatusOK)
	})

	writeBackFired := false
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBackFired = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	filter := &stubReferralFilter{deny: map[string]bool{"profile-xyz": true}}
	svc := newFilterTestService(t, messagesServer.URL, raiselyAPI.URL, filter)
	batch := threeEntryBatch()

	if err := svc.ProcessReferrals([]ReferralBatch{batch}, t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sendCount != 0 {
		t.Errorf("filter denial should short-circuit every send, got %d sent", sendCount)
	}
	if writeBackFired {
		t.Errorf("filter denial should skip the write-back; PATCH should not fire")
	}
	if len(filter.seenProfileIDs) != 1 || filter.seenProfileIDs[0] != "profile-xyz" {
		t.Errorf("filter should be consulted exactly once per batch, got %v", filter.seenProfileIDs)
	}
}

// TestProcessReferrals_MultipleBatches_FilterDeniesOne pins that
// filter denials are per-batch — one profile denied doesn't block
// another profile from proceeding through the same ProcessReferrals
// call (the team-branch fan-out case).
func TestProcessReferrals_MultipleBatches_FilterDeniesOne(t *testing.T) {
	var sentBodies [][]byte
	var mu sync.Mutex
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		body := readAll(t, r)
		mu.Lock()
		sentBodies = append(sentBodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	filter := &stubReferralFilter{deny: map[string]bool{"profile-B": true}}
	svc := newFilterTestService(t, messagesServer.URL, raiselyAPI.URL, filter)

	batchA := threeEntryBatch()
	batchA.ProfileID = "profile-A"
	batchB := threeEntryBatch()
	batchB.ProfileID = "profile-B"
	batchC := threeEntryBatch()
	batchC.ProfileID = "profile-C"

	if err := svc.ProcessReferrals([]ReferralBatch{batchA, batchB, batchC}, t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(sentBodies), 6; got != want {
		t.Errorf("expected %d sends (A and C, 3 each; B denied), got %d", want, got)
	}
	if got, want := len(filter.seenProfileIDs), 3; got != want {
		t.Errorf("filter should be consulted once per batch (3), got %d", got)
	}
}

// TestProcessReferrals_FilterError_FailClosed pins the fail-closed
// contract: a filter error on a profile skips its batch AND joins the
// error into ProcessReferrals's return so the outage surfaces in logs.
// Other batches still process.
func TestProcessReferrals_FilterError_FailClosed(t *testing.T) {
	sendCount := 0
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		sendCount++
		w.WriteHeader(http.StatusOK)
	})
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	sentinel := errors.New("backing store outage")
	filter := &stubReferralFilter{errOnProfile: map[string]error{"profile-A": sentinel}}
	svc := newFilterTestService(t, messagesServer.URL, raiselyAPI.URL, filter)

	batchA := threeEntryBatch()
	batchA.ProfileID = "profile-A"
	batchB := threeEntryBatch()
	batchB.ProfileID = "profile-B"

	err := svc.ProcessReferrals([]ReferralBatch{batchA, batchB}, t.Context())
	if err == nil {
		t.Fatal("expected error from filter to propagate, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel, got %v", err)
	}
	if sendCount != 3 {
		t.Errorf("expected 3 sends (batch B only; A skipped), got %d", sendCount)
	}
}

// TestProcessReferrals_ReleaseNeverCalled pins the core safety
// invariant: the release closure is NEVER invoked, regardless of send
// outcome. Calling release on failure would pop the reservation
// timestamp and let a concurrent worker's CheckAndReserve Proceed
// against a fresh slot — defeating the whole point of the gate.
// Referrals have no replay path anyway; a next-trigger retry waits
// for the reservation window to close naturally.
//
// The table covers all-success, partial-success, and zero-progress
// paths in one spec — the assertion is the same for every outcome.
func TestProcessReferrals_ReleaseNeverCalled(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		wantErr bool
	}{
		{
			name: "all sends succeed",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			wantErr: false,
		},
		{
			name: "partial send failure (only b@ fails)",
			handler: func(w http.ResponseWriter, r *http.Request) {
				body := readAll(t, r)
				email := gjson.GetBytes(body, "data.data.user.email").String()
				if strings.Contains(email, "b@") {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				w.WriteHeader(http.StatusOK)
			},
			wantErr: true,
		},
		{
			name: "every send fails (zero progress)",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			messagesServer := newTestRaiselyMessagesServer(t, tc.handler)
			raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(raiselyAPI.Close)

			filter := &stubReferralFilter{}
			svc := newFilterTestService(t, messagesServer.URL, raiselyAPI.URL, filter)
			batch := threeEntryBatch()

			err := svc.ProcessReferrals([]ReferralBatch{batch}, t.Context())
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(filter.released) != 0 {
				t.Errorf("release must NEVER be called by Service — got %v", filter.released)
			}
		})
	}
}

// --- test helpers ---

func readAll(t *testing.T, r *http.Request) []byte {
	t.Helper()
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	return buf
}
