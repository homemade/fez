package sync

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tidwall/gjson"
)

// newReferralsTestFetcher returns a RaiselyFetcherAndUpdater suitable
// for exercising MapFundraiserReferrals — no Raisely API calls happen
// inside that method, so the SyncContext can be minimal.
func newReferralsTestFetcher() *RaiselyFetcherAndUpdater {
	return &RaiselyFetcherAndUpdater{SyncContext: &SyncContext{Campaign: "test-campaign"}}
}

func referralsConfig(referralsField string, message RaiselyMessageMappings) Config {
	cfg := Config{Target: "ortto-activities"}
	cfg.API.Settings.RaiselyFundraiserReferralsField = referralsField
	cfg.FundraiserReferralFieldMappings = message
	return cfg
}

var standardMessageMapping = RaiselyMessageMappings{
	User: map[string]string{
		"email":     "email",
		"firstName": "firstName",
		"lastName":  "lastName",
	},
}

func TestMapFundraiserReferrals_NotConfigured(t *testing.T) {
	cfg := referralsConfig("", RaiselyMessageMappings{})
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{"uuid":"p1"}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch, got %+v", batch)
	}
}

func TestMapFundraiserReferrals_EmptyArray(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{"private":{"invitations":[]}}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch for empty array, got %+v", batch)
	}
}

func TestMapFundraiserReferrals_AllProcessed(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"email":"a@b.c","processedAt":"2026-03-18T10:00:00Z"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch when all processed, got %+v", batch)
	}
}

func TestMapFundraiserReferrals_MissingField(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{"private":{"name":"X"}}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch for missing field, got %+v", batch)
	}
}

func TestMapFundraiserReferrals_NotAnArray(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{"private":{"invitations":"not-array"}}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch for non-array, got %+v", batch)
	}
}

func TestMapFundraiserReferrals_OneUnprocessed(t *testing.T) {
	cfg := referralsConfig("private.invitations", RaiselyMessageMappings{
		User: map[string]string{
			"email":     "email",
			"firstName": "firstName",
			"lastName":  "lastName",
		},
		Custom: map[string]string{
			"referrer-id": "^.uuid",
		},
	})
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"uuid":"profile-123",
		"private":{"invitations":[
			{"firstName":"Jane","lastName":"Smith","email":"jane@example.com"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("profile-123", data, cfg, "campaign-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.ProfileID != "profile-123" {
		t.Errorf("ProfileID: got %q", batch.ProfileID)
	}
	if batch.ReferralsField != "private.invitations" {
		t.Errorf("ReferralsField: got %q", batch.ReferralsField)
	}
	if len(batch.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(batch.Messages))
	}
	if len(batch.EntryIndices) != 1 || batch.EntryIndices[0] != 0 {
		t.Errorf("EntryIndices: got %v", batch.EntryIndices)
	}
	if len(batch.SkippedIndices) != 0 {
		t.Errorf("expected no skipped, got %v", batch.SkippedIndices)
	}

	msg := batch.Messages[0]
	if msg.Source != "campaign:campaign-xyz" {
		t.Errorf("Source: got %q", msg.Source)
	}
	if msg.User["email"] != "jane@example.com" {
		t.Errorf("user.email: got %v", msg.User["email"])
	}
	if msg.Custom["referrer-id"] != "profile-123" {
		t.Errorf("custom.referrer-id (^.uuid): got %v", msg.Custom["referrer-id"])
	}
}

func TestMapFundraiserReferrals_MultipleUnprocessed(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"email":"jane@example.com"},
			{"email":"bob@example.com"},
			{"email":"alice@example.com"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil || len(batch.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %+v", batch)
	}

	wantEmails := []string{"jane@example.com", "bob@example.com", "alice@example.com"}
	for i, msg := range batch.Messages {
		if msg.User["email"] != wantEmails[i] {
			t.Errorf("msg %d email: got %v, want %q", i, msg.User["email"], wantEmails[i])
		}
		if batch.EntryIndices[i] != i {
			t.Errorf("EntryIndex %d: got %d", i, batch.EntryIndices[i])
		}
	}
}

func TestMapFundraiserReferrals_MixedProcessedAndUnprocessed(t *testing.T) {
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"email":"bob@example.com","processedAt":"2026-03-18T10:00:00Z"},
			{"email":"jane@example.com"},
			{"email":"alice@example.com","processedAt":"2026-03-18T11:00:00Z"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil || len(batch.Messages) != 1 {
		t.Fatalf("expected 1 message (only unprocessed), got %+v", batch)
	}
	if batch.EntryIndices[0] != 1 {
		t.Errorf("expected EntryIndex 1 (the unprocessed slot), got %d", batch.EntryIndices[0])
	}
	if batch.Messages[0].User["email"] != "jane@example.com" {
		t.Errorf("expected only jane to map, got %v", batch.Messages[0].User["email"])
	}
}

func TestMapFundraiserReferrals_MissingEmailRecordedAsSkipped(t *testing.T) {
	// Mapping path "email" doesn't resolve — referral has no email field.
	// The entry should land in SkippedIndices, not Messages.
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"firstName":"Jane"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages (skipped due to missing email), got %d", len(batch.Messages))
	}
	if len(batch.SkippedIndices) != 1 || batch.SkippedIndices[0] != 0 {
		t.Errorf("expected SkippedIndices=[0], got %v", batch.SkippedIndices)
	}
}

func TestMapFundraiserReferrals_ParentPathTraversal(t *testing.T) {
	cfg := referralsConfig("private.invitations", RaiselyMessageMappings{
		User: map[string]string{"email": "email"},
		Custom: map[string]string{
			"referrer-uuid":       "^.uuid",
			"referrer-first-name": "^.user.firstName",
		},
	})
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"uuid":"profile-abc",
		"user":{"firstName":"Alex"},
		"private":{"invitations":[
			{"email":"jane@example.com"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil || len(batch.Messages) != 1 {
		t.Fatalf("expected 1 message, got %+v", batch)
	}
	msg := batch.Messages[0]
	if msg.Custom["referrer-uuid"] != "profile-abc" {
		t.Errorf("^.uuid: got %v", msg.Custom["referrer-uuid"])
	}
	if msg.Custom["referrer-first-name"] != "Alex" {
		t.Errorf("^.user.firstName: got %v", msg.Custom["referrer-first-name"])
	}
}

func TestMapFundraiserReferrals_BacktickStaticString(t *testing.T) {
	cfg := referralsConfig("private.invitations", RaiselyMessageMappings{
		User: map[string]string{"email": "email"},
		Custom: map[string]string{
			"trigger": "`invitation`",
		},
	})
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"email":"jane@example.com"}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil || len(batch.Messages) != 1 {
		t.Fatalf("expected 1 message, got %+v", batch)
	}
	if batch.Messages[0].Custom["trigger"] != "invitation" {
		t.Errorf("backtick literal: got %v, want 'invitation'", batch.Messages[0].Custom["trigger"])
	}
}

func TestMapFundraiserReferrals_ReferralsJSONPreservesUnknownFields(t *testing.T) {
	// The raw ReferralsJSON the batch carries must include any unknown
	// fields on entries — Service.ProcessReferrals uses sjson over this
	// string to build the write-back, so unknowns need to survive.
	cfg := referralsConfig("private.invitations", standardMessageMapping)
	data := FundraiserData{Page: FundraisingPage{Source: Source{data: gjson.Parse(`{
		"private":{"invitations":[
			{"email":"jane@example.com","customField":"keep","metadata":{"source":"form-123"}}
		]}
	}`)}}}

	batch, err := newReferralsTestFetcher().MapFundraiserReferrals("p1", data, cfg, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}

	parsed := gjson.Parse(batch.ReferralsJSON)
	if got := parsed.Get("0.customField").String(); got != "keep" {
		t.Errorf("customField not preserved in ReferralsJSON: %q", got)
	}
	if got := parsed.Get("0.metadata.source").String(); got != "form-123" {
		t.Errorf("nested metadata.source not preserved: %q", got)
	}
}

// --- Service.ProcessReferrals ---

func TestProcessReferrals_NilBatch(t *testing.T) {
	sc := &SyncContext{Campaign: "c1"}
	sc.Config.API.Keys.Raisely = "k"
	svc := &Service{sc: sc, fetcher: &RaiselyFetcherAndUpdater{SyncContext: sc}}
	if err := svc.ProcessReferrals(nil, t.Context()); err != nil {
		t.Errorf("nil batch should be a no-op, got %v", err)
	}
}

// TestProcessReferrals_PartialFailure exercises the full send + per-entry
// write-back flow: one send succeeds, one fails, one is skipped for no
// email. The write-back must mark the successful and skipped entries
// only — the failed entry stays unmarked so the next webhook retries it.
func TestProcessReferrals_PartialFailure(t *testing.T) {
	var sent []string
	failingEmails := map[string]bool{"fail@example.com": true}

	// Raisely Messages mock — captures sends and fails the marked email.
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		data := body["data"].(map[string]interface{})
		inner := data["data"].(map[string]interface{})
		user := inner["user"].(map[string]interface{})
		email, _ := user["email"].(string)
		sent = append(sent, email)
		if failingEmails[email] {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Raisely API mock — captures the write-back PATCH body.
	var writeBackBody []byte
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBackBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	sc := &SyncContext{Campaign: "test-campaign"}
	sc.Config.API.Keys.Raisely = "k"
	sc.Config.API.Endpoints.Raisely = raiselyAPI.URL
	sc.Config.API.Endpoints.RaiselyMessages = messagesServer.URL
	svc := &Service{sc: sc, fetcher: &RaiselyFetcherAndUpdater{SyncContext: sc}}

	batch := &ReferralBatch{
		Messages: []RaiselyCustomMessageRequest{
			{Source: "campaign:c1", User: map[string]interface{}{"email": "a@example.com"}},
			{Source: "campaign:c1", User: map[string]interface{}{"email": "fail@example.com"}},
			{Source: "campaign:c1", User: map[string]interface{}{"email": "c@example.com"}},
		},
		// Entries in the source array (indices 0, 2, 4) are sent; index 1
		// was already processed and index 3 is skipped for no email.
		EntryIndices:   []int{0, 2, 4},
		SkippedIndices: []int{3},
		ProfileID:      "profile-xyz",
		ReferralsField: "private.invitations",
		ReferralsJSON: `[` +
			`{"email":"a@example.com"},` +
			`{"firstName":"AlreadyProcessed","processedAt":"2026-03-01T00:00:00Z"},` +
			`{"email":"fail@example.com"},` +
			`{"firstName":"NoEmail"},` +
			`{"email":"c@example.com"}` +
			`]`,
	}

	err := svc.ProcessReferrals(batch, t.Context())
	if err == nil {
		t.Fatal("expected error reflecting partial failure, got nil")
	}

	if len(sent) != 3 {
		t.Errorf("expected 3 send attempts, got %d (%v)", len(sent), sent)
	}

	if len(writeBackBody) == 0 {
		t.Fatal("expected write-back to fire even on partial failure")
	}
	body := gjson.ParseBytes(writeBackBody)

	// Index 0 (sent OK) → processed
	if !body.Get("data.private.invitations.0.processedAt").Exists() {
		t.Errorf("entry 0 should be processed after successful send")
	}
	// Index 1 (already processed; not in batch) → original timestamp preserved
	if got := body.Get("data.private.invitations.1.processedAt").String(); got != "2026-03-01T00:00:00Z" {
		t.Errorf("entry 1 should keep its original processedAt, got %q", got)
	}
	// Index 2 (failed send) → NOT processed (so it retries)
	if body.Get("data.private.invitations.2.processedAt").Exists() {
		t.Errorf("entry 2 should NOT be processed after failed send (so retry can fire)")
	}
	// Index 3 (skipped — no email) → processed
	if !body.Get("data.private.invitations.3.processedAt").Exists() {
		t.Errorf("entry 3 should be processed (skipped for missing email)")
	}
	// Index 4 (sent OK) → processed
	if !body.Get("data.private.invitations.4.processedAt").Exists() {
		t.Errorf("entry 4 should be processed after successful send")
	}
}

func TestProcessReferrals_AllSuccess(t *testing.T) {
	messagesServer := newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var writeBackBody []byte
	raiselyAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBackBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(raiselyAPI.Close)

	sc := &SyncContext{Campaign: "test-campaign"}
	sc.Config.API.Keys.Raisely = "k"
	sc.Config.API.Endpoints.Raisely = raiselyAPI.URL
	sc.Config.API.Endpoints.RaiselyMessages = messagesServer.URL
	svc := &Service{sc: sc, fetcher: &RaiselyFetcherAndUpdater{SyncContext: sc}}

	batch := &ReferralBatch{
		Messages: []RaiselyCustomMessageRequest{
			{Source: "campaign:c1", User: map[string]interface{}{"email": "a@x"}},
			{Source: "campaign:c1", User: map[string]interface{}{"email": "b@x"}},
		},
		EntryIndices:   []int{0, 1},
		ProfileID:      "p1",
		ReferralsField: "private.invitations",
		ReferralsJSON:  `[{"email":"a@x"},{"email":"b@x"}]`,
	}

	if err := svc.ProcessReferrals(batch, t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body := gjson.ParseBytes(writeBackBody)
	if !body.Get("data.private.invitations.0.processedAt").Exists() {
		t.Errorf("entry 0 should be processed")
	}
	if !body.Get("data.private.invitations.1.processedAt").Exists() {
		t.Errorf("entry 1 should be processed")
	}
}
