package sync

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

// fez's Ortto rate-limit detection + caller attribution log +
// User-Agent. The scenarios mirror Ortto's actual rate-limit envelope
// (`{error:{code:"rate-limit",try-in-seconds:N}}`) and the negative
// cases the validator must NOT match (non-rate-limit 429, non-429
// errors). End-to-end through SendActivitiesCreate so the validator
// chain order is the production one.

// TestSendActivitiesCreate_RateLimit429 verifies the nested
// rate-limit shape is captured as *OrttoRateLimitError with the
// hint, headers, and the matching IsRateLimited / RetryAfter
// helpers returning the expected values.
func TestSendActivitiesCreate_RateLimit429(t *testing.T) {
	t.Parallel()
	srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "ortto-edge")
		w.Header().Set("X-Ortto-Trace", "abc-123")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"rate-limit","message":"rate limit","try-in-seconds":12}}`))
	})

	o := newTestOrttoFetcher(srv.URL)
	_, err := o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())
	if err == nil {
		t.Fatal("expected an error on 429")
	}
	if !IsRateLimited(err) {
		t.Fatalf("IsRateLimited(err) = false; want true (err=%v)", err)
	}
	if got, want := RetryAfter(err), 12*time.Second; got != want {
		t.Fatalf("RetryAfter(err) = %v, want %v", got, want)
	}

	var rl *OrttoRateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("errors.As(*OrttoRateLimitError) failed for %v", err)
	}
	if rl.StatusCode != http.StatusTooManyRequests {
		t.Errorf("StatusCode = %d, want 429", rl.StatusCode)
	}
	if rl.Code != "rate-limit" {
		t.Errorf("Code = %q, want %q", rl.Code, "rate-limit")
	}
	if rl.TryInSeconds != 12 {
		t.Errorf("TryInSeconds = %d, want 12", rl.TryInSeconds)
	}
	if got := rl.ResponseHeaders.Get("Server"); got != "ortto-edge" {
		t.Errorf("ResponseHeaders[Server] = %q, want %q", got, "ortto-edge")
	}
	if got := rl.ResponseHeaders.Get("X-Ortto-Trace"); got != "abc-123" {
		t.Errorf("ResponseHeaders[X-Ortto-Trace] = %q, want %q", got, "abc-123")
	}
}

// TestSendActivitiesCreate_NonRateLimit429 verifies that a 429 with a
// NON-rate-limit body shape passes through the validator without
// returning *OrttoRateLimitError — the caller's ErrorJSON chain
// handles it as a generic error (matching today's behaviour).
func TestSendActivitiesCreate_NonRateLimit429(t *testing.T) {
	t.Parallel()
	srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"request_id":"req-99","code":429,"error":"too many requests"}`))
	})

	o := newTestOrttoFetcher(srv.URL)
	_, err := o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())
	if err == nil {
		t.Fatal("expected an error on 429")
	}
	if IsRateLimited(err) {
		t.Fatalf("IsRateLimited(err) = true; want false on non-rate-limit body (err=%v)", err)
	}
	if got := RetryAfter(err); got != 0 {
		t.Errorf("RetryAfter(err) = %v, want 0 (non-rate-limit error)", got)
	}
}

// TestSendActivitiesCreate_Non429Error verifies that other non-2xx
// statuses (5xx, etc.) are not mistakenly flagged as rate-limit.
// The validator only matches on status==429 AND nested rate-limit
// body shape.
func TestSendActivitiesCreate_Non429Error(t *testing.T) {
	t.Parallel()
	srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"request_id":"req-50","code":500,"error":"internal error"}`))
	})

	o := newTestOrttoFetcher(srv.URL)
	_, err := o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())
	if err == nil {
		t.Fatal("expected an error on 500")
	}
	if IsRateLimited(err) {
		t.Fatalf("IsRateLimited(err) = true; want false on 500 (err=%v)", err)
	}
}

// TestSendActivitiesCreate_Success verifies the validator passes
// through cleanly on a 2xx — no spurious error, the response decodes
// normally.
func TestSendActivitiesCreate_Success(t *testing.T) {
	t.Parallel()
	srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"activities":[{"person_id":"p-1","status":"ingested","activity_id":"a-1"}]}`))
	})

	o := newTestOrttoFetcher(srv.URL)
	resp, err := o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())
	if err != nil {
		t.Fatalf("unexpected error on 2xx: %v", err)
	}
	if len(resp.Activities) != 1 {
		t.Fatalf("Activities = %d, want 1", len(resp.Activities))
	}
	if IsRateLimited(err) {
		t.Errorf("IsRateLimited(nil) returned true; want false")
	}
}

// TestOrttoAPIBuilder_SetsUserAgent verifies the User-Agent header
// is set from SyncContext.UserAgent on every outgoing request — and
// falls back to DefaultUserAgent when empty.
func TestOrttoAPIBuilder_SetsUserAgent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		set    string
		expect string
	}{
		{"explicit", "app/test-sha", "app/test-sha"},
		{"empty falls back to default", "", DefaultUserAgent},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotUA string
			srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
				gotUA = r.Header.Get("User-Agent")
				_, _ = w.Write([]byte(`{}`))
			})

			o := newTestOrttoFetcher(srv.URL)
			o.SyncContext.UserAgent = tc.set
			_, _ = o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())

			if gotUA != tc.expect {
				t.Fatalf("User-Agent = %q, want %q", gotUA, tc.expect)
			}
		})
	}
}

// TestOrttoAPIBuilder_EmitsCallLog verifies the per-call attribution
// log line is emitted with source=<TriggerType> ua=… path=… status=…
// — the ua= field is load-bearing for future incident communication
// (a Cloudflare / edge operator can filter logs by the exact UA we
// were sending at the time of the incident).
func TestOrttoAPIBuilder_EmitsCallLog(t *testing.T) {
	// Not t.Parallel — mutates global log state.
	var logBuf bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})

	srv := newTestOrttoServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})

	o := newTestOrttoFetcher(srv.URL)
	o.SyncContext.TriggerInfo.TriggerType = "webhook"
	o.SyncContext.UserAgent = "app/test-sha"
	_, _ = o.SendActivitiesCreate(OrttoActivitiesRequest{Activities: []OrttoActivity{{}}}, context.Background())

	got := logBuf.String()
	want := []string{
		"Ortto call:",
		"source=webhook",
		"ua=app/test-sha",
		"path=/v1/activities/create",
		"status=200",
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("log output missing %q\nfull output: %s", w, got)
		}
	}
}

// TestOrttoRateLimitError_HelpersHandleNonRateLimit verifies the
// helpers' contract on nil / non-rate-limit / wrapped errors.
func TestOrttoRateLimitError_HelpersHandleNonRateLimit(t *testing.T) {
	t.Parallel()
	if IsRateLimited(nil) {
		t.Error("IsRateLimited(nil) = true; want false")
	}
	if got := RetryAfter(nil); got != 0 {
		t.Errorf("RetryAfter(nil) = %v, want 0", got)
	}
	plain := errors.New("plain error")
	if IsRateLimited(plain) {
		t.Error("IsRateLimited(plain) = true; want false")
	}
	if got := RetryAfter(plain); got != 0 {
		t.Errorf("RetryAfter(plain) = %v, want 0", got)
	}

	// A wrapped *OrttoRateLimitError is detected via errors.As.
	rl := &OrttoRateLimitError{StatusCode: 429, Code: "rate-limit", TryInSeconds: 5}
	wrapped := errors.Join(errors.New("outer"), rl)
	if !IsRateLimited(wrapped) {
		t.Error("IsRateLimited(wrapped) = false; want true")
	}
	if got, want := RetryAfter(wrapped), 5*time.Second; got != want {
		t.Errorf("RetryAfter(wrapped) = %v, want %v", got, want)
	}
}

// TestOrttoRateLimitError_ZeroTryInSecondsYieldsZeroDuration verifies
// that a malformed / missing try-in-seconds (zero or negative) maps
// to a zero Duration so callers treating zero as "no hint" can apply
// their own minimum-wait policy.
func TestOrttoRateLimitError_ZeroTryInSecondsYieldsZeroDuration(t *testing.T) {
	t.Parallel()
	rl := &OrttoRateLimitError{StatusCode: 429, Code: "rate-limit", TryInSeconds: 0}
	if got := RetryAfter(rl); got != 0 {
		t.Errorf("RetryAfter(TryInSeconds=0) = %v, want 0", got)
	}
	rl.TryInSeconds = -5
	if got := RetryAfter(rl); got != 0 {
		t.Errorf("RetryAfter(TryInSeconds=-5) = %v, want 0", got)
	}
}
