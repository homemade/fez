package sync

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestRaiselyAPIFetcher mirrors newTestRaiselyFetcher (in
// raisely_custom_messages_test.go) but configures the MAIN Raisely
// API endpoint instead of the Custom Messages one, plus a
// SyncContext.TriggerInfo / UserAgent the validator + builder read.
func newTestRaiselyAPIFetcher(apiURL, triggerType, ua string) *RaiselyFetcherAndUpdater {
	config := Config{}
	config.API.Keys.Raisely = "test-key"
	config.API.Endpoints.Raisely = apiURL
	return &RaiselyFetcherAndUpdater{SyncContext: &SyncContext{
		Config:      config,
		Campaign:    "test-campaign",
		TriggerInfo: TriggerInfo{TriggerType: triggerType},
		UserAgent:   ua,
	}}
}

// TestRaiselyAPIBuilder_SetsUserAgent verifies the User-Agent header
// is set on every outgoing Raisely API request, from
// SyncContext.UserAgent when set or DefaultUserAgent when empty.
// Symmetric with TestOrttoAPIBuilder_SetsUserAgent.
func TestRaiselyAPIBuilder_SetsUserAgent(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotUA string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUA = r.Header.Get("User-Agent")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)

			fetcher := newTestRaiselyAPIFetcher(srv.URL, "webhook", tc.set)
			err := fetcher.RaiselyAPIBuilder().Path("/v3/test").Fetch(context.Background())
			if err != nil {
				t.Fatalf("Fetch returned unexpected error: %v", err)
			}
			if gotUA != tc.expect {
				t.Fatalf("Raisely outbound User-Agent = %q, want %q", gotUA, tc.expect)
			}
		})
	}
}

// TestRaiselyAPIBuilder_EmitsCallLog verifies the per-call
// attribution log line carries source=<TriggerType> ua=<value>
// path=… status=…. The `ua=` field is load-bearing for future
// incident communication with Raisely / their edge layer — a
// grep on `Raisely call:` lets operators recover the exact UA
// we were sending at the time of any incident.
func TestRaiselyAPIBuilder_EmitsCallLog(t *testing.T) {
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	fetcher := newTestRaiselyAPIFetcher(srv.URL, "webhook", "app/test-sha")
	err := fetcher.RaiselyAPIBuilder().Path("/v3/test").Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned unexpected error: %v", err)
	}

	got := logBuf.String()
	want := []string{
		"Raisely call:",
		"source=webhook",
		"ua=app/test-sha",
		"path=/v3/test",
		"status=200",
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("log output missing %q\nfull output: %s", w, got)
		}
	}
}

// TestRaiselyAPIBuilder_RateLimit429 verifies that a 429 from the
// Raisely API surfaces as *RaiselyRateLimitError carrying the status
// code + captured response headers (no try-in-seconds, no caller
// stack — Cloudflare edge gives no hint), and that the shared
// IsRateLimited / RetryAfter helpers handle the Raisely type
// correctly: IsRateLimited is true, RetryAfter is zero (the caller's
// site supplies its own self-timed back-off, since there's no hint
// to honour).
func TestRaiselyAPIBuilder_RateLimit429(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Mimic the Cloudflare edge response shape: characteristic
		// CF-* headers + empty body. fez doesn't decode the body,
		// but a future incident could surface these headers via
		// the captured ResponseHeaders.
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("CF-Ray", "8abc12345-LHR")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	fetcher := newTestRaiselyAPIFetcher(srv.URL, "webhook", "")
	err := fetcher.RaiselyAPIBuilder().Path("/v3/test").Fetch(context.Background())
	if err == nil {
		t.Fatal("expected an error on 429")
	}
	if !IsRateLimited(err) {
		t.Fatalf("IsRateLimited(err) = false; want true (err=%v)", err)
	}
	if got := RetryAfter(err); got != 0 {
		t.Errorf("RetryAfter(err) = %v, want 0 (Raisely 429s carry no hint)", got)
	}

	var rl *RaiselyRateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("errors.As(*RaiselyRateLimitError) failed for %v", err)
	}
	if rl.StatusCode != http.StatusTooManyRequests {
		t.Errorf("StatusCode = %d, want 429", rl.StatusCode)
	}
	if got := rl.ResponseHeaders.Get("Server"); got != "cloudflare" {
		t.Errorf("ResponseHeaders[Server] = %q, want %q", got, "cloudflare")
	}
	if got := rl.ResponseHeaders.Get("CF-Ray"); got != "8abc12345-LHR" {
		t.Errorf("ResponseHeaders[CF-Ray] = %q, want %q", got, "8abc12345-LHR")
	}
}

// TestIsRateLimited_HandlesBothTypes pins the shared
// IsRateLimited / RetryAfter helpers' coverage of both downstream
// error types. The Ortto-side validator (sync/ortto_fetcher_and_updater.go)
// is the only path that constructs *OrttoRateLimitError, and the
// Raisely-side validator the only path for *RaiselyRateLimitError,
// so an unwrapped non-rate-limit error MUST NOT match either type
// (e.g. a fez or stdlib-shaped error from a 500). Covers the
// helpers directly rather than through the builders because Fetch
// itself doesn't error on a non-429 (no CheckStatus validator), so
// going through the builder for the negative case would require
// extra wiring with no added signal.
func TestIsRateLimited_HandlesBothTypes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		err       error
		want      bool
		wantAfter time.Duration
	}{
		{"OrttoRateLimitError with hint", &OrttoRateLimitError{StatusCode: 429, Code: "rate-limit", TryInSeconds: 12}, true, 12 * time.Second},
		{"OrttoRateLimitError zero hint", &OrttoRateLimitError{StatusCode: 429, Code: "rate-limit"}, true, 0},
		{"RaiselyRateLimitError", &RaiselyRateLimitError{StatusCode: 429}, true, 0},
		{"plain error", errors.New("not a rate limit"), false, 0},
		{"nil", nil, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRateLimited(tc.err); got != tc.want {
				t.Errorf("IsRateLimited(%v) = %v, want %v", tc.err, got, tc.want)
			}
			if got := RetryAfter(tc.err); got != tc.wantAfter {
				t.Errorf("RetryAfter(%v) = %v, want %v", tc.err, got, tc.wantAfter)
			}
		})
	}
}

// TestRaiselyMessagesAPIBuilder_RateLimit429 verifies the same 429
// detection applies to the Custom Messages builder — both API
// surfaces share raiselyCallValidator, so a single test pins the
// per-builder wiring.
func TestRaiselyMessagesAPIBuilder_RateLimit429(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	fetcher := newTestRaiselyFetcher("test-key", srv.URL)
	err := fetcher.RaiselyMessagesAPIBuilder().Path("/v1/events").Fetch(context.Background())
	if err == nil {
		t.Fatal("expected an error on 429")
	}
	if !IsRateLimited(err) {
		t.Fatalf("IsRateLimited(err) = false; want true (err=%v)", err)
	}
	var rl *RaiselyRateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("errors.As(*RaiselyRateLimitError) failed for %v", err)
	}
}

// TestRaiselyMessagesAPIBuilder_SetsUserAgent and EmitsCallLog —
// the Custom Messages builder carries the same UA + attribution
// machinery as the main API builder. Combined into one test
// because both behaviours hit on a single request.
func TestRaiselyMessagesAPIBuilder_SetsUserAgentAndEmitsCallLog(t *testing.T) {
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

	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	fetcher := newTestRaiselyFetcher("test-key", srv.URL)
	fetcher.SyncContext.TriggerInfo.TriggerType = "webhook"
	fetcher.SyncContext.UserAgent = "app/messages-sha"

	err := fetcher.RaiselyMessagesAPIBuilder().Path("/v1/events").Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned unexpected error: %v", err)
	}

	if gotUA != "app/messages-sha" {
		t.Errorf("Raisely Messages outbound User-Agent = %q, want app/messages-sha", gotUA)
	}

	got := logBuf.String()
	for _, w := range []string{"Raisely call:", "source=webhook", "ua=app/messages-sha", "path=/v1/events", "status=200"} {
		if !strings.Contains(got, w) {
			t.Errorf("log output missing %q\nfull output: %s", w, got)
		}
	}
}
