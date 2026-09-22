package sync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestOrttoServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func newTestOrttoFetcher(endpoint string) *OrttoFetcherAndUpdater {
	config := Config{}
	config.API.Endpoints.Ortto = endpoint
	config.API.Keys.Ortto = "test-key"
	return &OrttoFetcherAndUpdater{SyncContext: &SyncContext{Config: config, Campaign: "test-campaign"}}
}

// activityFeedTestServer captures call counts and serves pre-built feed pages
// in order. The first /v1/person/get call returns a fixed personID so callers
// reach the activity-feed path. Each /v1/person/get/activities call advances
// the page cursor so tests can assert exact fetch counts.
type activityFeedTestServer struct {
	pages           []OrttoActivityFeedResponse
	personLookups   int
	activityFetches int
}

func (s *activityFeedTestServer) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/person/get":
			s.personLookups++
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"contacts": []map[string]string{{"id": "test-person-id"}},
			})
		case "/v1/person/get/activities":
			idx := s.activityFetches
			s.activityFetches++
			if idx >= len(s.pages) {
				http.Error(w, "no more pages configured", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(s.pages[idx])
		default:
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)
		}
	}
}

func entry(matches bool) OrttoActivityFeedEntry {
	return OrttoActivityFeedEntry{
		ActivityID: "test-activity",
		Created:    "2026-06-22T00:00:00Z",
		Attributes: map[string]interface{}{"bln:cm:match": matches},
	}
}

func page(entries []OrttoActivityFeedEntry, hasMore bool, nextOffset int) OrttoActivityFeedResponse {
	resp := OrttoActivityFeedResponse{
		Activities: entries,
		NextOffset: nextOffset,
	}
	resp.Meta.HasMore = hasMore
	return resp
}

func matchKey(key string, want bool) func(OrttoActivityFeedEntry) bool {
	return func(e OrttoActivityFeedEntry) bool {
		v, ok := e.Attributes[key].(bool)
		return ok && v == want
	}
}

// Verify: latest-match with a match on page 1 fetches exactly one page
// (no extra pagination calls). No inter-page delay incurred here — the
// short-circuit hits before the loop reaches the time.After().
func TestGetActivityFeedForContact_LatestMatch_FirstPageMatch(t *testing.T) {
	t.Parallel()
	srv := &activityFeedTestServer{
		pages: []OrttoActivityFeedResponse{
			page([]OrttoActivityFeedEntry{entry(false), entry(true), entry(false)}, true, 40),
			page([]OrttoActivityFeedEntry{entry(false)}, false, 0), // must not be fetched
		},
	}
	server := newTestOrttoServer(t, srv.Handler())
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "alice@example.com", "test-activity",
		ActivityFeedLatestMatch, matchKey("bln:cm:match", true),
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.activityFetches != 1 {
		t.Errorf("expected 1 activity-feed page fetch, got %d", srv.activityFetches)
	}
	if len(activities) != 3 {
		t.Errorf("expected 3 activities (page 1 only), got %d", len(activities))
	}
}

// Verify: latest-match with a match on a later page fetches only up to and
// including that page. Test runs in parallel because the inter-page delay
// (ActivityFeedFirstMatchPageDelay) is a real time.After — sequential
// execution would add ~1s wall-clock per page-boundary.
func TestGetActivityFeedForContact_LatestMatch_LaterPageMatch(t *testing.T) {
	t.Parallel()
	srv := &activityFeedTestServer{
		pages: []OrttoActivityFeedResponse{
			page([]OrttoActivityFeedEntry{entry(false), entry(false)}, true, 40),
			page([]OrttoActivityFeedEntry{entry(true), entry(false)}, true, 80),
			page([]OrttoActivityFeedEntry{entry(false)}, false, 0), // must not be fetched
		},
	}
	server := newTestOrttoServer(t, srv.Handler())
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "bob@example.com", "test-activity",
		ActivityFeedLatestMatch, matchKey("bln:cm:match", true),
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.activityFetches != 2 {
		t.Errorf("expected 2 activity-feed page fetches (match on page 2), got %d", srv.activityFetches)
	}
	if len(activities) != 4 {
		t.Errorf("expected 4 activities (pages 1-2 combined), got %d", len(activities))
	}
}

// Verify: latest-match with no matching activity fetches the whole feed and
// selectActivity returns nil (empty enrichment columns).
func TestGetActivityFeedForContact_LatestMatch_NoMatch(t *testing.T) {
	t.Parallel()
	srv := &activityFeedTestServer{
		pages: []OrttoActivityFeedResponse{
			page([]OrttoActivityFeedEntry{entry(false), entry(false)}, true, 40),
			page([]OrttoActivityFeedEntry{entry(false)}, false, 0),
		},
	}
	server := newTestOrttoServer(t, srv.Handler())
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "carol@example.com", "test-activity",
		ActivityFeedLatestMatch, matchKey("bln:cm:match", true),
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.activityFetches != 2 {
		t.Errorf("expected all 2 pages fetched when no entry matches, got %d", srv.activityFetches)
	}
	if len(activities) != 3 {
		t.Errorf("expected 3 activities (no match, all pages walked), got %d", len(activities))
	}

	pick := &CSVEnrichmentPick{Attribute: "match", Equals: true}
	if got := selectActivity(activities, ActivityFeedLatestMatch, pick); got != nil {
		t.Errorf("expected selectActivity to return nil for no-match scenario, got %+v", got)
	}
}

// Verify: existing ActivityFeedLatest behaviour is unchanged — one page fetch
// with limit:1, returns the single most recent activity. No inter-page delay
// (single-fetch path).
func TestGetActivityFeedForContact_Latest_UnchangedBehaviour(t *testing.T) {
	t.Parallel()
	srv := &activityFeedTestServer{
		pages: []OrttoActivityFeedResponse{
			page([]OrttoActivityFeedEntry{entry(true)}, false, 0),
		},
	}
	server := newTestOrttoServer(t, srv.Handler())
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "dan@example.com", "test-activity",
		ActivityFeedLatest, nil,
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.activityFetches != 1 {
		t.Errorf("expected 1 page fetch for ActivityFeedLatest, got %d", srv.activityFetches)
	}
	if len(activities) != 1 {
		t.Errorf("expected 1 activity, got %d", len(activities))
	}
}

// Verify: existing ActivityFeedFirstMatch behaviour is unchanged — full feed
// is walked (no in-fetch short-circuit) and selectActivity picks the oldest
// match (i.e. the last matching entry in feed order, since the API returns
// newest-first).
func TestGetActivityFeedForContact_FirstMatch_UnchangedBehaviour(t *testing.T) {
	t.Parallel()
	srv := &activityFeedTestServer{
		pages: []OrttoActivityFeedResponse{
			page([]OrttoActivityFeedEntry{entry(false), entry(true)}, true, 40),
			page([]OrttoActivityFeedEntry{entry(false)}, false, 0),
		},
	}
	server := newTestOrttoServer(t, srv.Handler())
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "eve@example.com", "test-activity",
		ActivityFeedFirstMatch, nil,
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.activityFetches != 2 {
		t.Errorf("expected both pages fetched (first-match does not short-circuit during fetch), got %d", srv.activityFetches)
	}
	if len(activities) != 3 {
		t.Errorf("expected 3 activities, got %d", len(activities))
	}

	pick := &CSVEnrichmentPick{Attribute: "match", Equals: true}
	got := selectActivity(activities, ActivityFeedFirstMatch, pick)
	if got == nil {
		t.Fatal("expected a matching activity, got nil")
	}
}

// entryAt returns an activity feed entry with an explicit Created timestamp.
func entryAt(matches bool, createdAt string) OrttoActivityFeedEntry {
	e := entry(matches)
	e.Created = createdAt
	return e
}

// dupHandler serves per-person activity feeds keyed by person_id, returning the
// given list of contacts from /v1/person/get. Used by the duplicate-contact tests.
func dupHandler(t *testing.T, contactIDs []string, feeds map[string][]OrttoActivityFeedEntry) (http.HandlerFunc, func() int, func() string) {
	t.Helper()
	var activityFetches int
	var lastFetchedPerson string
	h := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/person/get":
			contacts := make([]map[string]string, len(contactIDs))
			for i, id := range contactIDs {
				contacts[i] = map[string]string{"id": id}
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"contacts": contacts})
		case "/v1/person/get/activities":
			activityFetches++
			var body struct {
				PersonID string `json:"person_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			lastFetchedPerson = body.PersonID
			_ = json.NewEncoder(w).Encode(page(feeds[body.PersonID], false, 0))
		default:
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)
		}
	}
	return h, func() int { return activityFetches }, func() string { return lastFetchedPerson }
}

// Verify: when /v1/person/get returns multiple candidates for the same identifying
// field (a Ortto data-side duplicate), GetActivityFeedForContact considers every
// candidate before choosing. Under ActivityFeedLatestMatch the candidate whose
// newest matching entry has the greatest created_at wins — so a single-match
// candidate loses to a candidate with an older non-match plus a newer match.
func TestGetActivityFeedForContact_LatestMatch_DuplicateContacts_NewestMatchWins(t *testing.T) {
	t.Parallel()
	feeds := map[string][]OrttoActivityFeedEntry{
		// person-a: matching entry dated 2026-08-01 (older match).
		"person-a": {entryAt(true, "2026-08-01T00:00:00Z")},
		// person-b: newer non-match on top, then matching entry dated 2026-09-01
		// (newer than person-a's match).
		"person-b": {entryAt(false, "2026-09-10T00:00:00Z"), entryAt(true, "2026-09-01T00:00:00Z")},
	}
	handler, fetches, _ := dupHandler(t, []string{"person-a", "person-b"}, feeds)
	server := newTestOrttoServer(t, http.HandlerFunc(handler))
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "shared@example.com", "test-activity",
		ActivityFeedLatestMatch, matchKey("bln:cm:match", true),
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fetches() != 2 {
		t.Errorf("expected 2 activity fetches (one per candidate — no short-circuit on first-match anymore), got %d", fetches())
	}
	// Returned feed should be person-b's (winner).
	if len(activities) != 2 {
		t.Fatalf("expected person-b's feed (2 entries), got %d entries", len(activities))
	}
	pick := &CSVEnrichmentPick{Attribute: "match", Equals: true}
	got := selectActivity(activities, ActivityFeedLatestMatch, pick)
	if got == nil {
		t.Fatal("expected selectActivity to return a match on the winning feed")
	}
	if got.Created != "2026-09-01T00:00:00Z" {
		t.Errorf("expected winning match dated 2026-09-01 (person-b), got %q", got.Created)
	}
}

// Verify: when only one duplicate candidate has any matching entry,
// GetActivityFeedForContact returns that candidate's feed even if a sibling
// candidate has newer non-matching activity.
func TestGetActivityFeedForContact_LatestMatch_DuplicateContacts_OnlyOneMatches(t *testing.T) {
	t.Parallel()
	feeds := map[string][]OrttoActivityFeedEntry{
		"person-a": {entryAt(false, "2026-09-15T00:00:00Z"), entryAt(false, "2026-09-14T00:00:00Z")},
		"person-b": {entryAt(true, "2026-06-01T00:00:00Z")},
	}
	handler, _, _ := dupHandler(t, []string{"person-a", "person-b"}, feeds)
	server := newTestOrttoServer(t, http.HandlerFunc(handler))
	fetcher := newTestOrttoFetcher(server.URL)

	activities, err := fetcher.GetActivityFeedForContact(
		"str::email", "shared@example.com", "test-activity",
		ActivityFeedLatestMatch, matchKey("bln:cm:match", true),
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("expected person-b's single-entry feed, got %d entries", len(activities))
	}
	if !matchKey("bln:cm:match", true)(activities[0]) {
		t.Errorf("expected the returned entry to be the matching one, got %+v", activities[0])
	}
}
