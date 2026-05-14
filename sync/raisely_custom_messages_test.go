package sync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// newTestRaiselyMessagesServer spins up an httptest server that captures
// the inbound request and points raiselyMessagesAPIBaseURL at it for the
// duration of the test.
func newTestRaiselyMessagesServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	originalURL := raiselyMessagesAPIBaseURL
	raiselyMessagesAPIBaseURL = server.URL
	t.Cleanup(func() {
		raiselyMessagesAPIBaseURL = originalURL
		server.Close()
	})
	return server
}

func newTestRaiselyFetcher(apiKey string) *RaiselyFetcherAndUpdater {
	config := Config{}
	config.API.Keys.Raisely = apiKey
	return &RaiselyFetcherAndUpdater{SyncContext: &SyncContext{Config: config, Campaign: "test-campaign"}}
}

func TestSendCustomMessage_PayloadShape(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	var capturedBody map[string]interface{}

	newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	fetcher := newTestRaiselyFetcher("test-key-123")
	err := fetcher.SendCustomMessage(RaiselyCustomMessageRequest{
		Source: "campaign:abc-def-123",
		User: map[string]interface{}{
			"email":     "jane@example.com",
			"firstName": "Jane",
			"lastName":  "Smith",
		},
		Custom: map[string]interface{}{
			"referrer-first-name": "Alex",
		},
	}, context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedPath != "/v1/events" {
		t.Errorf("expected path /v1/events, got %q", capturedPath)
	}

	// Bearer token must be "raisely:<apiKey>" (NOT just the API key)
	if capturedAuth != "Bearer raisely:test-key-123" {
		t.Errorf("expected Authorization 'Bearer raisely:test-key-123', got %q", capturedAuth)
	}

	// Outer envelope: data.{version,type,source,data}
	outer, ok := capturedBody["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected outer data object, got %T", capturedBody["data"])
	}
	if v, _ := outer["version"].(float64); v != 1 {
		t.Errorf("expected version 1, got %v", outer["version"])
	}
	if v, _ := outer["type"].(string); v != "raisely.custom" {
		t.Errorf("expected type 'raisely.custom', got %q", v)
	}
	if v, _ := outer["source"].(string); v != "campaign:abc-def-123" {
		t.Errorf("expected source 'campaign:abc-def-123', got %q", v)
	}

	// Inner payload: data.data.{user,custom}
	inner, ok := outer["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected inner data object, got %T", outer["data"])
	}
	user, ok := inner["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected user object, got %T", inner["user"])
	}
	if user["email"] != "jane@example.com" {
		t.Errorf("expected user.email pass-through, got %v", user["email"])
	}
	if user["firstName"] != "Jane" {
		t.Errorf("expected user.firstName pass-through, got %v", user["firstName"])
	}
	custom, ok := inner["custom"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected custom object, got %T", inner["custom"])
	}
	if custom["referrer-first-name"] != "Alex" {
		t.Errorf("expected custom pass-through, got %v", custom)
	}
}

func TestSendCustomMessage_OmitsEmptyCustom(t *testing.T) {
	var capturedBody map[string]interface{}

	newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusOK)
	})

	fetcher := newTestRaiselyFetcher("k")
	err := fetcher.SendCustomMessage(RaiselyCustomMessageRequest{
		Source: "campaign:c1",
		User:   map[string]interface{}{"email": "a@b.c"},
	}, context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inner := capturedBody["data"].(map[string]interface{})["data"].(map[string]interface{})
	if _, present := inner["custom"]; present {
		t.Errorf("expected custom field omitted when nil, got %v", inner["custom"])
	}
}

func TestSendCustomMessage_NonSuccessIsError(t *testing.T) {
	newTestRaiselyMessagesServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	})

	fetcher := newTestRaiselyFetcher("k")
	err := fetcher.SendCustomMessage(RaiselyCustomMessageRequest{
		Source: "campaign:c1",
		User:   map[string]interface{}{"email": "a@b.c"},
	}, context.Background())
	if err == nil {
		t.Fatal("expected error on 4xx response, got nil")
	}
}
