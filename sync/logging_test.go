package sync

import (
	"strings"
	"testing"
)

// testMaxActivityLogBytes is the cap passed to LoggableActivity in
// tests. Sized (~3,500 bytes) to match a representative production
// per-record log cap (e.g. Vercel's empirical drain-to-Axiom
// per-record limit of ~3,827 bytes, with headroom for the runtime
// log prefix), so size-driven branches exercise realistic
// thresholds rather than artificial small caps.
const testMaxActivityLogBytes = 3500

func TestLoggableSyncContext(t *testing.T) {
	sc := &SyncContext{
		Campaign:     "00000000-0000-0000-0000-000000000001",
		CampaignName: "Example Campaign 2026",
		TriggerInfo: TriggerInfo{
			Source:           "Raisely",
			TriggerType:      "webhook",
			TriggerSubType:   "profile.updated",
			TriggerID:        "00000000-0000-0000-0000-000000000002",
			TriggerCreatedAt: "2026-05-07T02:32:49Z",
		},
	}

	got := LoggableSyncContext(sc)

	for _, want := range []string{
		"Source:Raisely",
		"TriggerType:webhook",
		"TriggerSubType:profile.updated",
		"TriggerID:00000000-0000-0000-0000-000000000002",
		// RFC3339 is converted to RFC1123 for consistency with
		// the obj:cm:sync-context attribute the activities mapper
		// attaches.
		"TriggerCreatedAt:Thu, 07 May 2026 02:32:49 UTC",
		"Campaign:00000000-0000-0000-0000-000000000001",
		"CampaignName:Example Campaign 2026",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("LoggableSyncContext: missing %q in output\n  got: %s", want, got)
		}
	}
}

func TestLoggableSyncContext_NonRFC3339TimestampPassesThrough(t *testing.T) {
	sc := &SyncContext{
		TriggerInfo: TriggerInfo{
			Source:           "Manual",
			TriggerCreatedAt: "Thu, 07 May 2026 02:32:49 UTC", // already RFC1123
		},
	}
	got := LoggableSyncContext(sc)
	if !strings.Contains(got, "TriggerCreatedAt:Thu, 07 May 2026 02:32:49 UTC") {
		t.Errorf("expected RFC1123 input to be preserved, got: %s", got)
	}
}

func TestLoggableRequestEnvelope_Activities(t *testing.T) {
	req := OrttoActivitiesRequest{
		Activities: []OrttoActivity{
			{ActivityID: "act:cm:test", Fields: map[string]interface{}{}},
			{ActivityID: "act:cm:test", Fields: map[string]interface{}{}},
			{ActivityID: "act:cm:test", Fields: map[string]interface{}{}},
		},
		Async:         false,
		MergeBy:       []string{"str:cm:example-p2p-registration-id", "str::email"},
		MergeStrategy: 2,
	}

	got := LoggableRequestEnvelope(req)

	for _, want := range []string{
		"Activities:3",
		"Async:false",
		"MergeBy:[str:cm:example-p2p-registration-id str::email]",
		"MergeStrategy:2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("LoggableRequestEnvelope: missing %q in output\n  got: %s", want, got)
		}
	}
	// Ensure activities themselves aren't rendered into the envelope.
	if strings.Contains(got, "ActivityID") {
		t.Errorf("LoggableRequestEnvelope: envelope should not include activity bodies, got: %s", got)
	}
}

func TestLoggableRequestEnvelope_Contacts(t *testing.T) {
	req := OrttoContactsRequest{
		Contacts: []OrttoContact{
			{Fields: map[string]interface{}{"str::email": "a@example.com"}},
			{Fields: map[string]interface{}{"str::email": "b@example.com"}},
		},
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 2,
		FindStrategy:  0,
	}

	got := LoggableRequestEnvelope(req)

	for _, want := range []string{
		"Contacts:2",
		"MergeBy:[str::email]",
		"MergeStrategy:2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("LoggableRequestEnvelope: missing %q in output\n  got: %s", want, got)
		}
	}
	if strings.Contains(got, "a@example.com") {
		t.Errorf("LoggableRequestEnvelope: envelope should not include contact bodies, got: %s", got)
	}
}

func TestLoggableActivity_StripsMetaAttributes(t *testing.T) {
	act := OrttoActivity{
		ActivityID: "act:cm:test",
		Attributes: OrttoAttributes{
			"obj:cm:sync-context": map[string]interface{}{"Source": "Raisely"},
			"obj:cm:cdp-fields":   map[string]interface{}{"foo": "bar"},
			"str:cm:name":         "Alice",
		},
		Fields: map[string]interface{}{
			"str:cm:example-p2p-registration-id": "00000000-0000-0000-0000-000000000003",
			"str::email":                   "alice@example.com",
		},
	}

	got := LoggableActivity(act, testMaxActivityLogBytes)

	for _, want := range []string{
		"ActivityID:act:cm:test",
		"str:cm:name:Alice",
		"str:cm:example-p2p-registration-id:00000000-0000-0000-0000-000000000003",
		"str::email:alice@example.com",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("LoggableActivity: missing %q in output\n  got: %s", want, got)
		}
	}
	for _, banned := range []string{"obj:cm:sync-context", "obj:cm:cdp-fields"} {
		if strings.Contains(got, banned) {
			t.Errorf("LoggableActivity: should have stripped %q, got: %s", banned, got)
		}
	}
}

func TestLoggableActivity_DoesNotMutateOriginal(t *testing.T) {
	original := OrttoActivity{
		ActivityID: "act:cm:test",
		Attributes: OrttoAttributes{
			"obj:cm:sync-context": map[string]interface{}{"Source": "Raisely"},
			"obj:cm:cdp-fields":   map[string]interface{}{"foo": "bar"},
			"str:cm:name":         "Alice",
		},
		Fields: map[string]interface{}{
			"str::email": "alice@example.com",
		},
	}

	_ = LoggableActivity(original, testMaxActivityLogBytes)

	for _, key := range []string{"obj:cm:sync-context", "obj:cm:cdp-fields", "str:cm:name"} {
		if _, ok := original.Attributes[key]; !ok {
			t.Errorf("LoggableActivity mutated original: %q missing from Attributes", key)
		}
	}
	if _, ok := original.Fields["str::email"]; !ok {
		t.Errorf("LoggableActivity mutated original: str::email missing from Fields")
	}
}

func TestLoggableActivity_DropsVerboseFieldsWhenOversize(t *testing.T) {
	// Build an activity whose only oversize content is in a
	// txt:-prefixed field — dropping it should bring the body under
	// the cap, with the verbose-drop marker appended so a reader
	// sees that text content was elided.
	bigStory := strings.Repeat("x", testMaxActivityLogBytes)
	act := OrttoActivity{
		ActivityID: "act:cm:test",
		Attributes: OrttoAttributes{
			"txt:cm:profile-story": bigStory,
			"str:cm:name":          "Alice",
		},
		Fields: map[string]interface{}{
			"str:cm:example-p2p-registration-id": "00000000-0000-0000-0000-000000000003",
		},
	}

	got := LoggableActivity(act, testMaxActivityLogBytes)

	if len(got) > testMaxActivityLogBytes {
		t.Errorf("LoggableActivity: result %d bytes exceeds cap %d", len(got), testMaxActivityLogBytes)
	}
	if strings.Contains(got, "txt:cm:profile-story") {
		t.Errorf("LoggableActivity: should have dropped txt:cm:profile-story, got: %s", got[:200]+"...")
	}
	if !strings.HasSuffix(got, activityLogVerboseDropMarker) {
		t.Errorf("LoggableActivity: expected verbose-drop marker suffix, got tail: %s", got[max(0, len(got)-50):])
	}
	// The merge-field substring must survive — the timeline's
	// content-based seed match relies on it.
	if !strings.Contains(got, "str:cm:example-p2p-registration-id:00000000-0000-0000-0000-000000000003") {
		t.Errorf("LoggableActivity: merge-field substring must survive drops, got: %s", got)
	}
}

func TestLoggableActivity_DropsAnyTxtPrefixedField(t *testing.T) {
	// The drop heuristic is the `txt:` type prefix, not a hardcoded
	// list of field names — any customer's text fields get dropped
	// without library changes. Use a key that no prior version of
	// the library would have known about.
	bigBlob := strings.Repeat("z", testMaxActivityLogBytes)
	act := OrttoActivity{
		ActivityID: "act:cm:test",
		Attributes: OrttoAttributes{
			"txt:cm:something-customer-specific": bigBlob,
			"str:cm:name":                        "Alice",
		},
	}

	got := LoggableActivity(act, testMaxActivityLogBytes)

	if len(got) > testMaxActivityLogBytes {
		t.Errorf("LoggableActivity: result %d bytes exceeds cap %d", len(got), testMaxActivityLogBytes)
	}
	if strings.Contains(got, "txt:cm:something-customer-specific") {
		t.Errorf("LoggableActivity: should drop any txt:-prefixed field, not just known names")
	}
	if !strings.HasSuffix(got, activityLogVerboseDropMarker) {
		t.Errorf("LoggableActivity: expected verbose-drop marker suffix, got tail: %s", got[max(0, len(got)-50):])
	}
	if !strings.Contains(got, "str:cm:name:Alice") {
		t.Errorf("LoggableActivity: non-txt fields must survive, got: %s", got)
	}
}

func TestLoggableActivity_HardTruncationWhenAllDropsExhausted(t *testing.T) {
	// Build an activity whose oversize content lives in a key NOT
	// in optionalVerboseLogFields, so progressive drops don't bring
	// it under the cap. The helper should hard-truncate and append
	// the marker.
	bigBlob := strings.Repeat("y", testMaxActivityLogBytes*2)
	act := OrttoActivity{
		ActivityID: "act:cm:test",
		Attributes: OrttoAttributes{
			"str:cm:name":     "Alice",
			"str:cm:bigfield": bigBlob,
		},
	}

	got := LoggableActivity(act, testMaxActivityLogBytes)

	if len(got) > testMaxActivityLogBytes {
		t.Errorf("LoggableActivity: result %d bytes exceeds cap %d", len(got), testMaxActivityLogBytes)
	}
	if !strings.HasSuffix(got, activityLogTruncationMarker) {
		t.Errorf("LoggableActivity: expected truncation marker suffix, got tail: %s", got[max(0, len(got)-50):])
	}
}

func TestExtractActivities(t *testing.T) {
	activities := []OrttoActivity{
		{ActivityID: "act:cm:a"},
		{ActivityID: "act:cm:b"},
	}
	if got := ExtractActivities(OrttoActivitiesRequest{Activities: activities}); len(got) != 2 {
		t.Errorf("ExtractActivities(activities request): got %d activities, want 2", len(got))
	}
	if got := ExtractActivities(OrttoContactsRequest{}); got != nil {
		t.Errorf("ExtractActivities(contacts request): got %v, want nil", got)
	}
}

// max is in stdlib in Go 1.21+, but provide a fallback for older toolchains.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
