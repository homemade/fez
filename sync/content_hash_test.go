package sync

import "testing"

func TestContentHash_Identical(t *testing.T) {
	a := OrttoActivity{
		ActivityID: "act:cm:fundraiser-update",
		Fields: map[string]interface{}{
			"str:cm:reg-id":  "p-1",
			"str:first-name": "Alice",
		},
		Attributes: OrttoAttributes{
			"str:cm:goal":   "1000",
			"str:cm:status": "active",
		},
	}
	b := OrttoActivity{
		ActivityID: a.ActivityID,
		Fields: map[string]interface{}{
			"str:first-name": "Alice",
			"str:cm:reg-id":  "p-1",
		},
		Attributes: OrttoAttributes{
			"str:cm:status": "active",
			"str:cm:goal":   "1000",
		},
	}
	if a.ContentHash() != b.ContentHash() {
		t.Fatalf("identical activities should hash to the same value")
	}
}

func TestContentHash_VolatileOnlyDiffSameHash(t *testing.T) {
	base := OrttoActivity{
		ActivityID: "act:cm:fundraiser-update",
		Fields:     map[string]interface{}{"str:cm:reg-id": "p-1"},
		Attributes: OrttoAttributes{
			"str:cm:goal": "1000",
			"obj:cm:sync-context": map[string]interface{}{
				"trigger-id":         "evt-1",
				"trigger-created-at": "2026-05-08T10:00:00Z",
			},
			"obj:cm:cdp-fields": map[string]interface{}{
				"reg-id": "p-1",
			},
		},
	}
	other := base
	other.Attributes = OrttoAttributes{
		"str:cm:goal": "1000",
		"obj:cm:sync-context": map[string]interface{}{
			"trigger-id":         "evt-2",
			"trigger-created-at": "2026-05-08T10:00:01Z",
		},
		"obj:cm:cdp-fields": map[string]interface{}{
			"reg-id":      "p-1",
			"extra-field": "noise",
		},
	}
	if base.ContentHash() != other.ContentHash() {
		t.Fatalf("volatile-only differences should not affect the hash")
	}
}

func TestContentHash_NonVolatileAttributeChange(t *testing.T) {
	base := OrttoActivity{
		ActivityID: "act:cm:fundraiser-update",
		Fields:     map[string]interface{}{"str:cm:reg-id": "p-1"},
		Attributes: OrttoAttributes{"str:cm:goal": "1000"},
	}
	changed := base
	changed.Attributes = OrttoAttributes{"str:cm:goal": "2000"}

	if base.ContentHash() == changed.ContentHash() {
		t.Fatalf("a non-volatile attribute change should produce a different hash")
	}
}

func TestContentHash_FieldChange(t *testing.T) {
	base := OrttoActivity{
		ActivityID: "act:cm:fundraiser-update",
		Fields:     map[string]interface{}{"str:cm:reg-id": "p-1"},
	}
	changed := base
	changed.Fields = map[string]interface{}{"str:cm:reg-id": "p-2"}

	if base.ContentHash() == changed.ContentHash() {
		t.Fatalf("a field change should produce a different hash")
	}
}

func TestContentHash_ActivityIDChange(t *testing.T) {
	base := OrttoActivity{
		ActivityID: "act:cm:fundraiser-update",
		Fields:     map[string]interface{}{"str:cm:reg-id": "p-1"},
	}
	changed := base
	changed.ActivityID = "act:cm:donation"

	if base.ContentHash() == changed.ContentHash() {
		t.Fatalf("an ActivityID change should produce a different hash")
	}
}

func TestContentHash_Length(t *testing.T) {
	a := OrttoActivity{
		ActivityID: "act:cm:x",
		Fields:     map[string]interface{}{"str:cm:reg-id": "p-1"},
	}
	got := a.ContentHash()
	if len(got) != 16 {
		t.Fatalf("hash should be 16 hex chars, got %d (%q)", len(got), got)
	}
}
