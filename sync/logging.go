package sync

import (
	"fmt"
	"strings"
	"time"
)

// activityLogTruncationMarker is appended to a per-activity log line
// when, even after dropping every verbose-text field, the rendered
// body still exceeds the caller-supplied maxBytes cap. The marker is
// deliberately short and recognisable so timeline tooling can detect
// truncated lines if it ever needs to.
const activityLogTruncationMarker = " …(log-truncated)"

// activityLogVerboseDropMarker is appended to a per-activity log line
// when one or more Attributes / Fields entries with the `txt:` prefix
// were dropped to bring the body under the caller-supplied maxBytes
// cap. Signals to a log reader that text content was elided rather
// than the activity simply having no text fields. Distinct from
// activityLogTruncationMarker so the two outcomes are
// distinguishable.
const activityLogVerboseDropMarker = " …(verbose-fields-dropped)"

// verboseFieldPrefix marks Ortto Attributes / Fields keys whose values
// hold free-form text that may push a `Sync activity` log line past
// the cap. `txt:` is Ortto's type prefix for text / long-text fields
// (vs `str:` short string, `int:` integer, `obj:` object, etc.) — the
// natural category for verbose content like profile stories and bios.
// Treating the type prefix as the drop heuristic avoids hardcoding
// customer-specific field names in this library.
const verboseFieldPrefix = "txt:"

// LoggableSyncContext returns a single-line representation of the
// trigger metadata for a sync, suitable for emitting on a dedicated
// `Sync context: <this>` log line. The format mirrors the
// obj:cm:sync-context attribute the activities mapper attaches to
// each activity (RFC1123 timestamp), so consumers can parse either
// source with the same regex.
//
// The returned string is well under any per-record cap (~250 bytes
// for a typical sync).
//
// Stable-format guarantee: the rendered shape is intended to be
// matched and parsed by downstream log-analysis tooling. Field
// order, key names, and the RFC1123 timestamp encoding are part of
// the contract; do not change them without coordinating with
// consumers.
func LoggableSyncContext(sc *SyncContext) string {
	osc := NewOrttoSyncContext(sc)
	if t, err := time.Parse(time.RFC3339, osc.TriggerCreatedAt); err == nil {
		osc.TriggerCreatedAt = t.Format(time.RFC1123)
	}
	return fmt.Sprintf("%+v", osc)
}

// LoggableRequestEnvelope returns a single-line representation of
// an OrttoRequest's "shell" — every field except the
// activities/contacts slice itself, which is replaced with a count.
//
// Used so the per-activity logging can emit envelope fields once per
// request and iterate the slice elements into separate
// `Sync activity N/M:` log lines.
//
// Always small (< 200 bytes); fits comfortably under any cap.
func LoggableRequestEnvelope(req OrttoRequest) string {
	if a, ok := req.AsOrttoActivitiesRequest(); ok {
		envelope := struct {
			Activities    int
			Async         bool
			MergeBy       []string
			MergeStrategy uint8
		}{len(a.Activities), a.Async, a.MergeBy, a.MergeStrategy}
		return fmt.Sprintf("%+v", envelope)
	}
	if c, ok := req.AsOrttoContactsRequest(); ok {
		envelope := struct {
			Contacts      int
			Async         bool
			MergeBy       []string
			MergeStrategy uint8
			FindStrategy  uint8
		}{len(c.Contacts), c.Async, c.MergeBy, c.MergeStrategy, c.FindStrategy}
		return fmt.Sprintf("%+v", envelope)
	}
	return fmt.Sprintf("%+v", req)
}

// LoggableActivity returns a single-line representation of one
// OrttoActivity for emission on a `Sync activity N/M:` log line.
//
// maxBytes is the caller-supplied cap on the rendered length —
// typically driven by the deploy target's log-pipeline per-record
// limit (e.g. Vercel's empirical drain-to-Axiom cap of ~3,827 bytes).
// Callers should leave headroom for the log prefix that the runtime
// will prepend (Go's `YYYY/MM/DD HH:MM:SS ` is ~20 bytes).
//
// The returned string:
//   - has obj:cm:cdp-fields and obj:cm:sync-context entries
//     unconditionally stripped from Attributes (cdp-fields is a
//     flattened mirror of Fields, and sync-context is already
//     covered by the dedicated `Sync context:` line emitted once per
//     request);
//   - is capped at maxBytes — if the body exceeds the cap, every
//     Attributes / Fields entry whose key has the verboseFieldPrefix
//     (`txt:` — Ortto's text-field type prefix) is dropped and
//     activityLogVerboseDropMarker is appended so a log reader can
//     tell elision happened. As a last resort the body is
//     hard-truncated with activityLogTruncationMarker.
//
// The original activity is never mutated — Attributes/Fields maps
// are shallow-cloned before any keys are dropped.
//
// Load-bearing content guarantees: when an activity carries a merge
// field in its Fields map (e.g. `str:cm:<merge-field>:<uuid>`), the
// rendered string contains the substring so the timeline's
// content-based seed query can match it. Merge fields use Ortto's
// `str:` prefix (short string), never `txt:`, so they're never
// dropped by the size-based logic.
func LoggableActivity(activity OrttoActivity, maxBytes int) string {
	clone := activity
	if activity.Attributes != nil {
		clone.Attributes = make(OrttoAttributes, len(activity.Attributes))
		for k, v := range activity.Attributes {
			if _, ok := metaActivityAttributes[k]; ok { // skip meta data fields
				continue
			}
			clone.Attributes[k] = v
		}
	}

	out := fmt.Sprintf("%+v", clone)
	if len(out) <= maxBytes {
		return out
	}

	// Over the cap. Clone Fields too so verbose-field drops don't
	// mutate the original. Attributes is already a fresh map from
	// the strip step above.
	if activity.Fields != nil {
		clone.Fields = make(map[string]interface{}, len(activity.Fields))
		for k, v := range activity.Fields {
			clone.Fields[k] = v
		}
	}

	// Drop every entry whose key has the verbose-text type prefix.
	// All-at-once rather than one-at-a-time because (a) the goal is
	// to fit under the cap, not to preserve as much detail as
	// possible, and (b) Go map iteration is unordered, so a
	// one-at-a-time loop would produce non-deterministic output.
	droppedAny := false
	for k := range clone.Attributes {
		if strings.HasPrefix(k, verboseFieldPrefix) {
			delete(clone.Attributes, k)
			droppedAny = true
		}
	}
	for k := range clone.Fields {
		if strings.HasPrefix(k, verboseFieldPrefix) {
			delete(clone.Fields, k)
			droppedAny = true
		}
	}
	if droppedAny {
		out = fmt.Sprintf("%+v", clone)
		// Reserve room for the verbose-drop marker so the appended
		// signal doesn't push us back over the cap.
		if len(out)+len(activityLogVerboseDropMarker) <= maxBytes {
			return out + activityLogVerboseDropMarker
		}
	}

	// Pathological: dropping every txt:-prefixed entry didn't bring
	// the body under the cap. Hard-truncate and append a marker so
	// log readers can tell. Truncation point is byte-level rather
	// than rune-level — the body is ASCII-dominated Go %+v output;
	// no risk of mid-rune cuts in practice.
	cut := maxBytes - len(activityLogTruncationMarker)
	if cut < 0 {
		cut = 0
	}
	if cut > len(out) {
		cut = len(out)
	}
	return strings.TrimRight(out[:cut], " ") + activityLogTruncationMarker
}

// ExtractActivities returns the activities slice from an OrttoRequest,
// or nil if the request is not an activities request. Used by callers
// driving per-activity logging — see ExtractContacts for the contacts
// equivalent.
func ExtractActivities(req OrttoRequest) []OrttoActivity {
	if a, ok := req.AsOrttoActivitiesRequest(); ok {
		return a.Activities
	}
	return nil
}

// ExtractContacts returns the contacts slice from an OrttoRequest,
// or nil if the request is not a contacts request. Sibling of
// ExtractActivities — used by callers driving per-item logging so
// contacts batches (notably team-update batches with N members) split
// cleanly across separate log records under any per-record drain cap.
func ExtractContacts(req OrttoRequest) []OrttoContact {
	if c, ok := req.AsOrttoContactsRequest(); ok {
		return c.Contacts
	}
	return nil
}

// LoggableContact returns a single-line representation of one
// OrttoContact for emission on a per-item log line. Sibling of
// LoggableActivity — webhook handlers emit one of these per contact
// in a multi-contact batch so a team-update payload doesn't exceed
// the deploy target's per-record drain cap.
//
// maxBytes is the caller-supplied cap on the rendered length. See
// LoggableActivity for the headroom guidance.
//
// The returned string:
//   - is capped at maxBytes — if the body exceeds the cap, every
//     Fields entry whose key has the verboseFieldPrefix (`txt:`,
//     Ortto's text-field type prefix) is dropped and
//     activityLogVerboseDropMarker is appended. As a last resort the
//     body is hard-truncated with activityLogTruncationMarker.
//   - never mutates the input — Fields is shallow-cloned before any
//     keys are dropped.
//
// Load-bearing content guarantee: when a contact carries a merge
// field in its Fields map (e.g. `str:cm:<merge-field>:<uuid>` for
// the configured fundraiser merge field), the rendered string
// contains the substring so the timeline's content-based seed query
// matches it. Merge fields use Ortto's `str:` prefix (short string),
// never `txt:`, so they're never dropped by the size-based logic.
func LoggableContact(contact OrttoContact, maxBytes int) string {
	clone := contact
	out := fmt.Sprintf("%+v", clone)
	if len(out) <= maxBytes {
		return out
	}

	// Over the cap. Clone Fields so verbose-field drops don't mutate
	// the original.
	if contact.Fields != nil {
		clone.Fields = make(map[string]interface{}, len(contact.Fields))
		for k, v := range contact.Fields {
			clone.Fields[k] = v
		}
	}

	droppedAny := false
	for k := range clone.Fields {
		if strings.HasPrefix(k, verboseFieldPrefix) {
			delete(clone.Fields, k)
			droppedAny = true
		}
	}
	if droppedAny {
		out = fmt.Sprintf("%+v", clone)
		if len(out)+len(activityLogVerboseDropMarker) <= maxBytes {
			return out + activityLogVerboseDropMarker
		}
	}

	cut := maxBytes - len(activityLogTruncationMarker)
	if cut < 0 {
		cut = 0
	}
	if cut > len(out) {
		cut = len(out)
	}
	return strings.TrimRight(out[:cut], " ") + activityLogTruncationMarker
}
