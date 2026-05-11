package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// MetaActivityAttributes is the set of attribute keys that the fez
// mapper manages automatically rather than treating as user-defined
// activity content:
//
//   - obj:cm:sync-context — trigger identifiers and timestamps for
//     the inbound event that produced this activity.
//   - obj:cm:cdp-fields   — a flattened mirror of Fields, redundant
//     with the Fields map itself.
//
// Downstream operations that produce a representation of an activity
// for comparison, display, or snapshot purposes typically skip these
// entries: they vary independently of the activity's user-visible
// content (sync-context) or duplicate other parts of the payload
// (cdp-fields) and would otherwise add noise or false negatives.
//
// Exposed so callers building their own activity views (logs,
// fingerprints, exports, snapshots) can share the same skip set
// rather than duplicating the key list at each call site.
var MetaActivityAttributes = map[string]struct{}{
	"obj:cm:sync-context": {},
	"obj:cm:cdp-fields":   {},
}

// ContentHash returns a 16-hex-char content fingerprint for the
// activity. Two activities that differ only in volatile metadata
// (different sync-context trigger IDs, different cdp-fields
// mirrors, etc.) produce the same hash; any change in ActivityID,
// Fields, or non-volatile Attributes produces a different hash.
//
// Stability across Go's randomised map iteration order is provided
// by encoding/json's guarantee to sort string map keys at every
// level when marshalling.
//
// Request-level routing such as merge_by is not part of the input —
// the hash is a pure function of intrinsic activity content, so it
// can be computed on the activity alone before the surrounding
// request is finalised.
func (a OrttoActivity) ContentHash() string {
	var b strings.Builder
	b.WriteString(a.ActivityID)
	b.WriteByte('|')
	if a.Fields != nil {
		fieldsJSON, _ := json.Marshal(a.Fields)
		b.Write(fieldsJSON)
	}
	b.WriteByte('|')
	stripped := stripMetaAttributes(a.Attributes)
	if stripped != nil {
		attrsJSON, _ := json.Marshal(stripped)
		b.Write(attrsJSON)
	}

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:16]
}

// stripMetaAttributes returns a shallow copy of attrs with the
// keys listed in MetaActivityAttributes removed. nil in → nil out
// so callers can distinguish "no attributes" from "empty attributes"
// without an extra check.
func stripMetaAttributes(attrs OrttoAttributes) map[string]interface{} {
	if attrs == nil {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for k, v := range attrs {
		if _, ok := MetaActivityAttributes[k]; ok {
			continue
		}
		out[k] = v
	}
	return out
}
