package sync

import (
	"fmt"
	"net/http"
)

// RaiselyRateLimitError is the typed error returned by fez's Raisely
// HTTP layer when the API responds with HTTP 429. Distinct from the
// nested [OrttoRateLimitError] shape because Raisely's 429s originate
// at an edge layer (a Cloudflare security layer in front of the
// primary application servers, confirmed by Raisely 2026-06-15) — they
// carry **no `Retry-After` header** and **empty bodies**, so there is
// no hint to honour. Callers self-time the back-off (~30s + jitter)
// at the [PlaceRateLimitHold] site rather than reading a `TryInSeconds`
// from this error.
//
// ResponseHeaders preserves the outgoing edge / gateway attribution
// headers (Server / Via / CF-* style) so a future incident can be
// attributed back to its source without re-issuing the request — the
// same header-capture prereq that landed on [OrttoRateLimitError].
//
// **No CallerStack** — Cloudflare's edge filtering is opaque to both
// the application logs (Raisely has no visibility into Cloudflare's
// own logs) and our own stack (the failing call site doesn't predict
// which edge rule fired). The cost-vs-value tilt the Ortto side
// justified doesn't carry over.
type RaiselyRateLimitError struct {
	// StatusCode is the HTTP status code Raisely (or its edge layer)
	// returned. Always 429 for instances constructed by fez's
	// Raisely call validator.
	StatusCode int

	// ResponseHeaders is a snapshot of the response headers. Useful
	// for attributing the response source (edge vs app) on a future
	// incident — Cloudflare exposes `Server: cloudflare` / `CF-Ray`
	// / etc. on its edge responses, which are otherwise lost when
	// the request returns.
	ResponseHeaders http.Header
}

// Error implements the error interface. Brief by design: the message
// shape is the only signal there is (no body, no Retry-After).
func (e *RaiselyRateLimitError) Error() string {
	return fmt.Sprintf("raisely: rate-limited (status=%d)", e.StatusCode)
}
