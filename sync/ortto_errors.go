package sync

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// OrttoRateLimitError is the typed error returned by fez's Ortto HTTP
// layer when the API responds with a rate-limit shape. The body Ortto
// sends on a 429 is nested:
//
//	{"error":{"code":"rate-limit","message":"...","try-in-seconds":N}}
//
// Distinct from the generic flat [OrttoError] that other endpoints
// return (`{request_id, code int, error}`). The two shapes coexist
// because Ortto's rate-limit response is its own envelope; fez
// type-asserts on the nested shape and returns this typed error so
// callers can [IsRateLimited] and [RetryAfter] without re-parsing.
//
// fez itself does NOT retry, sleep, or apply the hint — it surfaces
// the typed error and returns. The durable wait is the caller's
// responsibility (record a hold in whatever state store the consumer
// uses so subsequent gated operations defer until the window passes).
//
// ResponseHeaders preserves the outgoing edge / gateway attribution
// headers (Server / Via / CF-* style) — load-bearing for shortening
// future edge-vs-app incident reconciliations.
//
// CallerStack carries up to a bounded number of non-fez frames
// captured at error-emission time, so a 429 in production identifies
// which consumer-side call-path drove it.
type OrttoRateLimitError struct {
	// StatusCode is the HTTP status code Ortto returned. Always 429
	// for instances constructed by fez's rate-limit validator.
	StatusCode int

	// Code is the body's `error.code` field — "rate-limit" for the
	// instances fez surfaces (the rate-limit detector matches on
	// this literal before returning *OrttoRateLimitError).
	Code string

	// Message is the body's `error.message` field — Ortto's
	// human-readable rate-limit description.
	Message string

	// TryInSeconds is the body's `error.try-in-seconds` field — the
	// authoritative wait hint Ortto provides on a 429. Carried
	// through to [RetryAfter] so callers can build a hold window.
	TryInSeconds int

	// ResponseHeaders is a snapshot of the response headers
	// (Server / Via / CF-* edge-attribution headers, plus any
	// future Retry-After-style hints Ortto might add). Preserved
	// so incident investigations can attribute the response source
	// without re-issuing the request.
	ResponseHeaders http.Header

	// CallerStack carries up to a bounded number of non-fez frames
	// captured at error-emission time. Empty in tests that don't go
	// through the production capture path.
	CallerStack string
}

// Error implements the error interface. The format includes the wait
// hint so log lines surface it without typed-error inspection.
func (e *OrttoRateLimitError) Error() string {
	return fmt.Sprintf("ortto: rate-limited (status=%d code=%q try-in-seconds=%d): %s",
		e.StatusCode, e.Code, e.TryInSeconds, e.Message)
}

// IsRateLimited reports whether err is — or wraps — a typed
// rate-limit error from either downstream:
//   - [*OrttoRateLimitError] (nested body shape with `try-in-seconds`)
//   - [*RaiselyRateLimitError] (Cloudflare-edge 429 with no hint)
//
// Callers use this at the post-fez error path to branch on rate-limit
// handling (defer/ack instead of 500, place a tenant-scoped hold).
// One predicate for both downstreams keeps the consumer-side branch
// uniform: `if sync.IsRateLimited(err) { … }` works regardless of which
// API returned the 429.
func IsRateLimited(err error) bool {
	var ortto *OrttoRateLimitError
	if errors.As(err, &ortto) {
		return true
	}
	var raisely *RaiselyRateLimitError
	return errors.As(err, &raisely)
}

// RetryAfter returns the wait hint the rate-limited downstream
// supplied, converted to [time.Duration]. For [*OrttoRateLimitError]
// this is the body's `try-in-seconds` value. For [*RaiselyRateLimitError]
// this is always zero — Raisely's 429s come from a Cloudflare edge
// layer with no `Retry-After` header and empty bodies (no hint to
// surface). Returns zero when err is not a typed rate-limit error or
// when the Ortto `try-in-seconds` field was absent / non-positive.
// Callers should treat zero as "no hint" and apply their own
// minimum-wait policy at the Raisely-side call site (a self-timed
// ~30s + jitter back-off is the typical shape).
func RetryAfter(err error) time.Duration {
	var ortto *OrttoRateLimitError
	if errors.As(err, &ortto) {
		if ortto.TryInSeconds <= 0 {
			return 0
		}
		return time.Duration(ortto.TryInSeconds) * time.Second
	}
	// Raisely: no hint to surface. errors.As on *RaiselyRateLimitError
	// would still match err, but the right answer is the same zero we
	// return for "not rate-limited at all" — the caller's site is
	// responsible for choosing the self-timed base. No type-assertion
	// branch needed.
	return 0
}
