package sync

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"time"
)

// HTTPRequestTimeout is the default timeout for all HTTP requests to external APIs.
const HTTPRequestTimeout = 60 * time.Second

// peekResponseBody reads resp.Body into memory and rewires resp.Body to
// a fresh reader holding the same bytes — letting a validator earlier
// in a [github.com/carlmjohnson/requests.Builder] chain inspect the body
// (e.g. to branch on a body-shape) without consuming it for downstream
// validators / response handlers. Returns the bytes for the caller to
// decode in-place.
//
// Use from a [requests.AddValidator] callback that needs to peek at the
// body. Without the restore step, subsequent validators or the final
// response handler (typically `.ErrorJSON(...)` or `.ToJSON(...)`) would
// see an empty Body and fail to decode.
//
// Precondition: resp and resp.Body are both non-nil. The requests
// library guarantees this for validator callbacks; misuse from outside
// that contract panics on nil dereference.
func peekResponseBody(resp *http.Response) ([]byte, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

// logAPICall emits the per-call attribution log line shared by the
// Ortto and Raisely validators:
//
//	<downstream> call: source=<triggerType> ua=<User-Agent> path=… status=…
//
// The downstream label ("Ortto" / "Raisely") distinguishes the two
// sources for grep-based filtering. triggerType is the consumer's
// caller-attribution tag pulled from SyncContext.TriggerType. ua= is
// read from the outbound request's User-Agent header so the value
// recorded in our own logs matches what downstream / edge logs see
// verbatim — load-bearing for incident communication where the
// operator filters by UA.
//
// Use from a [requests.AddValidator] callback; emits exactly one line
// per call regardless of response status.
func logAPICall(downstream, triggerType string, resp *http.Response) {
	log.Printf("%s call: source=%s ua=%s path=%s status=%d",
		downstream, triggerType,
		resp.Request.Header.Get("User-Agent"),
		resp.Request.URL.Path, resp.StatusCode)
}
