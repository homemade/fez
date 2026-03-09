package sync

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// RequireAPISecret validates that a request contains the expected secret.
// It reads the expected secret from the environment variable named by envVarName,
// then compares it against the value from the request header named by headerName.
// If bearerPrefix is true, the "Bearer " prefix is stripped from the header value
// before comparison (e.g. for Authorization: Bearer <secret>).
// Returns nil if the secret matches, or an error describing the failure.
func RequireAPISecret(r *http.Request, headerName string, envVarName string, bearerPrefix bool) error {
	expected := os.Getenv(envVarName)
	if expected == "" {
		return fmt.Errorf("%s environment variable is not set", envVarName)
	}

	actual := r.Header.Get(headerName)
	if actual == "" {
		return fmt.Errorf("missing %s header", headerName)
	}

	if bearerPrefix {
		if !strings.HasPrefix(actual, "Bearer ") {
			return fmt.Errorf("invalid %s header: expected Bearer prefix", headerName)
		}
		actual = strings.TrimPrefix(actual, "Bearer ")
	}

	if subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) != 1 {
		return fmt.Errorf("invalid secret")
	}

	return nil
}
