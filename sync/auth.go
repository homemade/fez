package sync

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// RequireOrgHeader reads and validates the X-Org header from the request.
// Returns the org value or an error if the header is missing or empty.
func RequireOrgHeader(r *http.Request) (string, error) {
	org := strings.TrimSpace(r.Header.Get("X-Org"))
	if org == "" {
		return "", fmt.Errorf("missing or empty X-Org header")
	}
	return org, nil
}

// ValidateOrgMatchesMappingPath checks that the mapping path starts with the
// expected org prefix (case-insensitive). Returns an error if it doesn't match.
func ValidateOrgMatchesMappingPath(org string, mappingPath string) error {
	expectedPrefix := strings.ToLower(org) + "/"
	if !strings.HasPrefix(strings.ToLower(mappingPath), expectedPrefix) {
		return fmt.Errorf("mapping path %q does not match org %q", mappingPath, org)
	}
	return nil
}

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
