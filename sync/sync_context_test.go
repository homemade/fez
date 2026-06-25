package sync

import (
	"runtime/debug"
	"strings"
	"testing"
)

// TestLastPathSegment pins the consumer-name derivation used by
// [computeDefaultUserAgent] — the last segment of the consumer's
// module path becomes the readable name in the outbound User-Agent
// (e.g. "bitbucket.org/example/myapp" → "myapp").
func TestLastPathSegment(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"bitbucket.org/example/myapp", "myapp"},
		{"github.com/example/single-segment-after-host", "single-segment-after-host"},
		{"plain", "plain"},
		{"", ""},
		{"trailing/", ""},
	}
	for _, tc := range cases {
		got := lastPathSegment(tc.in)
		if got != tc.want {
			t.Errorf("lastPathSegment(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestVcsRevision pins the helper used by [computeDefaultUserAgent]
// to dig the `vcs.revision` value out of buildinfo settings. Hand-
// constructs a [debug.BuildInfo] so the test doesn't depend on the
// runtime buildinfo of the fez test binary.
func TestVcsRevision(t *testing.T) {
	t.Parallel()

	got := vcsRevision(&debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs.modified", Value: "false"},
		{Key: "vcs.revision", Value: "abc1234567890def"},
		{Key: "GOOS", Value: "linux"},
	}})
	if got != "abc1234567890def" {
		t.Errorf("vcsRevision returned %q; want abc1234567890def", got)
	}

	got = vcsRevision(&debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "GOOS", Value: "linux"},
	}})
	if got != "" {
		t.Errorf("vcsRevision missing-setting case returned %q; want empty", got)
	}
}

// TestDefaultUserAgent_FezOwnTests pins the fez-is-main-module
// fall-through: when fez runs its own tests, the main module IS
// fez, so [DefaultUserAgent] becomes "fez/<fezVersion>" (which is
// "fez/unknown" in the test binary because fez doesn't list itself
// under its own Deps).
func TestDefaultUserAgent_FezOwnTests(t *testing.T) {
	t.Parallel()
	if !strings.HasPrefix(DefaultUserAgent, "fez/") {
		t.Errorf("DefaultUserAgent in fez's own test binary = %q; want fez/* prefix", DefaultUserAgent)
	}
}
