package sync

import (
	"runtime/debug"
	"strings"
)

// TriggerInfo holds metadata about what initiated a sync operation.
type TriggerInfo struct {
	Source           string // e.g. "Raisely", "Manual"
	ModelType        string // e.g. "INDIVIDUAL", GROUP
	ModelID          string // e.g. Raisely Profile UUID
	ParentType       string // e.g. "INDIVIDUAL", GROUP
	ParentID         string // e.g. Raisely Parent Profile UUID
	TriggerType      string // e.g. "webhook", "cli-sync-webhook", "webtracking", "admin-sync-preview", etc.
	TriggerSubType   string // e.g. Raisely webhook event type "profile.created"
	TriggerID        string // e.g. Raisely Event UUID
	TriggerCreatedAt string // RFC3339 timestamp
}

// SyncContext holds shared sync configuration and trigger metadata.
// It is immutable after construction — fields must not be modified
// except for CampaignName which may be set after fetching campaign data
// but before the mapper is created.
type SyncContext struct {
	Config         Config
	Campaign       string
	RecordRequests bool
	Debug          bool

	TriggerInfo
	CampaignName string

	// UserAgent overrides the outbound User-Agent header value fez sets
	// on every Ortto / Raisely HTTP call. Identifies the consumer to
	// downstream operators — a distinct UA shortens edge-log filtering
	// on future incidents. Leave empty to use [DefaultUserAgent], which
	// fez derives at package init from [runtime/debug.ReadBuildInfo] as
	// "<consumer-module-name>/<vcs-revision>". Set explicitly only when
	// the per-SyncContext UA differs from the build-derived default.
	UserAgent string
}

// fezVersion is fez's own resolved module version, read from buildinfo
// at package init. When fez is imported as a dependency, [debug.BuildInfo.Deps]
// carries the semver tag the consumer pinned (e.g. "v1.23.0"); when
// fez is itself the main module (e.g. fez's own tests), it isn't
// listed under Deps so this falls back to "unknown".
var fezVersion = lookupFezVersion()

func lookupFezVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, m := range info.Deps {
		if m.Path == "github.com/homemade/fez" {
			if m.Version == "" {
				return "unknown"
			}
			return m.Version
		}
	}
	return "unknown"
}

// DefaultUserAgent is the User-Agent fez emits when [SyncContext.UserAgent]
// is empty. Computed at package init from [runtime/debug.ReadBuildInfo]:
//
//   - "<consumer-module-name>/<vcs-revision>" when fez is imported by
//     another module built with Go's default VCS-stamping (the last
//     path segment of [debug.BuildInfo.Main.Path] joined with the
//     `vcs.revision` setting, truncated to 8 chars for readability).
//   - "<consumer-module-name>/unknown" when the consumer's binary was
//     built without VCS info (e.g. `go build -buildvcs=false`).
//   - "fez/<fez-version>" when fez is itself the main module (e.g.
//     fez's own tests).
//
// Defined as a var so consumers may replace it wholesale at process
// init when the build-derived default isn't appropriate. Per-call
// overrides go on [SyncContext.UserAgent] instead.
var DefaultUserAgent = computeDefaultUserAgent()

func computeDefaultUserAgent() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Path == "" {
		return "fez/" + fezVersion
	}
	// fez running its own tests: the main module IS fez.
	if info.Main.Path == "github.com/homemade/fez" {
		return "fez/" + fezVersion
	}
	name := lastPathSegment(info.Main.Path)
	rev := vcsRevision(info)
	if rev == "" {
		return name + "/unknown"
	}
	if len(rev) > 8 {
		rev = rev[:8]
	}
	return name + "/" + rev
}

// lastPathSegment returns everything after the final "/" in p,
// or p itself when there's no slash. Used to derive the human-
// readable consumer name from a fully-qualified Go module path
// (e.g. "bitbucket.org/example/myapp" → "myapp").
func lastPathSegment(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// vcsRevision returns info.Settings["vcs.revision"] or "" when absent.
// Populated by Go's default `-buildvcs=auto` mode when building from
// a clean checkout; absent for `-buildvcs=false` builds and for some
// CI environments that strip VCS info.
func vcsRevision(info *debug.BuildInfo) string {
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return ""
}

// userAgent returns the SyncContext's UserAgent, falling back to
// [DefaultUserAgent] when empty.
func (sc *SyncContext) userAgent() string {
	if sc == nil || sc.UserAgent == "" {
		return DefaultUserAgent
	}
	return sc.UserAgent
}
