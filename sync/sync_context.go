package sync

// TriggerInfo holds metadata about what initiated a sync operation.
type TriggerInfo struct {
	Source           string // e.g. "Raisely", "Manual"
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
}
