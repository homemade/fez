package sync

// SyncContext holds shared sync configuration and trigger metadata.
// It is immutable after construction — fields must not be modified
// except for CampaignName which may be set after fetching campaign data
// but before the mapper is created.
type SyncContext struct {
	Config         Config
	Campaign       string
	RecordRequests bool

	// Trigger metadata (previously on OrttoSyncContext)
	Source           string
	TriggerType      string
	TriggerSubType   string
	TriggerID        string
	TriggerCreatedAt string
	CampaignName     string
}
