package sync

import (
	"context"
	"fmt"
	"strings"
)

// Service provides campaign operations used by CLI commands,
// admin API routes, and Raisely webhook/tracking handlers.
//
// Usage:
//
//	svc := sync.NewService(config, campaignID, trigger, false)
//	svc.FetchCampaign(false, ctx)                          // required before Map/Send
//	req, _ := svc.MapFundraisingProfile(profileID, ctx)    // map without sending
//	svc.SendRequest(req, ctx)                               // send to Ortto
//
// Operations that do not require FetchCampaign:
//
//	svc.CheckOrttoFields(ctx)
//	svc.EnsureOrttoFields(ctx)
//	svc.CheckRaiselyWebhook(webhookURL, ctx)
//	svc.EnsureRaiselyWebhook(webhookURL, ctx)
type Service struct {
	sc      *SyncContext
	fetcher *RaiselyFetcherAndUpdater

	// Set after FetchCampaign — mapper creation is deferred because
	// CampaignName must be set on SyncContext first.
	campaign *FundraisingCampaign
	mapper   OrttoMapper
}

// NewService creates a Service for the given campaign configuration.
func NewService(config Config, campaignID string, trigger TriggerInfo, recordRequests bool) *Service {
	sc := &SyncContext{
		Config:         config,
		Campaign:       campaignID,
		RecordRequests: recordRequests,
		TriggerInfo:    trigger,
	}
	return &Service{
		sc:      sc,
		fetcher: &RaiselyFetcherAndUpdater{SyncContext: sc},
	}
}

// FetchCampaign fetches (or cache-hits) the fundraising campaign from Raisely.
// Must be called before Map and Send operations.
// Set refresh=true to force a fresh fetch (e.g. on new registrations).
func (s *Service) FetchCampaign(refresh bool, ctx context.Context) (*FundraisingCampaign, error) {
	fc, err := s.fetcher.CachedFundraisingCampaign(s.sc.Campaign, refresh, ctx)
	if err != nil {
		return nil, err
	}
	s.campaign = fc
	s.sc.CampaignName = fc.Name
	s.mapper = NewOrttoMapper(s.sc)
	return fc, nil
}

// requireMapper returns an error if FetchCampaign has not been called.
func (s *Service) requireMapper() error {
	if s.mapper == nil {
		return fmt.Errorf("FetchCampaign must be called before mapping or sending")
	}
	return nil
}

// --- Mapping ---

// MapFundraisingProfile fetches a profile, determines whether it belongs
// to a team or is an individual, and maps it to an Ortto request.
// FetchCampaign must be called first.
func (s *Service) MapFundraisingProfile(profileID string, ctx context.Context) (OrttoRequest, error) {
	if err := s.requireMapper(); err != nil {
		return nil, err
	}

	fundraisingPage, err := s.fetcher.FetchFundraisingPage(profileID, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch profile %s: %w", profileID, err)
	}

	fundraisingPageType, ok := fundraisingPage.Source.StringForPath("type")
	if !ok {
		return nil, fmt.Errorf("profile %s is missing a type", profileID)
	}

	profile := FundraisingProfile{
		P2PID: profileID,
		Type:  fundraisingPageType,
	}

	if parentP2PID, ok := fundraisingPage.Source.StringForPath("parent.uuid"); ok {
		profile.Parent.P2PID = parentP2PID
	}
	if parentType, ok := fundraisingPage.Source.StringForPath("parent.type"); ok {
		profile.Parent.Type = parentType
	}

	team := profile.TeamP2PID(s.campaign)
	if team != "" {
		return s.mapper.MapTeamFundraisingPage(s.campaign, team, ctx)
	}
	return s.mapper.MapFundraisingPage(s.campaign, profileID, ctx)
}

// MapByWebhookModel maps using model type information already known from
// the webhook payload, avoiding a redundant profile fetch.
// FetchCampaign must be called first.
func (s *Service) MapByWebhookModel(modelType, modelID, parentType, parentID string, parentIsCampaignProfile bool, ctx context.Context) (OrttoRequest, error) {
	if err := s.requireMapper(); err != nil {
		return nil, err
	}

	if modelType == "GROUP" ||
		(modelType == "INDIVIDUAL" && !parentIsCampaignProfile && parentType == "GROUP") {
		teamID := modelID
		if modelType == "INDIVIDUAL" {
			teamID = parentID
		}
		return s.mapper.MapTeamFundraisingPage(s.campaign, teamID, ctx)
	}

	if modelType == "INDIVIDUAL" {
		return s.mapper.MapFundraisingPage(s.campaign, modelID, ctx)
	}

	return nil, fmt.Errorf("unsupported model type: %s", modelType)
}

// MapTrackingData maps tracking key-value pairs to an Ortto request.
// FetchCampaign must be called first.
func (s *Service) MapTrackingData(data map[string]string, ctx context.Context) (OrttoRequest, error) {
	if err := s.requireMapper(); err != nil {
		return nil, err
	}
	return s.mapper.MapTrackingData(s.campaign, data, ctx)
}

// --- Send ---

// SendRequest sends a mapped request to Ortto.
// FetchCampaign must be called first.
func (s *Service) SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error) {
	if err := s.requireMapper(); err != nil {
		return nil, err
	}
	return s.mapper.SendRequest(req, ctx)
}

// --- Ortto field management ---

// buildMappers creates the mapper hierarchy needed for field operations.
// These do not require FetchCampaign since they don't need CampaignName.
func (s *Service) buildMappers() (RaiselyMapper, OrttoFetcherAndUpdater) {
	raiselyMapper := RaiselyMapper{SyncContext: s.sc, RaiselyFetcherAndUpdater: s.fetcher}
	orttoFetcherAndUpdater := OrttoFetcherAndUpdater{SyncContext: s.sc}
	return raiselyMapper, orttoFetcherAndUpdater
}

// CheckOrttoFields validates that all required Ortto custom fields exist.
// Returns a map of field ID → status ("✅", "❌", "⏳").
// Does NOT require FetchCampaign.
func (s *Service) CheckOrttoFields(ctx context.Context) (map[string]string, error) {
	raiselyMapper, orttoFetcherAndUpdater := s.buildMappers()

	switch s.sc.Config.Target {
	case "ortto-activities":
		mapper := OrttoActivitiesMapper{
			SyncContext: s.sc, RaiselyMapper: raiselyMapper, OrttoFetcherAndUpdater: orttoFetcherAndUpdater,
		}
		return mapper.CheckOrttoCustomFields("⏳", "✅", "❌", ctx)
	default:
		mapper := OrttoContactsMapper{
			SyncContext: s.sc, RaiselyMapper: raiselyMapper, OrttoFetcherAndUpdater: orttoFetcherAndUpdater,
		}
		return mapper.CheckOrttoCustomFields("⏳", "✅", "❌", ctx)
	}
}

// EnsureOrttoFields creates any missing Ortto custom person fields.
// Returns the list of created field IDs.
// Only valid for ortto-activities target.
// Does NOT require FetchCampaign.
func (s *Service) EnsureOrttoFields(ctx context.Context) ([]string, error) {
	if s.sc.Config.Target != "ortto-activities" {
		return nil, fmt.Errorf("EnsureOrttoFields requires ortto-activities target, got: %q", s.sc.Config.Target)
	}

	raiselyMapper, orttoFetcherAndUpdater := s.buildMappers()
	mapper := OrttoActivitiesMapper{
		SyncContext: s.sc, RaiselyMapper: raiselyMapper, OrttoFetcherAndUpdater: orttoFetcherAndUpdater,
	}
	return mapper.EnsureCustomPersonFields(ctx)
}

// --- Raisely webhook management ---

// WebhookStatus holds the result of checking a Raisely webhook.
type WebhookStatus struct {
	URL           string
	Exists        bool
	PresentEvents []string
	MissingEvents []string
}

// CheckRaiselyWebhook checks if a webhook exists at the given URL
// and reports which required events are present or missing.
// Does NOT require FetchCampaign.
func (s *Service) CheckRaiselyWebhook(webhookURL string, ctx context.Context) (*WebhookStatus, error) {
	requiredEvents := s.sc.Config.API.Settings.RaiselyWebhookEvents
	if len(requiredEvents) == 0 {
		return nil, fmt.Errorf("no webhook events configured in api.settings.raiselyWebhookEvents")
	}

	webhooks, err := s.fetcher.ListWebhooks(s.sc.Campaign, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list webhooks: %w", err)
	}

	// Find matching webhook by URL
	for _, wh := range webhooks {
		if wh.URL == webhookURL {
			return s.buildWebhookStatus(webhookURL, true, wh.Events, requiredEvents), nil
		}
	}

	return &WebhookStatus{URL: webhookURL, Exists: false}, nil
}

// EnsureRaiselyWebhook checks for an existing webhook and creates one if missing.
// Returns the webhook status and whether a new webhook was created.
// Does NOT require FetchCampaign.
func (s *Service) EnsureRaiselyWebhook(webhookURL string, ctx context.Context) (*WebhookStatus, bool, error) {
	status, err := s.CheckRaiselyWebhook(webhookURL, ctx)
	if err != nil {
		return nil, false, err
	}

	if status.Exists {
		return status, false, nil
	}

	// Create webhook
	requiredEvents := s.sc.Config.API.Settings.RaiselyWebhookEvents
	_, err = s.fetcher.CreateWebhook(webhookURL, s.sc.Campaign, requiredEvents, ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create webhook: %w", err)
	}

	return &WebhookStatus{
		URL:           webhookURL,
		Exists:        true,
		PresentEvents: requiredEvents,
	}, true, nil
}

func (s *Service) buildWebhookStatus(url string, exists bool, existingEvents, requiredEvents []string) *WebhookStatus {
	existingSet := make(map[string]bool)
	for _, e := range existingEvents {
		existingSet[e] = true
	}

	var present, missing []string
	for _, e := range requiredEvents {
		if existingSet[e] {
			present = append(present, e)
		} else {
			missing = append(missing, e)
		}
	}

	return &WebhookStatus{
		URL:           url,
		Exists:        exists,
		PresentEvents: present,
		MissingEvents: missing,
	}
}

// CheckRaiselyWebhooks checks the main webhook and, if an extensions config
// is provided, the extensions webhook too. Lists webhooks once and checks
// both URLs against the result.
// Does NOT require FetchCampaign.
func (s *Service) CheckRaiselyWebhooks(webhookDomain string, extensionsConfig *Config, ctx context.Context) ([]WebhookStatus, error) {
	mainURL := fmt.Sprintf("https://%s/api/raisely/webhooks", webhookDomain)
	mainEvents := s.sc.Config.API.Settings.RaiselyWebhookEvents
	if len(mainEvents) == 0 {
		return nil, fmt.Errorf("no webhook events configured in api.settings.raiselyWebhookEvents")
	}

	webhooks, err := s.fetcher.ListWebhooks(s.sc.Campaign, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list webhooks: %w", err)
	}

	// Build lookup of existing webhooks by URL
	webhooksByURL := make(map[string][]string)
	for _, wh := range webhooks {
		webhooksByURL[wh.URL] = wh.Events
	}

	var results []WebhookStatus

	// Check main webhook
	if events, ok := webhooksByURL[mainURL]; ok {
		results = append(results, *s.buildWebhookStatus(mainURL, true, events, mainEvents))
	} else {
		results = append(results, WebhookStatus{URL: mainURL, Exists: false, MissingEvents: mainEvents})
	}

	// Check extensions webhook if configured
	if extensionsConfig != nil {
		extEvents := extensionsConfig.API.Settings.RaiselyWebhookEvents
		if len(extEvents) > 0 {
			extURL := fmt.Sprintf("https://%s/api/raisely/extensions", webhookDomain)
			if events, ok := webhooksByURL[extURL]; ok {
				results = append(results, *s.buildWebhookStatus(extURL, true, events, extEvents))
			} else {
				results = append(results, WebhookStatus{URL: extURL, Exists: false, MissingEvents: extEvents})
			}
		}
	}

	return results, nil
}

// EnsureRaiselyWebhooks calls CheckRaiselyWebhooks, then creates any missing
// webhooks. Returns a status for each webhook and whether each was created.
// Does NOT require FetchCampaign.
func (s *Service) EnsureRaiselyWebhooks(webhookDomain string, extensionsConfig *Config, ctx context.Context) ([]WebhookStatus, []bool, error) {
	statuses, err := s.CheckRaiselyWebhooks(webhookDomain, extensionsConfig, ctx)
	if err != nil {
		return nil, nil, err
	}

	created := make([]bool, len(statuses))
	for i, status := range statuses {
		if status.Exists {
			continue
		}

		// Determine which events to use for creation
		var requiredEvents []string
		if i == 0 {
			requiredEvents = s.sc.Config.API.Settings.RaiselyWebhookEvents
		} else if extensionsConfig != nil {
			requiredEvents = extensionsConfig.API.Settings.RaiselyWebhookEvents
		}

		_, err := s.fetcher.CreateWebhook(status.URL, s.sc.Campaign, requiredEvents, ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create webhook at %s: %w", status.URL, err)
		}

		statuses[i] = WebhookStatus{
			URL:           status.URL,
			Exists:        true,
			PresentEvents: requiredEvents,
		}
		created[i] = true
	}

	return statuses, created, nil
}

// --- Accessors for advanced use cases ---

// Fetcher returns the underlying RaiselyFetcherAndUpdater.
// Use for operations not covered by the Service API (e.g., reconciliation).
func (s *Service) Fetcher() *RaiselyFetcherAndUpdater {
	return s.fetcher
}

// Campaign returns the fetched FundraisingCampaign, or nil if FetchCampaign
// has not been called.
func (s *Service) Campaign() *FundraisingCampaign {
	return s.campaign
}

// Mapper returns the OrttoMapper, or nil if FetchCampaign has not been called.
// Use for operations not covered by the Service API (e.g., reconciliation with
// type assertion to *OrttoContactsMapper).
func (s *Service) Mapper() OrttoMapper {
	return s.mapper
}

// Target returns the config target (e.g., "", "ortto-contacts", "ortto-activities").
func (s *Service) Target() string {
	return s.sc.Config.Target
}

// HasMissingFields checks the field status map for any missing ("❌") fields.
func HasMissingFields(fields map[string]string) bool {
	for _, status := range fields {
		if strings.Contains(status, "❌") {
			return true
		}
	}
	return false
}
