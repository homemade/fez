package sync

import (
	"context"
	"fmt"
)

type OrttoSyncContext struct {
	Source           string
	TriggerType      string
	TriggerSubType   string
	TriggerId        string
	TriggerCreatedAt string
	CampaignId       string
	CampaignName     string
	CampaignPrefix   string
}

func (c OrttoSyncContext) AsOrttoActivitiesAttributes() map[string]interface{} {
	attributes := make(map[string]interface{})
	attributes["str:cm:source"] = c.Source
	attributes["str:cm:trigger-type"] = c.TriggerType
	attributes["str:cm:trigger-subtype"] = c.TriggerSubType
	attributes["str:cm:trigger-id"] = c.TriggerId
	attributes["tme:cm:trigger-created-at"] = c.TriggerCreatedAt
	attributes["str:cm:campaign-id"] = c.CampaignId
	attributes["str:cm:campaign-name"] = c.CampaignName
	attributes["str:cm:campaign-prefix"] = c.CampaignPrefix
	return attributes
}

// OrttoRequest is the interface for all ortto-specific request types.
// Each target (e.g., ortto-contacts, ortto-activities) has its own request type
// that implements this interface.
type OrttoRequest interface {
	ItemCount() int                                    // Returns the number of items (contacts or activities)
	IsOrttoContactsRequest() bool                      // Returns true if this is a contacts request
	AsOrttoContactsRequest() *OrttoContactsRequest     // Returns self if contacts request, nil otherwise
	IsOrttoActivitiesRequest() bool                    // Returns true if this is an activities request
	AsOrttoActivitiesRequest() *OrttoActivitiesRequest // Returns self if activities request, nil otherwise
}

// OrttoResponse is the interface for all ortto-specific response types.
// Each target has its own response type that implements this interface.
type OrttoResponse interface {
	IsSuccess() bool
	GetError() error
}

// OrttoMapper is the interface for mapping Raisely data to ortto-specific formats.
// Implementations exist for each integration target (e.g., OrttoContactsMapper, OrttoActivitiesMapper).
type OrttoMapper interface {
	MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (OrttoRequest, error)
	MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) (OrttoRequest, error)
	MapTrackingData(data map[string]string, ctx context.Context) (OrttoRequest, error)
	SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error)
}

// NewOrttoMapper creates a OrttoMapper based on the target specified in the config.
// If target is empty or "ortto-contacts", it returns an OrttoContactsMapper.
// If target is "ortto-activities", it returns an OrttoActivitiesMapper.
func NewOrttoMapper(config Config, orttoctx OrttoSyncContext, recordRequests bool) OrttoMapper {
	mapper := RaiselyMapper{
		RaiselyFetcherAndUpdater: RaiselyFetcherAndUpdater{
			Campaign:       orttoctx.CampaignId,
			Config:         config,
			RecordRequests: recordRequests,
		},
	}
	switch config.Target {
	case "ortto-activities":
		return &OrttoActivitiesMapper{RaiselyMapper: mapper, OrttoSyncContext: orttoctx}
	default: // "", "ortto-contacts"
		return &OrttoContactsMapper{RaiselyMapper: mapper, OrttoSyncContext: orttoctx}
	}
}

// OrttoContact represents a single contact/person in the Ortto Contacts/CDP API.
type OrttoContact struct {
	Id     string                 `json:"id,omitempty"`
	Fields map[string]interface{} `json:"fields"`
}

// GetFields returns the contact's field map.
func (c *OrttoContact) GetFields() map[string]interface{} { return c.Fields }

// SetField sets a field on the contact.
func (c *OrttoContact) SetField(key string, value interface{}) { c.Fields[key] = value }

// DeleteField deletes a field from the contact.
func (c *OrttoContact) DeleteField(key string) { delete(c.Fields, key) }

type OrttoContactDiff struct {
	Id     string                           `json:"id"`
	Fields map[string]OrttoContactDiffField `json:"fields"`
}

type OrttoContactDiffField struct {
	Actual   interface{} `json:"actual"`
	Expected interface{} `json:"expected"`
}

// OrttoContactsRequest is the request type for the Ortto Contacts/CDP API.
type OrttoContactsRequest struct {
	Contacts      []OrttoContact `json:"people"`
	Async         bool           `json:"async"`
	MergeBy       []string       `json:"merge_by"`
	MergeStrategy uint8          `json:"merge_strategy"`
	FindStrategy  uint8          `json:"find_strategy"`
}

// ItemCount returns the number of contacts in the request.
func (r OrttoContactsRequest) ItemCount() int {
	return len(r.Contacts)
}

// IsOrttoContactsRequest returns true since this is a contacts request.
func (r OrttoContactsRequest) IsOrttoContactsRequest() bool {
	return true
}

// AsOrttoContactsRequest returns a pointer to this request.
func (r OrttoContactsRequest) AsOrttoContactsRequest() *OrttoContactsRequest {
	return &r
}

// IsOrttoActivitiesRequest returns false since this is not an activities request.
func (r OrttoContactsRequest) IsOrttoActivitiesRequest() bool {
	return false
}

// AsOrttoActivitiesRequest returns nil since this is not an activities request.
func (r OrttoContactsRequest) AsOrttoActivitiesRequest() *OrttoActivitiesRequest {
	return nil
}

// OrttoActivitiesRequest is the request type for the Ortto Activities API.
type OrttoActivitiesRequest struct {
	Activities    []OrttoActivity `json:"activities"`
	Async         bool            `json:"async"`
	MergeBy       []string        `json:"merge_by"`
	MergeStrategy uint8           `json:"merge_strategy,omitempty"`
}

// OrttoActivity represents a single activity in the Ortto Activities API.
type OrttoActivity struct {
	ActivityID string                 `json:"activity_id"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Fields     map[string]interface{} `json:"fields"`
	Location   map[string]interface{} `json:"location,omitempty"`
	PersonID   string                 `json:"person_id,omitempty"`
	Created    string                 `json:"created,omitempty"`
	Key        string                 `json:"key,omitempty"`
}

// GetFields returns the activity's field map.
func (a *OrttoActivity) GetFields() map[string]interface{} { return a.Fields }

// SetField sets a field on the activity.
func (a *OrttoActivity) SetField(key string, value interface{}) { a.Fields[key] = value }

// DeleteField deletes a field from the activity.
func (a *OrttoActivity) DeleteField(key string) { delete(a.Fields, key) }

// ItemCount returns the number of activities in the request.
func (r OrttoActivitiesRequest) ItemCount() int {
	return len(r.Activities)
}

// IsOrttoContactsRequest returns false since this is not a contacts request.
func (r OrttoActivitiesRequest) IsOrttoContactsRequest() bool {
	return false
}

// AsOrttoContactsRequest returns nil since this is not a contacts request.
func (r OrttoActivitiesRequest) AsOrttoContactsRequest() *OrttoContactsRequest {
	return nil
}

// IsOrttoActivitiesRequest returns true since this is an activities request.
func (r OrttoActivitiesRequest) IsOrttoActivitiesRequest() bool {
	return true
}

// AsOrttoActivitiesRequest returns a pointer to this request.
func (r OrttoActivitiesRequest) AsOrttoActivitiesRequest() *OrttoActivitiesRequest {
	return &r
}

type OrttoError struct {
	RequestID string `json:"request_id"`
	Code      int    `json:"code"`
	Error     string `json:"error"`
}

type OrttoContactsResult struct {
	PersonId string `json:"person_id"`
	Status   string `json:"status"`
}

// OrttoContactsResponse is the response type for the Ortto Contacts API.
type OrttoContactsResponse struct {
	Results []OrttoContactsResult `json:"people"`
	Error   OrttoError
}

// IsSuccess returns true if the response has no error code and has at least one result.
func (r OrttoContactsResponse) IsSuccess() bool {
	return r.Error.Code == 0 && len(r.Results) > 0
}

// GetError returns an error if the response contains an error code.
func (r OrttoContactsResponse) GetError() error {
	if r.Error.Code != 0 {
		return fmt.Errorf("%d: %s", r.Error.Code, r.Error.Error)
	}
	return nil
}

// OrttoActivitiesResponse is the response type for the Ortto Activities API.
type OrttoActivitiesResponse struct {
	Created int `json:"created"`
	Error   OrttoError
}

// IsSuccess returns true if the response has no error code and at least one activity was created.
func (r OrttoActivitiesResponse) IsSuccess() bool {
	return r.Error.Code == 0 && r.Created > 0
}

// GetError returns an error if the response contains an error code.
func (r OrttoActivitiesResponse) GetError() error {
	if r.Error.Code != 0 {
		return fmt.Errorf("%d: %s", r.Error.Code, r.Error.Error)
	}
	return nil
}
