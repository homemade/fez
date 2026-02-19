package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// OrttoAttributes is a map[string]interface{} that marshals to JSON with keys
// sorted alphabetically by the name portion of the key (the last segment after ':')
// with some special handling for obj:cm:sync-context to ensure it appears first
// and obj:cdp-fields to ensure it appears last.
type OrttoAttributes map[string]interface{}

// extractName returns the name portion of an Ortto field key.
// e.g., "str:cm:shirt-size" -> "shirt-size"
func extractName(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) >= 1 {
		return parts[len(parts)-1]
	}
	return key
}

func (a OrttoAttributes) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		// if key is "obj:cm:sync-context", make it sort first
		if keys[i] == "obj:cm:sync-context" {
			return true
		} else if keys[j] == "obj:cm:sync-context" {
			return false
		}
		// if key is "obj:cdp-fields", make it sort last
		if keys[i] == "obj:cdp-fields" {
			return false
		} else if keys[j] == "obj:cdp-fields" {
			return true
		}
		return extractName(keys[i]) < extractName(keys[j])
	})

	buf := []byte{'{'}
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		keyJSON, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		valJSON, err := json.Marshal(a[k])
		if err != nil {
			return nil, err
		}
		buf = append(buf, keyJSON...)
		buf = append(buf, ':')
		buf = append(buf, valJSON...)
	}
	buf = append(buf, '}')
	return buf, nil
}

type OrttoSyncContext struct {
	Source           string
	TriggerType      string
	TriggerSubType   string
	TriggerId        string
	TriggerCreatedAt string
	CampaignId       string
	CampaignName     string
}

func (c OrttoSyncContext) AsOrttoActivitiesAttributes() OrttoAttributes {
	// Try and convert timestamps to RFC1123 (Mon, 02 Jan 2006 15:04:05 MST)
	// as they are displayed as text when sent as an object
	triggerCreatedAtFormatted := c.TriggerCreatedAt
	if t, err := time.Parse(time.RFC3339, c.TriggerCreatedAt); err == nil {
		triggerCreatedAtFormatted = t.Format(time.RFC1123)
	} else if c.TriggerCreatedAt != "" {
		log.Printf("Warning: failed to parse TriggerCreatedAt %q as RFC3339: %v (using original value)", c.TriggerCreatedAt, err)
	}
	attributes := struct {
		Source           string `json:"Source"`
		TriggerType      string `json:"Trigger-type"`
		TriggerSubType   string `json:"Trigger-subtype,omitempty"`
		TriggerId        string `json:"Trigger-id,omitempty"`
		TriggerCreatedAt string `json:"Trigger-created-at"`
		CampaignId       string `json:"Campaign-id"`
		CampaignName     string `json:"Campaign-name"`
	}{
		Source:           c.Source,
		TriggerType:      c.TriggerType,
		TriggerSubType:   c.TriggerSubType,
		TriggerId:        c.TriggerId,
		TriggerCreatedAt: triggerCreatedAtFormatted,
		CampaignId:       c.CampaignId,
		CampaignName:     c.CampaignName,
	}
	objectWrapper := make(OrttoAttributes)
	objectWrapper["obj:cm:sync-context"] = attributes
	return objectWrapper
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
	MapTrackingData(campaign *FundraisingCampaign, data map[string]string, ctx context.Context) (OrttoRequest, error)
	SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error)
}

// NewOrttoMapper creates a OrttoMapper based on the target specified in the config.
// If target is empty or "ortto-contacts", it returns an OrttoContactsMapper.
// If target is "ortto-activities", it returns an OrttoActivitiesMapper.
func NewOrttoMapper(config Config, orttoctx OrttoSyncContext, recordRequests bool) OrttoMapper {
	mustBeInitialised()

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
	Attributes OrttoAttributes        `json:"attributes,omitempty"`
	Fields     map[string]interface{} `json:"fields"`
	Location   map[string]interface{} `json:"location,omitempty"`
	PersonID   string                 `json:"person_id,omitempty"`
	Created    string                 `json:"created,omitempty"`
	Key        string                 `json:"key,omitempty"`
}

func (a OrttoActivity) TakeSnapshot(field string) {
	// Snapshot field is an object containing all activity attributes
	// excluding the special "obj:cm:sync-context" and "obj:cm:cdp-fields"
	removeOrttoMetaPrefix := func(s string) string {
		_, afterFirst, foundFirst := strings.Cut(s, ":")
		if foundFirst {
			_, afterSecond, foundSecond := strings.Cut(afterFirst, ":")
			if foundSecond {
				return afterSecond
			}
			return afterFirst
		}
		return s
	}
	snapshot := make(map[string]interface{})
	for k, v := range a.Attributes {
		if k == "obj:cm:sync-context" || k == "obj:cm:cdp-fields" {
			continue
		}
		// remove nil fields
		if v == nil {
			continue
		}
		snapshot[removeOrttoMetaPrefix(k)] = v
	}
	a.Fields[field] = snapshot
}

// GetFields returns the activity's attributes map.
// For activities, Mappable operates on Attributes (activity-specific data).
// Person fields are later moved to Fields by SeparateFieldsAndAttributesAndSortAttributes.
func (a *OrttoActivity) GetFields() map[string]interface{} { return a.Attributes }

// SetField sets a field on the activity's attributes.
func (a *OrttoActivity) SetField(key string, value interface{}) { a.Attributes[key] = value }

// DeleteField deletes a field from the activity's attributes.
func (a *OrttoActivity) DeleteField(key string) { delete(a.Attributes, key) }

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
