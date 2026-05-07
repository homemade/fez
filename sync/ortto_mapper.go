package sync

import (
	"context"
	"encoding/json"
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
	ModelType        string
	ModelID          string
	ParentType       string
	ParentID         string
	TriggerType      string
	TriggerSubType   string
	TriggerID        string
	TriggerCreatedAt string
	Campaign         string
	CampaignName     string
}

// NewOrttoSyncContext creates an OrttoSyncContext from a SyncContext.
func NewOrttoSyncContext(sc *SyncContext) OrttoSyncContext {
	return OrttoSyncContext{
		Source:           sc.Source,
		ModelType:        sc.ModelType,
		ModelID:          sc.ModelID,
		ParentType:       sc.ParentType,
		ParentID:         sc.ParentID,
		TriggerType:      sc.TriggerType,
		TriggerSubType:   sc.TriggerSubType,
		TriggerID:        sc.TriggerID,
		TriggerCreatedAt: sc.TriggerCreatedAt,
		Campaign:         sc.Campaign,
		CampaignName:     sc.CampaignName,
	}
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
		ModelType        string `json:"Model-type,omitempty"`
		ModelID          string `json:"Model-id,omitempty"`
		ParentType       string `json:"Parent-type,omitempty"`
		ParentID         string `json:"Parent-id,omitempty"`
		TriggerType      string `json:"Trigger-type"`
		TriggerSubType   string `json:"Trigger-subtype,omitempty"`
		TriggerID        string `json:"Trigger-id,omitempty"`
		TriggerCreatedAt string `json:"Trigger-created-at"`
		Campaign         string `json:"Campaign"`
		CampaignName     string `json:"Campaign-name"`
	}{
		Source:           c.Source,
		ModelType:        c.ModelType,
		ModelID:          c.ModelID,
		ParentType:       c.ParentType,
		ParentID:         c.ParentID,
		TriggerType:      c.TriggerType,
		TriggerSubType:   c.TriggerSubType,
		TriggerID:        c.TriggerID,
		TriggerCreatedAt: triggerCreatedAtFormatted,
		Campaign:         c.Campaign,
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
	ItemCount() int                                           // Returns the number of items (contacts or activities)
	AsOrttoContactsRequest() (OrttoContactsRequest, bool)     // Returns (request, true) if contacts request, (zero, false) otherwise
	AsOrttoActivitiesRequest() (OrttoActivitiesRequest, bool) // Returns (request, true) if activities request, (zero, false) otherwise
}

// MapResult pairs an Ortto request with an optional Raisely write-back.
// Service methods return []MapResult — one per logical mapping step
// (e.g. profile mapping, referrals mapping).
type MapResult struct {
	Request       OrttoRequest
	RaiselyUpdate *UpdateRaiselyDataRequest
}

// OrttoResponse is a marker interface for all ortto-specific response
// types. Each target has its own concrete response type
// (OrttoContactsResponse, OrttoActivitiesResponse). The interface
// carries no methods.
//
// Error handling for Ortto API calls runs through the non-nil error
// returned by SendRequest, which is populated from any non-2xx HTTP
// response by the underlying carlmjohnson/requests Fetch call. Callers
// should branch on that error and never on body-derived "is success"
// methods — earlier iterations of this interface declared
// IsSuccess()/GetError() methods that depended on a top-level `created`
// count field that the Ortto Activities API does not actually return,
// so they reported false negatives on every successful sync. They have
// been removed; do not reintroduce them.
type OrttoResponse interface{}

// OrttoMapper is the interface for mapping Raisely data to ortto-specific formats.
// Implementations exist for each integration target (e.g., OrttoContactsMapper, OrttoActivitiesMapper).
type OrttoMapper interface {
	MapFundraisingPage(campaign *FundraisingCampaign, data FundraiserData) (OrttoRequest, error)
	MapTeamFundraisingPage(campaign *FundraisingCampaign, data TeamData) (OrttoRequest, error)
	MapTrackingData(campaign *FundraisingCampaign, data map[string]string, ctx context.Context) (OrttoRequest, error)
	SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error)
}

// NewOrttoMapper creates an OrttoMapper based on the target specified in the SyncContext's config.
// If target is empty or "ortto-contacts", it returns an OrttoContactsMapper.
// If target is "ortto-activities", it returns an OrttoActivitiesMapper.
func NewOrttoMapper(sc *SyncContext) OrttoMapper {
	mustBeInitialised()

	raiselyFetcherAndUpdater := &RaiselyFetcherAndUpdater{SyncContext: sc}
	orttoFetcherAndUpdater := OrttoFetcherAndUpdater{SyncContext: sc}
	raiselyMapper := RaiselyMapper{SyncContext: sc, RaiselyFetcherAndUpdater: raiselyFetcherAndUpdater}

	switch sc.Config.Target {
	case "ortto-activities":
		return &OrttoActivitiesMapper{
			SyncContext:            sc,
			RaiselyMapper:          raiselyMapper,
			OrttoFetcherAndUpdater: orttoFetcherAndUpdater,
		}
	default: // "", "ortto-contacts"
		return &OrttoContactsMapper{
			SyncContext:            sc,
			RaiselyMapper:          raiselyMapper,
			OrttoFetcherAndUpdater: orttoFetcherAndUpdater,
		}
	}
}

// OrttoContact represents a single contact/person in the Ortto Contacts/CDP API.
type OrttoContact struct {
	ID     string                 `json:"id,omitempty"`
	Fields map[string]interface{} `json:"fields"`
}

// GetFields returns the contact's field map.
func (c *OrttoContact) GetFields() map[string]interface{} { return c.Fields }

// SetField sets a field on the contact.
func (c *OrttoContact) SetField(key string, value interface{}) { c.Fields[key] = value }

// DeleteField deletes a field from the contact.
func (c *OrttoContact) DeleteField(key string) { delete(c.Fields, key) }

type OrttoContactDiff struct {
	ID     string                           `json:"id"`
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

// AsOrttoContactsRequest returns this request and true.
func (r OrttoContactsRequest) AsOrttoContactsRequest() (OrttoContactsRequest, bool) {
	return r, true
}

// AsOrttoActivitiesRequest returns a zero value and false since this is not an activities request.
func (r OrttoContactsRequest) AsOrttoActivitiesRequest() (OrttoActivitiesRequest, bool) {
	return OrttoActivitiesRequest{}, false
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

func (a *OrttoActivity) TakeSnapshot(field string) {
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

// AsOrttoContactsRequest returns a zero value and false since this is not a contacts request.
func (r OrttoActivitiesRequest) AsOrttoContactsRequest() (OrttoContactsRequest, bool) {
	return OrttoContactsRequest{}, false
}

// AsOrttoActivitiesRequest returns this request and true.
func (r OrttoActivitiesRequest) AsOrttoActivitiesRequest() (OrttoActivitiesRequest, bool) {
	return r, true
}

// OrttoActivityFeedEntry represents a single activity in a contact's activity feed.
type OrttoActivityFeedEntry struct {
	ActivityID string                 `json:"field_id"`
	Created    string                 `json:"created_at"`
	Attributes map[string]interface{} `json:"attr"`
}

// OrttoActivityFeedResponse is the response from the activity feed API.
type OrttoActivityFeedResponse struct {
	Activities []OrttoActivityFeedEntry `json:"activities"`
	Meta       struct {
		HasMore         bool `json:"has_more"`
		TotalActivities int  `json:"total_activities"`
	} `json:"meta"`
	NextOffset int `json:"next_offset"`
	Error      OrttoError
}

type OrttoError struct {
	RequestID string `json:"request_id"`
	Code      int    `json:"code"`
	Error     string `json:"error"`
}

type OrttoContactsResult struct {
	PersonID string `json:"person_id"`
	Status   string `json:"status"`
}

// OrttoActivityIngestResult is one entry from the
// /v1/activities/create success body's `activities` array. Per Ortto
// docs (help.ortto.com/a-271), each entry reports the per-activity
// ingestion outcome: status is typically "ingested" on success;
// person_status is "created" / "updated" / etc. depending on whether
// the merge resolved to a new or existing contact.
type OrttoActivityIngestResult struct {
	PersonID     string `json:"person_id"`
	Status       string `json:"status"`
	PersonStatus string `json:"person_status"`
	ActivityID   string `json:"activity_id"`
}

// OrttoContactsResponse is the response type for the Ortto Contacts API
// (POST /v1/person/merge). On a 2xx response the body is decoded into
// Results; on a non-2xx response the body is decoded into Error by
// ErrorJSON. Callers should determine success by branching on the
// non-nil error returned by SendContactsMerge, not by inspecting body
// content directly — Code:0 with empty Error is also the zero value
// of OrttoError, so a successful 2xx body that lacks any "error" key
// is indistinguishable from a malformed/empty body without the HTTP
// signal.
type OrttoContactsResponse struct {
	Results []OrttoContactsResult `json:"people"`
	Error   OrttoError
}

// OrttoActivitiesResponse is the response type for the Ortto Activities
// API (POST /v1/activities/create). Per Ortto's documented response
// shape the success body is
// `{"activities": [{"person_id": "...", "status": "ingested",
// "person_status": "created", "activity_id": "..."}]}` — there is no
// top-level `created` count field. The `activities` array is decoded
// into Activities so that callers logging the response (typically via
// fmt.Sprintf("%+v", resp)) capture the real per-activity ingestion
// outcomes; without this field the log line collapses to the
// zero-value Error struct, which is misleading because it parses as
// "Code:0 / no error" regardless of whether anything was decoded.
//
// Error handling remains via the non-nil error returned by
// SendActivitiesCreate; do not branch on Activities/Error content for
// success/failure detection.
type OrttoActivitiesResponse struct {
	Activities []OrttoActivityIngestResult `json:"activities"`
	Error      OrttoError
}
