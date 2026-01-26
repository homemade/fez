package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/carlmjohnson/requests"
	"github.com/iancoleman/strcase"
	"github.com/tidwall/gjson"
)

// OrttoActivitiesMapper maps Raisely data to Ortto Activities API format.
// This implements the TargetMapper interface for the ortto-activities target.
type OrttoActivitiesMapper struct {
	RaiselyMapper
	OrttoSyncContext
}

// IsPersonField returns true if the field should be mapped to the person/contact record.
// Person fields are:
// 1. All builtin fields (contain "::" pattern, e.g., "str::email")
// 2. The configured OrttoFundraiserMergeField (used to merge contacts in Ortto)
// All other fields are activity-only attributes.
func (o OrttoActivitiesMapper) IsPersonField(fieldID string) bool {
	// Builtin fields (e.g., str::email) are always person fields
	if strings.Contains(fieldID, "::") {
		return true
	}
	// The configured merge field is also a person field
	if fieldID == o.Config.API.Ids.OrttoFundraiserMergeField {
		return true
	}
	return false
}

// PersonFieldIDs returns the list of field IDs that should be mapped to the person/contact record.
func (o OrttoActivitiesMapper) PersonFieldIDs() []string {
	var result []string

	// Collect from FundraiserFieldMappings.Builtin
	for _, fieldID := range o.Config.FundraiserFieldMappings.Builtin.AllKeys() {
		if o.IsPersonField(fieldID) {
			result = append(result, fieldID)
		}
	}

	// Collect from FundraiserFieldMappings.Custom
	for _, fieldID := range o.Config.FundraiserFieldMappings.Custom.AllKeys() {
		if o.IsPersonField(fieldID) {
			result = append(result, fieldID)
		}
	}

	// Collect from TeamFieldMappings.Custom
	for _, fieldID := range o.Config.TeamFieldMappings.Custom.AllKeys() {
		if o.IsPersonField(fieldID) {
			result = append(result, fieldID)
		}
	}

	return result
}

// SeparateFieldsAndAttributes moves non-person fields from Fields to Attributes.
// This should be called after all field mapping and transforms are complete.
// According to the Ortto Activities API:
// - fields: Person/contact field data (updates the contact record)
// - attributes: Activity-specific field data (stored only on the activity)
func (o OrttoActivitiesMapper) SeparateFieldsAndAttributes(activity *OrttoActivity) {
	if activity.Attributes == nil {
		activity.Attributes = make(map[string]interface{})
	}

	// Move non-person fields from Fields to Attributes
	for fieldID, value := range activity.Fields {
		if !o.IsPersonField(fieldID) {
			activity.Attributes[fieldID] = value
			delete(activity.Fields, fieldID)
		}
	}
}

func (o OrttoActivitiesMapper) OrttoAPIBuilder() *requests.Builder {
	result := requests.
		URL(o.Config.API.Endpoints.Ortto)
	if o.RecordRequests {
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/ortto-activities", o.Campaign)))
	}
	return result
}

// MapFundraisingPage maps a fundraising page to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (OrttoRequest, error) {

	orttoRequest := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.OrttoActivityId == "" {
		return orttoRequest, errors.New("ortto activity Id is required for ortto-activities target (api.ids.orttoActivityId)")
	}

	data, err := o.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return orttoRequest, err
	}

	// Build the activity with person fields
	activity := OrttoActivity{
		ActivityID: o.Config.API.Ids.OrttoActivityId,
		Fields:     make(map[string]interface{}),
		Attributes: o.OrttoSyncContext.AsOrttoActivitiesAttributes(),
	}

	o.MapFundraiserFields(data.Page.Source, &activity)
	if err = o.ApplyFundraiserTransforms(&activity, campaign, ctx, false); err != nil {
		return orttoRequest, err
	}
	// To support people leaving teams we also need to set any team field mappings to empty
	o.ClearTeamFields(&activity)

	// Separate person fields (Fields) from activity attributes (Attributes)
	o.SeparateFieldsAndAttributes(&activity)

	orttoRequest.Activities = append(orttoRequest.Activities, activity)

	return orttoRequest, nil
}

// MapTeamFundraisingPage maps team members' fundraising pages to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) (OrttoRequest, error) {
	result := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.OrttoActivityId == "" {
		return result, errors.New("ortto activity Id is required for ortto-activities target (api.ids.orttoActivityId)")
	}

	team, teamPage, err := o.FetchTeam(p2pteamid, ctx)
	if err != nil {
		return result, err
	}

	var memberPages []FundraisingPage
	memberPages, err = o.FetchTeamMembers(team, ctx)
	if err != nil {
		return result, err
	}

	for _, page := range memberPages {
		activity := OrttoActivity{
			ActivityID: o.Config.API.Ids.OrttoActivityId,
			Fields:     make(map[string]interface{}),
			Attributes: o.OrttoSyncContext.AsOrttoActivitiesAttributes(),
		}

		o.MapFundraiserFields(page.Source, &activity)
		o.MapTeamFields(teamPage.Source, &activity)
		if err := o.ApplyFundraiserTransforms(&activity, campaign, ctx, false); err != nil {
			return result, err
		}
		if err := o.ApplyTeamTransforms(page, teamPage, &activity); err != nil {
			return result, err
		}

		// Separate person fields (Fields) from activity attributes (Attributes)
		o.SeparateFieldsAndAttributes(&activity)

		result.Activities = append(result.Activities, activity)
	}

	return result, nil
}

// MapTrackingData maps tracking form data to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTrackingData(data map[string]string, ctx context.Context) (OrttoRequest, error) {

	result := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.OrttoActivityId == "" {
		return result, errors.New("ortto activity ID is required for ortto-activities target (api.ids.orttoActivityId)")
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return result, err
	}
	if !gjson.ValidBytes(jsonData) {
		log.Printf("Invalid Tracking Data:\n%s", string(jsonData))
		return result, errors.New("invalid tracking data")
	}

	source := Source{
		data: gjson.ParseBytes(jsonData),
	}

	activity := OrttoActivity{
		ActivityID: o.Config.API.Ids.OrttoActivityId,
		Fields:     make(map[string]interface{}),
		Attributes: o.OrttoSyncContext.AsOrttoActivitiesAttributes(),
	}

	o.MapFundraiserFields(source, &activity)

	email, emailExists := activity.Fields["str::email"].(string)
	if !emailExists || email == "" {
		return result, errors.New("missing required field in tracking data")
	}

	var existingContacts []OrttoContact
	existingContacts, err = o.SearchForFundraiserByEmail(email, ctx)
	if err != nil {
		return result, err
	}

	if len(existingContacts) > 0 {
		log.Println("Found existing fundraising page for this tracking data in ortto")
		return result, nil
	}

	// Separate person fields (Fields) from activity attributes (Attributes)
	o.SeparateFieldsAndAttributes(&activity)

	// For activities, we always create the activity (unlike contacts where we check for existing)
	result.Activities = append(result.Activities, activity)

	return result, nil
}

// SearchForFundraiserByEmail searches Ortto for a contact by email that has the merge field set.
func (o *OrttoActivitiesMapper) SearchForFundraiserByEmail(email string, ctx context.Context) ([]OrttoContact, error) {

	response := struct {
		Contacts []OrttoContact `json:"contacts"`
		Error    OrttoError
	}{}

	err := o.OrttoAPIBuilder().
		Path("/v1/person/get").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		Post().
		BodyBytes([]byte(fmt.Sprintf(`
		{
			"limit": 1,
			"offset": 0,
			"fields": ["%s", "str::email"],
			"filter": {
				"$and": [
				{
					"$has_any_value": {
					"field_id": "%s"
					}
				},
				{
					"$str::is": {
					"field_id": "str::email",
					"value": "%s"
					}
				}
				]
			}
		}
		`, o.Config.API.Ids.OrttoFundraiserMergeField, o.Config.API.Ids.OrttoFundraiserMergeField, email))).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)
	if err != nil {
		return nil, err
	}

	return response.Contacts, nil

}

// SendRequest sends an Ortto activities request to the Ortto API.
func (o *OrttoActivitiesMapper) SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error) {
	activitiesReq, ok := req.(OrttoActivitiesRequest)
	if !ok {
		return nil, fmt.Errorf("expected OrttoActivitiesRequest, got %T", req)
	}

	var result OrttoActivitiesResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/activities/create").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&activitiesReq).
		ToJSON(&result).
		ErrorJSON(&result.Error).
		Fetch(ctx)

	return result, err
}

// ActivityDefinitionRequest represents the request to create an Ortto activity definition
type ActivityDefinitionRequest struct {
	Name                 string                        `json:"name"`
	IconID               string                        `json:"icon_id"`
	TrackConversionValue bool                          `json:"track_conversion_value"`
	Touch                bool                          `json:"touch"`
	Filterable           bool                          `json:"filterable"`
	VisibleInFeeds       bool                          `json:"visible_in_feeds"`
	DisplayStyle         ActivityDefinitionStyle       `json:"display_style"`
	Attributes           []ActivityDefinitionAttribute `json:"attributes"`
}

// ActivityDefinitionStyle configures how the activity is displayed
type ActivityDefinitionStyle struct {
	Type  string `json:"type"`
	Title string `json:"title"`
}

// ActivityDefinitionAttribute represents a custom field attached to the activity
type ActivityDefinitionAttribute struct {
	Name        string `json:"name"`
	DisplayType string `json:"display_type"`
	FieldID     string `json:"field_id"`
}

// ActivityDefinitionResponse represents the response from Ortto
type ActivityDefinitionResponse struct {
	CustomActivity struct {
		ActivityFieldID string `json:"activity_field_id"`
		State           string `json:"state"`
		Name            string `json:"name"`
		CreatedAt       string `json:"created_at"`
	} `json:"custom_activity"`
	Error *struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

// BuildActivityDefinitionRequest creates an activity definition request from the config field mappings.
func (o OrttoActivitiesMapper) BuildActivityDefinitionRequest(name string) ActivityDefinitionRequest {
	request := ActivityDefinitionRequest{
		Name:                 name,
		IconID:               "user-add",
		TrackConversionValue: false,
		Touch:                true,
		Filterable:           true,
		VisibleInFeeds:       true,
		DisplayStyle: ActivityDefinitionStyle{
			Type:  "activity",
			Title: name,
		},
		Attributes: []ActivityDefinitionAttribute{},
	}

	personFieldIDs := o.PersonFieldIDs()

	// Add context attributes
	for fieldID := range o.OrttoSyncContext.AsOrttoActivitiesAttributes() {
		displayType := "text"
		if strings.HasPrefix(fieldID, "tme:") {
			displayType = "date"
		}
		request.Attributes = append(request.Attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: displayType,
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Extract attributes from fundraiser field mappings (builtin)
	o.extractFieldMappings(&request.Attributes, o.Config.FundraiserFieldMappings.Builtin, personFieldIDs)

	// Extract attributes from fundraiser field mappings (custom)
	o.extractFieldMappings(&request.Attributes, o.Config.FundraiserFieldMappings.Custom, personFieldIDs)

	// Extract attributes from team field mappings (custom)
	o.extractFieldMappings(&request.Attributes, o.Config.TeamFieldMappings.Custom, personFieldIDs)

	return request
}

// extractFieldMappings extracts attributes from FieldMappings
func (o OrttoActivitiesMapper) extractFieldMappings(attributes *[]ActivityDefinitionAttribute, mappings FieldMappings, personFieldIDs []string) {

	// Strings -> text
	for fieldID := range mappings.Strings {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "text",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Texts -> large_text
	for fieldID := range mappings.Texts {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "large_text",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Decimals -> decimal
	for fieldID := range mappings.Decimals {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "decimal",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Integers -> integer
	for fieldID := range mappings.Integers {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "integer",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Booleans -> boolean
	for fieldID := range mappings.Booleans {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "boolean",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Timestamps -> date
	for fieldID := range mappings.Timestamps {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "date",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Phones -> phone
	for fieldID := range mappings.Phones {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "phone",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Geos -> geo
	for fieldID := range mappings.Geos {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "geo",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}
}

// resolveFieldID returns the field_id for the activity attribute.
// If the fieldID is in personFieldIDs, it maps to the person record.
// Otherwise it uses "do-not-map" (activity-only attribute).
func (o OrttoActivitiesMapper) resolveFieldID(fieldID string, personFieldIDs []string) string {
	for _, personFieldID := range personFieldIDs {
		if fieldID == personFieldID {
			return fieldID
		}
	}
	return "do-not-map"
}

// extractFieldName extracts the field name from an Ortto field ID
// e.g., "str:cm:campaign-field-name" -> "campaign-field-name"
// e.g., "str::email" -> "email"
func (o OrttoActivitiesMapper) extractFieldName(fieldID string) string {
	parts := strings.Split(fieldID, ":")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return fieldID
}

// CreateActivityDefinition creates an activity definition in Ortto.
func (o *OrttoActivitiesMapper) CreateActivityDefinition(ctx context.Context, name string) (ActivityDefinitionResponse, error) {
	request := o.BuildActivityDefinitionRequest(name)

	var response ActivityDefinitionResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/definitions/activity/create").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&request).
		ToJSON(&response).
		Fetch(ctx)

	if err != nil {
		return response, fmt.Errorf("failed to create activity definition: %w", err)
	}

	return response, nil
}

// CheckOrttoCustomFields checks that the Ortto custom fields are set up correctly for activities.
func (o *OrttoActivitiesMapper) CheckOrttoCustomFields(statusprocessing string, statusok string, statusmissing string, ctx context.Context) (map[string]string, error) {
	result := make(map[string]string)
	orttoTypes := make(map[string]string)
	result[o.Config.API.Ids.OrttoFundraiserMergeField] = statusprocessing
	orttoTypes[o.Config.API.Ids.OrttoFundraiserMergeField] = "Text" // TODO determine type from field

	response := struct {
		Fields []struct {
			Field struct {
				Id          string `json:"id"`
				DisplayType string `json:"display_type"`
			} `json:"field"`
		} `json:"fields"`
		Error OrttoError
	}{}
	err := o.OrttoAPIBuilder().
		Path("/v1/person/custom-field/get").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyBytes(nil).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)
	if err != nil {
		return result, err
	}

	for _, v := range response.Fields {
		if _, exists := result[v.Field.Id]; exists {
			// check Ortto type is correct for decimals and integers
			if strings.HasPrefix(v.Field.Id, "int:") {
				if v.Field.DisplayType == "decimal" {
					_, decimalMappingExists := o.Config.FundraiserFieldMappings.Custom.Decimals[v.Field.Id]
					if !decimalMappingExists {
						_, decimalMappingExists = o.Config.TeamFieldMappings.Custom.Decimals[v.Field.Id]
					}
					if decimalMappingExists {
						result[v.Field.Id] = statusok
					}
				}
				if v.Field.DisplayType == "integer" {
					_, integerMappingExists := o.Config.FundraiserFieldMappings.Custom.Integers[v.Field.Id]
					if !integerMappingExists {
						_, integerMappingExists = o.Config.TeamFieldMappings.Custom.Integers[v.Field.Id]
					}
					if integerMappingExists {
						result[v.Field.Id] = statusok
					}
				}
			} else {
				result[v.Field.Id] = statusok
			}
		}
	}

	for k, v := range result {
		if v != statusok {
			// generate ortto label
			orttoLabel := ""
			keyParts := strings.Split(k, ":")
			if len(keyParts) == 3 {
				// the field name is in the last part of the key
				fieldNameParts := strings.Split(keyParts[2], "-")
				for i, s := range fieldNameParts {
					if i == 0 { // first part of the field name is the prefix, which is upper cased in the label
						orttoLabel = strings.ToUpper(s)
					} else { // the other parts are converted to camel case for the label
						orttoLabel = orttoLabel + " " + strcase.ToCamel(s)
					}
				}
			}
			result[k] = fmt.Sprintf(`%s %s (%s)`, statusmissing, orttoLabel, orttoTypes[k])
		}
	}

	return result, err
}
