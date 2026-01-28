package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
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
// 3. The fields "str:cm:address" and "str:cm:address2" are also considered person fields.
func (o OrttoActivitiesMapper) IsPersonField(fieldID string) bool {
	// Builtin fields (e.g., str::email) are always person fields
	if strings.Contains(fieldID, "::") {
		return true
	}
	// The configured merge field is also a person field
	if fieldID == o.Config.API.Settings.OrttoFundraiserMergeField {
		return true
	}
	// Address fields are also person fields
	if fieldID == "str:cm:address" || fieldID == "str:cm:address-2" {
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

// SeparateFieldsAndAttributesAndSortAttributes moves person fields from Attributes to Fields.
// This should be called after all field mapping and transforms are complete.
// Mappable writes all mapped data to activity.Attributes initially.
// This method moves person fields from Attributes to Fields.
// According to the Ortto Activities API:
// - fields: Person/contact field data (updates the contact record)
// - attributes: Activity-specific field data (stored only on the activity)
func (o OrttoActivitiesMapper) SeparateFieldsAndAttributesAndSortAttributes(activity *OrttoActivity) {

	// Move person fields from Attributes to Fields
	for fieldID, value := range activity.Attributes {
		if o.IsPersonField(fieldID) {
			activity.Fields[fieldID] = value
			delete(activity.Attributes, fieldID)
		}
	}

	// Try and add the cdp-fields attribute - used to log the person fields
	flattenedPersonFields := make(map[string]interface{})
	for k, v := range activity.Fields {
		if nestedMap, ok := v.(map[string]string); ok {
			for nk, nv := range nestedMap {
				flattenedPersonFields[fmt.Sprintf("%s.%s", k, nk)] = nv
			}
		} else {
			flattenedPersonFields[k] = v
		}
	}
	activity.Attributes["obj:cm:cdp-fields"] = flattenedPersonFields

}

func (o OrttoActivitiesMapper) OrttoAPIBuilder() *requests.Builder {
	result := requests.
		URL(o.Config.API.Endpoints.Ortto).
		Client(&http.Client{Timeout: HTTPRequestTimeout})
	if o.RecordRequests {
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/ortto-activities", o.Campaign)))
	}
	return result
}

// MapFundraisingPage maps a fundraising page to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (OrttoRequest, error) {

	var orttoRequest OrttoActivitiesRequest

	// Validate that we have the merge field and activity id configured
	if o.Config.API.Settings.OrttoFundraiserMergeField == "" {
		return orttoRequest, errors.New("ortto fundraiser merge field is required for ortto-activities target config (api.settings.orttoFundraiserMergeField)")
	}
	if o.Config.API.Settings.OrttoActivityId == "" {
		return orttoRequest, errors.New("ortto activity id is required for ortto-activities target config (api.ids.orttoActivityId)")
	}

	orttoRequest = OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{o.Config.API.Settings.OrttoFundraiserMergeField, "str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	data, err := o.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return orttoRequest, err
	}

	// Build the activity with person fields
	activity := OrttoActivity{
		ActivityID: o.Config.API.Settings.OrttoActivityId,
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
	o.SeparateFieldsAndAttributesAndSortAttributes(&activity)

	orttoRequest.Activities = append(orttoRequest.Activities, activity)

	return orttoRequest, nil
}

// MapTeamFundraisingPage maps team members' fundraising pages to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) (OrttoRequest, error) {

	var result OrttoActivitiesRequest

	// Validate that we have the merge field and activity id configured
	if o.Config.API.Settings.OrttoFundraiserMergeField == "" {
		return result, errors.New("ortto fundraiser merge field is required for ortto-activities target config (api.settings.orttoFundraiserMergeField)")
	}
	if o.Config.API.Settings.OrttoActivityId == "" {
		return result, errors.New("ortto activity id is required for ortto-activities target config (api.ids.orttoActivityId)")
	}

	result = OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{o.Config.API.Settings.OrttoFundraiserMergeField, "str::email"},
		MergeStrategy: 2, // Overwrite existing
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
			ActivityID: o.Config.API.Settings.OrttoActivityId,
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
		o.SeparateFieldsAndAttributesAndSortAttributes(&activity)

		result.Activities = append(result.Activities, activity)
	}

	return result, nil
}

// MapTrackingData maps tracking form data to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTrackingData(data map[string]string, ctx context.Context) (OrttoRequest, error) {

	var result OrttoActivitiesRequest

	// Validate that we have the merge field and activity id configured
	if o.Config.API.Settings.OrttoFundraiserMergeField == "" {
		return result, errors.New("ortto fundraiser merge field is required for ortto-activities target config (api.settings.orttoFundraiserMergeField)")
	}
	if o.Config.API.Settings.OrttoActivityId == "" {
		return result, errors.New("ortto activity id is required for ortto-activities target config (api.ids.orttoActivityId)")
	}

	result = OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 2, // Overwrite existing
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
		ActivityID: o.Config.API.Settings.OrttoActivityId,
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
	o.SeparateFieldsAndAttributesAndSortAttributes(&activity)

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
		`, o.Config.API.Settings.OrttoFundraiserMergeField, o.Config.API.Settings.OrttoFundraiserMergeField, email))).
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
// The activity name is read from Config.API.Settings.OrttoActivityName.
func (o OrttoActivitiesMapper) BuildActivityDefinitionRequest(trackingconfig Config) (ActivityDefinitionRequest, error) {

	var request ActivityDefinitionRequest

	// Validate that we have the merge field and activity name configured
	if o.Config.API.Settings.OrttoFundraiserMergeField == "" {
		return request, errors.New("ortto fundraiser merge field is required for ortto-activities target config (api.settings.orttoFundraiserMergeField)")
	}
	if o.Config.API.Settings.OrttoActivityName == "" {
		return request, errors.New("ortto activity name is required for ortto-activities target config (api.settings.orttoActivityName)")
	}

	name := o.Config.API.Settings.OrttoActivityName
	request = ActivityDefinitionRequest{
		Name:                 o.Config.API.Settings.OrttoActivityName,
		IconID:               "reload-illustration-icon",
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

	// Add sync-context
	request.Attributes = append(request.Attributes, ActivityDefinitionAttribute{
		Name:        "sync-context",
		DisplayType: "object",
		FieldID:     "do-not-map",
	})

	// NOTE: We do not add attribute for person fields like First name, Last name and Email.
	// Person fields can be sent with the payload and you do not need to add attributes for these.
	// You can pass these into the create activity call along with the person to either automatically
	// create people to go with the activity, or update existing people with the new data.
	// See https://help.ortto.com/a-233-custom-activities-guide

	// We add cdp-fields object to log the person fields instead
	request.Attributes = append(request.Attributes, ActivityDefinitionAttribute{
		Name:        "cdp-fields",
		DisplayType: "object",
		FieldID:     "do-not-map",
	})

	// Extract attributes from custom fundraiser field mappings
	o.extractFieldMappings(&request.Attributes, o.Config.FundraiserFieldMappings.Custom, personFieldIDs)

	// Extract attributes from custom team field mappings
	o.extractFieldMappings(&request.Attributes, o.Config.TeamFieldMappings.Custom, personFieldIDs)

	// Merge in any extra custom fields from trackingconfig (not already included)
	trackingAttributes := []ActivityDefinitionAttribute{}
	o.extractFieldMappings(&trackingAttributes, trackingconfig.FundraiserFieldMappings.Custom, personFieldIDs)
	o.extractFieldMappings(&trackingAttributes, trackingconfig.TeamFieldMappings.Custom, personFieldIDs)
	for _, attr := range trackingAttributes {
		found := false // Check if field is already in attributes
		for _, existingAttr := range request.Attributes {
			if existingAttr.Name == attr.Name {
				found = true
				break
			}
		}
		if !found {
			request.Attributes = append(request.Attributes, attr)
		}
	}

	// Remove  any custom person fields from activity attributes
	// as these should only be mapped to the person/contact record
	// and not stored as activity attributes - see note above
	for i := len(request.Attributes) - 1; i >= 0; i-- {
		if o.IsPersonField(request.Attributes[i].FieldID) {
			// Remove the attribute
			request.Attributes = append(request.Attributes[:i], request.Attributes[i+1:]...)
		}

	}

	// Sort attributes alphabetically by name, with sync-context first and cdp-fields last
	sort.Slice(request.Attributes, func(i, j int) bool {
		ni := request.Attributes[i].Name
		nj := request.Attributes[j].Name
		if ni == "sync-context" {
			return true
		} else if nj == "sync-context" {
			return false
		}
		if ni == "cdp-fields" {
			return false
		} else if nj == "cdp-fields" {
			return true
		}
		return ni < nj
	})

	return request, nil
}

// extractFieldMappings extracts attributes from FieldMappings
func (o OrttoActivitiesMapper) extractFieldMappings(attributes *[]ActivityDefinitionAttribute, mappings FieldMappings, personFieldIDs []string) {

	// Strings -> text (also map email and links)
	for fieldID := range mappings.Strings {
		stringDisplayType := "text"
		if fieldID == "str::email" {
			stringDisplayType = "email"
		}
		if strings.HasSuffix(fieldID, "-url") {
			stringDisplayType = "link"
		}
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: stringDisplayType,
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
			DisplayType: "bool",
			FieldID:     o.resolveFieldID(fieldID, personFieldIDs),
		})
	}

	// Timestamps -> time
	for fieldID := range mappings.Timestamps {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "time",
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

	// Geos -> text
	for fieldID := range mappings.Geos {
		*attributes = append(*attributes, ActivityDefinitionAttribute{
			Name:        o.extractFieldName(fieldID),
			DisplayType: "text",
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
func (o *OrttoActivitiesMapper) CreateActivityDefinition(ctx context.Context, trackingconfig Config) (ActivityDefinitionResponse, error) {

	var response ActivityDefinitionResponse

	request, err := o.BuildActivityDefinitionRequest(trackingconfig)
	if err != nil {
		return response, err
	}

	err = o.OrttoAPIBuilder().
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
	result[o.Config.API.Settings.OrttoFundraiserMergeField] = statusprocessing
	orttoTypes[o.Config.API.Settings.OrttoFundraiserMergeField] = "Text" // TODO determine type from field

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
