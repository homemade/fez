package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	gosync "sync"

	"github.com/carlmjohnson/requests"
	"github.com/iancoleman/strcase"
)

const ActivityFeedConcurrencyLimit = 5

// OrttoFetcherAndUpdater handles all Ortto API operations.
// It embeds *SyncContext for shared sync configuration.
type OrttoFetcherAndUpdater struct {
	*SyncContext
}

// OrttoAPIBuilder returns a new requests.Builder configured for the Ortto API.
// The recording path uses Config.Target to distinguish between targets.
func (o OrttoFetcherAndUpdater) OrttoAPIBuilder() *requests.Builder {
	result := requests.
		URL(o.Config.API.Endpoints.Ortto).
		Client(&http.Client{Timeout: HTTPRequestTimeout})
	if o.RecordRequests {
		target := o.Config.Target
		if target == "" {
			target = "ortto-contacts"
		}
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/%s", o.Campaign, target)))
	}
	return result
}

// SendContactsMerge sends a contacts merge request to the Ortto API.
func (o OrttoFetcherAndUpdater) SendContactsMerge(req OrttoContactsRequest, ctx context.Context) (OrttoContactsResponse, error) {
	var result OrttoContactsResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/person/merge").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&req).
		ToJSON(&result).
		ErrorJSON(&result.Error).
		Fetch(ctx)

	return result, err
}

// SendActivitiesCreate sends an activities create request to the Ortto API.
func (o OrttoFetcherAndUpdater) SendActivitiesCreate(req OrttoActivitiesRequest, ctx context.Context) (OrttoActivitiesResponse, error) {
	var result OrttoActivitiesResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/activities/create").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&req).
		ToJSON(&result).
		ErrorJSON(&result.Error).
		Fetch(ctx)

	return result, err
}

// SearchForContactByEmail searches Ortto for a contact by email that has the specified merge field set.
func (o OrttoFetcherAndUpdater) SearchForContactByEmail(email string, mergeFieldID string, ctx context.Context) ([]OrttoContact, error) {
	// json.Marshal to safely escape email for interpolation into JSON body
	emailJSON, err := json.Marshal(email)
	if err != nil {
		return nil, err
	}

	response := struct {
		Contacts []OrttoContact `json:"contacts"`
		Error    OrttoError
	}{}

	err = o.OrttoAPIBuilder().
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
					"value": %s
					}
				}
				]
			}
		}
		`, mergeFieldID, mergeFieldID, emailJSON))).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)
	if err != nil {
		return nil, err
	}

	return response.Contacts, nil
}

// GetContact fetches a contact from Ortto by field values and filter.
func (o OrttoFetcherAndUpdater) GetContact(fields []byte, filterJSON string, ctx context.Context) ([]OrttoContact, error) {
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
			"fields": %s,
			"filter": %s
		}
		`, fields, filterJSON))).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)

	if err != nil {
		return nil, err
	}

	return response.Contacts, nil
}

// CreateActivityDefinition sends a request to create an activity definition in Ortto.
func (o OrttoFetcherAndUpdater) CreateActivityDefinition(req ActivityDefinitionRequest, ctx context.Context) (ActivityDefinitionResponse, error) {
	var response ActivityDefinitionResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/definitions/activity/create").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&req).
		ToJSON(&response).
		Fetch(ctx)

	if err != nil {
		return response, fmt.Errorf("failed to create activity definition: %w", err)
	}

	return response, nil
}

// ListCustomPersonFields returns the field IDs of all custom person fields in Ortto.
func (o OrttoFetcherAndUpdater) ListCustomPersonFields(ctx context.Context) ([]string, error) {
	response := struct {
		Fields []struct {
			Field struct {
				ID string `json:"id"`
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
		return nil, fmt.Errorf("failed to list custom person fields: %w", err)
	}

	var fieldIDs []string
	for _, v := range response.Fields {
		fieldIDs = append(fieldIDs, v.Field.ID)
	}
	return fieldIDs, nil
}

// CreateCustomPersonField creates a custom person field in Ortto.
func (o OrttoFetcherAndUpdater) CreateCustomPersonField(name, fieldType string, ctx context.Context) error {
	req := struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}{
		Name: name,
		Type: fieldType,
	}

	response := struct {
		Error OrttoError
	}{}

	err := o.OrttoAPIBuilder().
		Path("/v1/person/custom-field/create").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&req).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create custom person field %q: %w", name, err)
	}

	return nil
}

// GetActivityFeedForContact retrieves the activity feed for a contact from the Ortto API.
// It filters by the specified activity ID and returns the activities.
// The contact is identified by the provided field ID and value.
// Note: contactFieldValue is a string because fields used for contact
// identification have always been string-type fields (str:cm:... or str::email).
func (o OrttoFetcherAndUpdater) GetActivityFeedForContact(
	contactFieldID string,
	contactFieldValue string,
	activityID string,
	ctx context.Context,
) ([]OrttoActivityFeedEntry, error) {
	// Step 1: Look up the contact to get their person_id
	contactFieldValueJSON, err := json.Marshal(contactFieldValue)
	if err != nil {
		return nil, err
	}

	contactResponse := struct {
		Contacts []struct {
			ID string `json:"id"`
		} `json:"contacts"`
		Error OrttoError
	}{}

	filterOperator := "$str::is"
	if contactFieldID == "str::email" {
		filterOperator = "$str::is"
	}

	err = o.OrttoAPIBuilder().
		Path("/v1/person/get").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		Post().
		BodyBytes([]byte(fmt.Sprintf(`
		{
			"limit": 1,
			"offset": 0,
			"fields": ["%s"],
			"filter": {
				"%s": {
					"field_id": "%s",
					"value": %s
				}
			}
		}
		`, contactFieldID, filterOperator, contactFieldID, contactFieldValueJSON))).
		ToJSON(&contactResponse).
		ErrorJSON(&contactResponse.Error).
		Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to look up contact by %s: %w", contactFieldID, err)
	}

	if len(contactResponse.Contacts) == 0 {
		return nil, nil
	}

	personID := contactResponse.Contacts[0].ID

	// Step 2: Retrieve the activity feed for the contact
	var feedResponse OrttoActivityFeedResponse

	err = o.OrttoAPIBuilder().
		Path("/v1/person/get/activities").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		Post().
		BodyBytes([]byte(fmt.Sprintf(`
		{
			"person_id": "%s",
			"activities": ["%s"],
			"limit": 1
		}
		`, personID, activityID))).
		ToJSON(&feedResponse).
		ErrorJSON(&feedResponse.Error).
		Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get activity feed for contact %s: %w", personID, err)
	}

	return feedResponse.Activities, nil
}

// OrttoFieldDisplayLabel converts an Ortto field ID to its display label.
// e.g. "str:cm:prefix-field-name" → "PREFIX Field Name"
func OrttoFieldDisplayLabel(fieldID string) string {
	label := ""
	keyParts := strings.Split(fieldID, ":")
	if len(keyParts) == 3 {
		fieldNameParts := strings.Split(keyParts[2], "-")
		for i, s := range fieldNameParts {
			if i == 0 {
				label = strings.ToUpper(s)
			} else {
				label = label + " " + strcase.ToCamel(s)
			}
		}
	}
	return label
}

// CheckCustomFields checks that the specified custom fields exist in Ortto.
// fieldstocheck maps field IDs to the initial status (e.g. "processing").
// orttotypes maps field IDs to their expected Ortto type label.
// The config is used for int: decimal/integer type validation.
func (o OrttoFetcherAndUpdater) CheckCustomFields(fieldsToCheck map[string]string, orttoTypes map[string]string, config Config, statusProcessing string, statusOK string, statusMissing string, ctx context.Context) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range fieldsToCheck {
		result[k] = v
	}

	response := struct {
		Fields []struct {
			Field struct {
				ID          string `json:"id"`
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
		if _, exists := result[v.Field.ID]; exists {
			// check Ortto type is correct for decimals and integers
			if strings.HasPrefix(v.Field.ID, "int:") {
				if v.Field.DisplayType == "decimal" {
					_, decimalMappingExists := config.FundraiserFieldMappings.Custom.Decimals[v.Field.ID]
					if !decimalMappingExists {
						_, decimalMappingExists = config.TeamFieldMappings.Custom.Decimals[v.Field.ID]
					}
					if decimalMappingExists {
						result[v.Field.ID] = statusOK
					}
				}
				if v.Field.DisplayType == "integer" {
					_, integerMappingExists := config.FundraiserFieldMappings.Custom.Integers[v.Field.ID]
					if !integerMappingExists {
						_, integerMappingExists = config.TeamFieldMappings.Custom.Integers[v.Field.ID]
					}
					if integerMappingExists {
						result[v.Field.ID] = statusOK
					}
				}
			} else {
				result[v.Field.ID] = statusOK
			}
		}
	}

	for k, v := range result {
		if v != statusOK {
			orttoLabel := OrttoFieldDisplayLabel(k)
			result[k] = fmt.Sprintf(`%s %s (%s)`, statusMissing, orttoLabel, orttoTypes[k])
		}
	}

	if err != nil {
		log.Printf("Ortto custom field check error: %+v", err)
	}

	return result, err
}

// CSVEnrichmentRow represents a single row to enrich with Ortto activity data.
type CSVEnrichmentRow struct {
	ContactFieldID    string
	ContactFieldValue string
}

// CSVEnrichmentResult holds the enrichment result for a single row.
type CSVEnrichmentResult struct {
	Attributes map[string]string // attribute name → value
	Err        error
}

// EnrichCSVRows concurrently enriches rows with Ortto activity feed data using a
// worker pool pattern (fixed goroutines pulling from a shared channel).
// activityAttributes limits output to attributes present in the current activity definition;
// pass nil to include all attributes.
// Returns per-row results and the union of all attribute keys found.
func (o OrttoFetcherAndUpdater) EnrichCSVRows(rows []CSVEnrichmentRow, activityID string, activityAttributes map[string]bool, ctx context.Context) ([]CSVEnrichmentResult, []string) {
	results := make([]CSVEnrichmentResult, len(rows))
	errs := make([]error, len(rows))

	work := make(chan int, len(rows))
	for i := range rows {
		work <- i
	}
	close(work)

	var wg gosync.WaitGroup
	wg.Add(ActivityFeedConcurrencyLimit)
	for range ActivityFeedConcurrencyLimit {
		go func() {
			defer wg.Done()
			for i := range work {
				results[i] = o.enrichRow(rows[i], activityID, activityAttributes, ctx)
				errs[i] = results[i].Err
			}
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		log.Printf("csv enrichment errors: %v", err)
	}

	// Collect union of all attribute keys
	keySet := make(map[string]bool)
	for _, r := range results {
		for k := range r.Attributes {
			keySet[k] = true
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	return results, keys
}

func (o OrttoFetcherAndUpdater) enrichRow(row CSVEnrichmentRow, activityID string, activityAttributes map[string]bool, ctx context.Context) CSVEnrichmentResult {
	result := CSVEnrichmentResult{Attributes: make(map[string]string)}

	if row.ContactFieldValue == "" {
		return result
	}

	activities, err := o.GetActivityFeedForContact(
		row.ContactFieldID,
		row.ContactFieldValue,
		activityID,
		ctx,
	)
	if err != nil {
		result.Err = fmt.Errorf("contact %s: %w", row.ContactFieldValue, err)
		return result
	}

	if len(activities) == 0 {
		return result
	}

	// Use the latest activity (first entry)
	latest := activities[0]
	for k, v := range latest.Attributes {
		name := ExtractAttributeName(k)
		// Filter to attributes in the current activity definition
		if activityAttributes != nil && !activityAttributes[name] {
			continue
		}
		// Flatten object-type attributes into <name>.<child-key> columns
		if nested, ok := v.(map[string]interface{}); ok {
			for childKey, childValue := range nested {
				result.Attributes[name+"."+ExtractAttributeName(childKey)] = fmt.Sprintf("%v", childValue)
			}
			continue
		}
		result.Attributes[name] = fmt.Sprintf("%v", v)
	}

	return result
}

// ExtractAttributeName returns the name portion of an Ortto field key.
// e.g. "str:cm:shirt-size" → "shirt-size"
func ExtractAttributeName(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) >= 1 {
		return parts[len(parts)-1]
	}
	return key
}
