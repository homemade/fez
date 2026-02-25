package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/carlmjohnson/requests"
	"github.com/iancoleman/strcase"
)

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
			result[k] = fmt.Sprintf(`%s %s (%s)`, statusMissing, orttoLabel, orttoTypes[k])
		}
	}

	if err != nil {
		log.Printf("Ortto custom field check error: %+v", err)
	}

	return result, err
}
