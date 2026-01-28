package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/iancoleman/strcase"
	"github.com/tidwall/gjson"
)

// OrttoContactsMapper maps Raisely data to Ortto Contacts/CDP API format.
// This implements the TargetMapper interface for the ortto-contacts target.
type OrttoContactsMapper struct {
	RaiselyMapper
	OrttoSyncContext
}

func (o OrttoContactsMapper) OrttoAPIBuilder() *requests.Builder {
	result := requests.
		URL(o.Config.API.Endpoints.Ortto).
		Client(&http.Client{Timeout: HTTPRequestTimeout})
	if o.RecordRequests {
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/ortto-contacts", o.Campaign)))
	}
	return result
}

// MapFundraisingPage maps a fundraising page to an Ortto contacts request.
func (o *OrttoContactsMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (OrttoRequest, error) {

	orttoRequest := OrttoContactsRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
	}

	data, err := o.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return orttoRequest, err
	}

	var contact OrttoContact
	contact.Fields = make(map[string]interface{})
	o.MapFundraiserFields(data.Page.Source, &contact)
	if err = o.ApplyFundraiserTransforms(&contact, campaign, ctx, false); err != nil {
		return orttoRequest, err
	}
	// To support people leaving teams we also need to set any team field mappings to empty
	o.ClearTeamFields(&contact)

	orttoRequest.Contacts = append(orttoRequest.Contacts, contact)

	return orttoRequest, nil
}

// MapTeamFundraisingPage maps team members' fundraising pages to an Ortto contacts request.
func (o *OrttoContactsMapper) MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) (OrttoRequest, error) {
	result := OrttoContactsRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
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
		var contact OrttoContact
		contact.Fields = make(map[string]interface{})
		o.MapFundraiserFields(page.Source, &contact)
		o.MapTeamFields(teamPage.Source, &contact)
		if err := o.ApplyFundraiserTransforms(&contact, campaign, ctx, false); err != nil {
			return result, err
		}
		if err := o.ApplyTeamTransforms(page, teamPage, &contact); err != nil {
			return result, err
		}
		result.Contacts = append(result.Contacts, contact)
	}

	return result, nil
}

// MapTrackingData maps tracking form data to an Ortto contacts request.
func (o *OrttoContactsMapper) MapTrackingData(data map[string]string, ctx context.Context) (OrttoRequest, error) {

	result := OrttoContactsRequest{
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
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

	var contact OrttoContact
	contact.Fields = make(map[string]interface{})
	o.MapFundraiserFields(source, &contact)

	email, emailExists := contact.Fields["str::email"].(string)
	if !emailExists || email == "" {
		return result, errors.New("missing required field in tracking data")
	}
	var existingContacts []OrttoContact
	existingContacts, err = o.SearchForFundraisingPageByEmail(email, ctx)
	if err != nil {
		return result, err
	}

	if len(existingContacts) < 1 {
		result.Contacts = append(result.Contacts, contact)
	} else {
		log.Println("Found existing fundraising page for this tracking data in ortto")
	}

	return result, nil
}

// SendRequest sends an Ortto contacts request to the Ortto API.
func (o *OrttoContactsMapper) SendRequest(req OrttoRequest, ctx context.Context) (OrttoResponse, error) {
	contactsReq, ok := req.(OrttoContactsRequest)
	if !ok {
		return nil, fmt.Errorf("expected OrttoContactsRequest, got %T", req)
	}

	var result OrttoContactsResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/person/merge").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&contactsReq).
		ToJSON(&result).
		ErrorJSON(&result.Error).
		Fetch(ctx)

	return result, err
}

// SearchForFundraisingPageByEmail searches Ortto for a contact by email that has a p2p registration id.
func (o *OrttoContactsMapper) SearchForFundraisingPageByEmail(email string, ctx context.Context) ([]OrttoContact, error) {

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
			"fields": ["str:cm:%s-p2p-registration-id", "str::email"],
			"filter": {
				"$and": [
				{
					"$has_any_value": {
					"field_id": "str:cm:%s-p2p-registration-id"
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
		`, o.Config.CampaignPrefix, o.Config.CampaignPrefix, email))).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)
	if err != nil {
		return nil, err
	}

	return response.Contacts, nil

}

// ReconcileFundraisingPage compares a mapped contact against the existing Ortto contact and returns any differences.
// This is useful for dev mode reconciliation to identify data inconsistencies.
func (o *OrttoContactsMapper) ReconcileFundraisingPage(p2pregistrationid string, contact OrttoContact, ctx context.Context) (OrttoContactDiff, error) {

	var result OrttoContactDiff
	result.Fields = make(map[string]OrttoContactDiffField)

	fieldNamesArray := make([]string, len(contact.Fields))
	i := 0
	for k := range contact.Fields {
		fieldNamesArray[i] = k
		i = i + 1
	}
	fieldNames, err := json.Marshal(fieldNamesArray)
	if err != nil {
		return result, err
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
			"fields": %s,
			"filter": {
				"$and": [
				{
					"$has_any_value": {
					"field_id": "str::email"
					}
				},
				{
					"$str::is": {
					"field_id": "str:cm:%s-p2p-registration-id",
					"value": "%s"
					}
				}
				]
			}
		}
		`, fieldNames, o.Config.CampaignPrefix, p2pregistrationid))).
		ToJSON(&response).
		ErrorJSON(&response.Error).
		Fetch(ctx)

	if err != nil {
		return result, err
	}

	if len(response.Contacts) > 1 {
		return result, fmt.Errorf("multiple ortto contacts found for p2p registration id %s", p2pregistrationid)
	}

	if len(response.Contacts) == 1 {

		result.Id = response.Contacts[0].Id

		for k, orttoValue := range response.Contacts[0].Fields {

			sourceValue := contact.Fields[k]

			// some fields need specific handling for comparison

			if strings.HasPrefix(k, "geo:") { // Ortto adds an id field to geos (address fields)
				if geoMap, ok := orttoValue.(map[string]interface{}); ok {
					delete(geoMap, "id")
				}
			}

			if strings.HasPrefix(k, "tme:") { // Ortto returns timestamps in ISO 8601 format
				if sourceStr, ok := sourceValue.(string); ok {
					t, err := time.Parse(time.RFC3339, sourceStr)
					if err != nil {
						return result, err
					}
					sourceValue = t.Format(time.RFC3339)
				}
			}

			expected, err := json.Marshal(sourceValue)
			if err != nil {
				return result, err
			}
			actual, err := json.Marshal(orttoValue)
			if err != nil {
				return result, err
			}
			if !bytes.Equal(expected, actual) {
				result.Fields[k] = OrttoContactDiffField{
					Actual:   string(actual),
					Expected: string(expected),
				}
			}
			i = i + 1
		}
	}

	return result, nil

}

// CheckOrttoCustomFields checks that all configured custom fields exist in Ortto.
// Returns a map of field names to their status (ok/missing).
func (o *OrttoContactsMapper) CheckOrttoCustomFields(statusprocessing string, statusok string, statusmissing string, ctx context.Context) (map[string]string, error) {

	result := make(map[string]string)
	orttoTypes := make(map[string]string)
	for _, v := range o.Config.FundraiserFieldMappings.Custom.AllKeys() {
		result[v] = statusprocessing
		orttoTypes[v] = o.Config.FundraiserFieldMappings.Custom.AsOrttoFieldType(v)
	}
	for _, v := range o.Config.TeamFieldMappings.Custom.AllKeys() {
		result[v] = statusprocessing
		orttoTypes[v] = o.Config.TeamFieldMappings.Custom.AsOrttoFieldType(v)
	}
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
