package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// OrttoContactsMapper maps Raisely data to Ortto Contacts/CDP API format.
// This implements the OrttoMapper interface for the ortto-contacts target.
type OrttoContactsMapper struct {
	*SyncContext
	RaiselyMapper          RaiselyMapper
	OrttoFetcherAndUpdater OrttoFetcherAndUpdater
}

// MapFundraisingPage maps a fundraising page to an Ortto contacts request.
func (o *OrttoContactsMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (OrttoRequest, error) {

	orttoRequest := OrttoContactsRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
	}

	data, err := o.RaiselyMapper.RaiselyFetcherAndUpdater.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return orttoRequest, err
	}

	var contact OrttoContact
	contact.Fields = make(map[string]interface{})
	o.RaiselyMapper.MapFundraiserFields(data.Page.Source, &contact)
	if err = o.RaiselyMapper.ApplyFundraiserTransforms(&contact, campaign, ctx, false); err != nil {
		return orttoRequest, err
	}
	// To support people leaving teams we also need to set any team field mappings to empty
	o.RaiselyMapper.ClearTeamFields(&contact)

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

	team, teamPage, err := o.RaiselyMapper.RaiselyFetcherAndUpdater.FetchTeam(p2pteamid, ctx)
	if err != nil {
		return result, err
	}

	var memberPages []FundraisingPage
	memberPages, err = o.RaiselyMapper.RaiselyFetcherAndUpdater.FetchTeamMembers(team, ctx)
	if err != nil {
		return result, err
	}

	for _, page := range memberPages {
		var contact OrttoContact
		contact.Fields = make(map[string]interface{})
		o.RaiselyMapper.MapFundraiserFields(page.Source, &contact)
		o.RaiselyMapper.MapTeamFields(teamPage.Source, &contact)
		if err := o.RaiselyMapper.ApplyFundraiserTransforms(&contact, campaign, ctx, false); err != nil {
			return result, err
		}
		if err := o.RaiselyMapper.ApplyTeamTransforms(page, teamPage, &contact); err != nil {
			return result, err
		}
		result.Contacts = append(result.Contacts, contact)
	}

	return result, nil
}

// MapTrackingData maps tracking form data to an Ortto contacts request.
func (o *OrttoContactsMapper) MapTrackingData(campaign *FundraisingCampaign, data map[string]string, ctx context.Context) (OrttoRequest, error) {

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
	o.RaiselyMapper.MapFundraiserFields(source, &contact)
	if err = o.RaiselyMapper.ApplyFundraiserTransforms(&contact, campaign, ctx, false); err != nil {
		return result, err
	}

	email, emailExists := contact.Fields["str::email"].(string)
	if !emailExists || email == "" {
		return result, errors.New("missing required field in tracking data")
	}
	mergeFieldID := fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix)
	var existingContacts []OrttoContact
	existingContacts, err = o.OrttoFetcherAndUpdater.SearchForContactByEmail(email, mergeFieldID, ctx)
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

	result, err := o.OrttoFetcherAndUpdater.SendContactsMerge(contactsReq, ctx)
	return result, err
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

	filterJSON := fmt.Sprintf(`{
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
			}`, o.Config.CampaignPrefix, p2pregistrationid)

	contacts, err := o.OrttoFetcherAndUpdater.GetContact(fieldNames, filterJSON, ctx)
	if err != nil {
		return result, err
	}

	if len(contacts) > 1 {
		return result, fmt.Errorf("multiple ortto contacts found for p2p registration id %s", p2pregistrationid)
	}

	if len(contacts) == 1 {

		result.Id = contacts[0].Id

		for k, orttoValue := range contacts[0].Fields {

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
		}
	}

	return result, nil

}

// CheckOrttoCustomFields checks that all configured custom fields exist in Ortto.
// Returns a map of field names to their status (ok/missing).
func (o *OrttoContactsMapper) CheckOrttoCustomFields(statusprocessing string, statusok string, statusmissing string, ctx context.Context) (map[string]string, error) {

	fieldsToCheck := make(map[string]string)
	orttoTypes := make(map[string]string)
	for _, v := range o.Config.FundraiserFieldMappings.Custom.AllKeys() {
		fieldsToCheck[v] = statusprocessing
		orttoTypes[v] = o.Config.FundraiserFieldMappings.Custom.AsOrttoFieldType(v)
	}
	for _, v := range o.Config.TeamFieldMappings.Custom.AllKeys() {
		fieldsToCheck[v] = statusprocessing
		orttoTypes[v] = o.Config.TeamFieldMappings.Custom.AsOrttoFieldType(v)
	}

	return o.OrttoFetcherAndUpdater.CheckCustomFields(fieldsToCheck, orttoTypes, o.Config, statusprocessing, statusok, statusmissing, ctx)
}
