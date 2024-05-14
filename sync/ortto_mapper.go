package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/tidwall/gjson"
)

var cachedFundraisingCampaign *FundraisingCampaign

type OrttoMapper struct {
	Campaign       string
	Config         Config
	RecordRequests bool
}

type OrttoResponse struct {
	Results []OrttoResult `json:"people"`
	Error   OrttoError
}

type OrttoResult struct {
	PersonId string `json:"person_id"`
	Status   string `json:"status"`
}

type OrttoError struct {
	RequestID string `json:"request_id"`
	Code      int    `json:"code"`
	Error     string `json:"error"`
}

type OrttoRequest struct {
	Contacts      []OrttoContact `json:"people"`
	Async         bool           `json:"async"`
	MergeBy       []string       `json:"merge_by"`
	MergeStrategy uint8          `json:"merge_strategy"`
	FindStrategy  uint8          `json:"find_strategy"`
}

type OrttoContact struct {
	Fields map[string]interface{} `json:"fields"`
}

type OrttoMappingResult struct {
	UpdateFundraisingPageRequests []string
	OrttoRequest
	Error error
}

func (o OrttoMapper) OrttoAPIBuilder() *requests.Builder {
	result := requests.
		URL(o.Config.API.Endpoints.Ortto)
	if o.RecordRequests {
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/ortto", o.Campaign)))
	}
	return result
}

func (o OrttoMapper) RaiselyAPIBuilder() *requests.Builder {
	result := requests.
		URL("https://api.raisely.com")
	if o.RecordRequests {
		result = result.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/raisely", o.Campaign)))
	}
	return result
}

func (o OrttoMapper) CheckOrttoCustomFields(statusprocessing string, statusok string, statusmissing string, context context.Context) (map[string]string, error) {

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
		Fetch(context)
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
			result[k] = fmt.Sprintf("%s (%s)", statusmissing, orttoTypes[k])
		}
	}

	return result, err

}

func (o OrttoMapper) CachedFundraisingCampaign(p2pid string, refresh bool, context context.Context) (*FundraisingCampaign, error) {

	if cachedFundraisingCampaign == nil || refresh {
		fundraisingCampaign := &FundraisingCampaign{}
		err := fundraisingCampaign.FetchRaiselyData(FetchRaiselyDataParams{
			RaiselyAPIKey:     o.Config.API.Keys.Raisely,
			P2PId:             p2pid,
			Context:           context,
			RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
		})
		if err == nil {
			cachedFundraisingCampaign = fundraisingCampaign
		}
		if err != nil && cachedFundraisingCampaign == nil {
			// only return error if we don't have a cached
			// fundraising campaign to fallback on
			return nil, err
		}
	}

	return cachedFundraisingCampaign, nil

}

func (o OrttoMapper) MapTrackingData(data map[string]string, context context.Context) OrttoMappingResult {

	var result OrttoMappingResult

	result.OrttoRequest = OrttoRequest{
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 1, // Append only (fields with existing values in Ortto’s CDP are not changed)
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		result.Error = err
		return result
	}
	if !gjson.ValidBytes(jsonData) {
		log.Printf("Invalid Tracking Data:\n%s", string(jsonData))
		result.Error = errors.New("invalid tracking data")
		return result
	}

	source := Source{
		data: gjson.ParseBytes(jsonData),
	}

	var contact OrttoContact
	contact.Fields = make(map[string]interface{})
	// Map builtin fields
	mapContactFields(o.Config.FundraiserFieldMappings.Builtin, source, &contact)
	// Map custom fields
	mapContactFields(o.Config.FundraiserFieldMappings.Custom, source, &contact)

	result.Contacts = append(result.Contacts, contact)

	return result
}

func (o OrttoMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, context context.Context) OrttoMappingResult {

	var result OrttoMappingResult

	result.OrttoRequest = OrttoRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
	}

	var fundraiserRequestsWaitGroup sync.WaitGroup // add a wait group for the fundraiser requests
	var page FundraisingPage
	var profileExerciseLogs FundraisingProfileExerciseLogs
	var profileDonations FundraisingProfileDonations

	var errors []error
	fundraiserRequestsWaitGroup.Add(1)
	go func() {
		err := page.FetchRaiselyData(FetchRaiselyDataParams{
			RaiselyAPIKey:     o.Config.API.Keys.Raisely,
			P2PId:             p2pregistrationid,
			Context:           context,
			RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
		})
		if err != nil {
			errors = append(errors, err)
		}
		fundraiserRequestsWaitGroup.Done()
	}()

	if len(o.Config.FundraiserBadgeExtensions.Streaks.Activity.Days) > 0 {
		fundraiserRequestsWaitGroup.Add(1)
		go func() {
			err := profileExerciseLogs.FetchRaiselyData(FetchRaiselyDataParams{
				RaiselyAPIKey:     o.Config.API.Keys.Raisely,
				P2PId:             p2pregistrationid,
				Context:           context,
				RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
			})
			if err != nil {
				errors = append(errors, err)
			}
			fundraiserRequestsWaitGroup.Done()
		}()
	}

	if len(o.Config.FundraiserBadgeExtensions.Streaks.Donation.Days) > 0 {
		fundraiserRequestsWaitGroup.Add(1)
		go func() {
			err := profileDonations.FetchRaiselyData(FetchRaiselyDataParams{
				RaiselyAPIKey:     o.Config.API.Keys.Raisely,
				P2PId:             p2pregistrationid,
				Context:           context,
				RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
			})
			if err != nil {
				errors = append(errors, err)
			}
			fundraiserRequestsWaitGroup.Done()
		}()
	}

	fundraiserRequestsWaitGroup.Wait() // wait until all requests have completed
	if len(errors) > 0 {
		result.Error = fmt.Errorf("raisely errors: %v", errors)
		return result
	}

	fundraiserBadgeExtensions := FundraiserBadgeExtensions{o.Config.FundraiserBadgeExtensions, page}

	var err error
	if (fundraiserBadgeExtensions.MaxCurrentDaysForActivityStreak() < fundraiserBadgeExtensions.MaxConfiguredDaysForActivityStreak()) ||
		(fundraiserBadgeExtensions.MaxCurrentDaysForDonationStreak() < fundraiserBadgeExtensions.MaxConfiguredDaysForDonationStreak()) {
		var updateFundraisingPageRequestForBadgeExtensions string
		updateFundraisingPageRequestForBadgeExtensions, err = ApplyFundraiserBadgeExtensions(fundraiserBadgeExtensions, profileExerciseLogs.ExerciseLogs, profileDonations.Donations)
		if err != nil {
			result.Error = err
			return result
		}
		result.UpdateFundraisingPageRequests = append(result.UpdateFundraisingPageRequests, updateFundraisingPageRequestForBadgeExtensions)
	}

	var contact OrttoContact
	contact.Fields = make(map[string]interface{})
	// Map builtin fields
	mapContactFields(o.Config.FundraiserFieldMappings.Builtin, page.Source, &contact)
	// Map custom fields
	mapContactFields(o.Config.FundraiserFieldMappings.Custom, page.Source, &contact)
	// Apply any fundraiser transforms
	err = o.applyFundraiserFieldTransforms(campaign, &contact, context)
	if err != nil {
		result.Error = err
		return result
	}
	// To support people leaving teams we need to set any team field mappings to empty
	emptySource := Source{
		data: gjson.ParseBytes([]byte(`{}`)),
	}
	mapContactFields(o.Config.TeamFieldMappings.Custom, emptySource, &contact)

	result.OrttoRequest.Contacts = append(result.OrttoRequest.Contacts, contact)

	return result
}

func (o OrttoMapper) MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, context context.Context) OrttoMappingResult {

	var result OrttoMappingResult

	result.OrttoRequest = OrttoRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
		FindStrategy:  0, // Any  - first MergeBy field is prioritised, if a match is not found the second field is then used
	}

	var teamRequestsWaitGroup sync.WaitGroup // add a wait group for the team requests
	var team FundraisingTeam
	var teamFundraisingPage FundraisingPage
	var teamMemberFundraisingPages []FundraisingPage

	var errors []error
	teamRequestsWaitGroup.Add(1)
	go func() {
		// get team members
		err := team.FetchRaiselyData(FetchRaiselyDataParams{
			RaiselyAPIKey:     o.Config.API.Keys.Raisely,
			P2PId:             p2pteamid,
			Context:           context,
			RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
		})
		if err != nil {
			errors = append(errors, err)
		}
		teamRequestsWaitGroup.Done()
	}()
	teamRequestsWaitGroup.Add(1)
	go func() {
		// get team fundraising page
		err := teamFundraisingPage.FetchRaiselyData(FetchRaiselyDataParams{
			RaiselyAPIKey:     o.Config.API.Keys.Raisely,
			P2PId:             p2pteamid,
			Context:           context,
			RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
		})
		if err != nil {
			errors = append(errors, err)
		}
		teamRequestsWaitGroup.Done()
	}()
	teamRequestsWaitGroup.Wait() // wait until both requests have completed

	if len(errors) < 1 {
		if len(team.TeamMembers) > 0 {
			var fundraisingPageRequestsWaitGroup sync.WaitGroup // add a wait group for the team members fundraising page requests
			for _, teamMember := range team.TeamMembers {
				// fetch the fundraising page
				fundraisingPageRequestsWaitGroup.Add(1)
				go func(teamMemberP2PId string) {
					// read raisely data for fundraising page
					var p FundraisingPage
					err := p.FetchRaiselyData(FetchRaiselyDataParams{
						RaiselyAPIKey:     o.Config.API.Keys.Raisely,
						P2PId:             teamMemberP2PId,
						Context:           context,
						RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
					})
					if err != nil {
						errors = append(errors, err)
					}
					teamMemberFundraisingPages = append(teamMemberFundraisingPages, p)
					fundraisingPageRequestsWaitGroup.Done()
				}(teamMember.P2PId)
			}
			fundraisingPageRequestsWaitGroup.Wait() // wait until all team members fundraising page requests have completed

		}
	}

	if len(errors) > 0 {
		result.Error = fmt.Errorf("raisely errors: %v", errors)
		return result
	}

	for _, page := range teamMemberFundraisingPages {
		var contact OrttoContact
		contact.Fields = make(map[string]interface{})
		// Map fundraiser builtin fields
		mapContactFields(o.Config.FundraiserFieldMappings.Builtin, page.Source, &contact)
		// Map fundraiser custom fields
		mapContactFields(o.Config.FundraiserFieldMappings.Custom, page.Source, &contact)
		// Add team custom fields
		mapContactFields(o.Config.TeamFieldMappings.Custom, teamFundraisingPage.Source, &contact)
		// Apply any fundraiser transforms
		err := o.applyFundraiserFieldTransforms(campaign, &contact, context)
		if err != nil {
			result.Error = err
			return result
		}
		// Apply any team transforms
		err = o.applyTeamFieldTransforms(page, teamFundraisingPage, &contact)
		if err != nil {
			result.Error = err
			return result
		}
		result.Contacts = append(result.Contacts, contact)
	}

	return result
}

func (o OrttoMapper) SendRequest(req OrttoRequest, context context.Context) (OrttoResponse, error) {
	var result OrttoResponse

	err := o.OrttoAPIBuilder().
		Path("/v1/person/merge").
		Header("X-Api-Key", o.Config.API.Keys.Ortto).
		BodyJSON(&req).
		ToJSON(&result).
		ErrorJSON(&result.Error).
		Fetch(context)

	return result, err
}

func (o OrttoMapper) applyFundraiserFieldTransforms(campaign *FundraisingCampaign, contact *OrttoContact, context context.Context) error {
	var err error
	if len(o.Config.FundraiserFieldTransforms) > 0 {
		for field, transform := range o.Config.FundraiserFieldTransforms {
			if _, exists := contact.Fields[field]; !exists {
				return fmt.Errorf("invalid transform, field %s does not exist on contact", field)
			}
			parts := strings.Split(transform, ":")
			function := parts[0]
			arg := ""
			if len(parts) > 1 {
				arg = parts[1]
			}
			switch function {
			case "blankIfDefault":
				log.Println("Warning: modifier 'blankIfDefault' is deprecated please switch to using 'onlyIfNotDefault' instead")
				if fieldValue, ok := contact.Fields[field].(string); ok {
					for _, defaultObject := range campaign.FundraisingPageDefaults {
						if arg == defaultObject.Label &&
							fieldValue == defaultObject.Value {
							contact.Fields[field] = ""
						}
					}
				}
			case "onlyIfNotDefault":
				if fieldValue, ok := contact.Fields[field].(string); ok {
					for _, defaultObject := range campaign.FundraisingPageDefaults {
						if arg == defaultObject.Label &&
							fieldValue == defaultObject.Value {
							delete(contact.Fields, field)
						}
					}
				}
			case "warnIfEqual":
				if s := fmt.Sprintf("%v", contact.Fields[field]); arg == s {
					log.Printf("Warning: %s has value of '%v'\n", field, s)
				}
			case "onlyIfSelfDonatedDuringRegistrationWindow":
				// the arg for this transform has two params
				params := strings.Split(arg, ",") // split by ,
				if len(params) != 2 {
					return fmt.Errorf("invalid argument %s for transform %s expected two params", arg, transform)
				}
				// the first param is the donation amount as an integer
				var donationAmount int
				donationAmount, err = strconv.Atoi(params[0])
				if err != nil {
					return fmt.Errorf("invalid first param in argument %s for transform %s %w", arg, transform, err)
				}
				// the second param is the registration window duration
				var windowDuration time.Duration
				windowDuration, err = time.ParseDuration(params[1])
				if err != nil {
					return fmt.Errorf("invalid second param in argument %s for transform %s %w", arg, transform, err)
				}
				registrationDateField := fmt.Sprintf("tme:cm:%s-registration-date", o.Config.CampaignPrefix)
				if registrationDate, ok := contact.Fields[registrationDateField].(string); ok {
					var registrationTime time.Time
					registrationTime, err = time.Parse(time.RFC3339, registrationDate)
					if err != nil {
						return fmt.Errorf("failed to parse %s %w", registrationDateField, err)
					}
					p2pRegistrationId := contact.Fields[fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix)].(string)
					donations := FundraisingProfileDonationsUpTo{
						UpTo: registrationTime.Add(windowDuration),
					}
					err := donations.FetchRaiselyData(FetchRaiselyDataParams{
						RaiselyAPIKey:     o.Config.API.Keys.Raisely,
						P2PId:             p2pRegistrationId,
						Context:           context,
						RaiselyAPIBuilder: o.RaiselyAPIBuilder(),
					})
					if err != nil {
						return fmt.Errorf("failed to check self donation totals in registration window %w", err)
					}

					if donations.TotalDonationAmount >= donationAmount {
						continue // if all Ok keep the field
					}
				}
				// default to removing the field
				delete(contact.Fields, field)
			default:
				return fmt.Errorf("unsupported transform: %s", transform)
			}
		}
	}
	return nil
}

func (o OrttoMapper) applyTeamFieldTransforms(
	teamMemberFundraisingPage FundraisingPage,
	teamFundraisingPage FundraisingPage,
	contact *OrttoContact) error {
	if len(o.Config.TeamFieldTransforms) > 0 {
		captain, err := teamMemberFundraisingPage.HasSameOwnerAs(teamFundraisingPage)
		if err != nil {
			return err
		}
		for field, transform := range o.Config.TeamFieldTransforms {
			if _, exists := contact.Fields[field]; !exists {
				return fmt.Errorf("invalid transform, field %s does not exist on contact", field)
			}
			parts := strings.Split(transform, ":")
			function := parts[0]
			//arg := parts[1]
			switch function {
			case "isCaptain":
				contact.Fields[field] = captain
			case "isMember":
				contact.Fields[field] = !captain
			default:
				return fmt.Errorf("unsupported transform: %s", transform)
			}
		}
	}
	return nil
}

func mapContactFields(mappings FieldMappings, source Source, contact *OrttoContact) {
	if mappings.Strings != nil {
		for field, path := range mappings.Strings {
			if result, exists := source.StringForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
	if mappings.Texts != nil {
		for field, path := range mappings.Texts {
			if result, exists := source.StringForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
	if mappings.Decimals != nil {
		for field, path := range mappings.Decimals {
			if result, exists := source.IntForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
	if mappings.Booleans != nil {
		for field, path := range mappings.Booleans {
			if result, exists := source.BoolForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
	if mappings.Timestamps != nil {
		for field, path := range mappings.Timestamps {
			if result, exists := source.StringForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
	if mappings.Phones != nil {
		for field, v := range mappings.Phones {
			phoneObject := make(map[string]string)
			isEmptyObject := true
			for phoneField, path := range v {
				phoneObject[phoneField], _ = source.StringForPath(path)
				if isEmptyObject && phoneObject[phoneField] != "" {
					isEmptyObject = false
				}
			}
			if isEmptyObject {
				contact.Fields[field] = nil
			} else {
				contact.Fields[field] = phoneObject
			}
		}
	}
	if mappings.Geos != nil {
		for field, v := range mappings.Geos {
			geoObject := make(map[string]string)
			isEmptyObject := true
			for geoField, path := range v {
				geoObject[geoField], _ = source.StringForPath(path)
				if isEmptyObject && geoObject[geoField] != "" {
					isEmptyObject = false
				}
			}
			if isEmptyObject {
				contact.Fields[field] = nil
			} else {
				contact.Fields[field] = geoObject
			}
		}
	}
	if mappings.Integers != nil {
		for field, path := range mappings.Integers {
			if result, exists := source.IntForPath(path); exists {
				contact.Fields[field] = result
			} else {
				contact.Fields[field] = nil
			}
		}
	}
}
