package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/carlmjohnson/requests"
	"github.com/tidwall/gjson"
)

// OrttoActivitiesMapper maps Raisely data to Ortto Activities API format.
// This implements the TargetMapper interface for the ortto-activities target.
type OrttoActivitiesMapper struct {
	RaiselyMapper
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
func (o *OrttoActivitiesMapper) MapFundraisingPage(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (TargetRequest, error) {

	orttoRequest := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.Ortto == "" {
		return orttoRequest, errors.New("ortto activity ID is required for ortto-activities target (api.ids.ortto)")
	}

	data, err := o.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return orttoRequest, err
	}

	// Build the activity with person fields
	activity := OrttoActivity{
		ActivityID: o.Config.API.Ids.Ortto,
		Fields:     make(map[string]interface{}),
	}

	o.MapFundraiserFields(data.Page.Source, &activity)
	if err = o.ApplyFundraiserTransforms(&activity, campaign, ctx, true); err != nil {
		return orttoRequest, err
	}
	// To support people leaving teams we also need to set any team field mappings to empty
	o.ClearTeamFields(&activity)

	orttoRequest.Activities = append(orttoRequest.Activities, activity)

	return orttoRequest, nil
}

// MapTeamFundraisingPage maps team members' fundraising pages to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTeamFundraisingPage(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) (TargetRequest, error) {
	result := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{fmt.Sprintf("str:cm:%s-p2p-registration-id", o.Config.CampaignPrefix), "str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.Ortto == "" {
		return result, errors.New("ortto activity ID is required for ortto-activities target (api.ids.ortto)")
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
			ActivityID: o.Config.API.Ids.Ortto,
			Fields:     make(map[string]interface{}),
		}

		o.MapFundraiserFields(page.Source, &activity)
		o.MapTeamFields(teamPage.Source, &activity)
		if err := o.ApplyFundraiserTransforms(&activity, campaign, ctx, true); err != nil {
			return result, err
		}
		if err := o.ApplyTeamTransforms(page, teamPage, &activity); err != nil {
			return result, err
		}

		result.Activities = append(result.Activities, activity)
	}

	return result, nil
}

// MapTrackingData maps tracking form data to an Ortto activities request.
func (o *OrttoActivitiesMapper) MapTrackingData(data map[string]string, ctx context.Context) (TargetRequest, error) {

	result := OrttoActivitiesRequest{
		Async:         false,
		MergeBy:       []string{"str::email"},
		MergeStrategy: 2, // Overwrite existing
	}

	// Validate that we have an activity ID configured
	if o.Config.API.Ids.Ortto == "" {
		return result, errors.New("ortto activity ID is required for ortto-activities target (api.ids.ortto)")
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
		ActivityID: o.Config.API.Ids.Ortto,
		Fields:     make(map[string]interface{}),
	}

	o.MapFundraiserFields(source, &activity)

	email, emailExists := activity.Fields["str::email"].(string)
	if !emailExists || email == "" {
		return result, errors.New("missing required field in tracking data")
	}

	// For activities, we always create the activity (unlike contacts where we check for existing)
	result.Activities = append(result.Activities, activity)

	return result, nil
}

// SendRequest sends an Ortto activities request to the Ortto API.
func (o *OrttoActivitiesMapper) SendRequest(req TargetRequest, ctx context.Context) (TargetResponse, error) {
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
