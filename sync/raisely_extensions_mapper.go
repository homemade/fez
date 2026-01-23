package sync

import (
	"context"
	"fmt"
	gosync "sync"
)

// RaiselyExtensionsMapper handles computing and writing extension data back to Raisely.
// This is separate from Ortto integration - it reads from Raisely, computes extensions
// (like streaks), and writes the results back to Raisely fundraising pages.
type RaiselyExtensionsMapper struct {
	RaiselyFetcher
}

// MapFundraisingPageForExtensions computes extension data for a fundraising page
// and returns an UpdateRaiselyDataRequest to write the results back to Raisely.
func (r *RaiselyExtensionsMapper) MapFundraisingPageForExtensions(campaign *FundraisingCampaign, p2pregistrationid string, ctx context.Context) (UpdateRaiselyDataRequest, error) {

	updateFundraisingPageRequest := UpdateRaiselyDataRequest{
		P2PId: p2pregistrationid,
	}

	data, err := r.FetchFundraiserData(p2pregistrationid, ctx)
	if err != nil {
		return updateFundraisingPageRequest, err
	}

	fundraiserExtensions := FundraiserExtensions{r.Config.FundraiserExtensions, campaign, data.Page}

	updateFundraisingPageRequest.JSON, err = ApplyRaiselyFundraiserExtensions(fundraiserExtensions, data.ExerciseLogs.ExerciseLogs, data.Donations.Donations)
	return updateFundraisingPageRequest, err
}

// MapTeamFundraisingPageForExtensions computes extension data for a team and all its members,
// returning UpdateRaiselyDataRequests to write the results back to Raisely.
func (r *RaiselyExtensionsMapper) MapTeamFundraisingPageForExtensions(campaign *FundraisingCampaign, p2pteamid string, ctx context.Context) ([]UpdateRaiselyDataRequest, error) {

	var updateFundraisingPageRequests []UpdateRaiselyDataRequest

	var wg gosync.WaitGroup
	var team FundraisingTeam
	var teamFundraisingPage FundraisingPage

	var errs []error
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := team.FetchRaiselyData(r.fetchParams(p2pteamid, ctx)); err != nil {
			errs = append(errs, err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := teamFundraisingPage.FetchRaiselyData(r.fetchParams(p2pteamid, ctx)); err != nil {
			errs = append(errs, err)
		}
	}()
	wg.Wait()

	if len(errs) > 0 {
		return updateFundraisingPageRequests, fmt.Errorf("raisely errors: %v", errs)
	}

	teamExtensions := TeamExtensions{r.Config.TeamExtensions, campaign, teamFundraisingPage}

	var err error
	updateTeamFundraisingPageRequest := UpdateRaiselyDataRequest{
		P2PId: p2pteamid,
	}
	updateTeamFundraisingPageRequest.JSON, err = ApplyRaiselyTeamExtensions(teamExtensions)
	if err != nil {
		return updateFundraisingPageRequests, err
	}

	updateFundraisingPageRequests = append(updateFundraisingPageRequests, updateTeamFundraisingPageRequest)

	for _, teamMember := range team.TeamMembers {
		updateFundraisingPageRequest, err := r.MapFundraisingPageForExtensions(campaign, teamMember.P2PId, ctx)
		if err != nil {
			return updateFundraisingPageRequests, err
		}
		updateFundraisingPageRequests = append(updateFundraisingPageRequests, updateFundraisingPageRequest)
	}

	return updateFundraisingPageRequests, nil
}
