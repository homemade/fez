package sync

import (
	"context"
)

// RaiselyExtensionsMapper handles computing and writing extension data back to Raisely.
// This is separate from Ortto integration - it reads from Raisely, computes extensions
// (like streaks), and writes the results back to Raisely fundraising pages.
type RaiselyExtensionsMapper struct {
	*SyncContext
	RaiselyFetcherAndUpdater *RaiselyFetcherAndUpdater
}

// MapFundraisingPageForExtensions computes extension data for a fundraising page
// and returns an UpdateRaiselyDataRequest to write the results back to Raisely.
func (r *RaiselyExtensionsMapper) MapFundraisingPageForExtensions(campaign *FundraisingCampaign, p2pRegistrationID string, ctx context.Context) (UpdateRaiselyDataRequest, error) {

	updateFundraisingPageRequest := UpdateRaiselyDataRequest{
		P2PID: p2pRegistrationID,
	}

	data, err := r.RaiselyFetcherAndUpdater.FetchFundraiserData(p2pRegistrationID, ctx)
	if err != nil {
		return updateFundraisingPageRequest, err
	}

	fundraiserExtensions := FundraiserExtensions{r.Config.FundraiserExtensions, campaign, data.Page}

	updateFundraisingPageRequest.JSON, err = ApplyRaiselyFundraiserExtensions(fundraiserExtensions, data.ExerciseLogs.ExerciseLogs, data.Donations.Donations)
	return updateFundraisingPageRequest, err
}

// MapTeamFundraisingPageForExtensions computes extension data for a team and all its members,
// returning UpdateRaiselyDataRequests to write the results back to Raisely.
func (r *RaiselyExtensionsMapper) MapTeamFundraisingPageForExtensions(campaign *FundraisingCampaign, p2pTeamID string, ctx context.Context) ([]UpdateRaiselyDataRequest, error) {

	var updateFundraisingPageRequests []UpdateRaiselyDataRequest

	team, teamFundraisingPage, err := r.RaiselyFetcherAndUpdater.FetchTeam(p2pTeamID, ctx)
	if err != nil {
		return updateFundraisingPageRequests, err
	}

	teamExtensions := TeamExtensions{r.Config.TeamExtensions, campaign, teamFundraisingPage}

	updateTeamFundraisingPageRequest := UpdateRaiselyDataRequest{
		P2PID: p2pTeamID,
	}
	updateTeamFundraisingPageRequest.JSON, err = ApplyRaiselyTeamExtensions(teamExtensions)
	if err != nil {
		return updateFundraisingPageRequests, err
	}

	updateFundraisingPageRequests = append(updateFundraisingPageRequests, updateTeamFundraisingPageRequest)

	for _, teamMember := range team.TeamMembers {
		updateFundraisingPageRequest, err := r.MapFundraisingPageForExtensions(campaign, teamMember.P2PID, ctx)
		if err != nil {
			return updateFundraisingPageRequests, err
		}
		updateFundraisingPageRequests = append(updateFundraisingPageRequests, updateFundraisingPageRequest)
	}

	return updateFundraisingPageRequests, nil
}
