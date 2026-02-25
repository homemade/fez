package sync

import (
	"context"

	"github.com/tidwall/gjson"
)

// RaiselyMapper handles mapping Raisely data to target formats.
// It embeds *SyncContext for shared configuration and uses a named
// RaiselyFetcherAndUpdater field for Raisely API operations.
type RaiselyMapper struct {
	*SyncContext
	RaiselyFetcherAndUpdater *RaiselyFetcherAndUpdater
}

// MapFundraiserFields maps builtin and custom fundraiser fields from source to destination.
func (m *RaiselyMapper) MapFundraiserFields(source Source, destination Mappable) {
	MapFields(m.Config.FundraiserFieldMappings.Builtin, source, destination)
	MapFields(m.Config.FundraiserFieldMappings.Custom, source, destination)
}

// MapTeamFields maps team custom fields from source to destination.
func (m *RaiselyMapper) MapTeamFields(source Source, destination Mappable) {
	MapFields(m.Config.TeamFieldMappings.Custom, source, destination)
}

// ClearTeamFields maps empty team fields to a destination (for non-team members).
func (m *RaiselyMapper) ClearTeamFields(destination Mappable) {
	emptySource := Source{data: gjson.ParseBytes([]byte(`{}`))}
	MapFields(m.Config.TeamFieldMappings.Custom, emptySource, destination)
}

// ApplyFundraiserTransforms applies fundraiser field transforms and maps them to the provided destination.
func (m *RaiselyMapper) ApplyFundraiserTransforms(destination Mappable, campaign *FundraisingCampaign, ctx context.Context, skipdonationcheck bool) error {
	return ApplyFundraiserFieldTransforms(ApplyFundraiserFieldTransformsParams{
		Config:            m.Config,
		Campaign:          campaign,
		Destination:       destination,
		Ctx:               ctx,
		DonationsFetcher:  m.RaiselyFetcherAndUpdater,
		SkipDonationCheck: skipdonationcheck,
	})
}

// ApplyTeamTransforms applies team field transforms and maps them to the provided destination.
func (m *RaiselyMapper) ApplyTeamTransforms(page, teampage FundraisingPage, destination Mappable) error {
	return ApplyTeamFieldTransforms(m.Config, page, teampage, destination)
}
