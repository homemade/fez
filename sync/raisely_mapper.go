package sync

import (
	"context"

	"github.com/tidwall/gjson"
)

// RaiselyMapper handles mapping Raisely data to target formats.
// It embeds RaiselyFetcher to provide both fetching and mapping capabilities.
type RaiselyMapper struct {
	RaiselyFetcher
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
func (m *RaiselyMapper) ApplyFundraiserTransforms(destination Mappable, campaign *FundraisingCampaign, ctx context.Context, skipDonationCheck bool) error {
	return ApplyFundraiserFieldTransforms(ApplyFundraiserFieldTransformsParams{
		Config:            m.Config,
		Campaign:          campaign,
		Destination:       destination,
		Ctx:               ctx,
		DonationsFetcher:  &m.RaiselyFetcher,
		SkipDonationCheck: skipDonationCheck,
	})
}

// ApplyTeamTransforms applies team field transforms and maps them to the provided destination.
func (m *RaiselyMapper) ApplyTeamTransforms(page, teamPage FundraisingPage, destination Mappable) error {
	return ApplyTeamFieldTransforms(m.Config, page, teamPage, destination)
}
