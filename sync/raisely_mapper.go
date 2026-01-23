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

// MapFundraiserFields maps builtin and custom fundraiser fields from source to container.
func (m *RaiselyMapper) MapFundraiserFields(source Source, container FieldMapper) {
	mapFields(m.Config.FundraiserFieldMappings.Builtin, source, container)
	mapFields(m.Config.FundraiserFieldMappings.Custom, source, container)
}

// MapTeamFields maps team custom fields from source to container.
func (m *RaiselyMapper) MapTeamFields(source Source, container FieldMapper) {
	mapFields(m.Config.TeamFieldMappings.Custom, source, container)
}

// ClearTeamFields maps empty team fields to container (for non-team members).
func (m *RaiselyMapper) ClearTeamFields(container FieldMapper) {
	emptySource := Source{data: gjson.ParseBytes([]byte(`{}`))}
	mapFields(m.Config.TeamFieldMappings.Custom, emptySource, container)
}

// ApplyFundraiserTransforms applies fundraiser field transforms to the container.
func (m *RaiselyMapper) ApplyFundraiserTransforms(container FieldMapper, campaign *FundraisingCampaign, ctx context.Context, skipDonationCheck bool) error {
	return ApplyFundraiserFieldTransforms(ApplyFundraiserFieldTransformsParams{
		Config:            m.Config,
		Campaign:          campaign,
		Container:         container,
		Ctx:               ctx,
		DonationsFetcher:  &m.RaiselyFetcher,
		SkipDonationCheck: skipDonationCheck,
	})
}

// ApplyTeamTransforms applies team field transforms to the container.
func (m *RaiselyMapper) ApplyTeamTransforms(page, teamPage FundraisingPage, container FieldMapper) error {
	return ApplyTeamFieldTransforms(m.Config, page, teamPage, container)
}
