package sync

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

// DonationsUpToFetcher fetches donation data up to a specific time for a fundraising profile.
type DonationsUpToFetcher interface {
	FetchDonationsUpTo(p2pid string, upTo time.Time, ctx context.Context) (FundraisingProfileDonationsUpTo, error)
}

// ApplyFundraiserFieldTransformsParams contains parameters for applying fundraiser field transforms.
type ApplyFundraiserFieldTransformsParams struct {
	Config           Config
	Campaign         *FundraisingCampaign
	Destination      Mappable
	Ctx              context.Context
	DonationsFetcher DonationsUpToFetcher
	// SkipDonationCheck when true, logs a warning instead of checking donations for
	// the onlyIfSelfDonatedDuringRegistrationWindow transform. Used for activities.
	SkipDonationCheck bool
}

// ApplyFundraiserFieldTransforms applies configured transforms to a field and maps it to the provided destination.
// This is shared between OrttoContactsMapper and OrttoActivitiesMapper.
func ApplyFundraiserFieldTransforms(params ApplyFundraiserFieldTransformsParams) error {
	if len(params.Config.FundraiserFieldTransforms) == 0 {
		return nil
	}

	fields := params.Destination.GetFields()

	for field, transform := range params.Config.FundraiserFieldTransforms {
		if _, exists := fields[field]; !exists {
			return fmt.Errorf("invalid transform, field %s does not exist", field)
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
			if fieldValue, ok := fields[field].(string); ok {
				for _, defaultObject := range params.Campaign.FundraisingPageDefaults {
					if arg == defaultObject.Label && fieldValue == defaultObject.Value {
						params.Destination.SetField(field, "")
					}
				}
			}

		case "onlyIfNotDefault":
			if fieldValue, ok := fields[field].(string); ok {
				for _, defaultObject := range params.Campaign.FundraisingPageDefaults {
					if arg == defaultObject.Label && fieldValue == defaultObject.Value {
						params.Destination.DeleteField(field)
					}
				}
			}

		case "warnIfEqual":
			if s := fmt.Sprintf("%v", fields[field]); arg == s {
				log.Printf("Warning: %s has value of '%v'\n", field, s)
			}

		case "onlyIfSelfDonatedDuringRegistrationWindow":
			if params.SkipDonationCheck {
				// For activities, skip this transform as it requires complex donation checking
				// that is specific to contacts. The field is kept for activities.
				log.Printf("Warning: transform 'onlyIfSelfDonatedDuringRegistrationWindow' is not supported for activities, field %s kept", field)
				continue
			}

			// Parse the two params: donation amount and registration window duration
			argParams := strings.Split(arg, ",")
			if len(argParams) != 2 {
				return fmt.Errorf("invalid argument %s for transform %s expected two params", arg, transform)
			}

			donationAmount, err := strconv.Atoi(argParams[0])
			if err != nil {
				return fmt.Errorf("invalid first param in argument %s for transform %s %w", arg, transform, err)
			}

			windowDuration, err := time.ParseDuration(argParams[1])
			if err != nil {
				return fmt.Errorf("invalid second param in argument %s for transform %s %w", arg, transform, err)
			}

			registrationDateField := fmt.Sprintf("tme:cm:%s-registration-date", params.Config.CampaignPrefix)
			if registrationDate, ok := fields[registrationDateField].(string); ok {
				registrationTime, err := time.Parse(time.RFC3339, registrationDate)
				if err != nil {
					return fmt.Errorf("failed to parse %s %w", registrationDateField, err)
				}

				p2pRegistrationId := fields[fmt.Sprintf("str:cm:%s-p2p-registration-id", params.Config.CampaignPrefix)].(string)
				donations, err := params.DonationsFetcher.FetchDonationsUpTo(p2pRegistrationId, registrationTime.Add(windowDuration), params.Ctx)
				if err != nil {
					return fmt.Errorf("failed to check self donation totals in registration window %w", err)
				}

				if donations.TotalDonationAmount >= donationAmount {
					continue // if all Ok keep the field
				}
			}
			// default to removing the field
			params.Destination.DeleteField(field)

		default:
			return fmt.Errorf("unsupported transform: %s", transform)
		}
	}

	return nil
}

// ApplyTeamFieldTransforms applies team-specific transforms to a field and maps it to the provided destination.
// This is shared between OrttoContactsMapper and OrttoActivitiesMapper.
func ApplyTeamFieldTransforms(
	config Config,
	teamMemberPage FundraisingPage,
	teamPage FundraisingPage,
	destination Mappable,
) error {
	if len(config.TeamFieldTransforms) == 0 {
		return nil
	}

	captain, err := teamMemberPage.HasSameOwnerAs(teamPage)
	if err != nil {
		return err
	}

	orgType, _ := teamPage.Source.StringForPath("public.organisationType")
	if orgType != "" {
		orgType = strings.ToLower(orgType)
	}

	fields := destination.GetFields()

	for field, transform := range config.TeamFieldTransforms {
		if _, exists := fields[field]; !exists {
			return fmt.Errorf("invalid transform, field %s does not exist", field)
		}

		parts := strings.Split(transform, ":")
		function := parts[0]

		switch function {
		case "isCaptain":
			destination.SetField(field, captain)
		case "isMember":
			destination.SetField(field, !captain)
		case "onlyIfOrgTypeSchool":
			if orgType != "school" {
				destination.DeleteField(field)
			}
		default:
			return fmt.Errorf("unsupported transform: %s", transform)
		}
	}

	return nil
}
