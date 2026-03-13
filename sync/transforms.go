package sync

import (
	"fmt"
	"log"
	"strings"
)

// ApplyFundraiserFieldTransformsParams contains parameters for applying fundraiser field transforms.
type ApplyFundraiserFieldTransformsParams struct {
	Config      Config
	Campaign    *FundraisingCampaign
	Destination Mappable
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
			log.Println("Warning: transform 'blankIfDefault' is deprecated please switch to using 'onlyIfNotDefault' instead")
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

		case "toUpper":
			if fieldValue, ok := fields[field].(string); ok {
				params.Destination.SetField(field, strings.ToUpper(fieldValue))
			}

		case "toLower":
			if fieldValue, ok := fields[field].(string); ok {
				params.Destination.SetField(field, strings.ToLower(fieldValue))
			}

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
	teammemberpage FundraisingPage,
	teampage FundraisingPage,
	destination Mappable,
) error {
	if len(config.TeamFieldTransforms) == 0 {
		return nil
	}

	captain, err := teammemberpage.HasSameOwnerAs(teampage)
	if err != nil {
		return err
	}

	orgType, _ := teampage.Source.StringForPath("public.organisationType")
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
