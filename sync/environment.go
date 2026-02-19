package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// configOptions holds optional configuration for LoadCampaignConfigFromEnvironment.
type configOptions struct {
	crmFieldMapper CRMFieldMapper
}

// ConfigOption is a functional option for configuring LoadCampaignConfigFromEnvironment.
type ConfigOption func(*configOptions)

// ConfigWithCRMFieldMapper sets the CRMFieldMapper for expanding field mappings.
// This is required for Ortto integration but not needed for Raisely-only use cases.
func ConfigWithCRMFieldMapper(mapper CRMFieldMapper) ConfigOption {
	return func(o *configOptions) {
		o.crmFieldMapper = mapper
	}
}

// FindCampaignEnvVar scans environment variables for a JSON value containing
// a campaignUUIDKey key matching the given campaignUUID.
// The campaignUUIDKey parameter specifies the JSON key to look for
// (e.g. "RAISELY_CAMPAIGN_UUID" for the Raisely2Ortto flavour).
// Returns the env var name and the MAPPING_LABEL value.
// Returns an error if multiple env vars match the same UUID, or if MAPPING_LABEL is missing.
func FindCampaignEnvVar(campaignUUIDKey string, campaignUUID string) (envVarName string, mappingLabel string, err error) {
	type match struct {
		name  string
		label string
	}
	var matches []match

	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]

		var m map[string]string
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			// Most env vars are plain strings (e.g. PATH), not JSON — skip those silently
			continue
		}

		uuid, ok := m[campaignUUIDKey]
		if !ok || uuid != campaignUUID {
			continue
		}

		label, ok := m["MAPPING_LABEL"]
		if !ok || label == "" {
			return "", "", fmt.Errorf("env var %q contains %s but is missing MAPPING_LABEL", name, campaignUUIDKey)
		}

		matches = append(matches, match{name: name, label: label})
	}

	if len(matches) == 0 {
		return "", "", nil
	}
	if len(matches) > 1 {
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.name
		}
		return "", "", fmt.Errorf("found multiple env vars with %s %q: %s", campaignUUIDKey, campaignUUID, strings.Join(names, ", "))
	}

	return matches[0].name, matches[0].label, nil
}

// campaignUUIDKeyForFlavour returns the JSON key used to identify campaign UUIDs
// in environment variables for the given flavour.
func campaignUUIDKeyForFlavour(flavour Flavour) (string, error) {
	switch flavour {
	case Raisely2Ortto:
		return "RAISELY_CAMPAIGN_UUID", nil
	default:
		return "", fmt.Errorf("unsupported flavour %v", flavour)
	}
}

// CampaignEnvVar represents a campaign environment variable with its label and UUID.
type CampaignEnvVar struct {
	Label string
	UUID  string
}

// FindAllCampaignEnvVars scans environment variables for JSON values containing
// a campaign UUID key (determined by the initialised flavour) and a MAPPING_LABEL.
// Returns a map of label -> UUID for all matching env vars.
func FindAllCampaignEnvVars() (map[string]CampaignEnvVar, error) {
	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		return nil, err
	}

	result := make(map[string]CampaignEnvVar)
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		value := parts[1]

		var m map[string]string
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			continue
		}

		label, hasLabel := m["MAPPING_LABEL"]
		uuid, hasUUID := m[campaignUUIDKey]
		if hasLabel && hasUUID {
			result[label] = CampaignEnvVar{Label: label, UUID: uuid}
		}
	}
	return result, nil
}

func LoadCampaignConfigFromEnvironment(embeddedmappings EmbeddedMappings, campaign string, opts ...ConfigOption) (Config, error) {
	mustBeInitialised()

	var options configOptions
	for _, opt := range opts {
		opt(&options)
	}

	var result Config
	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		return result, err
	}
	envVarName, mappingLabel, err := FindCampaignEnvVar(campaignUUIDKey, campaign)
	if err != nil {
		return result, fmt.Errorf("failed to find campaign env var %w", err)
	}
	if envVarName == "" {
		return result, fmt.Errorf("no env var found with %s %q", campaignUUIDKey, campaign)
	}

	// Use mapping label to find file
	campaignMappingFile, target, err := embeddedmappings.MustFindFirstCampaignMappingFileWithTargetByLabel(mappingLabel)
	if err != nil {
		return result, fmt.Errorf("failed to read campaign mapping file %w", err)
	}

	// Load required and defaults for this target
	requiredMappingFile, err := embeddedmappings.MustFindRequiredMappingFileForTarget(target)
	if err != nil {
		return result, fmt.Errorf("failed to read required mapping file %w", err)
	}

	defaultsMappingFile, err := embeddedmappings.MustFindDefaultsMappingFileForTarget(target)
	if err != nil {
		return result, fmt.Errorf("failed to read defaults mapping file %w", err)
	}

	compositeEnvVar := JSONCompositeEnvVar{Parent: envVarName}

	yamlConfigUnmarshaler := YAMLConfigUnmarshaler{CRMFieldMapper: options.crmFieldMapper}

	// Load config for this campaign
	result, err = yamlConfigUnmarshaler.Unmarshal(
		compositeEnvVar,
		requiredMappingFile,
		defaultsMappingFile,
		campaignMappingFile,
	)
	if err != nil {
		return result, fmt.Errorf("failed to load config %w", err)
	}

	// Store target in config
	result.Target = target

	return result, nil
}
