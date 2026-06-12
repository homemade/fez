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
// a campaignUUIDKey key matching the given campaignUUID and returns the full
// CampaignEnvVar (Name, Path, UUID, parsed Config map).
//
// The campaignUUIDKey parameter specifies the JSON key to look for
// (e.g. "RAISELY_CAMPAIGN_UUID" for the Raisely2Ortto flavour).
//
// Returns a zero CampaignEnvVar with nil error if no env var matches.
// Returns an error if multiple env vars match the same UUID, or if MAPPING_PATH
// is missing.
func FindCampaignEnvVar(campaignUUIDKey string, campaignUUID string) (CampaignEnvVar, error) {
	var matches []CampaignEnvVar

	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]

		if !strings.HasPrefix(name, CampaignEnvVarPrefix) {
			continue
		}

		var m map[string]string
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			return CampaignEnvVar{}, fmt.Errorf("env var %q has %s prefix but contains invalid JSON: %w", name, CampaignEnvVarPrefix, err)
		}

		uuid, ok := m[campaignUUIDKey]
		if !ok || uuid != campaignUUID {
			continue
		}

		p, ok := m["MAPPING_PATH"]
		if !ok || p == "" {
			return CampaignEnvVar{}, fmt.Errorf("env var %q contains %s but is missing MAPPING_PATH", name, campaignUUIDKey)
		}

		matches = append(matches, CampaignEnvVar{Name: name, Path: p, UUID: uuid, Config: m})
	}

	if len(matches) == 0 {
		return CampaignEnvVar{}, nil
	}
	if len(matches) > 1 {
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.Name
		}
		return CampaignEnvVar{}, fmt.Errorf("found multiple env vars with %s %q: %s", campaignUUIDKey, campaignUUID, strings.Join(names, ", "))
	}

	return matches[0], nil
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

// CampaignEnvVarPrefix is the required prefix for campaign environment variables.
// This distinguishes campaign config vars from other env vars (e.g. PATH)
const CampaignEnvVarPrefix = "FEZ_"

// CampaignEnvVar represents a campaign environment variable with its path and UUID.
type CampaignEnvVar struct {
	Name   string            // Env var name (e.g. "FEZ_ORG_LABEL")
	Path   string            // MAPPING_PATH value (e.g. "ORG/LABEL")
	UUID   string            // Campaign UUID
	Config map[string]string // Full parsed JSON config from the env var
}

// FindAllCampaignEnvVars scans environment variables for JSON values containing
// a campaign UUID key (determined by the initialised flavour) and a MAPPING_PATH.
// Returns one entry per matching env var.
func FindAllCampaignEnvVars() ([]CampaignEnvVar, error) {
	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		return nil, err
	}

	var result []CampaignEnvVar
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]

		if !strings.HasPrefix(name, CampaignEnvVarPrefix) {
			continue
		}

		var m map[string]string
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			return nil, fmt.Errorf("env var %q has %s prefix but contains invalid JSON: %w", name, CampaignEnvVarPrefix, err)
		}

		p, hasPath := m["MAPPING_PATH"]
		uuid, hasUUID := m[campaignUUIDKey]
		if hasPath && hasUUID {
			result = append(result, CampaignEnvVar{Name: name, Path: p, UUID: uuid, Config: m})
		}
	}
	return result, nil
}

func LoadCampaignConfigFromEnvironment(embeddedMappings EmbeddedMappings, campaign string, opts ...ConfigOption) (Config, error) {
	mustBeInitialised()

	var options configOptions
	for _, opt := range opts {
		opt(&options)
	}

	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		return Config{}, err
	}
	envVar, err := FindCampaignEnvVar(campaignUUIDKey, campaign)
	if err != nil {
		return Config{}, fmt.Errorf("failed to find campaign env var %w", err)
	}
	if envVar.Name == "" {
		return Config{}, fmt.Errorf("no env var found with %s %q", campaignUUIDKey, campaign)
	}

	return loadCampaignConfig(embeddedMappings, envVar, JSONCompositeEnvVar{Parent: envVar.Name}, options.crmFieldMapper)
}

// MapCompositeEnvVar implements CompositeEnvVar using an in-memory map.
// Used by LoadCampaignConfigFromJSON to provide config values directly
// instead of reading from environment variables.
type MapCompositeEnvVar struct {
	Values map[string]string
}

func (c MapCompositeEnvVar) LookupEnv(child string) (string, bool) {
	v, ok := c.Values[child]
	return v, ok
}

// LoadCampaignConfigFromJSON loads campaign configuration from an in-memory
// config map (the same JSON structure stored in FEZ_ env vars) and embedded
// YAML mapping files. This is used by admin API routes where the config is
// received in the request body rather than read from environment variables.
func LoadCampaignConfigFromJSON(embeddedMappings EmbeddedMappings, configJSON map[string]string, opts ...ConfigOption) (Config, error) {
	mustBeInitialised()

	var options configOptions
	for _, opt := range opts {
		opt(&options)
	}

	mappingPath := configJSON["MAPPING_PATH"]
	if mappingPath == "" {
		return Config{}, fmt.Errorf("MAPPING_PATH is required")
	}

	// Synthesize a CampaignEnvVar from the provided JSON: there is no
	// real underlying env var, so Name is empty. UUID is read via the
	// flavour key when present (admin paths may omit it). Config carries
	// the full input map so consumers retain access to extra keys.
	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		return Config{}, err
	}
	envVar := CampaignEnvVar{
		Name:   "",
		Path:   mappingPath,
		UUID:   configJSON[campaignUUIDKey],
		Config: configJSON,
	}

	return loadCampaignConfig(embeddedMappings, envVar, MapCompositeEnvVar{Values: configJSON}, options.crmFieldMapper)
}

// loadCampaignConfig is the shared file-load + unmarshal + validation
// pipeline used by both LoadCampaignConfigFromEnvironment and
// LoadCampaignConfigFromJSON. Callers only differ in how they resolve
// envVar and which CompositeEnvVar implementation supplies config
// values. The CampaignEnvVar is stamped onto the result as Config.EnvVar
// so consumers can derive org/label without re-scanning the environment.
func loadCampaignConfig(embeddedMappings EmbeddedMappings, envVar CampaignEnvVar, compositeEnvVar CompositeEnvVar, crmFieldMapper CRMFieldMapper) (Config, error) {
	var result Config
	mappingPath := envVar.Path

	campaignMappingFile, target, err := embeddedMappings.MustFindFirstCampaignMappingFileWithTargetByPath(mappingPath)
	if err != nil {
		return result, fmt.Errorf("failed to read campaign mapping file %w", err)
	}

	requiredMappingFile, err := embeddedMappings.MustFindRequiredMappingFileForTarget(target)
	if err != nil {
		return result, fmt.Errorf("failed to read required mapping file %w", err)
	}

	defaultsMappingFile, err := embeddedMappings.MustFindDefaultsMappingFileForTarget(target)
	if err != nil {
		return result, fmt.Errorf("failed to read defaults mapping file %w", err)
	}

	// Optional referrals companion file (Raisely Custom Messages mapping)
	referralsCompanionFile, err := embeddedMappings.FindReferralsCompanionMappingFileByPath(mappingPath, target)
	if err != nil {
		return result, fmt.Errorf("failed to read referrals companion mapping file %w", err)
	}

	sources := []MappingFile{requiredMappingFile, defaultsMappingFile, campaignMappingFile}
	if referralsCompanionFile.Length > 0 {
		sources = append(sources, referralsCompanionFile)
	}

	result, err = YAMLConfigUnmarshaler{CRMFieldMapper: crmFieldMapper}.Unmarshal(compositeEnvVar, sources...)
	if err != nil {
		return result, fmt.Errorf("failed to load config %w", err)
	}

	result.Target = target
	result.EnvVar = envVar

	// Validate: if referrals trigger is set, the companion file is required.
	if result.API.Settings.RaiselyFundraiserReferralsField != "" && referralsCompanionFile.Length == 0 {
		return result, fmt.Errorf("raiselyFundraiserReferralsField is set but no referrals companion mapping file was found at %s.referrals.yaml", mappingPath)
	}

	return result, nil
}
