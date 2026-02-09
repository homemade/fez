package sync

import (
	"fmt"
	"path/filepath"
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

func LoadCampaignConfigFromEnvironment(embeddedmappings EmbeddedMappings, campaign string, opts ...ConfigOption) (Config, error) {
	var options configOptions
	for _, opt := range opts {
		opt(&options)
	}

	var result Config

	// Find campaign file and extract target from filename
	campaignMappingFile, target, err := embeddedmappings.MustFindFirstCampaignMappingFileWithTarget(campaign)
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

	// Use campaign mapping file's campaign label as parent for composite env var
	// Strip target suffix from campaign label for env var lookup
	parent := filepath.Base(campaignMappingFile.Name)
	parent = strings.Replace(parent, fmt.Sprintf("%s.", campaign), "", 1)
	parent = strings.Replace(parent, ".yaml", "", 1)
	parent = strings.Replace(parent, ".yml", "", 1)
	if target != "" {
		parent = strings.Replace(parent, fmt.Sprintf(".%s", target), "", 1)
	}

	compositeEnvVar := JSONCompositeEnvVar{Parent: parent}

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
