package sync

import (
	"fmt"
	"path/filepath"
	"strings"
)

func LoadCampaignConfigFromEnvironment(embeddedmappings EmbeddedMappings,
	crmfieldmapper CRMFieldMapper,
	campaign string) (Config, error) {
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

	yamlConfigUnmarshaler := YAMLConfigUnmarshaler{CRMFieldMapper: crmfieldmapper}

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
