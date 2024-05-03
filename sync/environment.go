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

	requiredMappingFile, err := embeddedmappings.MustFindRequiredMappingFile()
	if err != nil {
		return result, fmt.Errorf("failed to read required mapping file %w", err)
	}

	var defaultsMappingFile MappingFile
	defaultsMappingFile, err = embeddedmappings.MustFindDefaultsMappingFile()
	if err != nil {
		return result, fmt.Errorf("failed to read defaults mapping file %w", err)
	}

	var campaignMappingFile MappingFile
	campaignMappingFile, err = embeddedmappings.MustFindFirstCampaignMappingFile(campaign)
	if err != nil {
		return result, fmt.Errorf("failed to read campaign mapping file %w", err)
	}

	// Use campaign mapping file's campaign label as parent for composite env var
	parent := filepath.Base(campaignMappingFile.Name)
	parent = strings.Replace(parent, fmt.Sprintf("%s.", campaign), "", 1)
	parent = strings.Replace(parent, ".yaml", "", 1)
	parent = strings.Replace(parent, ".yml", "", 1)

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

	return result, nil
}
