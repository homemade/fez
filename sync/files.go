package sync

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
)

type MappingFile struct {
	Name   string
	Reader io.Reader
	Length int
}

type EmbeddedMappings struct {
	Root  string
	Files EmbeddedFS
}

type EmbeddedFS interface {
	Open(name string) (fs.File, error)
	ReadDir(name string) ([]fs.DirEntry, error)
	ReadFile(name string) ([]byte, error)
}

func (em EmbeddedMappings) MustFindRootMappingFile(filename string) (MappingFile, error) {
	var result MappingFile
	name := path.Join(em.Root, filename)
	requiredMappings, err := em.Files.ReadFile(name)
	if err == nil {
		result.Name = name
		result.Reader = bytes.NewReader(requiredMappings)
		result.Length = len(requiredMappings)
	}
	return result, err
}

func (em EmbeddedMappings) MustFindRequiredMappingFile() (MappingFile, error) {
	return em.MustFindRootMappingFile("required.yaml")
}

func (em EmbeddedMappings) MustFindDefaultsMappingFile() (MappingFile, error) {
	return em.MustFindRootMappingFile("defaults.yaml")
}

// MustFindRequiredMappingFileForTarget returns the required mapping file for a specific target.
// For empty target or "ortto-contacts", returns the default "required.yaml".
// For other targets, returns "required.<target>.yaml".
func (em EmbeddedMappings) MustFindRequiredMappingFileForTarget(target string) (MappingFile, error) {
	if target == "" || target == "ortto-contacts" {
		return em.MustFindRootMappingFile("required.yaml")
	}
	return em.MustFindRootMappingFile(fmt.Sprintf("required.%s.yaml", target))
}

// MustFindDefaultsMappingFileForTarget returns the defaults mapping file for a specific target.
// For empty target or "ortto-contacts", returns the default "defaults.yaml".
// For other targets, returns "defaults.<target>.yaml".
func (em EmbeddedMappings) MustFindDefaultsMappingFileForTarget(target string) (MappingFile, error) {
	if target == "" || target == "ortto-contacts" {
		return em.MustFindRootMappingFile("defaults.yaml")
	}
	return em.MustFindRootMappingFile(fmt.Sprintf("defaults.%s.yaml", target))
}

func (em EmbeddedMappings) MustFindFirstCampaignMappingFile(campaign string) (MappingFile, error) {
	result, _, err := em.MustFindFirstCampaignMappingFileWithTarget(campaign)
	return result, err
}

// MustFindFirstCampaignMappingFileWithTarget finds the campaign mapping file and extracts the target
// from the filename. Returns the mapping file, the target (e.g., "ortto-activities"), and any error.
// For legacy files without a target suffix, target will be empty string.
func (em EmbeddedMappings) MustFindFirstCampaignMappingFileWithTarget(campaign string) (MappingFile, string, error) {
	var result MappingFile
	var target string
	dir := path.Join(em.Root, "campaigns")
	files, err := em.Files.ReadDir(dir)
	if err != nil {
		return result, target, err
	}
	for _, file := range files {
		p := file.Name()
		if strings.HasPrefix(p, campaign) {
			// multiple matches are not supported - guard against misconfiguration
			if result.Name != "" {
				err = fmt.Errorf("found multiple mapping files with prefix: %s in dir: %s", campaign, dir)
				return result, target, err
			}

			// Extract target from filename
			target = extractTargetFromFilename(p)

			p = path.Join(dir, p)
			var campaignMappings []byte
			campaignMappings, err = em.Files.ReadFile(p)
			if err == nil {
				result.Name = p
				result.Reader = bytes.NewReader(campaignMappings)
				result.Length = len(campaignMappings)
			}
		}

	}
	if result.Name == "" {
		err = fmt.Errorf("failed to find mapping file with prefix: %s in dir: %s", campaign, dir)
	}
	return result, target, err
}

// knownTargets defines the recognized target suffixes for mapping files
var knownTargets = map[string]bool{
	"ortto-contacts":   true,
	"ortto-activities": true,
}

// extractTargetFromFilename extracts the target from a campaign mapping filename.
// Filename format: <uuid>.<label>[.<target>].yaml
// Returns empty string for legacy files without a target suffix.
func extractTargetFromFilename(filename string) string {
	name := strings.TrimSuffix(filename, ".yaml")
	name = strings.TrimSuffix(name, ".yml")
	parts := strings.Split(name, ".")
	if len(parts) >= 3 {
		lastPart := parts[len(parts)-1]
		if knownTargets[lastPart] {
			return lastPart
		}
	}
	return "" // legacy, defaults to ortto-contacts
}
