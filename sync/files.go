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

// MustFindFirstCampaignMappingFileWithTargetByPath finds a campaign mapping file by its path.
// The path must be in the format "<org>/<label>" where <org> is the org directory
// and <label> must exactly match the first dot-separated segment of the filename.
// Filename format: <LABEL>[.<target>].yaml
// Returns the mapping file, the target, and any error.
func (em EmbeddedMappings) MustFindFirstCampaignMappingFileWithTargetByPath(mappingpath string) (result MappingFile, target string, err error) {
	// Split path into org directory and file label
	// e.g. "STAR/SSS_V001" → dir: "<root>/STAR", fileLabel: "SSS_V001"
	index := strings.LastIndex(mappingpath, "/")
	if index == -1 {
		return result, target, fmt.Errorf("invalid mapping path %q: must contain org directory (e.g. ORG/LABEL)", mappingpath)
	}
	dir := path.Join(em.Root, mappingpath[:index])
	fileLabel := mappingpath[index+1:]

	var files []fs.DirEntry
	files, err = em.Files.ReadDir(dir)
	if err != nil {
		return result, target, err
	}
	for _, file := range files {
		p := file.Name()
		name := strings.TrimSuffix(p, ".yaml")
		name = strings.TrimSuffix(name, ".yml")
		parts := strings.SplitN(name, ".", 2)
		if parts[0] != fileLabel {
			continue
		}

		// multiple matches are not supported - guard against misconfiguration
		if result.Name != "" {
			err = fmt.Errorf("found multiple mapping files with path: %s in dir: %s", mappingpath, dir)
			return result, target, err
		}

		// Extract target from filename
		target = extractTargetFromFilename(p)

		fullpath := path.Join(dir, p)
		var campaignMappings []byte
		campaignMappings, err = em.Files.ReadFile(fullpath)
		if err == nil {
			result.Name = fullpath
			result.Reader = bytes.NewReader(campaignMappings)
			result.Length = len(campaignMappings)
		}
	}
	if result.Name == "" {
		err = fmt.Errorf("failed to find mapping file with path: %s in dir: %s", mappingpath, dir)
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
	if len(parts) >= 2 {
		lastPart := parts[len(parts)-1]
		if knownTargets[lastPart] {
			return lastPart
		}
	}
	return "" // legacy, defaults to ortto-contacts
}
