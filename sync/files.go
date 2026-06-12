package sync

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
)

// MappingFileNotFoundError is returned when a campaign mapping file cannot be found.
type MappingFileNotFoundError struct {
	Path string
	Dir  string
}

func (e *MappingFileNotFoundError) Error() string {
	return fmt.Sprintf("failed to find mapping file with path: %s in dir: %s", e.Path, e.Dir)
}

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
	orgDir, fileLabel, err := ParseMappingPath(mappingpath)
	if err != nil {
		return result, target, err
	}
	dir := path.Join(em.Root, orgDir)

	var files []fs.DirEntry
	files, err = em.Files.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return result, target, &MappingFileNotFoundError{Path: mappingpath, Dir: dir}
		}
		return result, target, err
	}
	for _, file := range files {
		p := file.Name()
		name := strings.TrimSuffix(p, ".yaml")
		name = strings.TrimSuffix(name, ".yml")

		// Tightened match: the stem must be exactly <fileLabel> (legacy, no
		// target suffix) or <fileLabel>.<knownTarget>. Anything with extra
		// dot-separated segments — e.g. <fileLabel>.<knownTarget>.referrals
		// — is a companion file and must not compete for the target slot.
		var matchedTarget string
		switch {
		case name == fileLabel:
			matchedTarget = ""
		case strings.HasPrefix(name, fileLabel+"."):
			rest := strings.TrimPrefix(name, fileLabel+".")
			if !knownTargets[rest] {
				continue
			}
			matchedTarget = rest
		default:
			continue
		}

		// multiple matches are not supported - guard against misconfiguration
		if result.Name != "" {
			err = fmt.Errorf("found multiple mapping files with path: %s in dir: %s", mappingpath, dir)
			return result, target, err
		}

		target = matchedTarget

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
		err = &MappingFileNotFoundError{Path: mappingpath, Dir: dir}
	}
	return result, target, err
}

// knownTargets defines the recognized target suffixes for mapping files
var knownTargets = map[string]bool{
	"ortto-contacts":   true,
	"ortto-activities": true,
}

// FindReferralsCompanionMappingFileByPath looks for a referrals
// companion file alongside the campaign mapping file at <mappingpath>.
// The companion filename is <label>[.<target>].referrals.yaml.
// Returns a zero MappingFile and a nil error if the companion does not
// exist (the companion is optional).
func (em EmbeddedMappings) FindReferralsCompanionMappingFileByPath(mappingpath, target string) (MappingFile, error) {
	var result MappingFile
	orgDir, label, err := ParseMappingPath(mappingpath)
	if err != nil {
		return result, err
	}
	dir := path.Join(em.Root, orgDir)

	companionName := label + ".referrals.yaml"
	if target != "" {
		companionName = label + "." + target + ".referrals.yaml"
	}

	fullpath := path.Join(dir, companionName)
	companionBytes, err := em.Files.ReadFile(fullpath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return result, nil
		}
		return result, err
	}
	result.Name = fullpath
	result.Reader = bytes.NewReader(companionBytes)
	result.Length = len(companionBytes)
	return result, nil
}
