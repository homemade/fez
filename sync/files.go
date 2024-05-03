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

func (em EmbeddedMappings) MustFindFirstCampaignMappingFile(campaign string) (MappingFile, error) {
	var result MappingFile
	dir := path.Join(em.Root, "campaigns")
	files, err := em.Files.ReadDir(dir)
	if err != nil {
		return result, err
	}
	for _, file := range files {
		p := file.Name()
		if strings.HasPrefix(p, campaign) {
			// multiple matches are not supported - guard against misconfiguration
			if result.Name != "" {
				err = fmt.Errorf("found multiple mapping files with prefix: %s in dir: %s", campaign, dir)
				return result, err
			}

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
	return result, err
}
