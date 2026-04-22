package sync

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
)

// CSVEnrichmentColumn represents a single header → attribute entry.
type CSVEnrichmentColumn struct {
	Header    string
	Attribute string
}

// CSVEnrichmentPick selects a specific activity from a contact's feed when
// CSVEnrichmentMapping.Mode is ActivityFeedFirstMatch or ActivityFeedLatestMatch.
// For ActivityFeedFirstMatch the feed is walked oldest-first; for ActivityFeedLatestMatch
// the feed is walked newest-first (natural API order). The first activity whose Attribute
// equals Equals is used. Comparisons are made on the stringified value, so YAML booleans
// and numbers match their JSON counterparts.
type CSVEnrichmentPick struct {
	Attribute string
	Equals    interface{}
}

// CSVEnrichmentMapping represents the parsed contents of a CSV enrichment mapping YAML file.
// Column order is preserved from the YAML file.
type CSVEnrichmentMapping struct {
	Mode    ActivityFeedMode
	Pick    *CSVEnrichmentPick
	Columns []CSVEnrichmentColumn
}

// AttributeSet returns the set of top-level attribute names referenced by the mapping,
// suitable for filtering in EnrichCSVRows. For dotted attribute references such as
// "cdp-fields.city.name", only the top-level segment ("cdp-fields") is included — the
// enricher filters by top-level name then flattens nested objects.
func (f CSVEnrichmentMapping) AttributeSet() map[string]bool {
	set := make(map[string]bool, len(f.Columns))
	for _, c := range f.Columns {
		top := c.Attribute
		if i := strings.Index(top, "."); i >= 0 {
			top = top[:i]
		}
		set[top] = true
	}
	return set
}

// OrderedHeaders returns the column headers in mapping-file order.
func (f CSVEnrichmentMapping) OrderedHeaders() []string {
	headers := make([]string, len(f.Columns))
	for i, c := range f.Columns {
		headers[i] = c.Header
	}
	return headers
}

// OrderedAttributes returns the attribute names (full dotted form where applicable)
// in mapping-file order.
func (f CSVEnrichmentMapping) OrderedAttributes() []string {
	attrs := make([]string, len(f.Columns))
	for i, c := range f.Columns {
		attrs[i] = c.Attribute
	}
	return attrs
}

// LoadCSVEnrichmentMapping loads and parses a CSV enrichment mapping YAML file from
// embedded mappings. The file is located at
// "<mappings.Root>/<mappingPath>.<purpose>.ortto-activities.yaml".
// Key order within the "columns" section is preserved.
//
// Top-level keys:
//   - mode: "latest" (default), "first-match", or "latest-match"
//   - pick: { attribute: <name>, equals: <value> } — required when mode is "first-match"
//     or "latest-match"
//   - columns: ordered header → attribute map
func LoadCSVEnrichmentMapping(mappings EmbeddedMappings, mappingPath string, purpose string) (CSVEnrichmentMapping, error) {
	filename := mappingPath + "." + purpose + ".ortto-activities.yaml"
	fullpath := path.Join(mappings.Root, filename)
	data, err := mappings.Files.ReadFile(fullpath)
	if err != nil {
		return CSVEnrichmentMapping{}, err
	}

	var top yaml.MapSlice
	if err := yaml.Unmarshal(data, &top); err != nil {
		return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: %w", fullpath, err)
	}

	result := CSVEnrichmentMapping{Mode: ActivityFeedLatest}
	for _, section := range top {
		key, _ := section.Key.(string)
		switch key {
		case "mode":
			modeStr, ok := section.Value.(string)
			if !ok {
				return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: \"mode\" must be a string", fullpath)
			}
			switch modeStr {
			case "", "latest":
				result.Mode = ActivityFeedLatest
			case "first-match":
				result.Mode = ActivityFeedFirstMatch
			case "latest-match":
				result.Mode = ActivityFeedLatestMatch
			default:
				return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: unknown mode %q (expected \"latest\", \"first-match\", or \"latest-match\")", fullpath, modeStr)
			}
		case "pick":
			pickMap, ok := section.Value.(yaml.MapSlice)
			if !ok {
				return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: \"pick\" must be a mapping", fullpath)
			}
			pick := &CSVEnrichmentPick{}
			for _, item := range pickMap {
				k, _ := item.Key.(string)
				switch k {
				case "attribute":
					s, _ := item.Value.(string)
					pick.Attribute = s
				case "equals":
					pick.Equals = item.Value
				}
			}
			if pick.Attribute == "" {
				return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: \"pick.attribute\" is required", fullpath)
			}
			result.Pick = pick
		case "columns":
			columns, ok := section.Value.(yaml.MapSlice)
			if !ok {
				return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: \"columns\" must be a mapping", fullpath)
			}
			for _, item := range columns {
				header, _ := item.Key.(string)
				attribute, _ := item.Value.(string)
				result.Columns = append(result.Columns, CSVEnrichmentColumn{
					Header:    header,
					Attribute: attribute,
				})
			}
		}
	}

	if (result.Mode == ActivityFeedFirstMatch || result.Mode == ActivityFeedLatestMatch) && result.Pick == nil {
		modeName := "first-match"
		if result.Mode == ActivityFeedLatestMatch {
			modeName = "latest-match"
		}
		return CSVEnrichmentMapping{}, fmt.Errorf("failed to parse CSV enrichment mapping %s: \"pick\" is required when mode is %q", fullpath, modeName)
	}

	return result, nil
}

// ListCSVEnrichmentPurposes returns the available enrichment purposes for a campaign
// by scanning the embedded mappings directory for files of the form
// "<mappingPath>.<purpose>.ortto-activities.yaml".
// Returns an empty slice (and no error) if the org directory has no matching files.
func ListCSVEnrichmentPurposes(mappings EmbeddedMappings, mappingPath string) ([]string, error) {
	index := strings.LastIndex(mappingPath, "/")
	if index == -1 {
		return nil, fmt.Errorf("invalid mapping path %q: must contain org directory (e.g. ORG/LABEL)", mappingPath)
	}
	dir := path.Join(mappings.Root, mappingPath[:index])
	label := mappingPath[index+1:]

	entries, err := mappings.Files.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}

	const suffix = ".ortto-activities.yaml"
	prefix := label + "."

	var purposes []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		purpose := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if purpose == "" {
			continue
		}
		purposes = append(purposes, purpose)
	}
	sort.Strings(purposes)
	return purposes, nil
}
