package sync

import (
	"fmt"
	"path"
	"strings"

	"gopkg.in/yaml.v2"
)

// CSVEnrichmentColumn represents a single header → attribute entry.
type CSVEnrichmentColumn struct {
	Header    string
	Attribute string
}

// CSVEnrichmentMapping represents the parsed contents of a CSV enrichment mapping YAML file.
// Column order is preserved from the YAML file.
type CSVEnrichmentMapping struct {
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

	var result CSVEnrichmentMapping
	for _, section := range top {
		key, _ := section.Key.(string)
		if key != "columns" {
			continue
		}
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

	return result, nil
}
