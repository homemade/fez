package sync

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"go.uber.org/config"
)

type Config struct {
	// Target identifies the integration target (e.g., "", "ortto-contacts", "ortto-activities").
	// Empty string indicates legacy files which default to ortto-contacts behavior.
	Target                  string
	API                     APISettings
	CampaignPrefix          string
	FundraiserFieldMappings struct {
		Builtin FieldMappings
		Custom  FieldMappings
	}
	FundraiserFieldTransforms map[string]string
	TeamFieldMappings         struct {
		Custom FieldMappings
	}
	TeamFieldTransforms  map[string]string
	FundraiserExtensions FundraiserExtensionsConfig
	TeamExtensions       TeamExtensionsConfig
}

type APISettings struct {
	Keys struct {
		Raisely   string
		Funraisin string
		Ortto     string
	}
	// Ids contains API identifiers needed for syncing.
	Ids struct {
		Ortto string // e.g. Activity ID if Target is "ortto-activities" (Ortto Activities API)
	}
	Endpoints struct {
		Ortto string
	}
}

type FundraiserExtensionsConfig struct {
	Streaks struct {
		Donation struct {
			Days    []int
			Mapping string
		}
		Activity struct {
			From    string
			To      string
			Filter  []string
			Days    []int
			Mapping string
		}
	}
	SplitExerciseTotals SplitExerciseTotals `yaml:"splitExerciseTotals"`
}

type TeamExtensionsConfig struct {
	SplitExerciseTotals SplitExerciseTotals `yaml:"splitExerciseTotals"`
}

type SplitExerciseTotals struct {
	From     string
	Mappings []string
}

func (s SplitExerciseTotals) IsConfigured() bool {
	return s.From != "" && len(s.Mappings) == 2
}

type FieldMappings struct {
	Strings    map[string]string
	Texts      map[string]string
	Decimals   map[string]string
	Booleans   map[string]string
	Timestamps map[string]string
	Phones     map[string]map[string]string
	Geos       map[string]map[string]string
	Integers   map[string]string
}

func (m FieldMappings) AllKeys() []string {
	var result []string
	result = append(result, FieldMapsKeys(m.Strings)...)
	result = append(result, FieldMapsKeys(m.Texts)...)
	result = append(result, FieldMapsKeys(m.Decimals)...)
	result = append(result, FieldMapsKeys(m.Booleans)...)
	result = append(result, FieldMapsKeys(m.Timestamps)...)
	result = append(result, NestedFieldMapsKeys(m.Phones)...)
	result = append(result, NestedFieldMapsKeys(m.Geos)...)
	result = append(result, FieldMapsKeys(m.Integers)...)
	return result
}

func (m FieldMappings) AllValues() []string {
	var result []string
	result = append(result, FieldMapsValues(m.Strings)...)
	result = append(result, FieldMapsValues(m.Texts)...)
	result = append(result, FieldMapsValues(m.Decimals)...)
	result = append(result, FieldMapsValues(m.Booleans)...)
	result = append(result, FieldMapsValues(m.Timestamps)...)
	result = append(result, NestedFieldMapsValues(m.Phones)...)
	result = append(result, NestedFieldMapsValues(m.Geos)...)
	result = append(result, FieldMapsValues(m.Integers)...)
	return result
}

func (m FieldMappings) AsOrttoFieldType(key string) string {
	if m.Strings != nil {
		if _, exists := m.Strings[key]; exists {
			return "Text"
		}
	}
	if m.Texts != nil {
		if _, exists := m.Texts[key]; exists {
			return "Long text"
		}
	}
	if m.Decimals != nil {
		if _, exists := m.Decimals[key]; exists {
			return "Decimal number"
		}
	}
	if m.Booleans != nil {
		if _, exists := m.Booleans[key]; exists {
			return "Boolean"
		}
	}
	if m.Timestamps != nil {
		if _, exists := m.Timestamps[key]; exists {
			return "Time and date"
		}
	}
	if m.Phones != nil {
		if _, exists := m.Phones[key]; exists {
			return "Phone number"
		}
	}
	if m.Geos != nil {
		if _, exists := m.Geos[key]; exists {
			return "Geo"
		}
	}
	if m.Integers != nil {
		if _, exists := m.Integers[key]; exists {
			return "Number"
		}
	}
	return "Unknown"
}

func FieldMapsKeys(m map[string]string) []string {
	result := make([]string, len(m))
	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	return result
}

func FieldMapsValues(m map[string]string) []string {
	result := make([]string, len(m))
	i := 0
	for _, v := range m {
		result[i] = v
		i++
	}
	return result
}

func NestedFieldMapsKeys(m map[string]map[string]string) []string {
	result := make([]string, len(m))
	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	return result
}

func NestedFieldMapsValues(m map[string]map[string]string) []string {
	var result []string
	for _, v := range m {
		for _, s := range v {
			result = append(result, s)
		}
	}
	return result
}

type SimpleFieldType int64

const (
	String SimpleFieldType = iota
	Text
	Decimal
	Boolean
	Timestamp
	Integer
)

type NestedFieldType int64

const (
	Phone NestedFieldType = iota
	Geo
)

type ConfigUnmarshaler interface {
	Unmarshal(compev CompositeEnvVar, sources ...io.Reader) (Config, error)
}

type CompositeEnvVar interface {
	LookupEnv(child string) (string, bool)
}

type JSONCompositeEnvVar struct {
	Parent string
}

func (c JSONCompositeEnvVar) LookupEnv(child string) (string, bool) {
	if c.Parent != "" {
		s := os.Getenv(c.Parent)
		if s != "" {
			m := make(map[string]string)
			err := json.Unmarshal([]byte(s), &m)
			if err == nil {
				v, exists := m[child]
				return v, exists
			}
		}
	}
	return "", false
}

type YAMLConfigUnmarshaler struct {
	CRMFieldMapper CRMFieldMapper
}

type CRMFieldMapper interface {
	ExpandFieldMappings(mappings *FieldMappings, custom bool) error
}

func (u YAMLConfigUnmarshaler) Unmarshal(compev CompositeEnvVar, sources ...MappingFile) (Config, error) {
	var result Config
	var options []config.YAMLOption
	for _, s := range sources {
		if s.Length > 0 {
			options = append(options, config.Source(s.Reader))
		}
	}
	options = append(options, config.Expand(compev.LookupEnv))
	yaml, err := config.NewYAML(options...)
	if err != nil {
		return result, fmt.Errorf("failed to read yaml config %w", err)
	}
	readError := func(key string, cause error) error {
		return fmt.Errorf("failed to read '%s' from yaml config %w", key, cause)
	}
	key := "api"
	err = yaml.Get(key).Populate(&result.API)
	if err != nil {
		return result, readError(key, err)
	}
	key = "campaignPrefix"
	result.CampaignPrefix = yaml.Get(key).String()
	key = "fundraiserFieldMappings"
	err = yaml.Get(key).Populate(&result.FundraiserFieldMappings)
	if err != nil {
		return result, readError(key, err)
	}
	key = "fundraiserFieldTransforms"
	if yaml.Get(key).HasValue() {
		err = yaml.Get(key).Populate(&result.FundraiserFieldTransforms)
		if err != nil {
			return result, readError(key, err)
		}
	}
	key = "teamFieldMappings"
	err = yaml.Get(key).Populate(&result.TeamFieldMappings)
	if err != nil {
		return result, readError(key, err)
	}
	key = "teamFieldTransforms"
	if yaml.Get(key).HasValue() {
		err = yaml.Get(key).Populate(&result.TeamFieldTransforms)
		if err != nil {
			return result, readError(key, err)
		}
	}
	key = "fundraiserExtensions"
	err = yaml.Get(key).Populate(&result.FundraiserExtensions)
	if err != nil {
		return result, readError(key, err)
	}
	key = "teamExtensions"
	err = yaml.Get(key).Populate(&result.TeamExtensions)
	if err != nil {
		return result, readError(key, err)
	}

	// Only expand field mappings if CRMFieldMapper is provided.
	// This allows loading config for Raisely-only use cases (like extensions)
	// without requiring Ortto/CRM dependencies.
	if u.CRMFieldMapper != nil {
		err = u.CRMFieldMapper.ExpandFieldMappings(&result.FundraiserFieldMappings.Builtin, false)
		if err == nil {
			err = u.CRMFieldMapper.ExpandFieldMappings(&result.FundraiserFieldMappings.Custom, true)
		}
		if err == nil {
			err = u.CRMFieldMapper.ExpandFieldMappings(&result.TeamFieldMappings.Custom, true)
		}
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (c Config) MapActivityLogs() bool {
	if len(c.FundraiserExtensions.Streaks.Activity.Days) > 0 {
		return true
	}
	return false
}

func (c Config) MapDonations() bool {
	if len(c.FundraiserExtensions.Streaks.Donation.Days) > 0 {
		return true
	}
	return false
}
