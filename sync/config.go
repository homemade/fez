package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"go.uber.org/config"
)

var YAMLConfigUnmarshaler = yamlConfigUnmarshaler{}
var JSONCompositeEnvVar = &jsonCompositeEnvVar{}

type Config struct {
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
	TeamFieldTransforms        map[string]string
	FundraiserBadgeExtensions  FundraiserBadgeExtensionsConfig
	FundraiserUpdateExtensions FundraiserUpdateExtensionsConfig
}

type APISettings struct {
	Keys struct {
		Raisely string
		Ortto   string
	}
	Endpoints struct {
		Ortto string
	}
}

type FundraiserBadgeExtensionsConfig struct {
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
}

type FundraiserUpdateExtensionsConfig struct {
	EnhancedExerciseTotals struct {
		PreEventTotal  string `yaml:"preEventTotal"`
		InEventTotal   string `yaml:"inEventTotal"`
		PostEventTotal string `yaml:"postEventTotal"`
		EventStart     string `yaml:"eventStart"`
		EventEnd       string `yaml:"eventEnd"`
	} `yaml:"enhancedExerciseTotals"`
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

func NestedFieldMapsKeys(m map[string]map[string]string) []string {
	result := make([]string, len(m))
	i := 0
	for k := range m {
		result[i] = k
		i++
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
	SetParent(parent string)
	LookupEnv(child string) (string, bool)
}

type jsonCompositeEnvVar struct {
	parent string
}

func (c *jsonCompositeEnvVar) SetParent(parent string) {
	c.parent = parent
}

func (c jsonCompositeEnvVar) LookupEnv(child string) (string, bool) {
	if c.parent != "" {
		s := os.Getenv(c.parent)
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

type MappingFile struct {
	Name   string
	Reader io.Reader
	Length int
}

func MustFindRequiredMappingFile(mappingsdir string) (MappingFile, error) {
	var result MappingFile
	name := path.Join(mappingsdir, "required.yaml")
	requiredMappings, err := Mappings.ReadFile(name)
	if err == nil {
		result.Name = name
		result.Reader = bytes.NewReader(requiredMappings)
		result.Length = len(requiredMappings)
	}
	return result, err
}

func MustFindDefaultsMappingFile(mappingsdir string) (MappingFile, error) {
	var result MappingFile
	name := path.Join(mappingsdir, "defaults.yaml")
	defaultMappings, err := Mappings.ReadFile(name)
	if err == nil {
		result.Name = name
		result.Reader = bytes.NewReader(defaultMappings)
		result.Length = len(defaultMappings)
	}
	return result, err
}

func MustFindFirstCampaignMappingFile(mappingsdir string, campaign string) (MappingFile, error) {
	var result MappingFile
	dir := path.Join(mappingsdir, "campaigns")
	files, err := Mappings.ReadDir(dir)
	if err != nil {
		return result, err
	}
	for _, file := range files {
		p := file.Name()
		if strings.HasPrefix(p, campaign) {
			// multiple matches are not supported - guard against misconfiguration
			if result.Name != "" {
				err = fmt.Errorf("found multpile files with prefix: %s in dir: %s", campaign, dir)
				return result, err
			}

			p = path.Join(dir, p)
			var campaignMappings []byte
			campaignMappings, err = Mappings.ReadFile(p)
			if err == nil {
				result.Name = p
				result.Reader = bytes.NewReader(campaignMappings)
				result.Length = len(campaignMappings)
			}
		}

	}
	if result.Name == "" {
		err = fmt.Errorf("failed to find file with prefix: %s in dir: %s", campaign, dir)
	}
	return result, err
}

type yamlConfigUnmarshaler struct {
}

func (u yamlConfigUnmarshaler) Unmarshal(compev CompositeEnvVar, sources ...MappingFile) (Config, error) {
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
	key = "fundraiserBadgeExtensions"
	err = yaml.Get(key).Populate(&result.FundraiserBadgeExtensions)
	if err != nil {
		return result, readError(key, err)
	}
	key = "fundraiserUpdateExtensions"
	err = yaml.Get(key).Populate(&result.FundraiserUpdateExtensions)
	if err != nil {
		return result, readError(key, err)
	}

	err = expandFieldMappings(&result.FundraiserFieldMappings.Builtin, false)
	if err == nil {
		err = expandFieldMappings(&result.FundraiserFieldMappings.Custom, true)
	}
	if err == nil {
		err = expandFieldMappings(&result.TeamFieldMappings.Custom, true)
	}
	if err != nil {
		return result, err
	}

	return result, nil
}

func expandFieldMappings(mappings *FieldMappings, custom bool) error {
	var err error
	if mappings.Strings != nil {
		mappings.Strings, err = expandSimpleFieldType(String, mappings.Strings, custom)
	}
	if mappings.Texts != nil {
		mappings.Texts, err = expandSimpleFieldType(Text, mappings.Texts, custom)
	}
	if mappings.Decimals != nil {
		mappings.Decimals, err = expandSimpleFieldType(Decimal, mappings.Decimals, custom)
	}
	if mappings.Booleans != nil {
		mappings.Booleans, err = expandSimpleFieldType(Boolean, mappings.Booleans, custom)
	}
	if mappings.Timestamps != nil {
		mappings.Timestamps, err = expandSimpleFieldType(Timestamp, mappings.Timestamps, custom)
	}
	if mappings.Phones != nil {
		mappings.Phones, err = expandNestedFieldType(Phone, mappings.Phones, custom)
	}
	if mappings.Geos != nil {
		mappings.Geos, err = expandNestedFieldType(Geo, mappings.Geos, custom)
	}
	if mappings.Integers != nil {
		mappings.Integers, err = expandSimpleFieldType(Integer, mappings.Integers, custom)
	}
	return err
}

func expandSimpleFieldType(fieldtype SimpleFieldType, fieldmappings map[string]string, custom bool) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range fieldmappings {
		s := ":" + k
		if custom {
			s = "cm" + s
		}
		switch fieldtype {
		case String:
			s = "str:" + s
		case Text:
			s = "txt:" + s
		case Decimal, Integer:
			s = "int:" + s
		case Boolean:
			s = "bol:" + s
		case Timestamp:
			s = "tme:" + s
		default:
			return result, fmt.Errorf("invalid simple field type %v", fieldtype)
		}
		result[s] = v
	}
	return result, nil
}

func expandNestedFieldType(fieldtype NestedFieldType, fieldmappings map[string]map[string]string, custom bool) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)
	for k, v := range fieldmappings {
		s := ":" + k
		if custom {
			s = "cm" + s
		}
		switch fieldtype {
		case Phone:
			s = "phn:" + s
		case Geo:
			s = "geo:" + s
		default:
			return result, fmt.Errorf("invalid nested field type %v", fieldtype)
		}
		result[s] = v
	}
	return result, nil
}
