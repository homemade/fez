package sync

import (
	"errors"
	"fmt"
)

var OrttoCRMFieldMapper = orttoCRMFieldMapper{}

type orttoCRMFieldMapper struct {
}

func (om orttoCRMFieldMapper) ExpandFieldMappings(mappings *FieldMappings, custom bool) error {
	var errs []error
	if mappings.Strings != nil {
		s, err := om.expandSimpleFieldType(String, mappings.Strings, custom)
		mappings.Strings = s
		errs = append(errs, err)
	}
	if mappings.Texts != nil {
		s, err := om.expandSimpleFieldType(Text, mappings.Texts, custom)
		mappings.Texts = s
		errs = append(errs, err)
	}
	if mappings.Decimals != nil {
		s, err := om.expandSimpleFieldType(Decimal, mappings.Decimals, custom)
		mappings.Decimals = s
		errs = append(errs, err)
	}
	if mappings.Booleans != nil {
		s, err := om.expandSimpleFieldType(Boolean, mappings.Booleans, custom)
		mappings.Booleans = s
		errs = append(errs, err)
	}
	if mappings.Timestamps != nil {
		s, err := om.expandSimpleFieldType(Timestamp, mappings.Timestamps, custom)
		mappings.Timestamps = s
		errs = append(errs, err)
	}
	if mappings.Phones != nil {
		s, err := om.expandNestedFieldType(Phone, mappings.Phones, custom)
		mappings.Phones = s
		errs = append(errs, err)
	}
	if mappings.Geos != nil {
		s, err := om.expandNestedFieldType(Geo, mappings.Geos, custom)
		mappings.Geos = s
		errs = append(errs, err)
	}
	if mappings.Integers != nil {
		s, err := om.expandSimpleFieldType(Integer, mappings.Integers, custom)
		mappings.Integers = s
		errs = append(errs, err)
	}
	return errors.Join(errs...) // nil error values are discarded
}

func (om orttoCRMFieldMapper) expandSimpleFieldType(fieldtype SimpleFieldType, fieldmappings map[string]string, custom bool) (map[string]string, error) {
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

func (om orttoCRMFieldMapper) expandNestedFieldType(fieldtype NestedFieldType, fieldmappings map[string]map[string]string, custom bool) (map[string]map[string]string, error) {
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
