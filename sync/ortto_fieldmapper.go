package sync

import "fmt"

var OrttoCRMFieldMapper = orttoCRMFieldMapper{}

type orttoCRMFieldMapper struct {
}

func (om orttoCRMFieldMapper) ExpandFieldMappings(mappings *FieldMappings, custom bool) error {
	var err error
	if mappings.Strings != nil {
		mappings.Strings, err = om.expandSimpleFieldType(String, mappings.Strings, custom)
	}
	if mappings.Texts != nil {
		mappings.Texts, err = om.expandSimpleFieldType(Text, mappings.Texts, custom)
	}
	if mappings.Decimals != nil {
		mappings.Decimals, err = om.expandSimpleFieldType(Decimal, mappings.Decimals, custom)
	}
	if mappings.Booleans != nil {
		mappings.Booleans, err = om.expandSimpleFieldType(Boolean, mappings.Booleans, custom)
	}
	if mappings.Timestamps != nil {
		mappings.Timestamps, err = om.expandSimpleFieldType(Timestamp, mappings.Timestamps, custom)
	}
	if mappings.Phones != nil {
		mappings.Phones, err = om.expandNestedFieldType(Phone, mappings.Phones, custom)
	}
	if mappings.Geos != nil {
		mappings.Geos, err = om.expandNestedFieldType(Geo, mappings.Geos, custom)
	}
	if mappings.Integers != nil {
		mappings.Integers, err = om.expandSimpleFieldType(Integer, mappings.Integers, custom)
	}
	return err
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
