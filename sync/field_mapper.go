package sync

// FieldMapper provides a common interface for types that hold field maps.
// This enables shared field mapping logic.
type FieldMapper interface {
	GetFields() map[string]interface{}
	SetField(key string, value interface{})
	DeleteField(key string)
}

// mapFields maps fields from a source to a FieldMapper using the provided mappings.
func mapFields(mappings FieldMappings, source Source, container FieldMapper) {
	if mappings.Strings != nil {
		for field, path := range mappings.Strings {
			if result, exists := source.StringForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
	if mappings.Texts != nil {
		for field, path := range mappings.Texts {
			if result, exists := source.StringForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
	if mappings.Decimals != nil {
		for field, path := range mappings.Decimals {
			if result, exists := source.IntForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
	if mappings.Booleans != nil {
		for field, path := range mappings.Booleans {
			if result, exists := source.BoolForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
	if mappings.Timestamps != nil {
		for field, path := range mappings.Timestamps {
			if result, exists := source.StringForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
	if mappings.Phones != nil {
		for field, v := range mappings.Phones {
			phoneObject := make(map[string]string)
			isEmptyObject := true
			for phoneField, path := range v {
				phoneObject[phoneField], _ = source.StringForPath(path)
				if isEmptyObject && phoneObject[phoneField] != "" {
					isEmptyObject = false
				}
			}
			if isEmptyObject {
				container.SetField(field, nil)
			} else {
				container.SetField(field, phoneObject)
			}
		}
	}
	if mappings.Geos != nil {
		for field, v := range mappings.Geos {
			geoObject := make(map[string]string)
			isEmptyObject := true
			for geoField, path := range v {
				geoObject[geoField], _ = source.StringForPath(path)
				if isEmptyObject && geoObject[geoField] != "" {
					isEmptyObject = false
				}
			}
			if isEmptyObject {
				container.SetField(field, nil)
			} else {
				container.SetField(field, geoObject)
			}
		}
	}
	if mappings.Integers != nil {
		for field, path := range mappings.Integers {
			if result, exists := source.IntForPath(path); exists {
				container.SetField(field, result)
			} else {
				container.SetField(field, nil)
			}
		}
	}
}
