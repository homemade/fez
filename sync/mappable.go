package sync

// Mappable provides a common interface for types that can be mapped.
// This enables shared field mapping logic.
type Mappable interface {
	GetFields() map[string]interface{}
	SetField(key string, value interface{})
	DeleteField(key string)
}

// MapFields maps fields from a source to a destination using the provided mappings.
func MapFields(mappings FieldMappings, source Source, destination Mappable) {
	if mappings.Strings != nil {
		for field, path := range mappings.Strings {
			// handle static strings as well as dynamic paths
			// escaping the value in backticks allows us to distinguish between the two
			if len(path) >= 2 && path[0] == '`' && path[len(path)-1] == '`' {
				destination.SetField(field, path[1:len(path)-1])
				continue
			}
			if result, exists := source.StringForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
			}
		}
	}
	if mappings.Texts != nil {
		for field, path := range mappings.Texts {
			if result, exists := source.StringForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
			}
		}
	}
	if mappings.Decimals != nil {
		for field, path := range mappings.Decimals {
			if result, exists := source.IntForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
			}
		}
	}
	if mappings.Booleans != nil {
		for field, path := range mappings.Booleans {
			if result, exists := source.BoolForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
			}
		}
	}
	if mappings.Timestamps != nil {
		for field, path := range mappings.Timestamps {
			if result, exists := source.StringForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
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
				destination.SetField(field, nil)
			} else {
				destination.SetField(field, phoneObject)
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
				destination.SetField(field, nil)
			} else {
				destination.SetField(field, geoObject)
			}
		}
	}
	if mappings.Integers != nil {
		for field, path := range mappings.Integers {
			if result, exists := source.IntForPath(path); exists {
				destination.SetField(field, result)
			} else {
				destination.SetField(field, nil)
			}
		}
	}
}
