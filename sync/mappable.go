package sync

import (
	"log"
	"time"
)

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
	if mappings.Dates != nil {
		for field, v := range mappings.Dates {
			if dateObject := buildDateObject(v, source); dateObject != nil {
				destination.SetField(field, dateObject)
			} else {
				destination.SetField(field, nil)
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

// dateLayouts are the date formats buildDateObject accepts, tried in order.
// RFC3339 (offset required) covers most feeds; the offset-less datetime covers
// sources like Raiser's Edge whose date field is "2026-06-26T00:00:00" (no zone);
// the date-only form covers plain calendar dates. Only year/month/day are used,
// so a missing offset is harmless — the zone comes from the "timezone" mapping.
var dateLayouts = []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"}

// buildDateObject converts a date-with-timezone (dtz:) field mapping into the
// object shape Ortto expects — {year, month, day, timezone} with integer
// year/month/day. The "value" key holds a source path (or backtick-wrapped
// literal) to a date string parsed against dateLayouts; the "timezone" key
// holds a backtick-wrapped IANA zone literal or a source path (defaults to UTC).
// Returns nil if the date value is missing or unparseable.
func buildDateObject(mapping map[string]string, source Source) map[string]interface{} {
	raw := resolveMappingValue(mapping["value"], source)
	if raw == "" {
		return nil
	}
	var t time.Time
	var err error
	for _, layout := range dateLayouts {
		if t, err = time.Parse(layout, raw); err == nil {
			break
		}
	}
	if err != nil {
		log.Printf("Warning: failed to parse date %q for dtz field (expected RFC3339, offset-less datetime, or YYYY-MM-DD): %v", raw, err)
		return nil
	}
	tz := resolveMappingValue(mapping["timezone"], source)
	if tz == "" {
		tz = "UTC"
	}
	return map[string]interface{}{
		"year":     t.Year(),
		"month":    int(t.Month()),
		"day":      t.Day(),
		"timezone": tz,
	}
}

// resolveMappingValue returns the literal when spec is wrapped in backticks
// (the same static-value convention used for Strings in MapFields), otherwise
// the source value at the given path ("" if absent).
func resolveMappingValue(spec string, source Source) string {
	if len(spec) >= 2 && spec[0] == '`' && spec[len(spec)-1] == '`' {
		return spec[1 : len(spec)-1]
	}
	if v, exists := source.StringForPath(spec); exists {
		return v
	}
	return ""
}
