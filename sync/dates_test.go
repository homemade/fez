package sync

import (
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

// orttoCRMFieldMapper is the CRM field mapper used to expand short field names
// into prefixed Ortto field IDs. It is referenced via OrttoCRMFieldMapper in the
// config loader; here we exercise it directly.
func dateMapper() orttoCRMFieldMapper { return orttoCRMFieldMapper{} }

func TestExpandFieldMappings_DatesGetDtzPrefix(t *testing.T) {
	mappings := FieldMappings{
		Dates: map[string]map[string]string{
			"last-gift-date": {"value": "gift.date", "timezone": "`Australia/Brisbane`"},
		},
	}
	if err := dateMapper().ExpandFieldMappings(&mappings, true); err != nil {
		t.Fatalf("ExpandFieldMappings: %v", err)
	}
	if _, ok := mappings.Dates["dtz:cm:last-gift-date"]; !ok {
		t.Fatalf("expected key dtz:cm:last-gift-date, got keys: %v", mappings.Dates)
	}
}

func TestAsOrttoFieldType_Date(t *testing.T) {
	m := FieldMappings{
		Dates: map[string]map[string]string{"dtz:cm:last-gift-date": {"value": "x"}},
	}
	if got := m.AsOrttoFieldType("dtz:cm:last-gift-date"); got != "Date" {
		t.Errorf("AsOrttoFieldType = %q, want Date", got)
	}
	if got := m.AsOrttoAPIFieldType("dtz:cm:last-gift-date"); got != "date" {
		t.Errorf("AsOrttoAPIFieldType = %q, want date", got)
	}
}

func TestMapFields_DateBuildsObject(t *testing.T) {
	cases := []struct {
		name string
		json string
		want map[string]interface{}
	}{
		{
			name: "RFC3339 with explicit timezone literal",
			json: `{"gift":{"date":"2026-06-30T09:15:00Z"}}`,
			want: map[string]interface{}{"year": 2026, "month": 6, "day": 30, "timezone": "Australia/Brisbane"},
		},
		{
			name: "offset-less datetime (Raiser's Edge shape)",
			json: `{"gift":{"date":"2026-06-26T00:00:00"}}`,
			want: map[string]interface{}{"year": 2026, "month": 6, "day": 26, "timezone": "Australia/Brisbane"},
		},
		{
			name: "date-only string",
			json: `{"gift":{"date":"2026-12-01"}}`,
			want: map[string]interface{}{"year": 2026, "month": 12, "day": 1, "timezone": "Australia/Brisbane"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mappings := FieldMappings{
				Dates: map[string]map[string]string{
					"dtz:cm:last-gift-date": {"value": "gift.date", "timezone": "`Australia/Brisbane`"},
				},
			}
			source := Source{data: gjson.Parse(tc.json)}
			dest := &OrttoContact{Fields: map[string]interface{}{}}
			MapFields(mappings, source, dest)

			got := dest.Fields["dtz:cm:last-gift-date"]
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestMapFields_DateMissingOrUnparseableIsNil(t *testing.T) {
	mappings := FieldMappings{
		Dates: map[string]map[string]string{
			"dtz:cm:last-gift-date": {"value": "gift.date", "timezone": "`Australia/Brisbane`"},
		},
	}
	for _, json := range []string{`{}`, `{"gift":{"date":"not-a-date"}}`} {
		source := Source{data: gjson.Parse(json)}
		dest := &OrttoContact{Fields: map[string]interface{}{}}
		MapFields(mappings, source, dest)
		if got, ok := dest.Fields["dtz:cm:last-gift-date"]; !ok || got != nil {
			t.Errorf("for %s: got %#v (present=%v), want nil", json, got, ok)
		}
	}
}

func TestMapFields_DateDefaultsTimezoneToUTC(t *testing.T) {
	mappings := FieldMappings{
		Dates: map[string]map[string]string{
			"dtz:cm:last-gift-date": {"value": "gift.date"},
		},
	}
	source := Source{data: gjson.Parse(`{"gift":{"date":"2026-06-30"}}`)}
	dest := &OrttoContact{Fields: map[string]interface{}{}}
	MapFields(mappings, source, dest)

	got, _ := dest.Fields["dtz:cm:last-gift-date"].(map[string]interface{})
	if got["timezone"] != "UTC" {
		t.Errorf("timezone = %v, want UTC", got["timezone"])
	}
}
