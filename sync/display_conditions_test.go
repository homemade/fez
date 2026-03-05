package sync

import (
	"testing"
)

func TestExtractActivityFieldNames(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     []string
	}{
		{
			name:     "single field",
			template: "{% if activity.custom.my-activity.my-field != blank %}",
			want:     []string{"my-field"},
		},
		{
			name:     "multiple fields",
			template: "{% if activity.custom.my-activity.amount >= 1.00 and activity.custom.my-activity.total == 0.00 %}",
			want:     []string{"amount", "total"},
		},
		{
			name:     "duplicate fields deduplicated",
			template: "{% if activity.custom.my-activity.amount >= 1.00 and activity.custom.my-activity.amount < 100.00 %}",
			want:     []string{"amount"},
		},
		{
			name:     "no activity fields",
			template: "{% if person.email != blank %}",
			want:     nil,
		},
		{
			name:     "sorted output",
			template: "{% if activity.custom.test.zebra != blank and activity.custom.test.alpha != blank %}",
			want:     []string{"alpha", "zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractActivityFieldNames(tt.template)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestValidateDisplayConditionFields(t *testing.T) {
	doc := FieldDocumentation{
		Target: "ortto-activities",
		Rows: []FieldDocRow{
			{FieldName: "facebook-fundraiser-id", Entity: "Activity"},
			{FieldName: "fundraising-total", Entity: "Activity"},
			{FieldName: "email", Entity: "Person"},
		},
	}

	entries := []DisplayConditionEntry{
		{
			Description: "Has facebook fundraiser",
			Condition: DisplayCondition{
				Begin: "{% if activity.custom.test.facebook-fundraiser-id != blank %}",
				End:   "{% endif %}",
			},
		},
		{
			Description: "Has unknown field",
			Condition: DisplayCondition{
				Begin: "{% if activity.custom.test.nonexistent-field != blank %}",
				End:   "{% endif %}",
			},
		},
		{
			Description: "Mixed valid and invalid",
			Condition: DisplayCondition{
				Begin: "{% if activity.custom.test.fundraising-total >= 1.00 and activity.custom.test.bad-field > 0 %}",
				End:   "{% endif %}",
			},
		},
	}

	results := ValidateDisplayConditionFields(entries, doc)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First: all valid
	if len(results[0].Invalid) != 0 {
		t.Errorf("expected no invalid fields for 'Has facebook fundraiser', got %v", results[0].Invalid)
	}
	if len(results[0].Valid) != 1 || results[0].Valid[0] != "facebook-fundraiser-id" {
		t.Errorf("expected valid [facebook-fundraiser-id], got %v", results[0].Valid)
	}

	// Second: all invalid
	if len(results[1].Invalid) != 1 || results[1].Invalid[0] != "nonexistent-field" {
		t.Errorf("expected invalid [nonexistent-field], got %v", results[1].Invalid)
	}

	// Third: mixed
	if len(results[2].Valid) != 1 || results[2].Valid[0] != "fundraising-total" {
		t.Errorf("expected valid [fundraising-total], got %v", results[2].Valid)
	}
	if len(results[2].Invalid) != 1 || results[2].Invalid[0] != "bad-field" {
		t.Errorf("expected invalid [bad-field], got %v", results[2].Invalid)
	}
}

func TestValidateDisplayConditionRendering(t *testing.T) {
	entries := []DisplayConditionEntry{
		{
			Description: "Not blank check with empty expectFalse",
			Condition: DisplayCondition{
				Begin:          "{% if activity.custom.test-act.my-field != blank %}",
				End:            "{% endif %}",
				ExpectTrue:     map[string]interface{}{"my-field": "some-value"},
				HasExpectTrue:  true,
				ExpectFalse:    nil, // empty context = field is blank
				HasExpectFalse: true,
			},
		},
		{
			Description: "Numeric comparison",
			Condition: DisplayCondition{
				Begin:          "{% if activity.custom.test-act.amount >= 50.00 %}",
				End:            "{% endif %}",
				ExpectTrue:     map[string]interface{}{"amount": 75.0},
				HasExpectTrue:  true,
				ExpectFalse:    map[string]interface{}{"amount": 25.0},
				HasExpectFalse: true,
			},
		},
		{
			Description: "Boolean check true",
			Condition: DisplayCondition{
				Begin:          "{% if activity.custom.test-act.flag == true %}",
				End:            "{% endif %}",
				ExpectTrue:     map[string]interface{}{"flag": true},
				HasExpectTrue:  true,
				ExpectFalse:    map[string]interface{}{"flag": false},
				HasExpectFalse: true,
			},
		},
	}

	results := ValidateDisplayConditionRendering(entries, "test-act")

	for _, r := range results {
		if r.Error != "" {
			t.Errorf("%q: unexpected error: %s", r.Description, r.Error)
			continue
		}

		if r.ExpectTrueOK == nil {
			t.Errorf("%q: expected ExpectTrueOK to be set", r.Description)
		} else if !*r.ExpectTrueOK {
			t.Errorf("%q: expectTrue failed", r.Description)
		}
		if r.ExpectFalseOK == nil {
			t.Errorf("%q: expected ExpectFalseOK to be set", r.Description)
		} else if !*r.ExpectFalseOK {
			t.Errorf("%q: expectFalse failed", r.Description)
		}
	}
}

func TestValidateDisplayConditionRendering_KeyAbsent(t *testing.T) {
	entries := []DisplayConditionEntry{
		{
			Description: "No expect keys at all",
			Condition: DisplayCondition{
				Begin:          "{% if activity.custom.test.field != blank %}",
				End:            "{% endif %}",
				HasExpectTrue:  false,
				HasExpectFalse: false,
			},
		},
	}

	results := ValidateDisplayConditionRendering(entries, "test")

	if results[0].ExpectTrueOK != nil {
		t.Error("expected nil ExpectTrueOK when key is absent")
	}
	if results[0].ExpectFalseOK != nil {
		t.Error("expected nil ExpectFalseOK when key is absent")
	}
}

func TestValidateDisplayConditionRendering_EmptyExpectTrue(t *testing.T) {
	// expectTrue: present but nil (bare key in YAML) = test with blank context
	// For == blank check, empty context should make it evaluate to true
	entries := []DisplayConditionEntry{
		{
			Description: "Blank check with empty expectTrue",
			Condition: DisplayCondition{
				Begin:          "{% if activity.custom.test.field == blank %}",
				End:            "{% endif %}",
				ExpectTrue:     nil, // blank context
				HasExpectTrue:  true,
				ExpectFalse:    map[string]interface{}{"field": "has-value"},
				HasExpectFalse: true,
			},
		},
	}

	results := ValidateDisplayConditionRendering(entries, "test")

	if results[0].ExpectTrueOK == nil || !*results[0].ExpectTrueOK {
		t.Error("expected expectTrue to pass: blank context should make == blank evaluate true")
	}
	if results[0].ExpectFalseOK == nil || !*results[0].ExpectFalseOK {
		t.Error("expected expectFalse to pass: field with value should make == blank evaluate false")
	}
}

func TestValidateDisplayConditionSyntax(t *testing.T) {
	entries := []DisplayConditionEntry{
		{
			Description: "Uses blank",
			Condition: DisplayCondition{
				Begin: "{% if activity.custom.test.field != blank %}",
				End:   "{% endif %}",
			},
		},
		{
			Description: "No unsupported syntax",
			Condition: DisplayCondition{
				Begin: `{% assign len = activity.custom.test.field | size %}{% if len > 0 %}`,
				End:   "{% endif %}",
			},
		},
		{
			Description: "Blank in end tag",
			Condition: DisplayCondition{
				Begin: "{% if activity.custom.test.a != blank %}",
				End:   "{% else %}{% if activity.custom.test.b == blank %}fallback{% endif %}{% endif %}",
			},
		},
	}

	results := ValidateDisplayConditionSyntax(entries)

	if len(results[0].UnsupportedTerms) != 1 || results[0].UnsupportedTerms[0] != "blank" {
		t.Errorf("expected [blank] for 'Uses blank', got %v", results[0].UnsupportedTerms)
	}
	if len(results[1].UnsupportedTerms) != 0 {
		t.Errorf("expected no unsupported terms for 'No unsupported syntax', got %v", results[1].UnsupportedTerms)
	}
	if len(results[2].UnsupportedTerms) != 1 || results[2].UnsupportedTerms[0] != "blank" {
		t.Errorf("expected [blank] for 'Blank in end tag', got %v", results[2].UnsupportedTerms)
	}
}

func TestValidateDisplayConditionRendering_FailingCondition(t *testing.T) {
	entries := []DisplayConditionEntry{
		{
			Description: "Wrong expectTrue",
			Condition: DisplayCondition{
				Begin:         "{% if activity.custom.test.amount >= 50.00 %}",
				End:           "{% endif %}",
				ExpectTrue:    map[string]interface{}{"amount": 25.0}, // Should fail
				HasExpectTrue: true,
			},
		},
	}

	results := ValidateDisplayConditionRendering(entries, "test")

	if results[0].ExpectTrueOK == nil || *results[0].ExpectTrueOK {
		t.Error("expected expectTrue to fail with amount 25.0 < 50.0")
	}
}
