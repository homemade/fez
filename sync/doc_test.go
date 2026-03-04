package sync

import (
	"strings"
	"testing"
)

func TestGenerateDisplayConditions_SortsAlphabetically(t *testing.T) {
	conditions := map[string]DisplayCondition{
		"Zebra condition": {
			Begin: "{% if activity.custom.test.zebra != blank %}",
			End:   "{% endif %}",
		},
		"Alpha condition": {
			Begin: "{% if activity.custom.test.alpha != blank %}",
			End:   "{% endif %}",
		},
		"Middle condition": {
			Begin: "{% if activity.custom.test.middle != blank %}",
			End:   "{% endif %}",
		},
	}

	doc := GenerateDisplayConditions("TEST_CAMPAIGN", "test-activity", conditions)

	if len(doc.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(doc.Rows))
	}
	if doc.Rows[0].Description != "Alpha condition" {
		t.Errorf("expected first row to be 'Alpha condition', got %q", doc.Rows[0].Description)
	}
	if doc.Rows[1].Description != "Middle condition" {
		t.Errorf("expected second row to be 'Middle condition', got %q", doc.Rows[1].Description)
	}
	if doc.Rows[2].Description != "Zebra condition" {
		t.Errorf("expected third row to be 'Zebra condition', got %q", doc.Rows[2].Description)
	}
}

func TestGenerateDisplayConditions_PreservesBeginEnd(t *testing.T) {
	conditions := map[string]DisplayCondition{
		"Has field": {
			Begin: "{% if activity.custom.my-activity.my-field != blank %}",
			End:   "{% endif %}",
		},
	}

	doc := GenerateDisplayConditions("CAMPAIGN", "my-activity", conditions)

	if doc.Rows[0].Begin != "{% if activity.custom.my-activity.my-field != blank %}" {
		t.Errorf("unexpected Begin: %q", doc.Rows[0].Begin)
	}
	if doc.Rows[0].End != "{% endif %}" {
		t.Errorf("unexpected End: %q", doc.Rows[0].End)
	}
}

func TestGenerateDisplayConditions_SetsMetadata(t *testing.T) {
	conditions := map[string]DisplayCondition{
		"Test": {Begin: "{% if true %}", End: "{% endif %}"},
	}

	doc := GenerateDisplayConditions("MY_CAMPAIGN", "my-activity", conditions)

	if doc.CampaignLabel != "MY_CAMPAIGN" {
		t.Errorf("expected CampaignLabel 'MY_CAMPAIGN', got %q", doc.CampaignLabel)
	}
	if doc.ActivityName != "my-activity" {
		t.Errorf("expected ActivityName 'my-activity', got %q", doc.ActivityName)
	}
}

func TestGenerateDisplayConditions_EmptyConditions(t *testing.T) {
	conditions := map[string]DisplayCondition{}

	doc := GenerateDisplayConditions("CAMPAIGN", "activity", conditions)

	if len(doc.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(doc.Rows))
	}
}

func TestDisplayConditionDocumentation_FormatCSV(t *testing.T) {
	conditions := map[string]DisplayCondition{
		"Has facebook fundraiser": {
			Begin: "{% if activity.custom.test.facebook-id != blank %}",
			End:   "{% endif %}",
		},
		"Has raised 50%": {
			Begin: "{% if activity.custom.test.pct >= 50.00 %}",
			End:   "{% endif %}",
		},
	}

	doc := GenerateDisplayConditions("TEST_CAMP", "test", conditions)
	csv, err := doc.FormatCSV()
	if err != nil {
		t.Fatalf("FormatCSV error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(csv), "\n")

	// Check comment rows
	if lines[0] != "# Campaign: TEST_CAMP" {
		t.Errorf("expected campaign comment, got %q", lines[0])
	}
	if lines[1] != "# Activity: test" {
		t.Errorf("expected activity comment, got %q", lines[1])
	}

	// Check header
	if !strings.HasPrefix(lines[2], "Description,") {
		t.Errorf("expected header row, got %q", lines[2])
	}

	// Check data rows are sorted
	if !strings.HasPrefix(lines[3], "Has facebook fundraiser,") {
		t.Errorf("expected 'Has facebook fundraiser' first, got %q", lines[3])
	}
	if !strings.HasPrefix(lines[4], "Has raised 50%,") {
		t.Errorf("expected 'Has raised 50%%' second, got %q", lines[4])
	}
}

func TestDisplayConditionDocumentation_FormatCSV_NoActivityName(t *testing.T) {
	doc := DisplayConditionDocumentation{
		CampaignLabel: "TEST",
		ActivityName:  "",
		Rows: []DisplayConditionRow{
			{Description: "Test", Begin: "{% if true %}", End: "{% endif %}"},
		},
	}

	csv, err := doc.FormatCSV()
	if err != nil {
		t.Fatalf("FormatCSV error: %v", err)
	}

	if strings.Contains(csv, "# Activity:") {
		t.Error("should not contain Activity comment when ActivityName is empty")
	}
}
