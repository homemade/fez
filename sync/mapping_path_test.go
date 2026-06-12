package sync

import (
	"strings"
	"testing"
)

func TestParseMappingPath(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantOrg   string
		wantLabel string
		wantErr   bool
	}{
		{
			name:      "standard two-segment",
			input:     "ACME/CAMPAIGN_V001",
			wantOrg:   "ACME",
			wantLabel: "CAMPAIGN_V001",
		},
		{
			name:      "casing preserved",
			input:     "Acme/campaign-v001",
			wantOrg:   "Acme",
			wantLabel: "campaign-v001",
		},
		{
			name:      "multi-segment org dir (last-slash split)",
			input:     "PARENT/CHILD/LABEL",
			wantOrg:   "PARENT/CHILD",
			wantLabel: "LABEL",
		},
		{
			name:      "trailing slash → empty label",
			input:     "ACME/",
			wantOrg:   "ACME",
			wantLabel: "",
		},
		{
			name:      "leading slash → empty org",
			input:     "/LABEL",
			wantOrg:   "",
			wantLabel: "LABEL",
		},
		{
			name:    "missing slash",
			input:   "no-separator",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			org, label, err := ParseMappingPath(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseMappingPath(%q) = (%q, %q, nil); want error", tc.input, org, label)
				}
				if !strings.Contains(err.Error(), "invalid mapping path") {
					t.Errorf("ParseMappingPath(%q) error = %v; want error containing %q", tc.input, err, "invalid mapping path")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseMappingPath(%q) unexpected error: %v", tc.input, err)
			}
			if org != tc.wantOrg {
				t.Errorf("ParseMappingPath(%q) org = %q; want %q", tc.input, org, tc.wantOrg)
			}
			if label != tc.wantLabel {
				t.Errorf("ParseMappingPath(%q) label = %q; want %q", tc.input, label, tc.wantLabel)
			}
		})
	}
}

func TestCampaignEnvVar_OrgLabel(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		wantOrg   string
		wantLabel string
	}{
		{name: "two segment", path: "ACME/CAMPAIGN_V001", wantOrg: "ACME", wantLabel: "CAMPAIGN_V001"},
		{name: "invalid → empty pair", path: "no-separator", wantOrg: "", wantLabel: ""},
		{name: "empty path → empty pair", path: "", wantOrg: "", wantLabel: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := CampaignEnvVar{Path: tc.path}
			if got := c.Org(); got != tc.wantOrg {
				t.Errorf("CampaignEnvVar{Path:%q}.Org() = %q; want %q", tc.path, got, tc.wantOrg)
			}
			if got := c.Label(); got != tc.wantLabel {
				t.Errorf("CampaignEnvVar{Path:%q}.Label() = %q; want %q", tc.path, got, tc.wantLabel)
			}
		})
	}
}
