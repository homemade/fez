package sync

import (
	"strings"
	"testing"
	"testing/fstest"
)

// memMappings returns an EmbeddedMappings backed by an in-memory MapFS.
func memMappings(t *testing.T, root string, files map[string]string) EmbeddedMappings {
	t.Helper()
	fs := fstest.MapFS{}
	for name, content := range files {
		fs[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return EmbeddedMappings{Root: root, Files: fs}
}

func TestRaiselyMessageMappings_YAMLRoundTrip(t *testing.T) {
	yamlBody := `
api:
  settings:
    raiselyFundraiserReferralsField: "private.invitations"

fundraiserReferralFieldMappings:
  user:
    email: "email"
    firstName: "firstName"
    lastName: "lastName"
  custom:
    "referrer-id": "^.uuid"
    "referrer-first-name": "^.user.firstName"
`
	file := MappingFile{
		Name:   "test.yaml",
		Reader: strings.NewReader(yamlBody),
		Length: len(yamlBody),
	}
	unmarshaler := YAMLConfigUnmarshaler{}
	cfg, err := unmarshaler.Unmarshal(JSONCompositeEnvVar{}, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.API.Settings.RaiselyFundraiserReferralsField != "private.invitations" {
		t.Errorf("expected referrals trigger 'private.invitations', got %q", cfg.API.Settings.RaiselyFundraiserReferralsField)
	}

	user := cfg.FundraiserReferralFieldMappings.User
	if got := user["email"]; got != "email" {
		t.Errorf("user.email: got %q, want %q", got, "email")
	}
	if got := user["firstName"]; got != "firstName" {
		t.Errorf("user.firstName: got %q, want %q", got, "firstName")
	}
	if got := user["lastName"]; got != "lastName" {
		t.Errorf("user.lastName: got %q, want %q", got, "lastName")
	}

	custom := cfg.FundraiserReferralFieldMappings.Custom
	if got := custom["referrer-id"]; got != "^.uuid" {
		t.Errorf("custom.referrer-id: got %q, want %q", got, "^.uuid")
	}
	if got := custom["referrer-first-name"]; got != "^.user.firstName" {
		t.Errorf("custom.referrer-first-name: got %q, want %q", got, "^.user.firstName")
	}
}

func TestCompanionLookup_Found(t *testing.T) {
	em := memMappings(t, "mappings", map[string]string{
		"mappings/ORG/LABEL.ortto-activities.yaml":           "api:\n  keys:\n    raisely: k\n",
		"mappings/ORG/LABEL.ortto-activities.referrals.yaml": "message:\n  user:\n    email: email\n",
	})

	companion, err := em.FindReferralsCompanionMappingFileByPath("ORG/LABEL", "ortto-activities")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if companion.Length == 0 {
		t.Fatalf("expected companion to be found, got zero-length file")
	}
	if !strings.Contains(companion.Name, "LABEL.ortto-activities.referrals.yaml") {
		t.Errorf("unexpected companion path: %s", companion.Name)
	}
}

func TestCompanionLookup_Missing(t *testing.T) {
	em := memMappings(t, "mappings", map[string]string{
		"mappings/ORG/LABEL.ortto-activities.yaml": "api:\n  keys:\n    raisely: k\n",
	})

	companion, err := em.FindReferralsCompanionMappingFileByPath("ORG/LABEL", "ortto-activities")
	if err != nil {
		t.Fatalf("missing companion should not error, got %v", err)
	}
	if companion.Length != 0 {
		t.Errorf("expected zero-length companion, got %d bytes", companion.Length)
	}
}

// TestTightenedTargetMatch_CompanionDoesNotCompete confirms that a
// companion file alongside the main target does not trigger the
// "multiple mapping files" guard or get returned as the target.
func TestTightenedTargetMatch_CompanionDoesNotCompete(t *testing.T) {
	em := memMappings(t, "mappings", map[string]string{
		"mappings/ORG/LABEL.ortto-activities.yaml":           "api:\n  keys:\n    raisely: k\n",
		"mappings/ORG/LABEL.ortto-activities.referrals.yaml": "message:\n  user:\n    email: email\n",
	})

	mf, target, err := em.MustFindFirstCampaignMappingFileWithTargetByPath("ORG/LABEL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != "ortto-activities" {
		t.Errorf("expected target 'ortto-activities', got %q", target)
	}
	if !strings.HasSuffix(mf.Name, "LABEL.ortto-activities.yaml") {
		t.Errorf("expected target file to be the main mapping, got %s", mf.Name)
	}
}

// TestTightenedTargetMatch_LegacyFile confirms that a file with no
// target suffix (legacy) still matches as the target with target == "".
func TestTightenedTargetMatch_LegacyFile(t *testing.T) {
	em := memMappings(t, "mappings", map[string]string{
		"mappings/ORG/LABEL.yaml": "api:\n  keys:\n    raisely: k\n",
	})

	mf, target, err := em.MustFindFirstCampaignMappingFileWithTargetByPath("ORG/LABEL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != "" {
		t.Errorf("expected empty target for legacy file, got %q", target)
	}
	if !strings.HasSuffix(mf.Name, "LABEL.yaml") {
		t.Errorf("unexpected target file: %s", mf.Name)
	}
}

// TestTightenedTargetMatch_RejectsExtraSegments confirms that files like
// LABEL.unknown.yaml are skipped (no known target suffix).
func TestTightenedTargetMatch_RejectsExtraSegments(t *testing.T) {
	em := memMappings(t, "mappings", map[string]string{
		"mappings/ORG/LABEL.unknown.yaml": "api:\n  keys:\n    raisely: k\n",
	})

	_, _, err := em.MustFindFirstCampaignMappingFileWithTargetByPath("ORG/LABEL")
	if err == nil {
		t.Fatal("expected error when only a non-target-suffix file is present")
	}
}
