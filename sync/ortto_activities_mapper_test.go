// go test github.com/homemade/fez/sync -v -run TestMapFundraiserReferrals
package sync

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func newTestActivitiesMapper(referralsField string, referralMappings FieldMappings) *OrttoActivitiesMapper {
	config := Config{
		Target: "ortto-activities",
	}
	config.API.Settings.OrttoActivityID = "act:cm:test-activity"
	config.API.Settings.OrttoFundraiserMergeField = "str:cm:raisely-user-id"
	config.API.Settings.RaiselyFundraiserReferralsField = referralsField
	config.FundraiserReferralFieldMappings.Custom = referralMappings

	sc := &SyncContext{Config: config, Campaign: "test-campaign"}
	return &OrttoActivitiesMapper{
		SyncContext: sc,
		RaiselyMapper: RaiselyMapper{
			SyncContext: sc,
		},
	}
}

func TestMapFundraiserReferrals_NotConfigured(t *testing.T) {
	mapper := newTestActivitiesMapper("", FieldMappings{})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"uuid": "profile-123"}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if activities != nil {
		t.Errorf("expected nil activities, got %v", activities)
	}
	if writeBack != nil {
		t.Errorf("expected nil writeBack, got %v", writeBack)
	}
}

func TestMapFundraiserReferrals_EmptyArray(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"private": {"invitations": []}}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if activities != nil {
		t.Errorf("expected nil activities, got %v", activities)
	}
	if writeBack != nil {
		t.Errorf("expected nil writeBack, got %v", writeBack)
	}
}

func TestMapFundraiserReferrals_AllProcessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{"email": "bob@example.com", "processedAt": "2026-03-18T10:00:00Z"}
					]
				}
			}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if activities != nil {
		t.Errorf("expected nil activities, got %v", activities)
	}
	if writeBack != nil {
		t.Errorf("expected nil writeBack, got %v", writeBack)
	}
}

func TestMapFundraiserReferrals_OneUnprocessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-first-name": "firstName",
			"str:cm:referral-last-name":  "lastName",
			"str:cm:referral-email":      "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{"firstName": "Jane", "lastName": "Smith", "email": "jane@example.com"}
					]
				}
			}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("expected 1 activity, got %d", len(activities))
	}

	// Check activity attributes contain mapped fields
	activity := activities[0]
	if activity.ActivityID != "act:cm:test-activity" {
		t.Errorf("expected activity ID 'act:cm:test-activity', got %q", activity.ActivityID)
	}
	if v, ok := activity.Attributes["str:cm:referral-first-name"]; !ok || v != "Jane" {
		t.Errorf("expected referral-first-name 'Jane', got %v", v)
	}
	if v, ok := activity.Attributes["str:cm:referral-email"]; !ok || v != "jane@example.com" {
		t.Errorf("expected referral-email 'jane@example.com', got %v", v)
	}

	// Check write-back
	if writeBack == nil {
		t.Fatal("expected writeBack, got nil")
	}
	if writeBack.P2PID != "profile-123" {
		t.Errorf("expected P2PID 'profile-123', got %q", writeBack.P2PID)
	}
	// Check the write-back JSON contains processedAt
	writeBackResult := gjson.Parse(writeBack.JSON)
	processedAt := writeBackResult.Get("data.private.invitations.0.processedAt")
	if !processedAt.Exists() || processedAt.String() == "" {
		t.Errorf("expected processedAt in write-back JSON, got %q", writeBack.JSON)
	}
}

func TestMapFundraiserReferrals_MultipleUnprocessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{"email": "jane@example.com"},
						{"email": "bob@example.com"},
						{"email": "alice@example.com"}
					]
				}
			}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(activities) != 3 {
		t.Fatalf("expected 3 activities, got %d", len(activities))
	}

	// Check each activity has correct email
	expectedEmails := []string{"jane@example.com", "bob@example.com", "alice@example.com"}
	for i, activity := range activities {
		email, ok := activity.Attributes["str:cm:referral-email"]
		if !ok || email != expectedEmails[i] {
			t.Errorf("activity %d: expected email %q, got %v", i, expectedEmails[i], email)
		}
	}

	// Check all entries have processedAt in write-back
	if writeBack == nil {
		t.Fatal("expected writeBack, got nil")
	}
	writeBackResult := gjson.Parse(writeBack.JSON)
	for i := range 3 {
		path := fmt.Sprintf("data.private.invitations.%d.processedAt", i)
		if !writeBackResult.Get(path).Exists() {
			t.Errorf("expected processedAt on entry %d in write-back JSON", i)
		}
	}
}

func TestMapFundraiserReferrals_MixedProcessedAndUnprocessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{"email": "bob@example.com", "processedAt": "2026-03-18T10:00:00Z"},
						{"email": "jane@example.com"},
						{"email": "alice@example.com", "processedAt": "2026-03-18T11:00:00Z"}
					]
				}
			}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("expected 1 activity (only unprocessed), got %d", len(activities))
	}

	// Only jane should be mapped
	email, ok := activities[0].Attributes["str:cm:referral-email"]
	if !ok || email != "jane@example.com" {
		t.Errorf("expected email 'jane@example.com', got %v", email)
	}

	// Check write-back preserves existing processedAt and adds new one
	if writeBack == nil {
		t.Fatal("expected writeBack, got nil")
	}
	writeBackResult := gjson.Parse(writeBack.JSON)

	// Bob's existing processedAt should be preserved
	bobProcessedAt := writeBackResult.Get("data.private.invitations.0.processedAt").String()
	if bobProcessedAt != "2026-03-18T10:00:00Z" {
		t.Errorf("expected Bob's processedAt preserved as '2026-03-18T10:00:00Z', got %q", bobProcessedAt)
	}

	// Jane should now have processedAt
	janeProcessedAt := writeBackResult.Get("data.private.invitations.1.processedAt")
	if !janeProcessedAt.Exists() || janeProcessedAt.String() == "" {
		t.Error("expected Jane's processedAt to be set")
	}

	// Alice's existing processedAt should be preserved
	aliceProcessedAt := writeBackResult.Get("data.private.invitations.2.processedAt").String()
	if aliceProcessedAt != "2026-03-18T11:00:00Z" {
		t.Errorf("expected Alice's processedAt preserved as '2026-03-18T11:00:00Z', got %q", aliceProcessedAt)
	}
}

func TestMapFundraiserReferrals_PreservesUnknownFields(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{
							"email": "jane@example.com",
							"customField": "preserved-value",
							"metadata": {"source": "form-123", "timestamp": 1234567890}
						}
					]
				}
			}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("expected 1 activity, got %d", len(activities))
	}

	// Check write-back preserves unknown fields
	if writeBack == nil {
		t.Fatal("expected writeBack, got nil")
	}
	writeBackResult := gjson.Parse(writeBack.JSON)

	customField := writeBackResult.Get("data.private.invitations.0.customField").String()
	if customField != "preserved-value" {
		t.Errorf("expected customField 'preserved-value', got %q", customField)
	}

	metadataSource := writeBackResult.Get("data.private.invitations.0.metadata.source").String()
	if metadataSource != "form-123" {
		t.Errorf("expected metadata.source 'form-123', got %q", metadataSource)
	}

	// processedAt should also be set
	processedAt := writeBackResult.Get("data.private.invitations.0.processedAt")
	if !processedAt.Exists() || processedAt.String() == "" {
		t.Error("expected processedAt to be set")
	}
}

func TestMapFundraiserReferrals_MissingField(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	// Profile has no private.invitations field at all
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"private": {"name": "Test"}}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if activities != nil {
		t.Errorf("expected nil activities, got %v", activities)
	}
	if writeBack != nil {
		t.Errorf("expected nil writeBack, got %v", writeBack)
	}
}

func TestMapFundraiserReferrals_NotAnArray(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	// Field exists but is not an array
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"private": {"invitations": "not-an-array"}}`)},
		},
	}

	activities, writeBack, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if activities != nil {
		t.Errorf("expected nil activities, got %v", activities)
	}
	if writeBack != nil {
		t.Errorf("expected nil writeBack, got %v", writeBack)
	}
}
