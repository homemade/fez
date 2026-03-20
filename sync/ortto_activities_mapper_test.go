// go test github.com/homemade/fez/sync -v -run TestMapFundraiserReferrals
package sync

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func newTestActivitiesMapper(referralsField string, builtinMappings FieldMappings, customMappings FieldMappings) *OrttoActivitiesMapper {
	config := Config{
		Target: "ortto-activities",
	}
	config.API.Settings.OrttoActivityID = "act:cm:test-activity"
	config.API.Settings.OrttoFundraiserMergeField = "str:cm:raisely-user-id"
	config.API.Settings.RaiselyFundraiserReferralsField = referralsField
	config.FundraiserReferralFieldMappings.Builtin = builtinMappings
	config.FundraiserReferralFieldMappings.Custom = customMappings

	sc := &SyncContext{Config: config, Campaign: "test-campaign"}
	return &OrttoActivitiesMapper{
		SyncContext: sc,
		RaiselyMapper: RaiselyMapper{
			SyncContext: sc,
		},
	}
}

// Standard builtin mappings for tests that need email for merge-by
var testReferralBuiltins = FieldMappings{
	Strings: map[string]string{
		"str::email": "email",
		"str::first": "firstName",
		"str::last":  "lastName",
	},
}

func TestMapFundraiserReferrals_NotConfigured(t *testing.T) {
	mapper := newTestActivitiesMapper("", FieldMappings{}, FieldMappings{})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"uuid": "profile-123"}`)},
		},
	}

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestMapFundraiserReferrals_EmptyArray(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{"private": {"invitations": []}}`)},
		},
	}

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestMapFundraiserReferrals_AllProcessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestMapFundraiserReferrals_OneUnprocessed(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	// Check the request
	activitiesReq, ok := result.Request.AsOrttoActivitiesRequest()
	if !ok {
		t.Fatal("expected OrttoActivitiesRequest")
	}
	if len(activitiesReq.Activities) != 1 {
		t.Fatalf("expected 1 activity, got %d", len(activitiesReq.Activities))
	}

	// Check merge_by uses email
	if len(activitiesReq.MergeBy) != 1 || activitiesReq.MergeBy[0] != "str::email" {
		t.Errorf("expected MergeBy [str::email], got %v", activitiesReq.MergeBy)
	}

	// Check activity fields contain builtin person fields (used for merge-by)
	activity := activitiesReq.Activities[0]
	if activity.ActivityID != "act:cm:test-activity" {
		t.Errorf("expected activity ID 'act:cm:test-activity', got %q", activity.ActivityID)
	}
	if v, ok := activity.Fields["str::email"]; !ok || v != "jane@example.com" {
		t.Errorf("expected Fields[str::email] 'jane@example.com', got %v", v)
	}

	// Check activity attributes contain custom mapped fields
	if v, ok := activity.Attributes["str:cm:referral-first-name"]; !ok || v != "Jane" {
		t.Errorf("expected referral-first-name 'Jane', got %v", v)
	}
	if v, ok := activity.Attributes["str:cm:referral-email"]; !ok || v != "jane@example.com" {
		t.Errorf("expected referral-email 'jane@example.com', got %v", v)
	}

	// Check write-back
	writeBack := result.RaiselyUpdate
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
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	activitiesReq, ok := result.Request.AsOrttoActivitiesRequest()
	if !ok {
		t.Fatal("expected OrttoActivitiesRequest")
	}
	if len(activitiesReq.Activities) != 3 {
		t.Fatalf("expected 3 activities, got %d", len(activitiesReq.Activities))
	}

	// Check each activity has correct email
	expectedEmails := []string{"jane@example.com", "bob@example.com", "alice@example.com"}
	for i, activity := range activitiesReq.Activities {
		email, ok := activity.Attributes["str:cm:referral-email"]
		if !ok || email != expectedEmails[i] {
			t.Errorf("activity %d: expected email %q, got %v", i, expectedEmails[i], email)
		}
	}

	// Check all entries have processedAt in write-back
	writeBack := result.RaiselyUpdate
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
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	activitiesReq, ok := result.Request.AsOrttoActivitiesRequest()
	if !ok {
		t.Fatal("expected OrttoActivitiesRequest")
	}
	if len(activitiesReq.Activities) != 1 {
		t.Fatalf("expected 1 activity (only unprocessed), got %d", len(activitiesReq.Activities))
	}

	// Only jane should be mapped
	email, ok := activitiesReq.Activities[0].Attributes["str:cm:referral-email"]
	if !ok || email != "jane@example.com" {
		t.Errorf("expected email 'jane@example.com', got %v", email)
	}

	// Check write-back preserves existing processedAt and adds new one
	writeBack := result.RaiselyUpdate
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
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	// Check write-back preserves unknown fields
	writeBack := result.RaiselyUpdate
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

func TestMapFundraiserReferrals_ParentPathTraversal(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email":          "email",
			"str:cm:referrer-registration-id": "^.uuid",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"uuid": "profile-123",
				"private": {
					"invitations": [
						{"email": "jane@example.com"}
					]
				}
			}`)},
		},
	}

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	activitiesReq, ok := result.Request.AsOrttoActivitiesRequest()
	if !ok {
		t.Fatal("expected OrttoActivitiesRequest")
	}
	if len(activitiesReq.Activities) != 1 {
		t.Fatalf("expected 1 activity, got %d", len(activitiesReq.Activities))
	}

	activity := activitiesReq.Activities[0]

	// ^.uuid should resolve to the profile's uuid
	if v, ok := activity.Attributes["str:cm:referrer-registration-id"]; !ok || v != "profile-123" {
		t.Errorf("expected referrer-registration-id 'profile-123', got %v", v)
	}

	// email should resolve from the referral entry
	if v, ok := activity.Attributes["str:cm:referral-email"]; !ok || v != "jane@example.com" {
		t.Errorf("expected referral-email 'jane@example.com', got %v", v)
	}
}

func TestMapFundraiserReferrals_MissingEmailSkipsActivity(t *testing.T) {
	// No builtin email mapping — referral has no str::email so it's skipped
	// but still marked as processed
	mapper := newTestActivitiesMapper("private.invitations", FieldMappings{}, FieldMappings{
		Strings: map[string]string{
			"str:cm:referral-email": "email",
		},
	})
	profileData := FundraiserData{
		Page: FundraisingPage{
			Source: Source{data: gjson.Parse(`{
				"private": {
					"invitations": [
						{"email": "jane@example.com"}
					]
				}
			}`)},
		},
	}

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result (for write-back), got nil")
	}

	// No activities should be sent (email missing from Fields)
	activitiesReq, ok := result.Request.AsOrttoActivitiesRequest()
	if !ok {
		t.Fatal("expected OrttoActivitiesRequest")
	}
	if len(activitiesReq.Activities) != 0 {
		t.Errorf("expected 0 activities (skipped due to missing email), got %d", len(activitiesReq.Activities))
	}

	// Write-back should still mark as processed
	if result.RaiselyUpdate == nil {
		t.Fatal("expected RaiselyUpdate for write-back, got nil")
	}
	writeBackResult := gjson.Parse(result.RaiselyUpdate.JSON)
	processedAt := writeBackResult.Get("data.private.invitations.0.processedAt")
	if !processedAt.Exists() || processedAt.String() == "" {
		t.Error("expected processedAt to be set despite skipped activity")
	}
}

func TestMapFundraiserReferrals_MissingField(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestMapFundraiserReferrals_NotAnArray(t *testing.T) {
	mapper := newTestActivitiesMapper("private.invitations", testReferralBuiltins, FieldMappings{
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

	result, err := mapper.MapFundraiserReferrals("profile-123", profileData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}
