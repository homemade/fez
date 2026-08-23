package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// teamMember describes one team member for [teamReferralsFixture].
// Invitations is the JSON snippet the fixture inlines into the member
// page's private.invitations — leave empty for no invitations.
type teamMember struct {
	UUID        string
	Invitations string // e.g. `{"email":"fresh@example.com"}`
}

// teamReferralsFixture wires an httptest.Server that answers the
// Raisely endpoints [Service.MapFundraisingProfile] and
// [Service.MapByWebhookModel] hit for a team + N member profiles:
//
//   - GET /v3/profiles/<team> (team page)
//   - GET /v3/profiles/<team>/members (members list)
//   - GET /v3/profiles/<memberID> (each member's individual page)
//
// It builds a [Service] whose mapper is the noop mapper (Target
// "ortto-none"), so the tests focus on the referrals detection path
// without wiring an outbound side.
type teamReferralsFixture struct {
	server  *httptest.Server
	svc     *Service
	team    string
	members []teamMember
}

func newTeamReferralsFixture(t *testing.T, members ...teamMember) *teamReferralsFixture {
	t.Helper()

	if len(members) == 0 {
		members = []teamMember{{
			UUID:        "member-uuid-abc",
			Invitations: `{"firstName":"Fresh","lastName":"Invitee","email":"fresh@example.com"}`,
		}}
	}

	f := &teamReferralsFixture{
		team:    "team-uuid-xyz",
		members: members,
	}

	memberByUUID := make(map[string]teamMember, len(members))
	for _, m := range members {
		memberByUUID[m.UUID] = m
	}

	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v3/profiles/"+f.team:
			_, _ = fmt.Fprintf(w, `{"data":{"uuid":%q,"type":"GROUP"}}`, f.team)

		case r.URL.Path == "/v3/profiles/"+f.team+"/members":
			uuids := make([]map[string]string, 0, len(f.members))
			for _, m := range f.members {
				uuids = append(uuids, map[string]string{"uuid": m.UUID})
			}
			body, _ := json.Marshal(map[string]interface{}{"data": uuids})
			_, _ = w.Write(body)

		default:
			// Assume any other /v3/profiles/<uuid> path is a member fetch.
			for uuid, m := range memberByUUID {
				if r.URL.Path == "/v3/profiles/"+uuid {
					_, _ = fmt.Fprintf(w,
						`{"data":{"uuid":%q,"type":"INDIVIDUAL","parent":{"uuid":%q,"type":"GROUP"},"private":{"invitations":[%s]}}}`,
						m.UUID, f.team, m.Invitations,
					)
					return
				}
			}
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	t.Cleanup(f.server.Close)

	config := Config{Target: "ortto-none"}
	config.API.Keys.Raisely = "test-key"
	config.API.Endpoints.Raisely = f.server.URL
	config.API.Settings.RaiselyFundraiserReferralsField = "private.invitations"
	config.FundraiserReferralFieldMappings = RaiselyMessageMappings{
		User: map[string]string{
			"email":     "email",
			"firstName": "firstName",
			"lastName":  "lastName",
		},
	}

	sc := &SyncContext{
		Config:      config,
		Campaign:    "campaign-uuid",
		TriggerInfo: TriggerInfo{TriggerType: "test"},
	}
	f.svc = &Service{
		sc:      sc,
		fetcher: &RaiselyFetcherAndUpdater{SyncContext: sc},
		// Campaign profile UUID must NOT match any member's parent UUID
		// — otherwise TeamP2PID returns "" for team-member triggers.
		campaign: &FundraisingCampaign{
			Profile: struct{ P2PID string }{P2PID: "campaign-owner-uuid"},
		},
		mapper: noopOrttoMapper{},
	}
	return f
}

// firstMember returns the UUID of the first member — a convenience
// for tests that pick one member as the "triggering" one.
func (f *teamReferralsFixture) firstMember() string { return f.members[0].UUID }

// TestMapFundraisingProfile_TeamMember_FansOutAcrossMembers pins the
// team-branch fan-out of [Service.MapFundraisingProfile]: a manual
// sync on a team-member profile scans every member's page (already
// loaded by FetchTeamData) and returns one batch per member that
// has unprocessed invitations.
func TestMapFundraisingProfile_TeamMember_FansOutAcrossMembers(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-alice", Invitations: `{"email":"alice-invite@example.com"}`},
		teamMember{UUID: "m-bob", Invitations: `{"email":"bob-invite@example.com","processedAt":"2026-03-01T00:00:00Z"}`},
		teamMember{UUID: "m-charlie", Invitations: `{"email":"charlie-invite@example.com"}`},
	)

	// Trigger on any team-member profile — fan-out covers the team.
	_, batches, err := f.svc.MapFundraisingProfile("m-alice", context.Background())
	if err != nil {
		t.Fatalf("MapFundraisingProfile returned error: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches (alice + charlie; bob already processed), got %d", len(batches))
	}
	profileIDs := map[string]bool{batches[0].ProfileID: true, batches[1].ProfileID: true}
	if !profileIDs["m-alice"] || !profileIDs["m-charlie"] {
		t.Errorf("batch ProfileIDs = %v, want {m-alice, m-charlie}", profileIDs)
	}
	if profileIDs["m-bob"] {
		t.Error("bob's already-processed invitation should not produce a batch")
	}
}

// TestMapFundraisingProfile_TeamAllProcessed_ReturnsNil pins that a
// team with every invitation already processed produces no batches.
func TestMapFundraisingProfile_TeamAllProcessed_ReturnsNil(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-1", Invitations: `{"email":"a@x","processedAt":"2026-03-01T00:00:00Z"}`},
		teamMember{UUID: "m-2", Invitations: `{"email":"b@x","processedAt":"2026-03-01T00:00:00Z"}`},
	)

	_, batches, err := f.svc.MapFundraisingProfile("m-1", context.Background())
	if err != nil {
		t.Fatalf("MapFundraisingProfile returned error: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected nil/empty batches when all processed, got %d", len(batches))
	}
}

// TestMapByWebhookModel_TeamMember_EligibleEvent_FansOut pins the
// team-branch fan-out from [Service.MapByWebhookModel]. A webhook on
// one team member with an eligible event scans every member — so
// stuck invitations across the team get cleared even if the trigger
// wasn't on that member.
func TestMapByWebhookModel_TeamMember_EligibleEvent_FansOut(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-trigger", Invitations: ""}, // triggering member has no invitations
		teamMember{UUID: "m-stuck", Invitations: `{"email":"stuck@example.com"}`},
	)

	_, batches, err := f.svc.MapByWebhookModel(
		"INDIVIDUAL", "m-trigger", "GROUP", f.team,
		false, "profile.updated",
		context.Background(),
	)
	if err != nil {
		t.Fatalf("MapByWebhookModel returned error: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (for m-stuck), got %d", len(batches))
	}
	if batches[0].ProfileID != "m-stuck" {
		t.Errorf("ProfileID = %q, want m-stuck", batches[0].ProfileID)
	}
}

// TestMapByWebhookModel_TeamMember_NonEligibleEvent_NoBatches pins
// that high-frequency totals events skip the whole team-referrals
// fan-out. Same gate as the individual-only path.
func TestMapByWebhookModel_TeamMember_NonEligibleEvent_NoBatches(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-stuck", Invitations: `{"email":"stuck@example.com"}`},
	)

	_, batches, err := f.svc.MapByWebhookModel(
		"INDIVIDUAL", f.firstMember(), "GROUP", f.team,
		false, "profile.totalUpdated",
		context.Background(),
	)
	if err != nil {
		t.Fatalf("MapByWebhookModel returned error: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected no batches for non-eligible event, got %d", len(batches))
	}
}

// TestMapByWebhookModel_GroupTrigger_EligibleEvent_FansOut pins that
// a GROUP-typed trigger (the team itself — e.g. team name edit) also
// fans referrals across every member on an eligible event. Matches
// the fact that MapTeamFundraisingPage already re-syncs every
// member's outbound state on any team-branch trigger.
func TestMapByWebhookModel_GroupTrigger_EligibleEvent_FansOut(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-1", Invitations: `{"email":"one@example.com"}`},
		teamMember{UUID: "m-2", Invitations: ""},
	)

	_, batches, err := f.svc.MapByWebhookModel(
		"GROUP", f.team, "", "",
		false, "profile.updated",
		context.Background(),
	)
	if err != nil {
		t.Fatalf("MapByWebhookModel returned error: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (for m-1), got %d", len(batches))
	}
	if batches[0].ProfileID != "m-1" {
		t.Errorf("ProfileID = %q, want m-1", batches[0].ProfileID)
	}
}

// TestMapByWebhookModel_GroupTrigger_NonEligibleEvent_NoBatches
// mirrors the INDIVIDUAL non-eligible case for GROUP triggers.
func TestMapByWebhookModel_GroupTrigger_NonEligibleEvent_NoBatches(t *testing.T) {
	t.Parallel()
	f := newTeamReferralsFixture(t,
		teamMember{UUID: "m-1", Invitations: `{"email":"one@example.com"}`},
	)

	_, batches, err := f.svc.MapByWebhookModel(
		"GROUP", f.team, "", "",
		false, "profile.totalUpdated",
		context.Background(),
	)
	if err != nil {
		t.Fatalf("MapByWebhookModel returned error: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected no batches, got %d", len(batches))
	}
}
