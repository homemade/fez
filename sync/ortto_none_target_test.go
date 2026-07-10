package sync

import (
	"context"
	"testing"
)

// Ensure the package is initialised for tests that reach
// NewOrttoMapper. Using the package-private flag avoids the env-var
// validation Init() runs, which is unrelated to what these tests
// exercise. If another test file already initialised the flavour,
// this is a no-op.
func init() {
	if initialisedFlavour == nil {
		f := Raisely2Ortto
		initialisedFlavour = &f
	}
}

func TestNewOrttoMapper_OrttoNoneReturnsNoop(t *testing.T) {
	sc := &SyncContext{Config: Config{Target: "ortto-none"}}
	m := NewOrttoMapper(sc)
	if _, ok := m.(noopOrttoMapper); !ok {
		t.Fatalf("expected noopOrttoMapper for target ortto-none, got %T", m)
	}
}

func TestNewOrttoMapper_OrttoContactsAndActivitiesUnchanged(t *testing.T) {
	// Guard against accidentally routing existing targets to the noop
	// mapper.
	tests := []struct {
		target string
		want   OrttoMapper
	}{
		{"", &OrttoContactsMapper{}},
		{"ortto-contacts", &OrttoContactsMapper{}},
		{"ortto-activities", &OrttoActivitiesMapper{}},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			sc := &SyncContext{Config: Config{Target: tc.target}}
			m := NewOrttoMapper(sc)
			switch tc.want.(type) {
			case *OrttoContactsMapper:
				if _, ok := m.(*OrttoContactsMapper); !ok {
					t.Fatalf("target %q: expected *OrttoContactsMapper, got %T", tc.target, m)
				}
			case *OrttoActivitiesMapper:
				if _, ok := m.(*OrttoActivitiesMapper); !ok {
					t.Fatalf("target %q: expected *OrttoActivitiesMapper, got %T", tc.target, m)
				}
			}
		})
	}
}

func TestNoopOrttoRequest_ZeroItemsAndFalseCasts(t *testing.T) {
	var r OrttoRequest = noopOrttoRequest{}
	if r.ItemCount() != 0 {
		t.Fatalf("expected ItemCount 0, got %d", r.ItemCount())
	}
	if _, ok := r.AsOrttoContactsRequest(); ok {
		t.Fatalf("AsOrttoContactsRequest should return false")
	}
	if _, ok := r.AsOrttoActivitiesRequest(); ok {
		t.Fatalf("AsOrttoActivitiesRequest should return false")
	}
}

func TestNoopOrttoMapper_AllMethodsReturnZeroRequest(t *testing.T) {
	m := noopOrttoMapper{}
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() (OrttoRequest, error)
	}{
		{"MapFundraisingPage", func() (OrttoRequest, error) {
			return m.MapFundraisingPage(nil, FundraiserData{})
		}},
		{"MapTeamFundraisingPage", func() (OrttoRequest, error) {
			return m.MapTeamFundraisingPage(nil, TeamData{})
		}},
		{"MapTrackingData", func() (OrttoRequest, error) {
			return m.MapTrackingData(nil, nil, ctx)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := tc.run()
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			// Zero-not-nil: callers like the tracking handler at
			// api/raisely/tracking/index.go:99 dereference
			// req.ItemCount() without a nil-guard, so returning nil
			// here would nil-pointer-crash them.
			if req == nil {
				t.Fatalf("expected non-nil OrttoRequest (zero-not-nil), got nil")
			}
			if req.ItemCount() != 0 {
				t.Fatalf("expected ItemCount 0, got %d", req.ItemCount())
			}
		})
	}
}

func TestNoopOrttoMapper_SendRequestReturnsNilNil(t *testing.T) {
	m := noopOrttoMapper{}
	resp, err := m.SendRequest(noopOrttoRequest{}, context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil OrttoResponse, got %v", resp)
	}
}

func TestMustFindFirstCampaignMappingFileWithTargetByPath_OrttoNone(t *testing.T) {
	em := memMappings(t, "webhooks", map[string]string{
		"webhooks/TOPB/LTL_2026_PRD.ortto-none.yaml": "api:\n settings: {}\n",
	})
	file, target, err := em.MustFindFirstCampaignMappingFileWithTargetByPath("TOPB/LTL_2026_PRD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != "ortto-none" {
		t.Fatalf("expected target ortto-none, got %q", target)
	}
	if file.Name == "" {
		t.Fatalf("expected non-empty file.Name")
	}
}

func TestMustFindRequiredMappingFileForTarget_OrttoNone(t *testing.T) {
	em := memMappings(t, "webhooks", map[string]string{
		"webhooks/required.ortto-none.yaml": "api:\n keys:\n  raisely: k\n",
	})
	file, err := em.MustFindRequiredMappingFileForTarget("ortto-none")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file.Name != "webhooks/required.ortto-none.yaml" {
		t.Fatalf("expected required.ortto-none.yaml lookup, got %q", file.Name)
	}
}

func TestService_CheckOrttoFields_OrttoNoneReturnsEmpty(t *testing.T) {
	svc := NewService(Config{Target: "ortto-none"}, "campaign-id", TriggerInfo{})
	got, err := svc.CheckOrttoFields(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}

func TestService_EnsureOrttoFields_OrttoNoneReturnsEmpty(t *testing.T) {
	svc := NewService(Config{Target: "ortto-none"}, "campaign-id", TriggerInfo{})
	got, err := svc.EnsureOrttoFields(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}
