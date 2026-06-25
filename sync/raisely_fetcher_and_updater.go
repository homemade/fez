package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	gosync "sync"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/tidwall/gjson"
)

const (
	FundraisingProfilesSinceTimestampFormat = "2006-01-02T15:04:05.999"
	FundraisingProfilesSinceLimit           = "1000"
	FundraisingProfileExerciseLogsLimit     = "1000"
	FundraisingProfileDonationsLimit        = "1000"
)

type fetchRaiselyDataParams struct {
	RaiselyAPIKey     string
	P2PID             string
	Context           context.Context
	RaiselyAPIBuilder *requests.Builder
}

type RaiselyError map[string]interface{}

type FundraisingPage struct {
	Source Source
}

type Source struct {
	data   gjson.Result
	parent *Source // optional parent for ^. path traversal
}

// resolve returns the Source and path to query. Paths prefixed with "^."
// are resolved against the parent Source (if set), stripping the prefix.
func (s Source) resolve(path string) (Source, string) {
	if strings.HasPrefix(path, "^.") && s.parent != nil {
		return *s.parent, strings.TrimPrefix(path, "^.")
	}
	return s, path
}

func (s Source) StringForPath(path string) (string, bool) {
	src, p := s.resolve(path)
	result := src.data.Get(p)
	return result.String(), result.Exists() && (result.Value() != nil)
}

func (s Source) IntForPath(path string) (int64, bool) {
	src, p := s.resolve(path)
	result := src.data.Get(p)
	return result.Int(), result.Exists() && (result.Value() != nil)
}

func (s Source) BoolForPath(path string) (bool, bool) {
	src, p := s.resolve(path)
	result := src.data.Get(p)
	return result.Bool(), result.Exists() && (result.Value() != nil)
}

func (s Source) Data() map[string]interface{} {
	if v := s.data.Value(); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}
	return nil
}

type FundraisingTeam struct {
	TeamMembers []TeamMember `json:"data"`
}

type TeamMember struct {
	P2PID string `json:"uuid"`
}

type FundraisingCampaign struct {
	Name    string
	Profile struct {
		P2PID string
	}
	FundraisingPageDefaults []CampaignDefault
}

type CampaignDefault struct {
	Label string
	Value string
}

type FundraisingProfilesSince struct {
	Timestamp time.Time
	Results   []FundraisingProfile `json:"data"`
}

type FundraisingProfileExerciseLogs struct {
	ExerciseLogs []ExerciseLogEntry `json:"data"`
}

type ExerciseLogEntry struct {
	Activity string  `json:"activity"`
	Date     string  `json:"date"`
	Distance float64 `json:"distance"`
}

func (e ExerciseLogEntry) IncludeForStreak(config FundraiserExtensionsConfig) bool {
	if e.Distance < 1 {
		return false
	}
	if len(config.Streaks.Activity.Filter) > 0 &&
		!slices.Contains(config.Streaks.Activity.Filter, e.Activity) {
		return false
	}
	if config.Streaks.Activity.From != "" {
		t1, err := time.Parse(time.RFC3339, config.Streaks.Activity.From)
		if err != nil {
			return false
		}
		t2, err := time.Parse(time.RFC3339, e.TimestampForStreak())
		if err != nil {
			return false
		}
		if t2.Before(t1) {
			return false
		}
	}
	if config.Streaks.Activity.To != "" {
		t1, err := time.Parse(time.RFC3339, config.Streaks.Activity.To)
		if err != nil {
			return false
		}
		t2, err := time.Parse(time.RFC3339, e.TimestampForStreak())
		if err != nil {
			return false
		}
		if t2.After(t1) {
			return false
		}
	}
	return true
}

func (e ExerciseLogEntry) TimestampForStreak() string {
	return e.Date
}

type FundraisingProfileDonations struct {
	Donations []Donation `json:"data"`
}

type Donation struct {
	User struct {
		Uuid string `json:"uuid"`
	} `json:"user"`
	CreatedAt string  `json:"createdAt"`
	Date      string  `json:"date"`
	Type      string  `json:"type"`
	Amount    float64 `json:"amount"`
}

func (d Donation) IncludeForStreak(config FundraiserExtensionsConfig) bool {
	return d.Amount > 0
}

func (d Donation) TimestampForStreak() string {
	if d.Type == "OFFLINE" {
		return d.Date
	}
	return d.CreatedAt
}

type FundraisingProfile struct {
	P2PID  string `json:"uuid"`
	Parent struct {
		P2PID string `json:"uuid"`
		Type  string `json:"type"`
	} `json:"parent"`
	Status    string `json:"status"`
	Type      string `json:"type"`
	UpdatedAt string `json:"updatedAt"`
}

type Webhook struct {
	Secret string `json:"secret"`
	Data   struct {
		Uuid      string                 `json:"uuid"`
		Type      string                 `json:"type"`
		CreatedAt string                 `json:"createdAt"`
		Source    string                 `json:"source"`
		Data      map[string]interface{} `json:"data"`
	} `json:"data"`
	Diff map[string]interface{} `json:"diff"`
}

func (p *FundraisingPage) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	var json string
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s", params.P2PID).
		Param("private", "true").
		Bearer(params.RaiselyAPIKey).
		ToString(&json).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err == nil {
		if !gjson.Valid(json) {
			log.Printf("Invalid Raisely Response:\n%s", json)
			return errors.New("invalid json response")
		}
	} else {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	p.Source.data = gjson.Parse(json).Get("data")
	return err
}

func (p FundraisingPage) HasSameOwnerAs(other FundraisingPage) (bool, error) {
	owner, exists := p.Source.StringForPath("user.uuid")
	if !exists {
		return false, errors.New("page is missing an owner")
	}
	otherOwner, otherOwnerExists := other.Source.StringForPath("user.uuid")
	if !otherOwnerExists {
		return false, errors.New("other page is missing an owner")
	}
	return (owner == otherOwner), nil
}

func (t *FundraisingTeam) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/members", params.P2PID).
		Param("private", "true").
		Bearer(params.RaiselyAPIKey).
		ToJSON(t).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	return err
}

func (c *FundraisingCampaign) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	var json string
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/campaigns/%s", params.P2PID).
		Param("private", "true").
		Bearer(params.RaiselyAPIKey).
		ToString(&json).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err == nil {
		if !gjson.Valid(json) {
			log.Printf("Invalid Raisely Response:\n%s", json)
			return errors.New("invalid json response")
		}
	} else {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	data := gjson.Parse(json).Get("data")
	c.Name = data.Get("name").String()
	c.Profile.P2PID = data.Get("profile.uuid").String()
	profileCustomFields := data.Get("config.customFields.profile")
	if profileCustomFields.Exists() {
		for _, v := range profileCustomFields.Array() {
			d := v.Get("default")
			l := v.Get("label")
			if d.Exists() && d.String() != "" && l.Exists() && l.String() != "" {
				c.FundraisingPageDefaults = append(c.FundraisingPageDefaults, CampaignDefault{
					Label: l.String(),
					Value: d.String(),
				})
			}
		}
	}

	return err
}

func (p *FundraisingProfilesSince) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/campaigns/%s/profiles", params.P2PID).
		Param("updatedAtAfter", p.Timestamp.Format(FundraisingProfilesSinceTimestampFormat)).
		Param("sort", "updatedAt").
		Param("order", "ASC").
		Param("limit", FundraisingProfilesSinceLimit).
		Bearer(params.RaiselyAPIKey).
		ToJSON(p).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	return err
}

func (p FundraisingProfile) TeamP2PID(fundraisingCampaign *FundraisingCampaign) string {
	if p.Type == "GROUP" {
		return p.P2PID
	}
	if p.Parent.Type == "GROUP" {
		if fundraisingCampaign.Profile.P2PID != p.Parent.P2PID {
			return p.Parent.P2PID
		}
	}

	return ""
}

func (d *FundraisingProfileDonations) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/donations", params.P2PID).
		Param("private", "true").
		Param("limit", FundraisingProfileDonationsLimit).
		Bearer(params.RaiselyAPIKey).
		ToJSON(d).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}

	return err
}

func (d *FundraisingProfileExerciseLogs) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/exercise-logs", params.P2PID).
		Param("private", "true").
		Param("limit", FundraisingProfileExerciseLogsLimit).
		Bearer(params.RaiselyAPIKey).
		ToJSON(d).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}

	return err
}

// RaiselyFetcherAndUpdater handles fetching data from the Raisely API.
// It embeds *SyncContext for shared sync configuration.
//
// FundraisingCampaignCache is an optional cross-call cache for
// FundraisingCampaign documents — see [FundraisingCampaignCache] for the
// interface and [CachedFundraisingCampaign] for the orchestration. nil
// means no caching at all: every CachedFundraisingCampaign call fetches
// directly from Raisely.
type RaiselyFetcherAndUpdater struct {
	*SyncContext

	// FundraisingCampaignCache supplies cross-call cache state for
	// CachedFundraisingCampaign. Set on construction (via
	// [ServiceWithFundraisingCampaignCache] or directly on a struct
	// literal); nil disables caching.
	FundraisingCampaignCache FundraisingCampaignCache
}

// FetchFundraisingCampaign fetches the campaign data from Raisely.
func (r *RaiselyFetcherAndUpdater) FetchFundraisingCampaign(p2pID string, ctx context.Context) (*FundraisingCampaign, error) {
	campaign := &FundraisingCampaign{}
	err := campaign.fetchRaiselyData(r.fetchParams(p2pID, ctx))
	if err != nil {
		return nil, err
	}
	return campaign, nil
}

// CachedFundraisingCampaign returns the campaign data for p2pID, consulting
// the configured [FundraisingCampaignCache] when present. With no cache
// configured (FundraisingCampaignCache == nil) every call fetches from
// Raisely. With a cache, refresh=false consults Get first and on miss
// falls through to Raisely + best-effort Set; refresh=true bypasses Get
// (write-through fetch + Set). A Raisely fetch error is returned to the
// caller; there is no stale fallback.
// Backing-store errors fail open: Get errors are logged
// and treated as a miss; Set errors are logged and the fresh value is
// returned regardless.
func (r *RaiselyFetcherAndUpdater) CachedFundraisingCampaign(p2pID string, refresh bool, ctx context.Context) (*FundraisingCampaign, error) {
	cache := r.FundraisingCampaignCache
	if cache == nil {
		return r.FetchFundraisingCampaign(p2pID, ctx)
	}

	if !refresh {
		hit, ok, err := cache.Get(ctx, p2pID)
		if err != nil {
			log.Printf("FundraisingCampaignCache.Get(%s): %v (treating as miss)", p2pID, err)
		} else if ok {
			return hit, nil
		}
	}

	fresh, err := r.FetchFundraisingCampaign(p2pID, ctx)
	if err != nil {
		return nil, err
	}

	if err := cache.Set(ctx, p2pID, r.Config.EnvVar.Org(), fresh); err != nil {
		log.Printf("FundraisingCampaignCache.Set(%s): %v (continuing)", p2pID, err)
	}
	return fresh, nil
}

// FundraiserData holds the fetched data for a single fundraiser.
type FundraiserData struct {
	Page         FundraisingPage
	ExerciseLogs FundraisingProfileExerciseLogs
	Donations    FundraisingProfileDonations
}

type TeamData struct {
	Team        FundraisingTeam
	TeamPage    FundraisingPage
	MemberPages []FundraisingPage
}

// RaiselyAPIKey returns the Raisely API key from the config.
func (r *RaiselyFetcherAndUpdater) RaiselyAPIKey() string {
	return r.Config.API.Keys.Raisely
}

// RaiselyAPIBuilder returns a new requests.Builder configured for the Raisely API.
//
// Layered behaviours mirror [OrttoFetcherAndUpdater.OrttoAPIBuilder]:
//
//   - **Distinct outbound User-Agent** — set from [SyncContext.UserAgent]
//     (falling back to [DefaultUserAgent]). Identifies the consumer to
//     Raisely + their edge layer; load-bearing for incident communication
//     where the operator filters edge logs by UA.
//   - **Per-call attribution log line** — `Raisely call: source=<TriggerType>
//     ua=<User-Agent> path=… status=…`. A grep on `Raisely call:` recovers
//     the exact UA we were sending at any moment in our own logs.
//   - **429 detection** — a 429 response is surfaced as
//     [*RaiselyRateLimitError] so callers can [IsRateLimited] and
//     branch (defer/ack instead of 500, place a tenant-scoped hold).
//     No body / `Retry-After` to parse — Raisely's 429s originate at
//     a Cloudflare edge layer that exposes neither — so the typed
//     error just carries the status code + captured response headers
//     for incident attribution. No `runtime.Callers` stack (Cloudflare
//     opacity reduces its value vs the Ortto side).
func (r *RaiselyFetcherAndUpdater) RaiselyAPIBuilder() *requests.Builder {
	apiBuilder := requests.
		URL(r.Config.API.Endpoints.Raisely).
		UserAgent(r.userAgent()).
		Client(&http.Client{Timeout: HTTPRequestTimeout}).
		AddValidator(raiselyCallValidator(r.TriggerType))
	if r.RecordRequests {
		apiBuilder = apiBuilder.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/raisely", r.Campaign)))
	}
	return apiBuilder
}

// raiselyCallValidator is the validator added to every Raisely request
// by [RaiselyFetcherAndUpdater.RaiselyAPIBuilder] (and its Custom
// Messages sibling). It does two jobs:
//
//  1. Emits a one-line per-call attribution log
//     `Raisely call: source=<triggerType> ua=<User-Agent> path=… status=…`
//     — `ua=` is pulled from the outbound request header so the value
//     downstream / edge logs see is recorded verbatim. Load-bearing
//     for incident communication where the operator filters edge logs
//     by UA.
//  2. Intercepts 429 responses and returns [*RaiselyRateLimitError]
//     (with captured response headers), so callers can [IsRateLimited]
//     without re-parsing. No body / `Retry-After` to read — Raisely's
//     429s come from a Cloudflare edge layer that exposes neither.
//
// Non-429 responses (success or other errors) fall through with `nil`
// — the caller's existing `.ErrorJSON(…)` / `.ToJSON(…)` chain handles
// success bodies and non-rate-limit errors.
func raiselyCallValidator(triggerType string) requests.ResponseHandler {
	return func(resp *http.Response) error {
		logAPICall("Raisely", triggerType, resp)

		if resp.StatusCode != http.StatusTooManyRequests {
			return nil
		}

		// 429: surface the typed error with the captured response
		// headers. No body parse — Raisely's edge 429s are empty;
		// any reuse-as-a-decoded-error attempt would just race the
		// downstream `.ErrorJSON` chain for no signal.
		return &RaiselyRateLimitError{
			StatusCode:      resp.StatusCode,
			ResponseHeaders: resp.Header.Clone(),
		}
	}
}

// fetchParams builds fetchRaiselyDataParams for a given P2P ID and context.
func (r *RaiselyFetcherAndUpdater) fetchParams(p2pID string, ctx context.Context) fetchRaiselyDataParams {
	return fetchRaiselyDataParams{
		RaiselyAPIKey:     r.RaiselyAPIKey(),
		P2PID:             p2pID,
		Context:           ctx,
		RaiselyAPIBuilder: r.RaiselyAPIBuilder(),
	}
}

// FetchFundraiserData fetches a fundraising page and optionally exercise logs and donations.
func (r *RaiselyFetcherAndUpdater) FetchFundraisingPage(p2pID string, ctx context.Context) (FundraisingPage, error) {

	var result FundraisingPage
	err := result.fetchRaiselyData(r.fetchParams(p2pID, ctx))
	return result, err

}

// FetchFundraiserData fetches a fundraising page and optionally exercise logs and donations.
func (r *RaiselyFetcherAndUpdater) FetchFundraiserData(p2pID string, ctx context.Context) (FundraiserData, error) {
	var result FundraiserData
	var wg gosync.WaitGroup
	var errPage, errLogs, errDonations error

	wg.Add(1)
	go func() {
		defer wg.Done()
		errPage = result.Page.fetchRaiselyData(r.fetchParams(p2pID, ctx))
	}()

	if r.Config.MapActivityLogs() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errLogs = result.ExerciseLogs.fetchRaiselyData(r.fetchParams(p2pID, ctx))
		}()
	}

	if r.Config.MapDonations() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errDonations = result.Donations.fetchRaiselyData(r.fetchParams(p2pID, ctx))
		}()
	}

	wg.Wait()
	if err := errors.Join(errPage, errLogs, errDonations); err != nil {
		return result, fmt.Errorf("raisely errors: %w", err)
	}

	return result, nil
}

// FetchTeamData fetches a team, its fundraising page, and all member pages.
func (r *RaiselyFetcherAndUpdater) FetchTeamData(p2pTeamID string, ctx context.Context) (TeamData, error) {
	team, teamPage, err := r.FetchTeam(p2pTeamID, ctx)
	if err != nil {
		return TeamData{}, err
	}
	memberPages, err := r.FetchTeamMembers(team, ctx)
	if err != nil {
		return TeamData{}, err
	}
	return TeamData{Team: team, TeamPage: teamPage, MemberPages: memberPages}, nil
}

// FetchTeam fetches a team and its fundraising page.
func (r *RaiselyFetcherAndUpdater) FetchTeam(p2pTeamID string, ctx context.Context) (FundraisingTeam, FundraisingPage, error) {
	var team FundraisingTeam
	var teamPage FundraisingPage
	var wg gosync.WaitGroup
	var errTeam, errPage error

	wg.Add(2)
	go func() {
		defer wg.Done()
		errTeam = team.fetchRaiselyData(r.fetchParams(p2pTeamID, ctx))
	}()
	go func() {
		defer wg.Done()
		errPage = teamPage.fetchRaiselyData(r.fetchParams(p2pTeamID, ctx))
	}()
	wg.Wait()

	if err := errors.Join(errTeam, errPage); err != nil {
		return team, teamPage, fmt.Errorf("raisely errors: %w", err)
	}
	return team, teamPage, nil
}

// FetchTeamMembers fetches fundraising pages for all members of a team.
func (r *RaiselyFetcherAndUpdater) FetchTeamMembers(team FundraisingTeam, ctx context.Context) ([]FundraisingPage, error) {
	if len(team.TeamMembers) == 0 {
		return nil, nil
	}

	pages := make([]FundraisingPage, len(team.TeamMembers))
	errs := make([]error, len(team.TeamMembers))

	var wg gosync.WaitGroup
	for i, member := range team.TeamMembers {
		wg.Add(1)
		go func(index int, memberP2PID string) {
			defer wg.Done()
			errs[index] = pages[index].fetchRaiselyData(r.fetchParams(memberP2PID, ctx))
		}(i, member.P2PID)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return pages, fmt.Errorf("raisely errors: %w", err)
	}
	return pages, nil
}

// FetchProfilesSince fetches fundraising profiles updated after the given timestamp.
func (r *RaiselyFetcherAndUpdater) FetchProfilesSince(campaignP2PID string, since time.Time, ctx context.Context) (FundraisingProfilesSince, error) {
	profiles := FundraisingProfilesSince{
		Timestamp: since,
	}
	err := profiles.fetchRaiselyData(r.fetchParams(campaignP2PID, ctx))
	return profiles, err
}

func (r *RaiselyFetcherAndUpdater) UpdateRaiselyData(request UpdateRaiselyDataRequest, ctx context.Context) (int, error) {
	raiselyError := RaiselyError{}
	var result int
	err := r.RaiselyAPIBuilder().
		Patch().
		Pathf("/v3/profiles/%s", request.P2PID).
		Param("partial", "true").
		Bearer(r.RaiselyAPIKey()).
		BodyBytes([]byte(request.JSON)).
		ContentType("application/json").
		ErrorJSON(&raiselyError).
		Handle(func(response *http.Response) error {
			result = response.StatusCode
			return nil
		}).
		Fetch(ctx)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	return result, err
}

type UpdateRaiselyDataRequest struct {
	P2PID string
	JSON  string
}

// RaiselyCustomMessageRequest is a single event POSTed to the Raisely
// Custom Messages API. User and Custom are pass-through maps that land
// at data.data.user.<key> and data.data.custom.<key> in the body.
type RaiselyCustomMessageRequest struct {
	Source string
	User   map[string]interface{}
	Custom map[string]interface{}
}

// ReferralBatch carries everything needed to send a set of Raisely
// Custom Message events and write back per-entry processedAt to Raisely.
// The send + write-back are bundled inside the receiving Service method
// so that a partial send failure only leaves failed entries unprocessed,
// letting the next webhook retry only those (no duplicate sends).
type ReferralBatch struct {
	Messages       []RaiselyCustomMessageRequest
	EntryIndices   []int  // same length as Messages — source-array index of each
	ProfileID      string // Raisely profile to write back to
	ReferralsField string // path to the referrals array on the profile (e.g. "private.invitations")
	ReferralsJSON  string // raw referrals array JSON (the sjson base)
	SkippedIndices []int  // entries marked processed without a send (e.g. missing email)
}

// raiselyCustomMessageEnvelope is the wire-format wrapper for the
// Raisely Custom Messages API. See SendCustomMessage.
type raiselyCustomMessageEnvelope struct {
	Data raiselyCustomMessageEnvelopeData `json:"data"`
}

type raiselyCustomMessageEnvelopeData struct {
	Version int                         `json:"version"`
	Type    string                      `json:"type"`
	Source  string                      `json:"source"`
	Data    raiselyCustomMessagePayload `json:"data"`
}

type raiselyCustomMessagePayload struct {
	User   map[string]interface{} `json:"user"`
	Custom map[string]interface{} `json:"custom,omitempty"`
}

// RaiselyMessagesAPIBuilder returns a new requests.Builder configured for
// the Raisely Custom Messages API (separate host from the main Raisely API).
// Carries the same UA + per-call attribution log machinery as
// [RaiselyFetcherAndUpdater.RaiselyAPIBuilder].
func (r *RaiselyFetcherAndUpdater) RaiselyMessagesAPIBuilder() *requests.Builder {
	apiBuilder := requests.
		URL(r.Config.API.Endpoints.RaiselyMessages).
		UserAgent(r.userAgent()).
		Client(&http.Client{Timeout: HTTPRequestTimeout}).
		AddValidator(raiselyCallValidator(r.TriggerType))
	if r.RecordRequests {
		apiBuilder = apiBuilder.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/raisely-messages", r.Campaign)))
	}
	return apiBuilder
}

// MapFundraiserReferrals reads the referrals array from the fundraiser
// profile and builds a ReferralBatch of unprocessed entries. Each entry
// either becomes a RaiselyCustomMessageRequest (tagged with its source
// index) or, if it has no resolvable email, is recorded as a skipped
// index. The write-back is deferred to Service.ProcessReferrals so
// processedAt can be set per-success instead of per-batch.
// Returns (nil, nil) if the trigger is not configured, the field is
// absent/empty/non-array, or there are no unprocessed entries.
func (r *RaiselyFetcherAndUpdater) MapFundraiserReferrals(
	profileID string,
	profileData FundraiserData,
	config Config,
	campaignUUID string,
) (*ReferralBatch, error) {

	referralsField := config.API.Settings.RaiselyFundraiserReferralsField
	if referralsField == "" {
		return nil, nil
	}

	referralsJSON, exists := profileData.Page.Source.StringForPath(referralsField)
	if !exists || referralsJSON == "" || referralsJSON == "null" {
		return nil, nil
	}

	referralsArray := gjson.Parse(referralsJSON)
	if !referralsArray.IsArray() {
		log.Printf("Warning: referrals field %q is not a JSON array, skipping", referralsField)
		return nil, nil
	}

	var unprocessedIndices []int
	referralsArray.ForEach(func(key, value gjson.Result) bool {
		if !value.Get("processedAt").Exists() || value.Get("processedAt").String() == "" {
			unprocessedIndices = append(unprocessedIndices, int(key.Int()))
		}
		return true
	})

	if len(unprocessedIndices) == 0 {
		return nil, nil
	}

	log.Printf("Referrals sync: found %d unprocessed referral(s)", len(unprocessedIndices))

	batch := &ReferralBatch{
		ProfileID:      profileID,
		ReferralsField: referralsField,
		ReferralsJSON:  referralsJSON,
	}
	entries := referralsArray.Array()
	eventSource := "campaign:" + campaignUUID

	for _, idx := range unprocessedIndices {
		entry := entries[idx]
		// Parent set to the fundraiser profile so "^." paths resolve against
		// the profile fields rather than the referral entry.
		source := Source{data: entry, parent: &profileData.Page.Source}

		userMap := resolveMessageMap(source, config.FundraiserReferralFieldMappings.User)
		customMap := resolveMessageMap(source, config.FundraiserReferralFieldMappings.Custom)

		email, _ := userMap["email"].(string)
		if email == "" {
			log.Printf("Warning: referral %d has no email, skipping Raisely Custom Message (will still mark as processed)", idx)
			batch.SkippedIndices = append(batch.SkippedIndices, idx)
			continue
		}

		batch.Messages = append(batch.Messages, RaiselyCustomMessageRequest{
			Source: eventSource,
			User:   userMap,
			Custom: customMap,
		})
		batch.EntryIndices = append(batch.EntryIndices, idx)
	}

	return batch, nil
}

// resolveMessageMap resolves each path in the flat mapping against the
// source and returns the resulting key→value map. Backtick-wrapped
// values are treated as literal strings (matching MapFields). Missing
// values are omitted from the result — the receiving Raisely message
// template branches on key presence.
func resolveMessageMap(source Source, mapping map[string]string) map[string]interface{} {
	if len(mapping) == 0 {
		return nil
	}
	result := make(map[string]interface{}, len(mapping))
	for key, path := range mapping {
		if len(path) >= 2 && path[0] == '`' && path[len(path)-1] == '`' {
			result[key] = path[1 : len(path)-1]
			continue
		}
		if v, ok := source.StringForPath(path); ok {
			result[key] = v
		}
	}
	return result
}

// SendCustomMessage POSTs one event to the Raisely Custom Messages API.
// The API authenticates with a bearer token of the form "raisely:<apiKey>".
func (r *RaiselyFetcherAndUpdater) SendCustomMessage(request RaiselyCustomMessageRequest, ctx context.Context) error {
	body := raiselyCustomMessageEnvelope{
		Data: raiselyCustomMessageEnvelopeData{
			Version: 1,
			Type:    "raisely.custom",
			Source:  request.Source,
			Data: raiselyCustomMessagePayload{
				User:   request.User,
				Custom: request.Custom,
			},
		},
	}
	raiselyError := RaiselyError{}
	err := r.RaiselyMessagesAPIBuilder().
		Post().
		Path("/v1/events").
		Bearer("raisely:" + r.RaiselyAPIKey()).
		BodyJSON(body).
		ErrorJSON(&raiselyError).
		Fetch(ctx)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	return err
}

// RaiselyWebhookConfig represents a webhook configuration in Raisely.
type RaiselyWebhookConfig struct {
	UUID         string   `json:"uuid"`
	CampaignUUID string   `json:"campaignUuid"`
	URL          string   `json:"url"`
	Events       []string `json:"events"`
}

type RaiselyWebhooksResponse struct {
	Data []RaiselyWebhookConfig `json:"data"`
}

type RaiselyWebhookResponse struct {
	Data RaiselyWebhookConfig `json:"data"`
}

type RaiselyWebhookRequest struct {
	Data RaiselyWebhookConfig `json:"data"`
}

// ListWebhooks retrieves webhook configurations for a campaign via GET /v3/webhooks.
func (r *RaiselyFetcherAndUpdater) ListWebhooks(campaignUUID string, ctx context.Context) ([]RaiselyWebhookConfig, error) {
	raiselyError := RaiselyError{}
	var result RaiselyWebhooksResponse
	err := r.RaiselyAPIBuilder().
		Path("/v3/webhooks").
		Param("campaign", campaignUUID).
		Bearer(r.RaiselyAPIKey()).
		ToJSON(&result).
		ErrorJSON(&raiselyError).
		Fetch(ctx)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
		return nil, err
	}
	return result.Data, nil
}

// CreateWebhook creates a new webhook configuration via POST /v3/webhooks.
func (r *RaiselyFetcherAndUpdater) CreateWebhook(url string, campaignUUID string, events []string, ctx context.Context) (RaiselyWebhookConfig, error) {
	raiselyError := RaiselyError{}
	var result RaiselyWebhookResponse
	body := RaiselyWebhookRequest{
		Data: RaiselyWebhookConfig{
			URL:          url,
			CampaignUUID: campaignUUID,
			Events:       events,
		},
	}
	err := r.RaiselyAPIBuilder().
		Post().
		Path("/v3/webhooks").
		Bearer(r.RaiselyAPIKey()).
		BodyJSON(body).
		ToJSON(&result).
		ErrorJSON(&raiselyError).
		Fetch(ctx)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
		return RaiselyWebhookConfig{}, err
	}
	return result.Data, nil
}
