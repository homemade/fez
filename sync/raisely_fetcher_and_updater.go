package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	gosync "sync"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/tidwall/gjson"
)

const (
	FundraisingProfilesSinceTimestampFormat        = "2006-01-02T15:04:05.999"
	FundraisingProfilesSinceLimit                  = "1000"
	FundraisingProfileDonationsUpToTimestampFormat = "2006-01-02T15:04:05.999"
	FundraisingProfileDonationsUpToLimit           = "1000"
	FundraisingProfileExerciseLogsLimit            = "1000"
	FundraisingProfileDonationsLimit               = "1000"
)

var cachedFundraisingCampaigns gosync.Map // map[string]*FundraisingCampaign

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
	data gjson.Result
}

func (s Source) StringForPath(path string) (string, bool) {
	result := s.data.Get(path)
	return result.String(), result.Exists() && (result.Value() != nil)
}

func (s Source) IntForPath(path string) (int64, bool) {
	result := s.data.Get(path)
	return result.Int(), result.Exists() && (result.Value() != nil)
}

func (s Source) BoolForPath(path string) (bool, bool) {
	result := s.data.Get(path)
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

type FundraisingProfileDonationsUpTo struct {
	UpTo                       time.Time
	Donations                  []map[string]interface{} `json:"data"`
	TotalDonationAmount        int
	TotalRegistrationFeeAmount int
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

func (d *FundraisingProfileDonationsUpTo) fetchRaiselyData(params fetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/donations", params.P2PID).
		Param("private", "true").
		Param("createdAtTo", d.UpTo.Format(FundraisingProfileDonationsUpToTimestampFormat)).
		Param("limit", FundraisingProfileDonationsUpToLimit).
		Bearer(params.RaiselyAPIKey).
		ToJSON(d).
		ErrorJSON(&raiselyError).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}

	// Sum any registration fee
	for _, donation := range d.Donations {
		if items, itemsOk := donation["items"].([]map[string]interface{}); itemsOk {
			for _, i := range items {
				if fmt.Sprintf("%s", i["type"]) == "REGISTRATION" {
					if itemAmount, itemAmountOk := i["amount"].(float64); itemAmountOk {
						d.TotalRegistrationFeeAmount = d.TotalRegistrationFeeAmount + int(itemAmount)
					}
				}
			}
		}
	}

	// Sum donations
	for _, donation := range d.Donations {
		if amount, amountOk := donation["amount"].(float64); amountOk {
			d.TotalDonationAmount = d.TotalDonationAmount + int(amount)
		}
	}
	// Remove registration fee total from the donations total
	d.TotalDonationAmount = d.TotalDonationAmount - d.TotalRegistrationFeeAmount

	return err
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
type RaiselyFetcherAndUpdater struct {
	*SyncContext
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

// CachedFundraisingCampaign fetches and caches the fundraising campaign data from Raisely.
// Thread-safe: uses sync.Map keyed by campaign ID.
func (r *RaiselyFetcherAndUpdater) CachedFundraisingCampaign(p2pID string, refresh bool, ctx context.Context) (*FundraisingCampaign, error) {
	if !refresh {
		if v, ok := cachedFundraisingCampaigns.Load(p2pID); ok {
			return v.(*FundraisingCampaign), nil
		}
	}

	fundraisingCampaign, err := r.FetchFundraisingCampaign(p2pID, ctx)
	if err != nil {
		// On error, fall back to cached value if available
		if v, ok := cachedFundraisingCampaigns.Load(p2pID); ok {
			return v.(*FundraisingCampaign), nil
		}
		return nil, err
	}

	cachedFundraisingCampaigns.Store(p2pID, fundraisingCampaign)
	return fundraisingCampaign, nil
}

// FundraiserData holds the fetched data for a single fundraiser.
type FundraiserData struct {
	Page         FundraisingPage
	ExerciseLogs FundraisingProfileExerciseLogs
	Donations    FundraisingProfileDonations
}

// RaiselyAPIKey returns the Raisely API key from the config.
func (r *RaiselyFetcherAndUpdater) RaiselyAPIKey() string {
	return r.Config.API.Keys.Raisely
}

// RaiselyAPIBuilder returns a new requests.Builder configured for the Raisely API.
func (r *RaiselyFetcherAndUpdater) RaiselyAPIBuilder() *requests.Builder {
	apiBuilder := requests.
		URL("https://api.raisely.com").
		Client(&http.Client{Timeout: HTTPRequestTimeout})
	if r.RecordRequests {
		apiBuilder = apiBuilder.Transport(requests.Record(nil, fmt.Sprintf("pkg/testdata/.requests/%s/raisely", r.Campaign)))
	}
	return apiBuilder
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

// FetchDonationsUpTo fetches donations for a profile up to the given time.
func (r *RaiselyFetcherAndUpdater) FetchDonationsUpTo(profileP2PID string, upTo time.Time, ctx context.Context) (FundraisingProfileDonationsUpTo, error) {
	donations := FundraisingProfileDonationsUpTo{
		UpTo: upTo,
	}
	err := donations.fetchRaiselyData(r.fetchParams(profileP2PID, ctx))
	return donations, err
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
