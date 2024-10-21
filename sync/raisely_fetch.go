package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/tidwall/gjson"
)

const (
	FundraisingProfilesSinceTimestampFormat        = "2006-01-02T15:04:05.999"
	FundraisingProfilesSinceLimit                  = "100"
	FundraisingProfileDonationsUpToTimestampFormat = "2006-01-02T15:04:05.999"
	FundraisingProfileDonationsUpToLimit           = "100"
	FundraisingProfileExerciseLogsLimit            = "1000"
	FundraisingProfileDonationsLimit               = "1000"
)

type FetchRaiselyDataParams struct {
	RaiselyAPIKey     string
	P2PId             string
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
	return s.data.Value().(map[string]interface{})
}

type FundraisingTeam struct {
	TeamMembers []TeamMember `json:"data"`
}

type TeamMember struct {
	P2PId string `json:"uuid"`
}

type FundraisingCampaign struct {
	Profile struct {
		P2PId string
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
	P2PId  string `json:"uuid"`
	Parent struct {
		P2PId string `json:"uuid"`
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

func (p *FundraisingPage) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	var json string
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s", params.P2PId).
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

func (t *FundraisingTeam) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/members", params.P2PId).
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

func (c *FundraisingCampaign) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	var json string
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/campaigns/%s", params.P2PId).
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
	c.Profile.P2PId = data.Get("profile.uuid").String()
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

func (p *FundraisingProfilesSince) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/campaigns/%s/profiles", params.P2PId).
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
		return p.P2PId
	}
	if p.Parent.Type == "GROUP" {
		if fundraisingCampaign.Profile.P2PId != p.Parent.P2PId {
			return p.Parent.P2PId
		}
	}

	return ""
}

func (d *FundraisingProfileDonationsUpTo) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/donations", params.P2PId).
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

func (d *FundraisingProfileDonations) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/donations", params.P2PId).
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

func (d *FundraisingProfileExerciseLogs) FetchRaiselyData(params FetchRaiselyDataParams) error {
	raiselyError := RaiselyError{}
	err := params.RaiselyAPIBuilder.
		Pathf("/v3/profiles/%s/exercise-logs", params.P2PId).
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
