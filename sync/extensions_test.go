// go test github.com/homemade/fez/sync -v
package sync

import (
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

var testFundraiserExtensionsConfig FundraiserExtensionsConfig

func init() {
	testFundraiserExtensionsConfig.Streaks.Donation.Days = []int{3, 5}
	testFundraiserExtensionsConfig.Streaks.Donation.Mapping = "public.donationStreaksAwarded"
	testFundraiserExtensionsConfig.Streaks.Activity.Days = []int{10, 15, 20}
	testFundraiserExtensionsConfig.Streaks.Activity.From = "2023-10-01T00:00:00.000Z"
	testFundraiserExtensionsConfig.Streaks.Activity.To = "2023-10-31T00:00:00.000Z"
	testFundraiserExtensionsConfig.Streaks.Activity.Filter = []string{"OTHER", "SWIMMING"}
	testFundraiserExtensionsConfig.Streaks.Activity.Mapping = "public.activityStreaksAwarded"

	testFundraiserExtensionsConfig.SplitExerciseTotals.From = "Challenge Start"
	testFundraiserExtensionsConfig.SplitExerciseTotals.Mappings = []string{"public.challenge_training_total", "public.challenge_total"}
}

func TestRaiselyFundraiserExtensions(t *testing.T) {

	now := time.Now()

	extensions := FundraiserExtensions{
		Config: testFundraiserExtensionsConfig,
		Campaign: &FundraisingCampaign{
			FundraisingPageDefaults: []CampaignDefault{{
				Label: "Challenge Start",
				Value: now.Add(time.Hour * 48).UTC().Format(time.RFC3339), // set challenge to start in 48 hours
			}},
		},
		Page: FundraisingPage{
			Source: Source{
				data: gjson.Parse(`{"exerciseTotal": 4321}`),
			},
		},
	}

	exerciselogs := []ExerciseLogEntry{
		{Activity: "SWIMMING", Date: "2023-10-01T03:09:38.979Z", Distance: 100},
		{Activity: "SWIMMING", Date: "2023-10-02T03:09:38.979Z", Distance: 200},
		{Activity: "SWIMMING", Date: "2023-10-03T03:09:38.979Z", Distance: 300},
		{Activity: "SWIMMING", Date: "2023-10-04T03:09:38.979Z", Distance: 400},
		{Activity: "SWIMMING", Date: "2023-10-05T03:09:38.979Z", Distance: 500},
		{Activity: "SWIMMING", Date: "2023-10-06T03:09:38.979Z", Distance: 600},
		{Activity: "SWIMMING", Date: "2023-10-07T03:09:38.979Z", Distance: 700},
		{Activity: "SWIMMING", Date: "2023-10-08T03:09:38.979Z", Distance: 800},
		{Activity: "SWIMMING", Date: "2023-10-09T03:09:38.979Z", Distance: 900},
		{Activity: "OTHER", Date: "2023-10-10T03:09:38.979Z", Distance: 1000},
		{Activity: "SWIMMING", Date: "2023-10-11T03:09:38.979Z", Distance: 1100},
		{Activity: "SWIMMING", Date: "2023-10-12T03:09:38.979Z", Distance: 1200},
		{Activity: "SWIMMING", Date: "2023-10-13T03:09:38.979Z", Distance: 1300},
		{Activity: "SWIMMING", Date: "2023-10-14T03:09:38.979Z", Distance: 1400},
		{Activity: "SWIMMING", Date: "2023-10-15T03:09:38.979Z", Distance: 1500},
		{Activity: "SWIMMING", Date: "2023-10-16T03:09:38.979Z", Distance: 1600},
		{Activity: "SWIMMING", Date: "2023-10-17T03:09:38.979Z", Distance: 1700},
		{Activity: "SWIMMING", Date: "2023-10-18T03:09:38.979Z", Distance: 1800},
		{Activity: "SWIMMING", Date: "2023-10-19T03:09:38.979Z", Distance: 1900},
		{Activity: "OTHER", Date: "2023-10-20T03:09:38.979Z", Distance: 2000},
	}
	donations := []Donation{
		{CreatedAt: "2023-11-01T03:09:38.979Z", Amount: 1000},
		{CreatedAt: "2023-11-02T03:09:38.979Z", Amount: 2000},
		{CreatedAt: "2023-11-03T03:09:38.979Z", Amount: 3000},
		{CreatedAt: "2023-11-04T03:09:38.979Z", Amount: 4000},
		{CreatedAt: "2023-11-05T03:09:38.979Z", Amount: 5000},
		{CreatedAt: "2023-11-06T03:09:38.979Z", Amount: 6000},
	}

	testCase := "before challenge"

	result, err := ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected := `{"data":{"public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":4321}}}`
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "before challenge with data already added"
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 4321, "public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":4321}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = ``
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "before challenge with reduced exerciseTotal more than 24 hours before challenge start"
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 3210, "public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":4321}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = `{"data":{"public":{"challenge_training_total":3210}}}`
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "before challenge with reduced exerciseTotal less than 24 hours before challenge start"
	extensions.Campaign.FundraisingPageDefaults[0].Value = now.Add(time.Hour * 23).UTC().Format(time.RFC3339)
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 3210, "public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":4321}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = `` // challenge_training_total should not be reduced within 24 hours of challenge starting
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "before challenge with increased exerciseTotal less than 24 hours before challenge start"
	extensions.Campaign.FundraisingPageDefaults[0].Value = now.Add(time.Hour * 23).UTC().Format(time.RFC3339)
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 5432, "public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":4321}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = `{"data":{"public":{"challenge_training_total":5432}}}`
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "from challenge"
	extensions.Campaign.FundraisingPageDefaults[0].Value = now.UTC().Format(time.RFC3339)
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 1000, "public":{"challenge_training_total":5432}}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = `{"data":{"public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_total":1000}}}`
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

	testCase = "from challenge with data already added"
	extensions.Page = FundraisingPage{
		Source: Source{
			data: gjson.Parse(`{"exerciseTotal": 1000, "public":{"activityStreaksAwarded":"010|015|020","donationStreaksAwarded":"003|005","challenge_training_total":5432,"challenge_total":1000}`),
		},
	}
	result, err = ApplyRaiselyFundraiserExtensions(extensions, exerciselogs, donations)
	if err != nil {
		t.Error(err)
	}
	expected = ``
	if result != expected {
		t.Errorf("Expected %s result of: %s but have: %s", testCase, expected, result)
	}
	t.Logf("%s result: %s", testCase, result)

}
