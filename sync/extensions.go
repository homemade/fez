package sync

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/sjson"
)

var (
	EpochDaySeconds int64 = 86400
)

// Keys returns the keys of the map m.
// The keys will be an indeterminate order.
// This function may end up in the builtin maps packagevin a future release of Go
// Sourced from https://cs.opensource.google/go/x/exp
func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

type EpochDays struct {
	Entries map[int64][]string
}

func (e EpochDays) FirstDay() int64 {
	if len(e.Entries) < 1 {
		return 0
	}
	return slices.Min(Keys(e.Entries))
}

func (e EpochDays) LastDay() int64 {
	if len(e.Entries) < 1 {
		return 0
	}
	return slices.Max(Keys(e.Entries))
}

func (e EpochDays) MaxConsecutiveDays() int {
	var maxConsecutiveDays int
	var currentConsecutiveDays int
	for i := e.FirstDay(); i <= e.LastDay(); i++ {
		_, exists := e.Entries[i]
		if exists {
			currentConsecutiveDays = currentConsecutiveDays + 1
		} else {
			currentConsecutiveDays = 0
		}
		if currentConsecutiveDays > maxConsecutiveDays {
			maxConsecutiveDays = currentConsecutiveDays
		}
	}
	return maxConsecutiveDays
}

type StreakableEntry struct {
	TimestampForStreak string
}

func CalcDaysForStreakFromEntries(entries []StreakableEntry) EpochDays {
	var result EpochDays
	result.Entries = make(map[int64][]string)
	for _, e := range entries {
		var nextEpochDay int64
		t, err := time.Parse(time.RFC3339, e.TimestampForStreak)
		if err == nil {
			nextEpochDay = t.Unix() / EpochDaySeconds
		}
		if v, exists := result.Entries[nextEpochDay]; exists {
			result.Entries[nextEpochDay] = append(v, e.TimestampForStreak)
		} else {
			result.Entries[nextEpochDay] = []string{e.TimestampForStreak}
		}
	}
	return result
}

type FundraiserExtensions struct {
	Config   FundraiserExtensionsConfig
	Campaign *FundraisingCampaign
	Page     FundraisingPage
}

func (e FundraiserExtensions) MaxConfiguredDaysForActivityStreak() int {
	if len(e.Config.Streaks.Activity.Days) < 1 {
		return 0
	}
	return slices.Max(e.Config.Streaks.Activity.Days)
}

func (e FundraiserExtensions) MaxCurrentDaysForActivityStreak() int {
	if len(e.Config.Streaks.Activity.Days) < 1 {
		return 0
	}
	currentValue, _ := e.Page.Source.StringForPath(e.Config.Streaks.Activity.Mapping)
	days := CurrentDaysForStreak(currentValue)
	if len(days) < 1 {
		return 0
	}
	return slices.Max(days)
}

func (e FundraiserExtensions) MaxConfiguredDaysForDonationStreak() int {
	if len(e.Config.Streaks.Donation.Days) < 1 {
		return 0
	}
	return slices.Max(e.Config.Streaks.Donation.Days)
}

func (e FundraiserExtensions) MaxCurrentDaysForDonationStreak() int {
	if len(e.Config.Streaks.Donation.Days) < 1 {
		return 0
	}
	currentValue, _ := e.Page.Source.StringForPath(e.Config.Streaks.Donation.Mapping)
	days := CurrentDaysForStreak(currentValue)
	if len(days) < 1 {
		return 0
	}
	return slices.Max(days)
}

func CurrentDaysForStreak(value string) []int {
	var result []int
	if value == "" {
		return result
	}
	awarded := strings.Split(value, "|")
	for _, s := range awarded {
		i, err := strconv.Atoi(s)
		if err == nil {
			result = append(result, i)
		}
	}
	return result
}

func AddMissingDaysForStreak(max int, days []int, value string) string {
	if max < 1 {
		return value
	}
	current := CurrentDaysForStreak(value)
	var missing []int
	if len(current) < 1 {
		missing = days
	} else {
		for _, d := range days {
			if !slices.Contains(current, d) {
				missing = append(missing, d)
			}
		}
	}
	if len(missing) < 1 {
		return value
	}
	result := value
	for _, d := range missing {
		if max >= d {
			if result == "" {
				result = fmt.Sprintf("%03d", d)
			} else {
				result = fmt.Sprintf("%s|%03d", result, d)
			}
		}
	}
	return result
}

func (e FundraiserExtensions) HasSplitExerciseTotals() bool {
	return e.Config.SplitExerciseTotals.From != "" && len(e.Config.SplitExerciseTotals.Mappings) == 2
}

func ApplyFundraiserExtensions(extensions FundraiserExtensions, exerciselogs []ExerciseLogEntry, donations []Donation) (string, error) {
	var err error
	var result string

	// streaks

	configuredMaxActivityDays := extensions.MaxConfiguredDaysForActivityStreak()
	currentMaxActivityDays := extensions.MaxCurrentDaysForActivityStreak()
	if currentMaxActivityDays < configuredMaxActivityDays {
		var exerciselogEntries []StreakableEntry
		for _, el := range exerciselogs {
			if el.IncludeForStreak(extensions.Config) {
				exerciselogEntries = append(exerciselogEntries, StreakableEntry{el.TimestampForStreak()})
			}
		}
		exerciseLogDays := CalcDaysForStreakFromEntries(exerciselogEntries)
		exerciseLogMaxDays := exerciseLogDays.MaxConsecutiveDays()
		if currentMaxActivityDays < exerciseLogMaxDays {
			mapping := extensions.Config.Streaks.Activity.Mapping
			currentValue, _ := extensions.Page.Source.StringForPath(mapping)
			newValue := AddMissingDaysForStreak(exerciseLogMaxDays, extensions.Config.Streaks.Activity.Days, currentValue)
			if currentValue != newValue {
				result, err = sjson.Set(result, "data."+mapping, newValue)
				if err != nil {
					return result, err
				}
			}
		}
	}

	configuredMaxDonationDays := extensions.MaxConfiguredDaysForDonationStreak()
	currentMaxDonationDays := extensions.MaxCurrentDaysForDonationStreak()

	if currentMaxDonationDays < configuredMaxDonationDays {
		var donationEntries []StreakableEntry
		for _, d := range donations {
			if d.IncludeForStreak(extensions.Config) {
				donationEntries = append(donationEntries, StreakableEntry{d.TimestampForStreak()})
			}
		}
		donationDays := CalcDaysForStreakFromEntries(donationEntries)
		donationMaxDays := donationDays.MaxConsecutiveDays()
		if currentMaxDonationDays < donationMaxDays {
			mapping := extensions.Config.Streaks.Donation.Mapping
			currentValue, _ := extensions.Page.Source.StringForPath(mapping)
			newValue := AddMissingDaysForStreak(donationMaxDays, extensions.Config.Streaks.Donation.Days, currentValue)
			if currentValue != newValue {
				result, err = sjson.Set(result, "data."+mapping, newValue)
				if err != nil {
					return result, err
				}
			}
		}
	}

	// split exercise totals
	if extensions.HasSplitExerciseTotals() {

		now := time.Now()

		// use `from` mapping to retrieve timestamp
		var fromTimestamp time.Time
		for _, defaultObject := range extensions.Campaign.FundraisingPageDefaults {
			if extensions.Config.SplitExerciseTotals.From == defaultObject.Label {
				fromTimestamp, err = time.Parse(time.RFC3339, defaultObject.Value)
				if err != nil {
					return result, err
				}
			}
		}

		// apply split mappings based on `from` timestamp compared to current time
		beforeMapping := extensions.Config.SplitExerciseTotals.Mappings[0]
		fromMapping := extensions.Config.SplitExerciseTotals.Mappings[1]
		exerciseTotal, _ := extensions.Page.Source.IntForPath("exerciseTotal")
		beforeExerciseTotalCurrentValue, _ := extensions.Page.Source.IntForPath(beforeMapping)
		fromExerciseTotalCurrentValue, _ := extensions.Page.Source.IntForPath(fromMapping)
		if now.Before(fromTimestamp) {
			if exerciseTotal != beforeExerciseTotalCurrentValue {
				// Defensive code to ensure that 24 hours either side of the `from` timestamp the before total is only ever increased
				// NOTE: this is required because the exerciseTotal is reset to 0 prior to the challenge starting but exact timing is undetermined
				if exerciseTotal > beforeExerciseTotalCurrentValue ||
					now.Add(time.Hour*24).After(fromTimestamp) ||
					now.Add(time.Hour*-24).Before(fromTimestamp) {
					result, err = sjson.Set(result, "data."+beforeMapping, exerciseTotal)
					if err != nil {
						return result, err
					}
				}
			}
		} else {
			if exerciseTotal != fromExerciseTotalCurrentValue {
				result, err = sjson.Set(result, "data."+fromMapping, exerciseTotal)
				if err != nil {
					return result, err
				}
			}
		}
	}

	return result, nil
}
