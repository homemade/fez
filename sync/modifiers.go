package sync

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/tidwall/gjson"
	"github.com/ttacon/libphonenumber"
)

func init() {

	gjson.AddModifier("pathJoinURL", func(json, arg string) string {
		var result string
		path := gjson.Parse(json)
		if !path.Exists() {
			return ""
		}
		if s, err := url.JoinPath(arg, path.String()); err == nil {
			result = s
		}
		return fmt.Sprintf(`"%s"`, result)
	})

	gjson.AddModifier("currency", func(json, arg string) string {
		res := gjson.Parse(json)
		if !res.Exists() {
			return ""
		}
		if arg == "RAISLEY_2DP" {
			i := res.Int()
			// raisely stores currency in the smallest currency unit (e.g. cents/pence)
			// but ortto expects currencies in the larger unit (e.g. $,£,€)
			// ortto also expects decimals to be sent as an integer multiplied by 1000
			// so the net result of dividing by 100 to convert to the the larger unit (e.g. $,£,€)
			// then multiply by 1000 for the ortto format is to multiply the raisely total by 10
			d := i * 10
			return fmt.Sprintf("%d", d)
		}
		return json
	})

	gjson.AddModifier("distance", func(json, arg string) string {
		res := gjson.Parse(json)
		if !res.Exists() {
			return ""
		}
		if arg == "RAISELY_KM" {
			i := res.Int()
			// raisely stores kilometres in metres (divide by 1000)
			// and ortto expects decimals to be sent as an integer multiplied by 1000
			// so we don't need to do anything for this conversion :)
			return fmt.Sprintf("%d", i)
		}
		return json
	})

	gjson.AddModifier("contains", func(json, arg string) string {
		res := gjson.Parse(json)
		if res.IsArray() {
			values := res.Array()
			for _, v := range values {
				if strings.Contains(v.String(), arg) {
					return fmt.Sprintf("%t", true)
				}
			}
			return fmt.Sprintf("%t", false)
		}
		return fmt.Sprintf("%t", strings.Contains(res.String(), arg))
	})

	gjson.AddModifier("phone", func(json, arg string) string {
		countryCode := arg
		number := gjson.Parse(json).String()
		// if present, remove extra " from number
		number = strings.Trim(number, `"`)
		// if default country code is present, strip it from the number
		if strings.HasPrefix(number, fmt.Sprintf("+%s", countryCode)) {
			number = strings.TrimPrefix(number, fmt.Sprintf("+%s", countryCode))
		} else { // otherwise try and parse the number using libphonenumber
			i, err := strconv.Atoi(countryCode)
			if err == nil {
				var num *libphonenumber.PhoneNumber
				num, err = libphonenumber.Parse(number, libphonenumber.GetRegionCodeForCountryCode(i))
				if err == nil {
					countryCode = fmt.Sprintf("%d", num.GetCountryCode())
					number = libphonenumber.GetNationalSignificantNumber(num)
				}
			}
			if err != nil {
				countryCode = ""
			}
		}

		return fmt.Sprintf(`{"c":"%s","n":"%s"}`, countryCode, number)
	})

	gjson.AddModifier("countryName", func(json, arg string) string {
		s := gjson.Parse(json).String()
		c := countries.ByName(s) // will match on Alpha-2 / Alpha-3 / Name
		if countries.Unknown == c {
			return ""
		}
		return fmt.Sprintf(`"%s"`, c.String()) // returns Country Name

	})

	gjson.AddModifier("now", func(json, arg string) string {
		return fmt.Sprintf(`"%s"`, time.Now().UTC().Format(time.RFC3339))
	})

	gjson.AddModifier("gte", func(json, arg string) string {
		res := gjson.Parse(json)
		if !res.Exists() || arg == "" {
			return ""
		}
		f, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("%t", res.Float() >= f)
	})

	gjson.AddModifier("percent", func(json, arg string) string {
		res := gjson.Parse(json)
		if !res.Exists() {
			return ""
		}
		f := res.Float()
		// ortto expects decimals to be sent as an integer multiplied by 1000
		f = f * 1000
		return fmt.Sprintf("%d", int(f))
	})

}
