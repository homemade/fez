package sync

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/tidwall/gjson"
	"github.com/ttacon/libphonenumber"
)

// initialisedFlavour stores the flavour set by Init.
// A nil value means Init has not been called.
var initialisedFlavour *Flavour

// mustBeInitialised panics if Init has not been called.
// This should be called at the entry points of the library
// to catch programming errors early.
func mustBeInitialised() Flavour {
	if initialisedFlavour == nil {
		panic("sync: Init() must be called before using this package")
	}
	return *initialisedFlavour
}

// GetInitialisedFlavour returns the flavour set by Init.
// Panics if Init has not been called.
func GetInitialisedFlavour() Flavour {
	return mustBeInitialised()
}

func Init(flavour Flavour) {

	f := flavour
	initialisedFlavour = &f

	// Validate no duplicate campaign UUIDs across env vars
	validateNoDuplicateCampaignUUIDs()

	// Validate env var names match org prefix from MAPPING_PATH
	validateEnvVarOrgPrefix()

	if flavour == Raisely2Ortto { // currently the only flavour, but structure allows for easy addition of new flavours in the future

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
			if arg == "RAISELY_2DP" {
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
					log.Printf("Warning: failed to parse phone number %q with country code %q: %v (using empty country code)", number, arg, err)
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

}

// validateEnvVarOrgPrefix scans all environment variables for JSON values
// containing a MAPPING_PATH and validates that the env var name starts with
// the org prefix from the path (the portion before the "/").
func validateEnvVarOrgPrefix() {
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]

		var m map[string]string
		// Most env vars are plain strings (e.g. PATH), not JSON — skip those silently
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			continue
		}

		mappingPath, ok := m["MAPPING_PATH"]
		if !ok {
			continue
		}

		index := strings.Index(mappingPath, "/")
		if index == -1 {
			log.Fatalf("MAPPING_PATH %q in env var %q must contain org directory (e.g. ORG/LABEL)", mappingPath, name)
		}

		org := mappingPath[:index]
		if !strings.HasPrefix(name, org+"_") {
			log.Fatalf("env var name %q must start with org prefix %q (from MAPPING_PATH %q)", name, org+"_", mappingPath)
		}
	}
}

// validateNoDuplicateCampaignUUIDs scans all environment variables for JSON values
// containing the campaign UUID key for the initialised flavour
// and fatals if any UUID appears in more than one env var.
func validateNoDuplicateCampaignUUIDs() {
	campaignUUIDKey, err := campaignUUIDKeyForFlavour(GetInitialisedFlavour())
	if err != nil {
		log.Fatalf("failed to validate campaign UUIDs: %v", err)
	}

	// map of UUID -> env var name
	seen := make(map[string]string)

	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]

		var m map[string]string
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			// Most env vars are plain strings (e.g. PATH), not JSON — skip those silently
			continue
		}

		uuid, ok := m[campaignUUIDKey]
		if !ok {
			continue
		}

		if existing, found := seen[uuid]; found {
			log.Fatalf("duplicate %s %q found in env vars %q and %q", campaignUUIDKey, uuid, existing, name)
		}
		seen[uuid] = name
	}
}
