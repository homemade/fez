package sync

import (
	"fmt"
	"strings"
)

// ParseMappingPath splits a MAPPING_PATH value into its org and label
// segments.
//
// The expected format is "ORG/LABEL": an organisation directory containing
// one or more campaign mapping files. Both segments are returned with their
// original casing preserved.
//
// The split happens at the last "/" so that, if the format is ever extended
// to nest organisations under multi-segment paths ("PARENT/CHILD/LABEL"),
// the label segment remains the trailing name and the org segment retains
// any directory structure. For the documented two-segment input this is
// equivalent to splitting at the first "/".
//
// Returns an error if the value contains no "/" separator (no org segment).
// Callers that need the org as a single-segment identifier — for example,
// matching it against another scoping value at startup — should validate
// the absence of additional "/" characters themselves.
func ParseMappingPath(p string) (org, label string, err error) {
	i := strings.LastIndex(p, "/")
	if i == -1 {
		return "", "", fmt.Errorf("invalid mapping path %q: must contain org directory (e.g. ORG/LABEL)", p)
	}
	return p[:i], p[i+1:], nil
}

// Org returns the org segment of the CampaignEnvVar's MAPPING_PATH value.
// Returns the empty string if Path is missing a "/" separator. Path is
// validated at process initialisation (see init.go); a well-formed
// CampaignEnvVar produced by FindAllCampaignEnvVars / FindCampaignEnvVar
// always yields a non-empty Org.
func (c CampaignEnvVar) Org() string {
	org, _, _ := ParseMappingPath(c.Path)
	return org
}

// Label returns the label segment of the CampaignEnvVar's MAPPING_PATH
// value. Returns the empty string if Path is missing a "/" separator;
// see Org for the validation contract.
func (c CampaignEnvVar) Label() string {
	_, label, _ := ParseMappingPath(c.Path)
	return label
}
