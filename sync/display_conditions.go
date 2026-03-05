package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/osteele/liquid"
)

// DisplayConditionFieldResult represents the result of validating field references
// in a single display condition.
type DisplayConditionFieldResult struct {
	Description string
	Fields      []string // Extracted activity field names
	Valid       []string // Fields found in mapping
	Invalid     []string // Fields not found in mapping
}

// DisplayConditionRenderResult represents the result of Liquid rendering validation
// for a single display condition.
type DisplayConditionRenderResult struct {
	Description    string
	ExpectTrueOK   *bool  // nil if no expectTrue data, true/false for pass/fail
	ExpectFalseOK  *bool  // nil if no expectFalse data, true/false for pass/fail
	Error          string // Parse/render error if any
}

// activityFieldPattern matches activity.custom.<activity-name>.<field-name> references.
var activityFieldPattern = regexp.MustCompile(`activity\.custom\.[a-z0-9-]+\.([a-z0-9-]+)`)

// ExtractActivityFieldNames extracts unique activity field names from a Liquid template string.
// It parses references like activity.custom.<activity-name>.<field-name> and returns
// the field-name portions in sorted order.
func ExtractActivityFieldNames(template string) []string {
	matches := activityFieldPattern.FindAllStringSubmatch(template, -1)
	seen := make(map[string]bool)
	var fields []string
	for _, match := range matches {
		fieldName := match[1]
		if !seen[fieldName] {
			seen[fieldName] = true
			fields = append(fields, fieldName)
		}
	}
	sort.Strings(fields)
	return fields
}

// ValidateDisplayConditionFields validates that field names referenced in display conditions
// exist in the campaign's field documentation. Only activity fields (non-person fields) are
// checked since display conditions reference activity attributes.
func ValidateDisplayConditionFields(entries []DisplayConditionEntry, doc FieldDocumentation) []DisplayConditionFieldResult {
	// Build set of known activity field names from documentation
	knownFields := make(map[string]bool)
	for _, row := range doc.Rows {
		if row.Entity == "Activity" {
			knownFields[row.FieldName] = true
		}
	}

	var results []DisplayConditionFieldResult
	for _, entry := range entries {
		combined := entry.Condition.Begin + " " + entry.Condition.End
		fields := ExtractActivityFieldNames(combined)

		result := DisplayConditionFieldResult{
			Description: entry.Description,
			Fields:      fields,
		}

		for _, f := range fields {
			if knownFields[f] {
				result.Valid = append(result.Valid, f)
			} else {
				result.Invalid = append(result.Invalid, f)
			}
		}

		results = append(results, result)
	}

	return results
}

// ValidateDisplayConditionRendering validates display conditions by rendering their Liquid
// templates against inline sample data. For each condition, it combines begin + sentinel + end
// to form a complete template, then renders against expectTrue and expectFalse data.
func ValidateDisplayConditionRendering(entries []DisplayConditionEntry, activityName string) []DisplayConditionRenderResult {
	engine := liquid.NewEngine()

	var results []DisplayConditionRenderResult
	for _, entry := range entries {
		result := DisplayConditionRenderResult{
			Description: entry.Description,
		}

		// Combine begin + sentinel + end to form complete template
		template := entry.Condition.Begin + "VISIBLE" + entry.Condition.End

		// Validate expectTrue (present in YAML, possibly with empty data meaning "blank context")
		if entry.Condition.HasExpectTrue {
			ok := renderAndCheck(engine, template, activityName, entry.Condition.ExpectTrue, true)
			result.ExpectTrueOK = &ok
		}

		// Validate expectFalse (present in YAML, possibly with empty data meaning "blank context")
		if entry.Condition.HasExpectFalse {
			ok := renderAndCheck(engine, template, activityName, entry.Condition.ExpectFalse, false)
			result.ExpectFalseOK = &ok
		}

		results = append(results, result)
	}

	return results
}

// renderAndCheck renders a Liquid template with sample data and checks whether
// the output contains the sentinel "VISIBLE" string. If expectVisible is true,
// the check passes when VISIBLE is present; if false, it passes when VISIBLE is absent.
func renderAndCheck(engine *liquid.Engine, template string, activityName string, sampleData map[string]interface{}, expectVisible bool) bool {
	bindings := buildLiquidContext(activityName, sampleData)

	out, err := engine.ParseAndRenderString(template, bindings)
	if err != nil {
		return false
	}

	containsVisible := strings.Contains(out, "VISIBLE")
	if expectVisible {
		return containsVisible
	}
	return !containsVisible
}

// buildLiquidContext constructs the nested Liquid context from sample field values.
// Field values are placed at activity.custom.<activity-name>.<field-name> to match
// the template variable paths used in Ortto display conditions.
func buildLiquidContext(activityName string, data map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{})
	for k, v := range data {
		fields[k] = v
	}

	return map[string]interface{}{
		"activity": map[string]interface{}{
			"custom": map[string]interface{}{
				activityName: fields,
			},
		},
	}
}

// DisplayConditionSyntaxResult represents the result of checking a single display condition
// for unsupported Liquid syntax.
type DisplayConditionSyntaxResult struct {
	Description      string
	UnsupportedTerms []string // e.g. ["blank"]
}

// unsupportedLiquidTerms lists Liquid keywords that are not supported by Ortto's Liquid implementation.
var unsupportedLiquidTerms = []string{"blank"}

// ValidateDisplayConditionSyntax checks display conditions for Liquid syntax that is not
// supported by Ortto's Liquid implementation. Standard Liquid supports keywords like "blank"
// but Ortto does not.
func ValidateDisplayConditionSyntax(entries []DisplayConditionEntry) []DisplayConditionSyntaxResult {
	var results []DisplayConditionSyntaxResult
	for _, entry := range entries {
		combined := entry.Condition.Begin + " " + entry.Condition.End
		result := DisplayConditionSyntaxResult{
			Description: entry.Description,
		}
		for _, term := range unsupportedLiquidTerms {
			if strings.Contains(combined, " "+term+" ") ||
				strings.Contains(combined, " "+term+"%") {
				result.UnsupportedTerms = append(result.UnsupportedTerms, term)
			}
		}
		results = append(results, result)
	}
	return results
}

// FormatSyntaxValidationResults formats syntax validation results as a human-readable string.
func FormatSyntaxValidationResults(results []DisplayConditionSyntaxResult) string {
	var sb strings.Builder
	for _, r := range results {
		if len(r.UnsupportedTerms) > 0 {
			sb.WriteString(fmt.Sprintf("❌ %q — unsupported Liquid syntax: %s (not supported by Ortto)\n",
				r.Description,
				strings.Join(r.UnsupportedTerms, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf("✅ %q\n", r.Description))
		}
	}
	return sb.String()
}

// FormatFieldValidationResults formats field validation results as a human-readable string.
func FormatFieldValidationResults(results []DisplayConditionFieldResult) string {
	var sb strings.Builder
	for _, r := range results {
		if len(r.Invalid) > 0 {
			sb.WriteString(fmt.Sprintf("❌ %q — fields: %s ✓, %s ✗ (not in webhook mapping)\n",
				r.Description,
				strings.Join(r.Valid, ", "),
				strings.Join(r.Invalid, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf("✅ %q — fields: %s ✓\n",
				r.Description,
				strings.Join(r.Fields, ", ")))
		}
	}
	return sb.String()
}

// FormatRenderValidationResults formats render validation results as a human-readable string.
func FormatRenderValidationResults(results []DisplayConditionRenderResult) string {
	var sb strings.Builder
	for _, r := range results {
		if r.Error != "" {
			sb.WriteString(fmt.Sprintf("❌ %q — error: %s\n", r.Description, r.Error))
			continue
		}

		// No sample data at all
		if r.ExpectTrueOK == nil && r.ExpectFalseOK == nil {
			sb.WriteString(fmt.Sprintf("⚠️  %q — missing expectTrue/expectFalse\n", r.Description))
			continue
		}

		parts := []string{}
		allPass := true

		if r.ExpectTrueOK != nil {
			if *r.ExpectTrueOK {
				parts = append(parts, "expectTrue: pass")
			} else {
				parts = append(parts, "expectTrue: fail (evaluated to false)")
				allPass = false
			}
		}

		if r.ExpectFalseOK != nil {
			if *r.ExpectFalseOK {
				parts = append(parts, "expectFalse: pass")
			} else {
				parts = append(parts, "expectFalse: fail (evaluated to true)")
				allPass = false
			}
		}

		icon := "✅"
		if !allPass {
			icon = "❌"
		}
		sb.WriteString(fmt.Sprintf("%s %q — %s\n", icon, r.Description, strings.Join(parts, ", ")))
	}
	return sb.String()
}
