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
	Description string   `json:"description"`
	Fields      []string `json:"fields,omitempty"`      // Extracted activity field names
	Valid       []string `json:"valid,omitempty"`        // Fields found in mapping
	Invalid     []string `json:"invalid,omitempty"`      // Fields not found in mapping
}

// DisplayConditionRenderResult represents the result of Liquid rendering validation
// for a single display condition.
type DisplayConditionRenderResult struct {
	Description   string `json:"description"`
	ExpectTrueOK  *bool  `json:"expectTrueOK"`  // nil if no expectTrue data, true/false for pass/fail
	ExpectFalseOK *bool  `json:"expectFalseOK"` // nil if no expectFalse data, true/false for pass/fail
	Error         string `json:"error,omitempty"` // Parse/render error if any
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
	Description      string   `json:"description"`
	UnsupportedTerms []string `json:"unsupportedTerms,omitempty"` // e.g. ["blank"]
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

// FormatDisplayConditionTable formats all display condition results as a combined table.
// All three slices must be the same length and in the same order (from the same entries).
func FormatDisplayConditionTable(
	syntax []DisplayConditionSyntaxResult,
	fields []DisplayConditionFieldResult,
	render []DisplayConditionRenderResult,
) string {
	if len(syntax) == 0 {
		return ""
	}

	// Find max description width
	descWidth := len("Description")
	for _, s := range syntax {
		if len(s.Description) > descWidth {
			descWidth = len(s.Description)
		}
	}
	// Cap at a reasonable width
	if descWidth > 60 {
		descWidth = 60
	}

	var sb strings.Builder
	var notes []string

	// Header
	sb.WriteString(fmt.Sprintf("  %-*s | Syntax | Fields | Liquid\n", descWidth, "Description"))
	sb.WriteString(fmt.Sprintf("  %s-|--------|--------|-------\n", strings.Repeat("-", descWidth)))

	for i := range syntax {
		desc := syntax[i].Description
		if len(desc) > descWidth {
			desc = desc[:descWidth-1] + "…"
		}

		// Syntax column
		syntaxIcon := "✅"
		if len(syntax[i].UnsupportedTerms) > 0 {
			syntaxIcon = "❌"
			notes = append(notes, fmt.Sprintf("  %q — unsupported syntax: %s",
				syntax[i].Description, strings.Join(syntax[i].UnsupportedTerms, ", ")))
		}

		// Fields column
		fieldsIcon := "✅"
		if i < len(fields) && len(fields[i].Invalid) > 0 {
			fieldsIcon = "❌"
			notes = append(notes, fmt.Sprintf("  %q — invalid fields: %s",
				fields[i].Description, strings.Join(fields[i].Invalid, ", ")))
		}

		// Liquid column
		liquidIcon := "✅"
		if i < len(render) {
			r := render[i]
			if r.Error != "" {
				liquidIcon = "❌"
				notes = append(notes, fmt.Sprintf("  %q — render error: %s", r.Description, r.Error))
			} else if r.ExpectTrueOK == nil && r.ExpectFalseOK == nil {
				liquidIcon = "⚠️ "
				notes = append(notes, fmt.Sprintf("  %q — missing expectTrue/expectFalse", r.Description))
			} else {
				if r.ExpectTrueOK != nil && !*r.ExpectTrueOK {
					liquidIcon = "❌"
					notes = append(notes, fmt.Sprintf("  %q — expectTrue: fail (evaluated to false)", r.Description))
				}
				if r.ExpectFalseOK != nil && !*r.ExpectFalseOK {
					liquidIcon = "❌"
					notes = append(notes, fmt.Sprintf("  %q — expectFalse: fail (evaluated to true)", r.Description))
				}
			}
		}

		sb.WriteString(fmt.Sprintf("  %-*s | %s   | %s   | %s\n", descWidth, desc, syntaxIcon, fieldsIcon, liquidIcon))
	}

	if len(notes) > 0 {
		sb.WriteString("\n  Notes:\n")
		for _, n := range notes {
			sb.WriteString(n + "\n")
		}
	}

	return sb.String()
}
