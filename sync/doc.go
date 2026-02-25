package sync

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strings"
)

// FieldDocRow represents a single row in the field mapping documentation.
type FieldDocRow struct {
	FieldName   string // Display name without type prefix (e.g., "email", "shirt-size")
	FieldID     string // Full field ID (e.g., "str::email", "str:cm:shirt-size")
	Entity      string // "Person" or "Activity" (only for ortto-activities)
	IsBuiltin   bool   // Whether this is a built-in Ortto field
	FieldType   string // Ortto field type (Text, Long text, Boolean, etc.)
	SourcePath  string // Raisely source path
	Notes       string // Mapping notes (transforms, warnings, team indicator)
	IsTeamField bool   // Whether this is a team field
}

// FieldDocumentation contains all field documentation for a campaign configuration.
type FieldDocumentation struct {
	CampaignLabel string
	Target        string
	Rows          []FieldDocRow
}

// GenerateFieldDocumentation generates field documentation from a campaign configuration.
// For ortto-activities, it uses the isPersonFieldFn to determine if a field is a Person or Activity field.
func GenerateFieldDocumentation(config Config, campaignlabel string, ispersonfieldfn func(fieldid string) bool) FieldDocumentation {
	doc := FieldDocumentation{
		CampaignLabel: campaignlabel,
		Target:        config.Target,
		Rows:          []FieldDocRow{},
	}

	// Process fundraiser builtin fields
	processFieldMappings(&doc.Rows, config.FundraiserFieldMappings.Builtin, config.FundraiserFieldTransforms, true, false, ispersonfieldfn)

	// Process fundraiser custom fields
	processFieldMappings(&doc.Rows, config.FundraiserFieldMappings.Custom, config.FundraiserFieldTransforms, false, false, ispersonfieldfn)

	// Process team custom fields
	processFieldMappings(&doc.Rows, config.TeamFieldMappings.Custom, config.TeamFieldTransforms, false, true, ispersonfieldfn)

	// Sort rows for deterministic output:
	// - For ortto-activities: Person fields first, then Activity fields
	// - Within each group: builtin fields first, then custom fields
	// - Within each subgroup: alphabetically by field name
	sort.SliceStable(doc.Rows, func(i, j int) bool {
		// For ortto-activities, Person fields come before Activity fields
		if config.Target == "ortto-activities" {
			if doc.Rows[i].Entity != doc.Rows[j].Entity {
				return doc.Rows[i].Entity == "Person"
			}
		}
		// Within same entity (or for ortto-contacts), builtin fields come before custom
		if doc.Rows[i].IsBuiltin != doc.Rows[j].IsBuiltin {
			return doc.Rows[i].IsBuiltin
		}
		// Within same builtin status, sort alphabetically by field name
		return doc.Rows[i].FieldName < doc.Rows[j].FieldName
	})

	return doc
}

// processFieldMappings extracts field documentation from a FieldMappings struct.
// Fields are processed in sorted order by field ID for deterministic output.
func processFieldMappings(rows *[]FieldDocRow, mappings FieldMappings, transforms map[string]string, isbuiltin bool, isteamfield bool, ispersonfieldfn func(fieldid string) bool) {
	// Strings
	for _, fieldID := range sortedKeys(mappings.Strings) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Strings[fieldID], "Text", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Texts
	for _, fieldID := range sortedKeys(mappings.Texts) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Texts[fieldID], "Long text", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Decimals
	for _, fieldID := range sortedKeys(mappings.Decimals) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Decimals[fieldID], "Decimal number", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Integers
	for _, fieldID := range sortedKeys(mappings.Integers) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Integers[fieldID], "Whole number", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Booleans
	for _, fieldID := range sortedKeys(mappings.Booleans) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Booleans[fieldID], "Boolean", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Timestamps
	for _, fieldID := range sortedKeys(mappings.Timestamps) {
		*rows = append(*rows, createFieldDocRow(fieldID, mappings.Timestamps[fieldID], "Time and date", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Phones (nested type)
	for _, fieldID := range sortedKeysPhones(mappings.Phones) {
		sourcePath := extractNestedSourcePath(mappings.Phones[fieldID])
		*rows = append(*rows, createFieldDocRow(fieldID, sourcePath, "Phone", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}

	// Geos (nested type)
	for _, fieldID := range sortedKeysGeos(mappings.Geos) {
		sourcePath := extractNestedSourcePath(mappings.Geos[fieldID])
		*rows = append(*rows, createFieldDocRow(fieldID, sourcePath, "Geo", transforms, isbuiltin, isteamfield, ispersonfieldfn))
	}
}

// sortedKeys returns the keys of a map[string]string in sorted order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedKeysPhones returns the keys of the Phones map in sorted order.
func sortedKeysPhones(m map[string]map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedKeysGeos returns the keys of the Geos map in sorted order.
func sortedKeysGeos(m map[string]map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// extractNestedSourcePath extracts the source path from a nested field mapping.
// For complex types like phones and geos, we show "(complex)" since they have multiple sub-fields.
func extractNestedSourcePath(nestedMap map[string]string) string {
	// For nested types, we just indicate it's complex since they have multiple sub-fields
	return "(complex)"
}

// createFieldDocRow creates a FieldDocRow from field mapping data.
func createFieldDocRow(fieldid string, sourcepathwithtransforms string, fieldtype string, transforms map[string]string, isbuiltin bool, isteamfield bool, ispersonfieldfn func(fieldid string) bool) FieldDocRow {
	row := FieldDocRow{
		FieldName:   extractFieldName(fieldid),
		FieldID:     fieldid,
		IsBuiltin:   isbuiltin,
		FieldType:   fieldtype,
		IsTeamField: isteamfield,
	}

	// Extract source path and inline transforms
	sourcePath, inlineTransforms := parseSourcePath(sourcepathwithtransforms)
	row.SourcePath = sourcePath

	// Build notes from inline transforms and field transforms
	notes := []string{}

	// Add inline transforms to notes
	for _, transform := range inlineTransforms {
		notes = append(notes, formatTransformNote(transform))
	}

	// Add field transforms to notes
	if transform, exists := transforms[fieldid]; exists {
		notes = append(notes, formatTransformNote(transform))
	}

	// Add team field indicator
	if isteamfield {
		notes = append(notes, "Team field")
	}

	row.Notes = strings.Join(notes, " | ")

	// Determine entity for ortto-activities
	if ispersonfieldfn != nil {
		if ispersonfieldfn(fieldid) {
			row.Entity = "Person"
		} else {
			row.Entity = "Activity"
		}
	}

	return row
}

// extractFieldName extracts the field name from an Ortto field ID.
// e.g., "str:cm:campaign-field-name" -> "campaign-field-name"
// e.g., "str::email" -> "email"
func extractFieldName(fieldid string) string {
	parts := strings.Split(fieldid, ":")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return fieldid
}

// parseSourcePath extracts the source path and inline transforms from a mapping value.
// e.g., "user.country|@countryName" -> ("user.country", ["@countryName"])
func parseSourcePath(value string) (string, []string) {
	if value == "" {
		return "(computed)", nil
	}

	parts := strings.Split(value, "|")
	sourcePath := parts[0]
	var transforms []string

	for i := 1; i < len(parts); i++ {
		part := parts[i]
		// Skip non-transform parts (like "c" or "n" in phone mappings)
		if strings.HasPrefix(part, "@") {
			transforms = append(transforms, part)
		}
	}

	return sourcePath, transforms
}

// formatTransformNote formats a transform into a human-readable note.
func formatTransformNote(transform string) string {
	switch {
	case transform == "warnIfEqual:":
		return "Warns if empty"
	case transform == "warnIfEqual:<nil>":
		return "Warns if nil"
	case strings.HasPrefix(transform, "onlyIfNotDefault:"):
		arg := strings.TrimPrefix(transform, "onlyIfNotDefault:")
		return fmt.Sprintf("Only syncs if not default (%q)", arg)
	case transform == "isCaptain":
		return "Computed via isCaptain transform"
	case transform == "isMember":
		return "Computed via isMember transform"
	case transform == "onlyIfOrgTypeSchool":
		return "Only syncs if org type is school"
	case strings.HasPrefix(transform, "@countryName"):
		return "Uses @countryName transform"
	case strings.HasPrefix(transform, "@pathJoinURL"):
		return "Uses @pathJoinURL transform"
	case strings.HasPrefix(transform, "@currency:"):
		arg := strings.TrimPrefix(transform, "@currency:")
		return fmt.Sprintf("Uses @currency:%s transform", arg)
	case strings.HasPrefix(transform, "@distance:"):
		arg := strings.TrimPrefix(transform, "@distance:")
		return fmt.Sprintf("Uses @distance:%s transform", arg)
	case strings.HasPrefix(transform, "@phone:"):
		arg := strings.TrimPrefix(transform, "@phone:")
		return fmt.Sprintf("Uses @phone:%s transform", arg)
	case strings.HasPrefix(transform, "@gte:"):
		arg := strings.TrimPrefix(transform, "@gte:")
		return fmt.Sprintf("Uses @gte:%s transform", arg)
	case strings.HasPrefix(transform, "@contains:"):
		arg := strings.TrimPrefix(transform, "@contains:")
		return fmt.Sprintf("Uses @contains:%s transform", arg)
	case transform == "@now":
		return "Uses @now transform"
	case strings.HasPrefix(transform, "onlyIfSelfDonatedDuringRegistrationWindow:"):
		return "Only syncs if self-donated during registration window"
	case transform == "toLower":
		return "Converts to lowercase"
	case transform == "toUpper":
		return "Converts to uppercase"
	default:
		// Return the transform as-is if not recognized
		return fmt.Sprintf("Transform: %s", transform)
	}
}

// FormatCSV formats the field documentation as CSV.
func (d FieldDocumentation) FormatCSV() (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write campaign comment
	if err := writer.Write([]string{fmt.Sprintf("# Campaign: %s", d.CampaignLabel)}); err != nil {
		return "", err
	}

	// Write headers based on target
	var headers []string
	if d.Target == "ortto-activities" {
		headers = []string{"Ortto Field Name", "Ortto Entity", "Ortto Built-in Field", "Ortto Field Type", "Raisely Source Path", "Mapping Notes"}
	} else {
		headers = []string{"Ortto Field Name", "Ortto Built-in Field", "Ortto Field Type", "Raisely Source Path", "Mapping Notes"}
	}
	if err := writer.Write(headers); err != nil {
		return "", err
	}

	// Write data rows
	for _, row := range d.Rows {
		builtinMark := ""
		if row.IsBuiltin {
			builtinMark = "✓"
		}

		var record []string
		if d.Target == "ortto-activities" {
			record = []string{row.FieldName, row.Entity, builtinMark, row.FieldType, row.SourcePath, row.Notes}
		} else {
			record = []string{row.FieldName, builtinMark, row.FieldType, row.SourcePath, row.Notes}
		}
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}

	return buf.String(), nil
}
