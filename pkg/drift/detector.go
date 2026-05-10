package drift

import "fmt"

// DriftKind identifies the category of a drift event.
type DriftKind string

const (
	KindTypeChange        DriftKind = "type_change"
	KindMissingField      DriftKind = "missing_field"
	KindNewField          DriftKind = "new_field"
	KindNullabilityChange DriftKind = "nullability_change"
)

// DriftEvent is a single structural difference between the baseline and an
// incoming message.
type DriftEvent struct {
	Kind     DriftKind `json:"kind"`
	Field    string    `json:"field"`
	Expected string    `json:"expected"`
	Got      string    `json:"got"`
}

func (e DriftEvent) String() string {
	return fmt.Sprintf("[%s] field=%s expected=%s got=%s", e.Kind, e.Field, e.Expected, e.Got)
}

// DriftReport groups all events detected for one message.
type DriftReport struct {
	Source string       `json:"source"`
	Events []DriftEvent `json:"events"`
}

// HasDrift reports whether any drift events were found.
func (r DriftReport) HasDrift() bool { return len(r.Events) > 0 }

// Detect compares an incoming Schema against a frozen baseline and returns
// the full DriftReport (zero events == no drift).
func Detect(source string, incoming Schema, baseline map[string]FieldMeta) DriftReport {
	report := DriftReport{Source: source}

	for path, meta := range baseline {
		inType, present := incoming[path]
		if !present {
			if meta.Required {
				report.Events = append(report.Events, DriftEvent{
					Kind:     KindMissingField,
					Field:    path,
					Expected: string(meta.Type),
					Got:      "absent",
				})
			}
			continue
		}
		if inType == TypeNull {
			if !meta.Nullable {
				report.Events = append(report.Events, DriftEvent{
					Kind:     KindNullabilityChange,
					Field:    path,
					Expected: string(meta.Type),
					Got:      "null",
				})
			}
			continue
		}
		if inType != meta.Type {
			report.Events = append(report.Events, DriftEvent{
				Kind:     KindTypeChange,
				Field:    path,
				Expected: string(meta.Type),
				Got:      string(inType),
			})
		}
	}

	for path, inType := range incoming {
		if _, ok := baseline[path]; !ok {
			report.Events = append(report.Events, DriftEvent{
				Kind:     KindNewField,
				Field:    path,
				Expected: "absent",
				Got:      string(inType),
			})
		}
	}

	return report
}
