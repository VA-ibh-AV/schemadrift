package drift

import (
	"encoding/json"
	"fmt"
)

// FieldType is the JSON wire type of a field path.
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
	TypeNull    FieldType = "null"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeMixed   FieldType = "mixed"
)

// Schema maps dot-notation field paths to their FieldType.
// Array element object fields use the "path[].field" convention.
type Schema map[string]FieldType

// Fingerprint parses a JSON payload and returns its Schema.
// The root value must be a JSON object.
func Fingerprint(data []byte) (Schema, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("fingerprint: %w", err)
	}
	s := make(Schema)
	walkValue(v, "", s)
	return s, nil
}

func walkValue(v interface{}, prefix string, s Schema) {
	switch val := v.(type) {
	case map[string]interface{}:
		if prefix != "" {
			s[prefix] = TypeObject
		}
		for k, child := range val {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			walkValue(child, key, s)
		}
	case []interface{}:
		if prefix != "" {
			s[prefix] = TypeArray
		}
		walkArrayElems(val, prefix, s)
	case string:
		if prefix != "" {
			s[prefix] = TypeString
		}
	case float64:
		if prefix != "" {
			s[prefix] = TypeNumber
		}
	case bool:
		if prefix != "" {
			s[prefix] = TypeBoolean
		}
	case nil:
		if prefix != "" {
			s[prefix] = TypeNull
		}
	}
}

// walkArrayElems infers element schema for homogeneous object arrays.
// Heterogeneous arrays contribute no element-level schema entries.
func walkArrayElems(elems []interface{}, prefix string, s Schema) {
	if len(elems) == 0 {
		return
	}
	first := jsonTypeOf(elems[0])
	for _, e := range elems[1:] {
		if jsonTypeOf(e) != first {
			return // mixed — can't infer
		}
	}
	if first != TypeObject {
		return
	}
	for _, e := range elems {
		if m, ok := e.(map[string]interface{}); ok {
			for k, child := range m {
				walkValue(child, prefix+"[]."+k, s)
			}
		}
	}
}

func jsonTypeOf(v interface{}) FieldType {
	switch v.(type) {
	case map[string]interface{}:
		return TypeObject
	case []interface{}:
		return TypeArray
	case string:
		return TypeString
	case float64:
		return TypeNumber
	case bool:
		return TypeBoolean
	case nil:
		return TypeNull
	default:
		return TypeMixed
	}
}
