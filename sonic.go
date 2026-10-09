package actae

import (
	"bytes"
	"encoding/json"
)

const sonicNumberKey = "$sonic_rs::private::JsonNumber"

// unwrapSonic recursively replaces sonic-rs number wrappers
// ({"$sonic_rs::private::JsonNumber": "12345"}) with native int64/float64,
// mirroring the Python SDK's _unwrap_sonic. Values that fail conversion are
// left untouched.
func unwrapSonic(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			if raw, ok := t[sonicNumberKey].(string); ok && raw != "" {
				if num, ok := parseNumber(raw); ok {
					return num
				}
				return v
			}
		}
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = unwrapSonic(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = unwrapSonic(val)
		}
		return out
	default:
		return v
	}
}

// parseNumber converts a numeric string into int64 when integral (or within
// int64 range) and float64 otherwise, mirroring Python's int()/float()
// coercion in _unwrap_sonic.
func parseNumber(raw string) (any, bool) {
	if i, err := json.Number(raw).Int64(); err == nil {
		return i, true
	}
	if f, err := json.Number(raw).Float64(); err == nil {
		return f, true
	}
	return nil, false
}

// decodeValue decodes a JSON object into any, converting numbers with
// int64/float64 fidelity and unwrapping sonic-rs markers.
func decodeValue(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return normalizeJSON(v), nil
}

// normalizeJSON converts json.Number values to int64/float64 and unwraps
// sonic-rs markers recursively.
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return string(t)
	case map[string]any:
		if len(t) == 1 {
			if raw, ok := t[sonicNumberKey].(string); ok && raw != "" {
				if num, ok := parseNumber(raw); ok {
					return num
				}
			}
		}
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	default:
		return v
	}
}

// --- small JSON accessor helpers over parsed map bodies -------------------- //

func str(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func int64ptr(m map[string]any, key string) *int64 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case int64:
		return &t
	case float64:
		i := int64(t)
		return &i
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return &i
		}
	case string:
		var i int64
		if err := json.Unmarshal([]byte(t), &i); err == nil {
			return &i
		}
	}
	return nil
}

func intOf(m map[string]any, key string, def int64) int64 {
	if p := int64ptr(m, key); p != nil {
		return *p
	}
	return def
}

func boolOf(m map[string]any, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return def
}

func float64ptr(m map[string]any, key string) *float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		return &t
	case int64:
		f := float64(t)
		return &f
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return &f
		}
	}
	return nil
}

func strptr(m map[string]any, key string) *string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	return &s
}

func mapOf(m map[string]any, key string) map[string]any {
	if v, ok := m[key].(map[string]any); ok {
		return v
	}
	return nil
}

func listOf(m map[string]any, key string) []any {
	if v, ok := m[key].([]any); ok {
		return v
	}
	return nil
}

// sonicMap returns the value of key as a map, with sonic-rs number wrappers
// unwrapped. Returns nil when the key is absent or not an object.
func sonicMap(m map[string]any, key string) map[string]any {
	raw := mapOf(m, key)
	if raw == nil {
		return nil
	}
	unwrapped, ok := unwrapSonic(raw).(map[string]any)
	if !ok {
		return nil
	}
	return unwrapped
}

// --- typed accessors for payloads ---------------------------------------- //
//
// JSON numbers are decoded as int64 when integral and float64 otherwise, so
// a payload value can be either type depending on how the server stored it
// (e.g. {"v": 1} → int64, {"v": 1.0} → float64). AsInt64 normalizes both,
// plus numeric strings and json.Number, so payload access does not need
// type-switch boilerplate. For structured payloads, json.Marshal the
// whole Event.Payload into your own struct instead.

// AsInt64 returns v as an int64 when it is an integer (int64/int/float64
// with an integral value) or a numeric string; otherwise def.
func AsInt64(v any, def int64) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
	case string:
		var i int64
		if err := json.Unmarshal([]byte(t), &i); err == nil {
			return i
		}
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
	}
	return def
}

// AsString returns v as a string, or "" when it is not one.
func AsString(v any) string {
	s, _ := v.(string)
	return s
}

// AsMap returns v as a map, or nil when it is not one.
func AsMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// AsList returns v as a list, or nil when it is not one.
func AsList(v any) []any {
	l, _ := v.([]any)
	return l
}
