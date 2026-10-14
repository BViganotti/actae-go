package actae

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func bytesReader(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

func TestParseNumber(t *testing.T) {
	cases := []struct {
		in   string
		want any
		ok   bool
	}{
		{"12345", int64(12345), true},
		{"-42", int64(-42), true},
		{"0", int64(0), true},
		{"9999999999999999999", 1e19, true}, // exceeds int64 → float64, mirrors Python
		{"3.14", 3.14, true},
		{"-2.5", -2.5, true},
		{"1e3", 1000.0, true},
		{"abc", nil, false},
		{"", nil, false},
	}
	for _, c := range cases {
		got, ok := parseNumber(c.in)
		if ok != c.ok {
			t.Errorf("parseNumber(%q) ok = %v, want %v", c.in, ok, c.ok)
			continue
		}
		if c.ok && !numbersEqual(got, c.want) {
			t.Errorf("parseNumber(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func numbersEqual(a, b any) bool {
	if af, ok := a.(float64); ok {
		if bf, ok := b.(float64); ok {
			return af == bf
		}
	}
	return reflect.DeepEqual(a, b)
}

func TestUnwrapSonic(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want any
	}{
		{
			name: "top-level number wrapper",
			in:   map[string]any{sonicNumberKey: "12345"},
			want: int64(12345),
		},
		{
			name: "nested number wrapper",
			in: map[string]any{
				"a": map[string]any{sonicNumberKey: "42"},
				"b": map[string]any{"c": map[string]any{sonicNumberKey: "3.5"}},
			},
			want: map[string]any{"a": int64(42), "b": map[string]any{"c": 3.5}},
		},
		{
			name: "wrapper inside list",
			in:   []any{map[string]any{sonicNumberKey: "7"}},
			want: []any{int64(7)},
		},
		{
			name: "object with extra keys is left intact",
			in: map[string]any{
				sonicNumberKey: "1",
				"other":        "value",
			},
			want: map[string]any{
				sonicNumberKey: "1",
				"other":        "value",
			},
		},
		{
			name: "unparseable wrapper left intact",
			in:   map[string]any{sonicNumberKey: "not-a-number"},
			want: map[string]any{sonicNumberKey: "not-a-number"},
		},
		{
			name: "plain values untouched",
			in:   map[string]any{"x": "y"},
			want: map[string]any{"x": "y"},
		},
		{
			name: "scalar untouched",
			in:   "hello",
			want: "hello",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := unwrapSonic(c.in)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("unwrapSonic(%v) = %#v, want %#v", c.in, got, c.want)
			}
		})
	}
}

func TestDecodeValue(t *testing.T) {
	raw := []byte(`{"cursor": 123, "items": [{"count": "7", "n": 3.5}], "sonic": {"$sonic_rs::private::JsonNumber": "999"}}`)
	v, err := decodeValue(raw)
	if err != nil {
		t.Fatalf("decodeValue: %v", err)
	}
	m := v.(map[string]any)
	if m["cursor"] != int64(123) {
		t.Errorf("cursor = %#v, want int64(123)", m["cursor"])
	}
	items := m["items"].([]any)
	first := items[0].(map[string]any)
	if first["count"] != "7" {
		t.Errorf("count = %#v, want string", first["count"])
	}
	if first["n"] != 3.5 {
		t.Errorf("n = %#v, want 3.5", first["n"])
	}
	if m["sonic"] != int64(999) {
		t.Errorf("sonic = %#v, want int64(999)", m["sonic"])
	}
}

func TestDecodeValueBadJSON(t *testing.T) {
	if _, err := decodeValue([]byte(`{not json`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMapAccessors(t *testing.T) {
	decoded, err := decodeValue([]byte(`{"s": "str", "i": 5, "f": 2.5, "b": true, "o": {"k": 1}, "l": [1,2]}`))
	if err != nil {
		t.Fatal(err)
	}
	m := decoded.(map[string]any)
	if str(m, "s") != "str" {
		t.Error("str() failed")
	}
	if intOf(m, "i", 0) != 5 {
		t.Error("intOf() failed")
	}
	if !boolOf(m, "b", false) {
		t.Error("boolOf() failed")
	}
	if mapOf(m, "o") == nil {
		t.Error("mapOf() failed")
	}
	if len(listOf(m, "l")) != 2 {
		t.Error("listOf() failed")
	}
	if str(m, "missing") != "" {
		t.Error("str() default failed")
	}
	if int64ptr(m, "f") == nil || *int64ptr(m, "f") != 2 {
		t.Error("int64ptr() float conversion failed")
	}
}

func TestNormalizeJSONIntegers(t *testing.T) {
	var v any
	dec := json.NewDecoder(bytesReader(`{"a": 1, "b": 9007199254740993}`))
	_ = dec
	_ = v
	// 9007199254740993 exceeds float64 precision; ensure UseNumber path keeps it.
	dec = json.NewDecoder(bytesReader(`{"b": 9007199254740993}`))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	got := normalizeJSON(v).(map[string]any)["b"]
	if got != int64(9007199254740993) {
		t.Errorf("big int = %#v, want int64(9007199254740993)", got)
	}
}
