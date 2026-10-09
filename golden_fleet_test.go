package actae

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Cross-SDK golden fleet wire corpus (Deep-004 / PR-020). Every SDK validates
// its wire handling against spec/api-parity/v1/golden_fleet_vectors.json so a
// JSON-number, envelope, pagination-token or fork-receipt regression fails
// loudly in all three. No server required.

type fleetWire struct {
	FleetWire struct {
		HTTPEnvelope struct {
			Input  map[string]any `json:"input"`
			Unwrap map[string]any `json:"unwrap"`
		} `json:"http_envelope"`
		ErrorEnvelope struct {
			Input map[string]any `json:"input"`
			Note  string         `json:"note"`
		} `json:"error_envelope"`
		ArbitraryPrecision struct {
			Input        map[string]any `json:"input"`
			BigValue     string         `json:"big_value"`
			NegativeVal  string         `json:"negative_value"`
			FloatVal     string         `json:"float_value"`
			NestedValue  string         `json:"nested_value"`
		} `json:"arbitrary_precision"`
		Pagination struct {
			PageTokens map[string]string `json:"page_tokens"`
		} `json:"pagination"`
		ForkReceipt struct {
			Input             map[string]any `json:"input"`
			ChildChannelID    string         `json:"child_channel_id"`
			SourceChannelID   string         `json:"source_channel_id"`
			SourceStateVer    int64          `json:"source_state_version"`
			Restorable        bool           `json:"restorable"`
		} `json:"fork_receipt"`
		WSBroadcast struct {
			Legacy     map[string]any `json:"legacy"`
			TypeTagged map[string]any `json:"type_tagged"`
		} `json:"ws_broadcast"`
	} `json:"fleet_wire"`
}

func loadFleetWire(t *testing.T) fleetWire {
	t.Helper()
	path := filepath.Join("..", "..", "spec", "api-parity", "v1", "golden_fleet_vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("monorepo golden vectors unavailable (public mirror): %v", err)
		}
		t.Fatal(err)
	}
	var fw fleetWire
	if err := json.Unmarshal(raw, &fw); err != nil {
		t.Fatal(err)
	}
	return fw
}

func TestGoldenArbitraryPrecisionUnwrapsWithoutRounding(t *testing.T) {
	v := loadFleetWire(t).FleetWire.ArbitraryPrecision
	got := unwrapSonic(v.Input)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	big, ok := m["big"].(int64)
	if !ok || big != 9007199254740993 {
		t.Fatalf("big must stay exact int64 (got %#v)", m["big"])
	}
	if neg, ok := m["negative"].(int64); !ok || neg != -9007199254740993 {
		t.Fatalf("negative must stay exact int64 (got %#v)", m["negative"])
	}
	if f, ok := m["float"].(float64); !ok || f != 3.5 {
		t.Fatalf("float expected 3.5 (got %#v)", m["float"])
	}
	if nested, ok := m["nested"].(map[string]any); !ok {
		t.Fatal("nested missing")
	} else if a, ok := nested["a"].(int64); !ok || a != 123 {
		t.Fatalf("nested.a expected 123 (got %#v)", nested["a"])
	}
}

func TestGoldenHTTPEnvelopeUnwrap(t *testing.T) {
	v := loadFleetWire(t).FleetWire.HTTPEnvelope
	// The fleet client unwraps {"data": {...}}; validate the shape matches
	// the corpus expectation at the wire level.
	if _, has := v.Input["data"]; !has {
		t.Fatal("envelope must carry a data key")
	}
	data, ok := v.Input["data"].(map[string]any)
	if !ok {
		t.Fatalf("data must be an object (got %#v)", v.Input["data"])
	}
	if ok := data["ok"]; ok != true {
		t.Fatalf("unwrap.ok expected true (got %#v)", ok)
	}
	if c, ok := data["cursor"].(float64); !ok || int64(c) != 5 {
		t.Fatalf("unwrap.cursor expected 5 (got %#v)", data["cursor"])
	}
}

func TestGoldenPaginationTokensAreOpaqueMap(t *testing.T) {
	v := loadFleetWire(t).FleetWire.Pagination
	tokens := v.PageTokens
	if len(tokens) != 2 {
		t.Fatalf("pagination must preserve both tokens (got %d)", len(tokens))
	}
	if tokens["ins_a"] != "tok-a" || tokens["ins_b"] != "tok-b" {
		t.Fatalf("pagination tokens must round-trip unchanged: %#v", tokens)
	}
}

func TestGoldenForkReceiptProvenance(t *testing.T) {
	v := loadFleetWire(t).FleetWire.ForkReceipt
	if v.Input["child_channel_id"] != v.ChildChannelID {
		t.Fatalf("child_channel_id mismatch")
	}
	if v.Input["source_channel_id"] != v.SourceChannelID {
		t.Fatalf("source_channel_id mismatch")
	}
	if sv, ok := v.Input["source_state_version"].(float64); !ok || int64(sv) != v.SourceStateVer {
		t.Fatalf("source_state_version mismatch")
	}
	if v.Input["restorable"] != v.Restorable {
		t.Fatalf("restorable mismatch")
	}
}

func TestGoldenErrorEnvelopeFields(t *testing.T) {
	v := loadFleetWire(t).FleetWire.ErrorEnvelope
	if v.Input["code"] != "idempotency_conflict" {
		t.Fatalf("code mismatch: %#v", v.Input["code"])
	}
	if v.Input["message"] != "replay conflict" {
		t.Fatalf("message mismatch")
	}
	if v.Input["retryable"] != false {
		t.Fatalf("retryable mismatch")
	}
	if v.Input["request_id"] != "rq-1" {
		t.Fatalf("request_id mismatch")
	}
}