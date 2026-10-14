package actae

import (
	"testing"
)

// TestDeterministicOperationKeyKnownVectors pins the cross-SDK parity
// vectors produced by the Python SDK's _step_operation_id (which mirrors
// this derivation byte-for-byte — see sdks/python/tests/test_session.py).
// If this test fails, the canonical serialization or UUIDv5 derivation
// drifted between SDKs.
func TestDeterministicOperationKeyKnownVectors(t *testing.T) {

	// Plain vector: channel "ch-det", type "s", step 1.
	got := DeterministicOperationKey(
		"agent-session", "s", "ch-det", "1",
		CanonicalStepContent(
			map[string]any{"step_number": int64(1), "input": "p", "output": "r"},
			map[string]any{"step_number": int64(1)},
		),
	)
	if got != "07ca53db-606e-5b76-95bc-697e73bc4c41" {
		t.Errorf("plain vector drift: got %q", got)
	}

	// Escaping vector: non-ASCII + HTML chars + nested dicts.
	got = DeterministicOperationKey(
		"agent-session", "inference", "ch-x", "3",
		CanonicalStepContent(
			map[string]any{
				"input": "a < b & c > d", "z": int64(1),
				"ctx": map[string]any{"nested": "é<&>"},
			},
			map[string]any{"note": "x&y"},
		),
	)
	if got != "f1f23da4-abd0-50f9-b62c-1f1d0edd2fbf" {
		t.Errorf("escaping vector drift: got %q", got)
	}
}
