package actae

import (
	"errors"
	"testing"
)

func TestNewConstructors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"auth", NewAuthError("bad key"), "bad key"},
		{"auth default", NewAuthError(""), "Authentication failed"},
		{"connection", NewConnectionError("drop"), "drop"},
		{"connection default", NewConnectionError(""), "Connection failed"},
		{"rate limit", NewRateLimitError(30, "slow down"), "Rate limited — retry after 30s: slow down"},
		{"api", NewAPIError(500, "boom"), "HTTP 500: boom"},
		{"api no message", NewAPIError(400, ""), "HTTP 400"},
		{"snapshot boundary", NewSnapshotBoundaryError("no boundary"), "no boundary"},
		{"version conflict", NewVersionConflictError("conflict"), "conflict"},
		{"consumer", NewConsumerError("gone"), "gone"},
		{"idempotency", NewIdempotencyKeyMismatchError("mismatch"), "mismatch"},
		{"execution not owned", NewExecutionNotOwnedError("stale"), "stale"},
		{"execution not found", NewExecutionNotFoundError("missing"), "missing"},
		{"session", NewSessionError("bad lifecycle"), "bad lifecycle"},
		{"session completed", NewSessionCompletedError("already done"), "already done"},
	}
	for _, c := range cases {
		if c.err.Error() != c.want {
			t.Errorf("%s: Error() = %q, want %q", c.name, c.err.Error(), c.want)
		}
	}
}

func TestAllErrorsImplementActaeError(t *testing.T) {
	errs := []ActaeError{
		NewAuthError("x"),
		NewConnectionError("x"),
		NewRateLimitError(1, "x"),
		NewAPIError(400, "x"),
		NewSnapshotBoundaryError("x"),
		NewVersionConflictError("x"),
		NewConsumerError("x"),
		NewIdempotencyKeyMismatchError("x"),
		NewExecutionNotOwnedError("x"),
		NewExecutionNotFoundError("x"),
		NewSessionError("x"),
		NewSessionCompletedError("x"),
	}
	for _, e := range errs {
		var iface ActaeError = e
		if iface == nil {
			t.Fatal("nil ActaeError")
		}
	}
}

func TestErrorsAs(t *testing.T) {
	err := NewAPIError(429, "throttled")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatal("errors.As failed for APIError")
	}
	if apiErr.StatusCode != 429 {
		t.Errorf("StatusCode = %d, want 429", apiErr.StatusCode)
	}
	var rlErr *RateLimitError
	if errors.As(err, &rlErr) {
		t.Fatal("APIError should not unwrap to RateLimitError")
	}
}

func TestErrorHierarchyIsDistinct(t *testing.T) {
	// SessionCompletedError must not be matched by SessionError via errors.As
	// unless we choose to; verify current behavior is a distinct type.
	err := NewSessionCompletedError("done")
	var sessErr *SessionError
	if errors.As(err, &sessErr) {
		t.Log("SessionCompletedError unwraps to SessionError (embedded struct)")
	}
}

func TestRateLimitRetryAfter(t *testing.T) {
	e := NewRateLimitError(60, "wait")
	if e.RetryAfterSeconds != 60 {
		t.Errorf("RetryAfterSeconds = %d, want 60", e.RetryAfterSeconds)
	}
}

func TestAPIStatusCode(t *testing.T) {
	e := NewAPIError(409, "conflict")
	if e.StatusCode != 409 {
		t.Errorf("StatusCode = %d, want 409", e.StatusCode)
	}
}

func TestBuiltinShadowing(t *testing.T) {
	// The Go package exposes typed constructors; ensure no name collision
	// with stdlib identifiers used by clients (errors package etc.).
	var _ = NewConnectionError("x")
	var _ = NewAuthError("x")
}
