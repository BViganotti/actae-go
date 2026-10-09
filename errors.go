package actae

import (
	"errors"
	"fmt"
)

// ActaeError is the base interface implemented by all Actae SDK errors.
// Use errors.As with the concrete types below to inspect failures.
type ActaeError interface {
	error
	isActaeError()
}

type actaeError struct{ message string }

func (e *actaeError) Error() string { return e.message }
func (e *actaeError) isActaeError() {}

// AuthError is raised when authentication with the Actae server fails:
// missing/invalid API keys, or JWT expiry during WebSocket auth.
type AuthError struct{ actaeError }

// NewAuthError creates an AuthError.
func NewAuthError(message string) *AuthError {
	if message == "" {
		message = "Authentication failed"
	}
	return &AuthError{actaeError{message}}
}

// ConnectionError is raised when the WebSocket connection to Actae fails or
// drops: server unreachable, handshake failure, transport timeout, or
// subscription ack timeout.
type ConnectionError struct{ actaeError }

// NewConnectionError creates a ConnectionError.
func NewConnectionError(message string) *ConnectionError {
	if message == "" {
		message = "Connection failed"
	}
	return &ConnectionError{actaeError{message}}
}

// RateLimitError is raised on HTTP 429 (Too Many Requests).
type RateLimitError struct {
	actaeError
	RetryAfterSeconds int
}

// NewRateLimitError creates a RateLimitError.
func NewRateLimitError(retryAfter int, message string) *RateLimitError {
	msg := fmt.Sprintf("Rate limited — retry after %ds", retryAfter)
	if message != "" {
		msg += ": " + message
	}
	return &RateLimitError{actaeError{msg}, retryAfter}
}

// APIError is raised when an HTTP API call returns a non-success status code.
type APIError struct {
	actaeError
	StatusCode int
}

// NewAPIError creates an APIError.
func NewAPIError(statusCode int, message string) *APIError {
	msg := fmt.Sprintf("HTTP %d", statusCode)
	if message != "" {
		msg += ": " + message
	}
	return &APIError{actaeError{msg}, statusCode}
}

// SnapshotBoundaryError is raised on HTTP 409 "snapshot_boundary_required":
// a fork's at_cursor has no saved state boundary at or before it.
type SnapshotBoundaryError struct{ actaeError }

// NewSnapshotBoundaryError creates a SnapshotBoundaryError.
func NewSnapshotBoundaryError(message string) *SnapshotBoundaryError {
	if message == "" {
		message = "No saved state boundary at or before the requested cursor"
	}
	return &SnapshotBoundaryError{actaeError{message}}
}

// VersionConflictError is raised on HTTP 409 "version_conflict": an
// optimistic-concurrency guard rejected a write.
type VersionConflictError struct{ actaeError }

// NewVersionConflictError creates a VersionConflictError.
func NewVersionConflictError(message string) *VersionConflictError {
	if message == "" {
		message = "State version conflict — reload latest state and retry"
	}
	return &VersionConflictError{actaeError{message}}
}

// NoRestorableCheckpointError is raised by AgentSession in strict ("exact")
// boundary mode when a fork cannot restore a checkpoint at the requested
// step. The server reported snapshot_boundary_required and the session
// refused to silently fall forward to the latest state (which would
// contaminate the fork with state from after the requested boundary).
type NoRestorableCheckpointError struct{ actaeError }

// NewNoRestorableCheckpointError creates a NoRestorableCheckpointError.
func NewNoRestorableCheckpointError(message string) *NoRestorableCheckpointError {
	if message == "" {
		message = "No restorable checkpoint at the requested fork boundary"
	}
	return &NoRestorableCheckpointError{actaeError{message}}
}

// IdempotencyConflictError is raised on HTTP 409 "idempotency_conflict": a
// fork operation_id was reused with a different request.
type IdempotencyConflictError struct{ actaeError }

// NewIdempotencyConflictError creates an IdempotencyConflictError.
func NewIdempotencyConflictError(message string) *IdempotencyConflictError {
	if message == "" {
		message = "Idempotency key already used with a different request"
	}
	return &IdempotencyConflictError{actaeError{message}}
}

// ChannelConflictError is raised on HTTP 409 "channel_conflict": a fork's
// new_channel_id already exists under a different source or boundary.
type ChannelConflictError struct{ actaeError }

// NewChannelConflictError creates a ChannelConflictError.
func NewChannelConflictError(message string) *ChannelConflictError {
	if message == "" {
		message = "Channel already exists under a different fork definition"
	}
	return &ChannelConflictError{actaeError{message}}
}

// CounterfactualBlockedError reports a frozen-member or side-effect-policy boundary.
type CounterfactualBlockedError struct{ actaeError }

func NewCounterfactualBlockedError(message string) *CounterfactualBlockedError {
	if message == "" {
		message = "counterfactual execution blocked"
	}
	return &CounterfactualBlockedError{actaeError{message}}
}

// ForkToolBlockedError reports a forked channel's tool policy refusing to
// execute a tool (HTTP 409 fork_tool_blocked): the effective policy is
// `block`, or `replay` with no exact source match. The tool was not executed
// and an immutable `tool.blocked` boundary was persisted.
type ForkToolBlockedError struct{ actaeError }

func NewForkToolBlockedError(message string) *ForkToolBlockedError {
	if message == "" {
		message = "fork tool policy blocked the tool execution"
	}
	return &ForkToolBlockedError{actaeError{message}}
}

// ConsumerError is raised for consumer-group errors (missing group, expired
// lease) — HTTP 409 "consumer_not_found".
type ConsumerError struct{ actaeError }

// NewConsumerError creates a ConsumerError.
func NewConsumerError(message string) *ConsumerError {
	if message == "" {
		message = "Consumer group error"
	}
	return &ConsumerError{actaeError{message}}
}

// IdempotencyKeyMismatchError is raised on HTTP 409 "idempotency_key_mismatch":
// an execution claim collided with a different request.
type IdempotencyKeyMismatchError struct{ actaeError }

// NewIdempotencyKeyMismatchError creates an IdempotencyKeyMismatchError.
func NewIdempotencyKeyMismatchError(message string) *IdempotencyKeyMismatchError {
	if message == "" {
		message = "Idempotency key already used with a different request"
	}
	return &IdempotencyKeyMismatchError{actaeError{message}}
}

// ExecutionNotOwnedError is raised on HTTP 409 "execution_not_owned": an
// execution operation supplied a stale claim token.
type ExecutionNotOwnedError struct{ actaeError }

// NewExecutionNotOwnedError creates an ExecutionNotOwnedError.
func NewExecutionNotOwnedError(message string) *ExecutionNotOwnedError {
	if message == "" {
		message = "Execution is owned by another attempt"
	}
	return &ExecutionNotOwnedError{actaeError{message}}
}

// ExecutionNotFoundError is raised on HTTP 404 "execution_not_found".
type ExecutionNotFoundError struct{ actaeError }

// NewExecutionNotFoundError creates an ExecutionNotFoundError.
func NewExecutionNotFoundError(message string) *ExecutionNotFoundError {
	if message == "" {
		message = "Execution not found"
	}
	return &ExecutionNotFoundError{actaeError{message}}
}

// SessionError is the base error for AgentSession lifecycle violations.
type SessionError struct{ actaeError }

// NewSessionError creates a SessionError.
func NewSessionError(message string) *SessionError {
	return &SessionError{actaeError{message}}
}

// SessionCompletedError is raised when stepping on a completed session.
type SessionCompletedError struct{ actaeError }

// NewSessionCompletedError creates a SessionCompletedError.
func NewSessionCompletedError(message string) *SessionCompletedError {
	return &SessionCompletedError{actaeError{message}}
}

// ErrWakeupAlreadyFired is returned by CancelWakeup when the wake-up was
// already fired, failed, or cancelled (HTTP 409). It is a benign no-op
// outcome, not a transport or API failure:
//
//	cancelled, err := client.CancelWakeup(ctx, id)
//	if errors.Is(err, actae.ErrWakeupAlreadyFired) { /* nothing to cancel */ }
var ErrWakeupAlreadyFired = errors.New("actae: wakeup already fired, failed, or cancelled")
