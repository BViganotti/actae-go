package actae

import (
	"context"
	"errors"
	"time"
)

// Durable human-in-the-loop approval events (see docs/HUMAN_APPROVAL.md).
const (
	ApprovalRequested = "approval.requested"
	ApprovalDecided   = "approval.decided"
	ApprovalExpired   = "approval.expired"
)

// RequestApprovalOptions configures RequestApproval.
type RequestApprovalOptions struct {
	// Summary is a one-line human-readable description of the request.
	Summary string
	// Details is optional structured context for the approver.
	Details map[string]any
	// TimeoutSeconds, when set, schedules a durable wake-up at the deadline.
	TimeoutSeconds *float64
	// Requester is the actor label stamped on the event (default "agent").
	Requester string
	// RequestID is a stable id for retries; generated when empty.
	RequestID string
}

// RequestApproval records an `approval.requested` event and returns its
// request id. POST /api/v1/events/record (+ POST /api/v1/scheduler/wakeups
// when TimeoutSeconds is set). The agent process need not stay alive: the
// request and its timeout are durable.
func (c *Client) RequestApproval(ctx context.Context, channelID string, opts RequestApprovalOptions) (string, error) {
	if opts.Summary == "" {
		return "", errors.New("summary is required")
	}
	requester := opts.Requester
	if requester == "" {
		requester = "agent"
	}
	requestID := opts.RequestID
	if requestID == "" {
		requestID = newUUID()
	}
	payload := map[string]any{"request_id": requestID, "summary": opts.Summary}
	if opts.Details != nil {
		payload["details"] = opts.Details
	}
	if opts.TimeoutSeconds != nil {
		payload["timeout_seconds"] = *opts.TimeoutSeconds
	}
	operationID := requestID
	if _, err := c.Record(ctx, channelID, ApprovalRequested, payload, RecordOptions{
		Actor:       requester,
		OperationID: &operationID,
	}); err != nil {
		return "", err
	}
	if opts.TimeoutSeconds != nil {
		deadline := time.Now().UTC().Add(time.Duration(*opts.TimeoutSeconds * float64(time.Second)))
		if _, err := c.ScheduleWakeup(ctx, channelID, deadline.Format(time.RFC3339), map[string]any{
			"approval_request_id": requestID,
			"kind":                "approval_timeout",
		}); err != nil {
			return "", err
		}
	}
	return requestID, nil
}

// DecideApprovalOptions configures DecideApproval.
type DecideApprovalOptions struct {
	// Decision is "approved" or "rejected".
	Decision string
	// Actor is the human/service resolving the request.
	Actor string
	// Reason is optional free-text rationale.
	Reason string
	// OperationID makes the decision idempotent across retries.
	OperationID string
}

// DecideApproval records the human decision as an `approval.decided` event.
// The agent discovers it by replay/stream; Actae never resumes anything itself.
func (c *Client) DecideApproval(ctx context.Context, channelID, requestID string, opts DecideApprovalOptions) (Event, error) {
	if opts.Decision != "approved" && opts.Decision != "rejected" {
		return Event{}, errors.New("decision must be 'approved' or 'rejected'")
	}
	// No volatile field (e.g. a timestamp) belongs in the payload: the
	// deterministic operation id must make an identical decision replay, and
	// the event's own Timestamp records when it was committed.
	payload := map[string]any{
		"request_id": requestID,
		"decision":   opts.Decision,
	}
	if opts.Reason != "" {
		payload["reason"] = opts.Reason
	}
	ro := RecordOptions{Actor: opts.Actor}
	// A deterministic operation id keyed by (request_id, decision) makes a
	// retried/duplicated decision idempotent (same decision -> the original
	// event). A different decision derives a different id and is recorded as a
	// new event (the application applies last-wins).
	op := opts.OperationID
	if op == "" {
		op = DeterministicOperationKey("approval", opts.Decision, requestID)
	}
	ro.OperationID = &op
	return c.Record(ctx, channelID, ApprovalDecided, payload, ro)
}

// WaitForApproval polls the channel until the decision for requestID appears.
// On timeout it records an `approval.expired` event and returns (nil, nil).
// A zero timeout waits indefinitely (respecting ctx).
func (c *Client) WaitForApproval(ctx context.Context, channelID, requestID string, timeout, pollInterval time.Duration, startCursor int64) (*Event, error) {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	cursor := startCursor
	for {
		events, err := c.Replay(ctx, channelID, ReplayOptions{Cursor: &cursor, Limit: 1000})
		if err != nil {
			return nil, err
		}
		for i := range events {
			ev := events[i]
			if ev.Cursor > cursor {
				cursor = ev.Cursor
			}
			if ev.EventType == ApprovalDecided && payloadString(ev.Payload, "request_id") == requestID {
				return &ev, nil
			}
		}
		if timeout > 0 && time.Now().After(deadline) {
			_, _ = c.Record(ctx, channelID, ApprovalExpired, map[string]any{"request_id": requestID}, RecordOptions{Actor: "system"})
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func payloadString(payload any, key string) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	value, _ := m[key].(string)
	return value
}
