package actae

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// Live side-effect-aware fork + recovery + approval suite (opt-in).
//
// Skipped unless ACTAE_URL and ACTAE_API_KEY are set:
//
//	ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... \
//	  go test -run TestForkInterventionLive .

func forkInterventionLiveClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("ACTAE_URL") == "" || os.Getenv("ACTAE_API_KEY") == "" {
		t.Skip("ACTAE_URL/ACTAE_API_KEY not set")
	}
	c, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	return c
}

func forkInterventionChan(prefix string) string {
	return fmt.Sprintf("go-intervention-%s-%d", prefix, time.Now().UnixNano())
}

func forkInterventionBaseline(t *testing.T, c *Client, name string) *AgentSession {
	t.Helper()
	ctx := context.Background()
	s, err := NewAgentSession(c, name, AgentSessionOptions{
		StateFn:          func() map[string]any { return map[string]any{"step": 2} },
		SnapshotInterval: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "research", StepOptions{Output: map[string]any{"sources": 3}}); err != nil {
		t.Fatal(err)
	}
	claim, err := c.ClaimExecution(ctx, name, "email-1", "email.send", map[string]any{"to": "a@b.c"}, ClaimExecutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != "claimed" {
		t.Fatalf("claim status = %q", claim.Status)
	}
	if _, err := c.CompleteExecution(ctx, claim.Execution.ID, claim.ClaimToken, map[string]any{"messageId": "m1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "send_email", StepOptions{Output: map[string]any{"messageId": "m1"}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestForkInterventionLive(t *testing.T) {
	c := forkInterventionLiveClient(t)
	ctx := context.Background()
	session := forkInterventionBaseline(t, c, forkInterventionChan("base"))

	// auto (default): inherited effect replays.
	auto, err := session.Fork(ctx, 2, forkInterventionChan("auto"), ForkSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.ClaimExecution(ctx, auto.ChannelID(), "email-1", "email.send", map[string]any{"to": "a@b.c"}, ClaimExecutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != "replayed" {
		t.Fatalf("auto status = %q, want replayed", replayed.Status)
	}

	// block: a new irreversible call is refused.
	blocked, err := session.Fork(ctx, 2, forkInterventionChan("block"), ForkSessionOptions{
		ToolPolicies: map[string]any{"email.send": "block"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ClaimExecution(ctx, blocked.ChannelID(), "email-2", "email.send", map[string]any{"to": "x@y.z"}, ClaimExecutionOptions{})
	var blockedErr *ForkToolBlockedError
	if !errors.As(err, &blockedErr) {
		t.Fatalf("block claim err = %v, want ForkToolBlockedError", err)
	}

	// intervention round-trips.
	intervention := map[string]any{"model": "candidate-model"}
	child, err := session.Fork(ctx, 1, forkInterventionChan("intervention"), ForkSessionOptions{Intervention: intervention})
	if err != nil {
		t.Fatal(err)
	}
	if child.Intervention()["model"] != "candidate-model" {
		t.Fatalf("intervention = %v", child.Intervention())
	}

	// latestStepNumber reflects progress; resume recovers it.
	last, err := c.LatestStepNumber(ctx, session.ChannelID())
	if err != nil || last == nil {
		t.Fatalf("LatestStepNumber: %v %v", last, err)
	}
	if *last != 2 {
		t.Fatalf("latest step = %d, want 2", *last)
	}
	resumed, err := Resume(ctx, c, session.ChannelID(), ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.StepCount() != 2 {
		t.Fatalf("resumed step count = %d, want 2", resumed.StepCount())
	}
}

func TestApprovalIdempotentLive(t *testing.T) {
	c := forkInterventionLiveClient(t)
	ctx := context.Background()
	name := forkInterventionChan("approval")
	if _, err := c.Record(ctx, name, "seed", map[string]any{}, RecordOptions{Actor: "test"}); err != nil {
		t.Fatal(err)
	}

	requestID, err := c.RequestApproval(ctx, name, RequestApprovalOptions{Summary: "Approve invoice"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.DecideApproval(ctx, name, requestID, DecideApprovalOptions{Decision: "approved", Actor: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := c.DecideApproval(ctx, name, requestID, DecideApprovalOptions{Decision: "approved", Actor: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if retry.ID != first.ID {
		t.Fatalf("idempotent retry id = %s, want %s", retry.ID, first.ID)
	}
	eventType := "approval.decided"
	decided, err := c.Replay(ctx, name, ReplayOptions{EventType: &eventType, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(decided) != 1 {
		t.Fatalf("decided events = %d, want 1", len(decided))
	}
}
