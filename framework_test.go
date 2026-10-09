package actae

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type fakeToolClient struct {
	mu           sync.Mutex
	claim        ExecutionClaim
	completed    int
	failed       int
	heartbeats   int
	cancelled    int
	heartbeatErr error
}

func (f *fakeToolClient) counts() (completed, failed, heartbeats, cancelled int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed, f.failed, f.heartbeats, f.cancelled
}

func (f *fakeToolClient) ClaimExecution(context.Context, string, string, string, any, ClaimExecutionOptions) (ExecutionClaim, error) {
	return f.claim, nil
}
func (f *fakeToolClient) CompleteExecution(context.Context, string, *string, any) (ExecutionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed++
	return ExecutionInfo{}, nil
}
func (f *fakeToolClient) FailExecution(context.Context, string, string, FailExecutionOptions) (ExecutionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed++
	return ExecutionInfo{}, nil
}
func (f *fakeToolClient) HeartbeatExecution(context.Context, string, *string, *int) (ExecutionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats++
	return ExecutionInfo{}, f.heartbeatErr
}
func (f *fakeToolClient) CancelExecution(context.Context, string, *string) (ExecutionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled++
	return ExecutionInfo{}, nil
}

func ownedClaim(status string) ExecutionClaim {
	token := "token-1"
	return ExecutionClaim{Status: status, ClaimToken: &token, Execution: ExecutionInfo{ID: "execution-1"}}
}

func TestFrameworkCapabilitiesAndCheckpointEnvelope(t *testing.T) {
	if err := CopilotCapabilities.Validate(); err != nil {
		t.Fatal(err)
	}
	if CopilotCapabilities.FullParity() {
		t.Fatal("native profile must remain honest")
	}
	if !CommonSurfaceCapabilities(CopilotCapabilities).FullParity() {
		t.Fatal("common surface should expose all features")
	}

	envelope := NewCheckpointEnvelope("github-copilot-sdk", "2", "copilot:s1")
	envelope.PortableState = map[string]any{"messages": 2}
	state, err := EmbedCheckpointMetadata(map[string]any{"messages": []any{"hi"}}, envelope)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := ExtractCheckpointEnvelope(state)
	if err != nil {
		t.Fatal(err)
	}
	if restored == nil || restored.Framework != envelope.Framework {
		t.Fatalf("bad envelope: %#v", restored)
	}
	if _, ok := StripCheckpointMetadata(state)[CheckpointMetadataKey]; ok {
		t.Fatal("metadata was not stripped")
	}
}

func TestCheckpointMatchesCrossSDKFixture(t *testing.T) {
	raw, err := os.ReadFile("../../tests/fixtures/framework_checkpoint_v1.json")
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("monorepo fixture unavailable (public mirror): %v", err)
		}
		t.Fatal(err)
	}
	var expected map[string]any
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	var envelope CheckpointEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if !mapsEqualJSON(actual, expected) {
		t.Fatalf("round-trip mismatch\nactual=%#v\nexpected=%#v", actual, expected)
	}
}

func mapsEqualJSON(left, right map[string]any) bool {
	l, _ := json.Marshal(left)
	r, _ := json.Marshal(right)
	return string(l) == string(r)
}

func TestToolExecutorCompletesReplaysAndHeartbeats(t *testing.T) {
	fake := &fakeToolClient{claim: ownedClaim("claimed")}
	executor := newToolExecutor(fake, "run:test")
	invoked := 0
	result, err := executor.Execute(context.Background(), "step-1", "lookup", map[string]any{}, func(context.Context) (any, error) {
		invoked++
		time.Sleep(20 * time.Millisecond)
		return map[string]any{"ok": true}, nil
	}, ToolExecutionOptions{LeaseSeconds: 1, HeartbeatInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	completed, _, heartbeats, _ := fake.counts()
	if result == nil || invoked != 1 || completed != 1 || heartbeats == 0 {
		t.Fatalf("unexpected result/calls: %#v %+v", result, fake)
	}

	fake.claim = ExecutionClaim{Status: "replayed", Result: map[string]any{"ok": true}, Execution: ExecutionInfo{ID: "execution-1"}}
	_, err = executor.Execute(context.Background(), "step-1", "lookup", nil, func(context.Context) (any, error) { invoked++; return nil, nil }, ToolExecutionOptions{})
	if err != nil || invoked != 1 {
		t.Fatalf("replay invoked tool: %v count=%d", err, invoked)
	}
}

func TestToolExecutorFailureContentionAndCancellation(t *testing.T) {
	fake := &fakeToolClient{claim: ownedClaim("claimed")}
	executor := newToolExecutor(fake, "run:test")
	boom := errors.New("boom")
	_, err := executor.Execute(context.Background(), "fail", "tool", nil, func(context.Context) (any, error) { return nil, boom }, ToolExecutionOptions{})
	if !errors.Is(err, boom) || fake.failed != 1 {
		t.Fatalf("failure not persisted: %v %+v", err, fake)
	}

	fake.claim = ExecutionClaim{Status: "in_progress", Execution: ExecutionInfo{ID: "execution-2"}}
	_, err = executor.Execute(context.Background(), "busy", "tool", nil, func(context.Context) (any, error) { return nil, nil }, ToolExecutionOptions{})
	var busy *ToolExecutionInProgressError
	if !errors.As(err, &busy) {
		t.Fatalf("expected contention error, got %v", err)
	}

	fake.claim = ownedClaim("claimed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.Execute(ctx, "cancel", "tool", nil, func(ctx context.Context) (any, error) { <-ctx.Done(); return nil, ctx.Err() }, ToolExecutionOptions{})
	if !errors.Is(err, context.Canceled) || fake.cancelled != 1 {
		t.Fatalf("cancellation not persisted: %v %+v", err, fake)
	}
}

func TestToolExecutorReturnsOnHeartbeatLeaseLoss(t *testing.T) {
	fake := &fakeToolClient{claim: ownedClaim("claimed"), heartbeatErr: errors.New("lease lost")}
	executor := newToolExecutor(fake, "run:test")
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), "lease-1", "slow", nil, func(context.Context) (any, error) {
			<-release // deliberately ignores cancellation; Actae must still return.
			return "late", nil
		}, ToolExecutionOptions{LeaseSeconds: 1, HeartbeatInterval: time.Millisecond})
		done <- err
	}()
	select {
	case err := <-done:
		fake.mu.Lock()
		failed, completed := fake.failed, fake.completed
		fake.mu.Unlock()
		if err == nil || err.Error() != "lease lost" || failed != 1 || completed != 0 {
			t.Fatalf("lease loss was not terminal: err=%v calls=%+v", err, fake)
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss did not stop waiting for the tool")
	}
	close(release)
}
