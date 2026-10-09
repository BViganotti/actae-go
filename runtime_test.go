package actae

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func runtimeRecordResponse(w http.ResponseWriter) {
	_, _ = w.Write([]byte(`{"event":{"id":"evt-1","channel_id":"runtime","type":"framework.run","payload":{},"actor":"runtime","cursor":1,"timestamp":"2026-08-27T00:00:00Z"}}`))
}

func TestChannelForRunStableReadableAndCollisionSafe(t *testing.T) {
	got, err := ChannelForRun("ticket-42", "langgraph")
	if err != nil || got != "langgraph:ticket-42" {
		t.Fatalf("readable channel: %q %v", got, err)
	}
	first, _ := ChannelForRun("tenant/a workflow", "temporal")
	again, _ := ChannelForRun("tenant/a workflow", "temporal")
	other, _ := ChannelForRun("tenant:a workflow", "temporal")
	if first != again || first == other || !strings.HasPrefix(first, "temporal:tenant-a-workflow:") {
		t.Fatalf("unstable or colliding channels: %q %q %q", first, again, other)
	}
	long, err := ChannelForRun(strings.Repeat("x", 1000), "run")
	if err != nil || len(long) > 256 {
		t.Fatalf("long channel: %d %v", len(long), err)
	}
}

func TestRuntimeOperationKeyCrossSDKVector(t *testing.T) {
	got := DeterministicOperationKey("agent-session", "tool", "channel-1", "7", "payload")
	if got != "f059dcde-a40f-5156-9ba8-ad9246cf929d" {
		t.Fatalf("cross-SDK vector drift: %s", got)
	}
}

func TestRuntimeActivityRestoresCarrierLineageAndAttempt(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, _ *http.Request, _ map[string]any) {
			runtimeRecordResponse(w)
		},
	})
	runtime, err := NewRuntime(newTestClient(t, ts))
	if err != nil {
		t.Fatal(err)
	}
	attempt := 2
	temporal, err := runtime.Orchestrator("temporal")
	if err != nil {
		t.Fatal(err)
	}
	carrier, err := temporal.Carrier("wf/42")
	if err != nil {
		t.Fatal(err)
	}
	err = temporal.Activity(context.Background(), carrier, RunOptions{ID: "agent-0", Attempt: &attempt}, func(ctx context.Context, activity Scope) error {
		current, ok := runtime.Current(ctx)
		if !ok || current.RunID() != "agent-0" || activity.WorkflowID != "wf/42" {
			t.Fatalf("workflow context missing: %#v", current)
		}
		return runtime.Run(ctx, activity.ChildOptions("agent-1"), func(childCtx context.Context, child Scope) error {
			if child.ParentRunID() != "agent-0" {
				t.Fatalf("child parent: %q", child.ParentRunID())
			}
			if got, ok := runtime.Current(childCtx); !ok || got.RunID() != "agent-1" {
				t.Fatal("child context missing")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	reqs := ts.requests()
	if len(reqs) != 4 {
		t.Fatalf("lifecycle request count: %d", len(reqs))
	}
	firstPayload := reqs[0].body["payload"].(map[string]any)
	data := firstPayload["data"].(map[string]any)
	if data["attempt"] != float64(2) || reqs[0].body["operation_id"] == "" {
		t.Fatalf("attempt/idempotency missing: %#v", reqs[0].body)
	}
}

func TestRunCarrierFixtureRejectsCredentialFields(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "tests", "fixtures", "run_carrier_v1.json"))
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("monorepo fixture unavailable (public mirror): %v", err)
		}
		t.Fatal(err)
	}
	carrier, err := ParseRunCarrier(fixture)
	if err != nil || carrier.WorkflowID != "workflow-42" {
		t.Fatalf("fixture carrier: %#v %v", carrier, err)
	}
	if _, err := ParseRunCarrier([]byte(`{"schema":"actae.run-carrier/v1","channel_id":"temporal:workflow-42","run_id":"workflow-42","framework":"temporal","workflow_id":"workflow-42","api_key":"never-accepted"}`)); err == nil {
		t.Fatal("carrier accepted an unknown credential field")
	}
}

func TestRuntimePreservesApplicationFailure(t *testing.T) {
	var writes atomic.Int32
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, _ *http.Request, _ map[string]any) {
			if writes.Add(1) == 1 {
				runtimeRecordResponse(w)
				return
			}
			http.Error(w, `{"error":"storage down"}`, http.StatusInternalServerError)
		},
	})
	runtime, _ := NewRuntime(newTestClient(t, ts))
	applicationErr := errors.New("application failed")
	err := runtime.Run(context.Background(), RunOptions{ID: "r1"}, func(context.Context, Scope) error {
		return applicationErr
	})
	if !errors.Is(err, applicationErr) {
		t.Fatalf("application error was masked: %v", err)
	}
}

func TestRuntimeRecordsCancellationDistinctly(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, _ *http.Request, _ map[string]any) {
			runtimeRecordResponse(w)
		},
	})
	runtime, _ := NewRuntime(newTestClient(t, ts))
	ctx, cancel := context.WithCancel(context.Background())
	err := runtime.Run(ctx, RunOptions{ID: "r1"}, func(context.Context, Scope) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	reqs := ts.requests()
	if len(reqs) != 2 || reqs[0].body["event_type"] != "framework.run.started" || reqs[1].body["event_type"] != "framework.run.cancelled" {
		t.Fatalf("unexpected cancellation lifecycle: %#v", reqs)
	}
}

func TestRuntimeEffectUsesAmbientChannelAndReplays(t *testing.T) {
	var claims atomic.Int32
	execJSON := `{"id":"ex1","channel_id":"custom:checkout-1","key_name":"order:o-1:charge","tool_name":"charge","status":"claimed","attempts":1,"params":{"order_id":"o-1"},"created_at":"t","updated_at":"t"}`
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, _ *http.Request, _ map[string]any) { runtimeRecordResponse(w) },
		"POST /api/v1/executions/claim": func(w http.ResponseWriter, _ *http.Request, body map[string]any) {
			if body["channel_id"] != "custom:checkout-1" || body["key_name"] != "order:o-1:charge" {
				t.Errorf("bad effect identity: %#v", body)
			}
			if claims.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"status":"claimed","claim_token":"ct-1","execution":` + execJSON + `}`))
			} else {
				_, _ = w.Write([]byte(`{"status":"replayed","result":{"receipt":"r-1"},"execution":` + strings.Replace(execJSON, `"status":"claimed"`, `"status":"completed"`, 1) + `}`))
			}
		},
		"POST /api/v1/executions/ex1/complete": func(w http.ResponseWriter, _ *http.Request, _ map[string]any) {
			_, _ = w.Write([]byte(`{"execution":` + strings.Replace(execJSON, `"status":"claimed"`, `"status":"completed"`, 1) + `,"result":{"receipt":"r-1"}}`))
		},
	})
	runtime, _ := NewRuntime(newTestClient(t, ts))
	invoked := 0
	err := runtime.Run(context.Background(), RunOptions{ID: "checkout-1"}, func(ctx context.Context, _ Scope) error {
		invoke := func(context.Context) (any, error) {
			invoked++
			return map[string]any{"receipt": "r-1"}, nil
		}
		for i := 0; i < 2; i++ {
			result, effectErr := runtime.Effect(ctx, "order:o-1:charge", "charge", map[string]any{"order_id": "o-1"}, invoke, ToolExecutionOptions{})
			if effectErr != nil {
				return effectErr
			}
			if _, err := json.Marshal(result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || invoked != 1 {
		t.Fatalf("effect replay failed: invoked=%d err=%v", invoked, err)
	}
}
