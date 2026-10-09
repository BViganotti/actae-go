package actae

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeEvent is a stored event in the in-memory fake Actae.
type fakeEvent struct {
	ID        string         `json:"id"`
	ChannelID string         `json:"channel_id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Actor     string         `json:"actor"`
	Cursor    int64          `json:"cursor"`
	Timestamp string         `json:"timestamp"`
	Metadata  map[string]any `json:"metadata"`
}

// fakeState is a stored state snapshot.
type fakeState struct {
	Version   int64          `json:"version"`
	Cursor    int64          `json:"cursor"`
	State     map[string]any `json:"state"`
	Timestamp string         `json:"timestamp"`
}

// fakeActaeServer is a stateful in-memory Actae for session/manager tests.
type fakeActaeServer struct {
	t   *testing.T
	srv *httptest.Server

	mu           sync.Mutex
	events       map[string][]fakeEvent
	metadata     map[string]map[string]any
	states       map[string][]fakeState
	operationIDs map[string][]string
	toolPolicies map[string]map[string]any
	nextCursor   int64
	nextVersion  int64
	// boundaryFails makes fork with at_cursor>0 return 409
	// snapshot_boundary_required (for boundary-fallback tests).
	boundaryFails bool
	// permanentFail makes every request to "METHOD path" return 400 forever
	// (for non-retryable error propagation tests).
	permanentFail map[string]bool
	// failures counts failing attempts per route key ("METHOD path") before
	// succeeding; used to test retry logic.
	failures map[string]int
}

func newFakeActae(t *testing.T) *fakeActaeServer {
	f := &fakeActaeServer{
		t:             t,
		events:        map[string][]fakeEvent{},
		metadata:      map[string]map[string]any{},
		states:        map[string][]fakeState{},
		operationIDs:  map[string][]string{},
		toolPolicies:  map[string]map[string]any{},
		failures:      map[string]int{},
		permanentFail: map[string]bool{},
		nextVersion:   1,
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeActaeServer) url() string { return f.srv.URL }

// failNext makes the next n requests to "METHOD path" return 503.
func (f *fakeActaeServer) failNext(route string, n int) {
	f.mu.Lock()
	f.failures[route] = n
	f.mu.Unlock()
}

// maybeFail consumes one failure token for the route and writes a 503 when
// one is pending. Caller must hold f.mu.
func (f *fakeActaeServer) maybeFail(route string, w http.ResponseWriter) bool {
	if f.permanentFail[route] {
		w.WriteHeader(400)
		w.Write([]byte(`{"error": "permanent"}`))
		return true
	}
	n := f.failures[route]
	if n > 0 {
		f.failures[route] = n - 1
	}
	if n > 0 {
		w.WriteHeader(503)
		w.Write([]byte(`{"error": "temporary"}`))
		return true
	}
	return false
}

func (f *fakeActaeServer) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handleUnlocked(w, r)
}

func (f *fakeActaeServer) handleUnlocked(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	route := r.Method + " " + r.URL.Path
	if f.maybeFail(route, w) {
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/events/record":
		var body struct {
			ChannelID   string         `json:"channel_id"`
			EventType   string         `json:"event_type"`
			Payload     map[string]any `json:"payload"`
			Metadata    map[string]any `json:"metadata"`
			OperationID *string        `json:"operation_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.nextCursor++
		ev := fakeEvent{
			ID:        fmt.Sprintf("evt-%d", f.nextCursor),
			ChannelID: body.ChannelID,
			Type:      body.EventType,
			Payload:   body.Payload,
			Actor:     body.Metadata["actor"].(string),
			Cursor:    f.nextCursor,
			Timestamp: "2026-08-03T00:00:00Z",
			Metadata:  body.Metadata["metadata"].(map[string]any),
		}
		f.events[body.ChannelID] = append(f.events[body.ChannelID], ev)
		if body.OperationID != nil {
			f.operationIDs[body.ChannelID] = append(f.operationIDs[body.ChannelID], *body.OperationID)
		}
		w.Write([]byte(`{"event": ` + mustJSON(ev) + `}`))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/events/replay/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/events/replay/")
		out := f.events[channel]
		if out == nil {
			out = []fakeEvent{}
		}
		// Mirror the real server: cursor is exclusive and results are
		// capped at 1000 per page.
		if raw := r.URL.Query().Get("cursor"); raw != "" {
			if after, err := strconv.ParseInt(raw, 10, 64); err == nil {
				filtered := []fakeEvent{}
				for _, ev := range out {
					if ev.Cursor > after {
						filtered = append(filtered, ev)
					}
				}
				out = filtered
			}
		}
		if len(out) > 1000 {
			out = out[:1000]
		}
		json.NewEncoder(w).Encode(map[string]any{"events": out})

	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/steps"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/channels/"), "/steps")
		var last int64
		for _, ev := range f.events[channel] {
			if ev.Payload == nil {
				continue
			}
			if sn, ok := anyInt64(ev.Payload["step_number"]); ok && sn > last {
				last = sn
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"channel_id": channel, "last_step_number": last})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/events/cursor/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/events/cursor/")
		evs := f.events[channel]
		var cursor int64
		if len(evs) > 0 {
			cursor = evs[len(evs)-1].Cursor
		}
		w.Write([]byte(fmt.Sprintf(`{"latest_cursor": %d}`, cursor)))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/channels/") && strings.HasSuffix(r.URL.Path, "/metadata"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/channels/"), "/metadata")
		meta, ok := f.metadata[channel]
		if !ok {
			w.WriteHeader(404)
			w.Write([]byte(`{"error": "not_found"}`))
			return
		}
		// parent_channel_id is a top-level field (mirrors the real server).
		parent := ""
		if p, ok := meta["parent_channel_id"].(string); ok {
			parent = p
		}
		w.Write([]byte(`{"channel_id": "` + channel + `", "parent_channel_id": ` + mustJSON(parent) + `, "display_name": "` + channel + `", "created_at": "t", "experiment_metadata": ` + mustJSON(meta) + `}`))

	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/metadata"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/channels/"), "/metadata")
		var body struct {
			DisplayName        *string        `json:"display_name"`
			ExperimentMetadata map[string]any `json:"experiment_metadata"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		meta := f.metadata[channel]
		if meta == nil {
			meta = map[string]any{}
			f.metadata[channel] = meta
		}
		if body.DisplayName != nil {
			meta["display_name"] = *body.DisplayName
		}
		for k, v := range body.ExperimentMetadata {
			meta[k] = v
		}
		w.Write([]byte(`{"channel_id": "` + channel + `", "created_at": "t", "experiment_metadata": ` + mustJSON(meta) + `}`))

	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/channels/fork":
		var body struct {
			SourceChannelID    string         `json:"source_channel_id"`
			NewChannelID       string         `json:"new_channel_id"`
			AtCursor           int64          `json:"at_cursor"`
			OperationID        string         `json:"operation_id"`
			DisplayName        string         `json:"display_name"`
			Reason             string         `json:"reason"`
			ExperimentMetadata map[string]any `json:"experiment_metadata"`
			ToolPolicies       map[string]any `json:"tool_policies"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if len(body.ToolPolicies) > 0 {
			f.toolPolicies[body.NewChannelID] = body.ToolPolicies
		}
		if f.boundaryFails && body.AtCursor > 0 {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "snapshot_boundary_required", "error": "no saved state boundary"}`))
			return
		}
		if f.metadata[body.NewChannelID] != nil {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "version_conflict", "error": "already exists"}`))
			return
		}
		f.metadata[body.NewChannelID] = body.ExperimentMetadata
		if body.ExperimentMetadata == nil {
			f.metadata[body.NewChannelID] = map[string]any{}
		}
		// Mirror the real server: the fork child inherits the nearest saved
		// state snapshot AT-OR-BEFORE the requested cursor (never a later
		// one), and reports the resolved boundary in the receipt.
		resolvedCursor := int64(0)
		if parentStates := f.states[body.SourceChannelID]; len(parentStates) > 0 {
			var pick *fakeState
			for i := range parentStates {
				st := &parentStates[i]
				if body.AtCursor <= 0 || st.Cursor <= body.AtCursor {
					pick = st
				} else {
					break
				}
			}
			if pick == nil {
				pick = &parentStates[0]
			}
			f.nextVersion++
			f.states[body.NewChannelID] = append(f.states[body.NewChannelID], fakeState{
				Version:   f.nextVersion,
				Cursor:    pick.Cursor,
				State:     pick.State,
				Timestamp: pick.Timestamp,
			})
			resolvedCursor = pick.Cursor
		}
		policiesJSON := "null"
		if len(body.ToolPolicies) > 0 {
			if encoded, err := json.Marshal(body.ToolPolicies); err == nil {
				policiesJSON = string(encoded)
			}
		}
		receipt := fmt.Sprintf(
			`{"fork_id": %q, "source_channel_id": %q, "child_channel_id": %q, "requested_cursor": %d, "resolved_cursor": %d, "resolved_event_id": null, "source_state_version": 1, "source_state_sha256": "fake", "restorable": %t, "replayed": false, "manifest": null, "reproducibility": "state_exact", "tool_policies": %s}`,
			body.NewChannelID, body.SourceChannelID, body.NewChannelID, body.AtCursor, resolvedCursor, resolvedCursor > 0, policiesJSON,
		)
		w.Write([]byte(receipt))

	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/state/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/state/")
		if channel == "" {
			http.Error(w, "bad", 400)
			return
		}
		var body struct {
			Cursor int64          `json:"cursor"`
			State  map[string]any `json:"state"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		st := fakeState{Version: f.nextVersion, Cursor: body.Cursor, State: body.State, Timestamp: "t"}
		f.nextVersion++
		f.states[channel] = append(f.states[channel], st)
		w.Write([]byte(fmt.Sprintf(`{"version": %d}`, st.Version)))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/state/") && strings.Contains(r.URL.Path, "/versions"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/state/"), "/versions")
		out := []map[string]any{}
		for _, st := range f.states[channel] {
			out = append(out, map[string]any{"version": st.Version, "cursor": st.Cursor, "timestamp": st.Timestamp})
		}
		json.NewEncoder(w).Encode(map[string]any{"versions": out})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/state/") && strings.Contains(r.URL.Path, "/version/"):
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/state/")
		parts := strings.Split(rest, "/version/")
		channel, verStr := parts[0], parts[1]
		var version int64
		fmt.Sscanf(verStr, "%d", &version)
		for _, st := range f.states[channel] {
			if st.Version == version {
				json.NewEncoder(w).Encode(st)
				return
			}
		}
		w.WriteHeader(404)
		w.Write([]byte(`{"error": "not_found"}`))

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/state/") && strings.Contains(r.URL.Path, "/version/"):
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/state/")
		parts := strings.Split(rest, "/version/")
		channel, verStr := parts[0], parts[1]
		var version int64
		fmt.Sscanf(verStr, "%d", &version)
		for i, st := range f.states[channel] {
			if st.Version == version {
				f.states[channel] = append(f.states[channel][:i], f.states[channel][i+1:]...)
				w.Write([]byte(`{"deleted": true}`))
				return
			}
		}
		w.WriteHeader(404)
		w.Write([]byte(`{"error": "not_found"}`))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/state/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/state/")
		sts := f.states[channel]
		if len(sts) == 0 {
			w.Write([]byte(`{"message": "no state yet"}`))
			return
		}
		st := sts[len(sts)-1]
		json.NewEncoder(w).Encode(st)

	default:
		http.Error(w, `{"error": "not found"}`, 404)
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func sessionTestClient(t *testing.T, f *fakeActaeServer) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{APIKey: "sk-test", Endpoint: f.url()})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSessionLifecycle(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, err := NewAgentSession(c, "exp-v1", AgentSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Status() != SessionStatusCreated {
		t.Fatalf("status = %q", s.Status())
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Status() != SessionStatusStarted {
		t.Fatalf("status = %q", s.Status())
	}
	if s.ChannelID() != "exp-v1" {
		t.Fatalf("channel = %q", s.ChannelID())
	}

	ev, err := s.Step(ctx, "inference", StepOptions{Input: "q", Output: "a", Metadata: map[string]any{"model": "gpt"}})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Cursor != 2 { // session.started + step
		t.Errorf("step cursor = %d", ev.Cursor)
	}
	if s.StepCount() != 1 {
		t.Errorf("step count = %d", s.StepCount())
	}
	if s.Status() != SessionStatusStepping {
		t.Errorf("status after step = %q", s.Status())
	}
	if cursors := s.Cursors(); len(cursors) != 1 || cursors[0] != 2 {
		t.Errorf("cursors = %v", cursors)
	}

	// event payload recorded
	f.mu.Lock()
	evs := f.events["exp-v1"]
	f.mu.Unlock()
	if len(evs) != 2 || evs[1].Type != "inference" {
		t.Fatalf("events = %v", evs)
	}
	meta := evs[1].Metadata
	if meta["step_number"] != float64(1) {
		t.Errorf("step_number metadata = %v", meta["step_number"])
	}

	if err := s.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Status() != SessionStatusCompleted {
		t.Fatalf("status = %q", s.Status())
	}
	f.mu.Lock()
	evs = f.events["exp-v1"]
	f.mu.Unlock()
	if evs[len(evs)-1].Type != SessionCompleted {
		t.Errorf("last event = %s", evs[len(evs)-1].Type)
	}

	// stepping on completed session fails
	if _, err := s.Step(ctx, "x", StepOptions{}); err == nil {
		t.Error("step on completed session should fail")
	}
	// complete twice is a no-op
	if err := s.Complete(ctx); err != nil {
		t.Errorf("second complete: %v", err)
	}
}

func TestSessionStepBeforeStart(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	s, err := NewAgentSession(c, "noop", AgentSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(context.Background(), "x", StepOptions{}); err == nil {
		t.Error("step before start should fail")
	}
}

func TestSessionSnapshots(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	snapshots := []map[string]any{}
	s, err := NewAgentSession(c, "snap", AgentSessionOptions{
		StateFn: func() map[string]any {
			if len(snapshots) == 0 {
				return nil
			}
			return map[string]any{"step": snapshots[0]["step"]}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshots = append(snapshots, map[string]any{"step": 1})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "s", StepOptions{}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	sts := f.states["snap"]
	f.mu.Unlock()
	if len(sts) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(sts))
	}
}

func TestSessionFork(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "src", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}

	fork, err := s.Fork(ctx, 2, "fork-2", ForkSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if fork.ChannelID() != "fork-2" {
		t.Fatalf("fork channel = %q", fork.ChannelID())
	}
	f.mu.Lock()
	meta := f.metadata["fork-2"]
	f.mu.Unlock()
	if meta["forked_from"] != "src" {
		t.Errorf("forked_from = %v", meta["forked_from"])
	}
	// cursor of step 2: session.started(1) + step1(2) + step2(3)
	if meta["forked_at_cursor"] != float64(3) {
		t.Errorf("forked_at_cursor = %v, want 3", meta["forked_at_cursor"])
	}
	if meta["status"] != SessionStatusCreated {
		t.Errorf("fork status = %v", meta["status"])
	}
}

func TestResumeRecoversProgressFromEventLog(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "hard-kill", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a hard process death: metadata never learned about steps 2..3.
	f.mu.Lock()
	f.metadata["hard-kill"]["step_count"] = float64(1)
	f.metadata["hard-kill"]["cursors"] = []any{float64(2)}
	f.mu.Unlock()

	resumed, err := Resume(ctx, c, "hard-kill", ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.StepCount() != 3 {
		t.Fatalf("StepCount = %d, want 3 (recovered from the event log)", resumed.StepCount())
	}
	if len(resumed.Cursors()) != 3 {
		t.Fatalf("cursors = %v, want 3 entries", resumed.Cursors())
	}
}

func TestSessionForkInterventionAndToolPolicies(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "int-src", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}

	intervention := map[string]any{"model": "candidate", "temperature": 0.2}
	policies := map[string]any{"email.send": "block", "*": "auto"}
	fork, err := s.Fork(ctx, 2, "int-fork", ForkSessionOptions{
		Intervention: intervention,
		ToolPolicies: policies,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := fork.Intervention(); got["model"] != "candidate" || got["temperature"] != 0.2 {
		t.Errorf("Intervention() = %v", got)
	}
	gotPolicies := fork.ToolPolicies()
	if gotPolicies["email.send"] != "block" || gotPolicies["*"] != "auto" {
		t.Errorf("ToolPolicies() = %v", gotPolicies)
	}
	// Intervention is recorded on the fork metadata; tool_policies reaches the
	// server fork request (both are what the real server persists).
	f.mu.Lock()
	storedMeta := f.metadata["int-fork"]["intervention"]
	storedPolicies := f.toolPolicies["int-fork"]
	f.mu.Unlock()
	if storedMeta == nil {
		t.Fatalf("intervention not recorded on fork metadata: %v", f.metadata["int-fork"])
	}
	if storedPolicies["email.send"] != "block" {
		t.Errorf("tool_policies not sent to server: %v", storedPolicies)
	}

	// resume(fork_at_step=) forwards intervention + policies too.
	resumed, err := Resume(ctx, c, "int-src", ResumeOptions{
		ForkAtStep:   intPtr(1),
		Name:         "int-resume",
		Intervention: map[string]any{"model": "resumed"},
		ToolPolicies: map[string]any{"email.send": "replay"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Intervention()["model"] != "resumed" {
		t.Errorf("resume intervention = %v", resumed.Intervention())
	}
	if resumed.ToolPolicies()["email.send"] != "replay" {
		t.Errorf("resume tool policies = %v", resumed.ToolPolicies())
	}
}

func intPtr(v int) *int { return &v }

func TestSessionForkContinuesAtStepAndInheritsState(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Parent runs steps 1-5, saving a state snapshot at each step.
	s, _ := NewAgentSession(c, "parent", AgentSessionOptions{
		StateFn: func() map[string]any {
			return map[string]any{"step": "live"}
		},
	})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}
	// Ensure the parent has a saved snapshot for the fork to inherit.
	if _, err := c.SaveState(ctx, "parent", 5, map[string]any{
		"step": 5, "accumulator": "steps 1-5",
	}, SaveStateOptions{}); err != nil {
		t.Fatal(err)
	}

	fork, err := s.Fork(ctx, 5, "fork-5", ForkSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The fork continues at step 6, not step 1.
	if fork.StepCount() != 5 {
		t.Errorf("fork StepCount = %d, want 5 (continue at step 6)", fork.StepCount())
	}
	// It inherited the parent's state at the fork boundary.
	inherited := fork.InheritedState()
	if inherited == nil || inherited["accumulator"] != "steps 1-5" {
		t.Errorf("fork inherited state = %v, want steps 1-5 data", inherited)
	}

	// Start the fork and step; the next recorded step is step 6.
	if err := fork.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fork.Step(ctx, "step6", StepOptions{Input: 6}); err != nil {
		t.Fatal(err)
	}
	if fork.StepCount() != 6 {
		t.Errorf("fork StepCount after step6 = %d, want 6", fork.StepCount())
	}
}

func TestSessionResumeForkInheritsState(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// A completed source session with metadata cursors + a saved state.
	f.mu.Lock()
	f.metadata["source"] = map[string]any{
		"session_name": "source",
		"params":       map[string]any{},
		"status":       SessionStatusCompleted,
		"cursors":      []any{float64(2), float64(4), float64(6)},
	}
	f.states["source"] = []fakeState{
		{Version: 1, Cursor: 4, State: map[string]any{"n": 42, "done": true}, Timestamp: "t"},
	}
	f.mu.Unlock()

	step := 2
	opts := ResumeOptions{ForkAtStep: &step, Name: "fork-2", StateFn: func() map[string]any {
		return map[string]any{"live": true}
	}}
	fork, err := Resume(ctx, c, "source", opts)
	if err != nil {
		t.Fatal(err)
	}
	if fork.StepCount() != 2 {
		t.Errorf("resumed fork StepCount = %d, want 2", fork.StepCount())
	}
	inh := fork.InheritedState()
	if inh == nil {
		t.Fatalf("resumed fork inherited state = nil, want state data")
	}
	// JSON-decoded numbers arrive as int64 in Go.
	if got := inh["n"]; got != float64(42) && got != int(42) && got != int64(42) {
		t.Errorf("resumed fork inherited n = %v (%T), want 42", got, got)
	}
}

func TestForkStateFnNotWrapped(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// The caller's StateFn returns the evolving live state.
	live := map[string]any{"prefix": true}
	s, _ := NewAgentSession(c, "parent", AgentSessionOptions{
		StateFn: func() map[string]any { return live },
	})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		live["step"] = i
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}

	fork, err := s.Fork(ctx, 5, "fork-5", ForkSessionOptions{StateFn: func() map[string]any { return live }})
	if err != nil {
		t.Fatal(err)
	}
	if fork.InheritedState() == nil {
		t.Fatal("fork should inherit the parent snapshot")
	}

	// The fork continues with the caller's live state evolving (its own work).
	if err := fork.Start(ctx); err != nil {
		t.Fatal(err)
	}
	live["prefix"] = false
	live["refined"] = true
	if _, err := fork.Step(ctx, "step6", StepOptions{Input: 6}); err != nil {
		t.Fatal(err)
	}

	// The fork's saved snapshot must reflect the fork's OWN evolving live
	// state — the StateFn is intentionally NOT wrapped (Python parity). A
	// frozen inherited-prefix closure would persist only the parent's data.
	f.mu.Lock()
	defer f.mu.Unlock()
	sts := f.states["fork-5"]
	if len(sts) == 0 {
		t.Fatal("fork saved no state snapshot")
	}
	last := sts[len(sts)-1].State
	if last["refined"] != true {
		t.Errorf("fork snapshot does not reflect the fork's own steps: %v", last)
	}
}

func TestSessionForkBoundaryStrictRaises(t *testing.T) {
	f := newFakeActae(t)
	f.mu.Lock()
	f.boundaryFails = true
	f.mu.Unlock()

	c := sessionTestClient(t, f)
	ctx := context.Background()
	s, _ := NewAgentSession(c, "src", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "step", StepOptions{}); err != nil {
		t.Fatal(err)
	}
	// Strict by default: a missing snapshot at the boundary must raise
	// NoRestorableCheckpointError instead of silently falling forward.
	_, err := s.Fork(ctx, 1, "fork-1", ForkSessionOptions{})
	if _, ok := err.(*NoRestorableCheckpointError); !ok {
		t.Fatalf("expected NoRestorableCheckpointError, got %v", err)
	}
	f.mu.Lock()
	_, exists := f.metadata["fork-1"]
	f.mu.Unlock()
	if exists {
		t.Error("no fork channel should have been created in strict mode")
	}
}

func TestSessionForkBoundaryApproximateFallsBack(t *testing.T) {
	f := newFakeActae(t)
	f.mu.Lock()
	f.boundaryFails = true
	f.mu.Unlock()

	c := sessionTestClient(t, f)
	ctx := context.Background()
	s, _ := NewAgentSession(c, "src", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "step", StepOptions{}); err != nil {
		t.Fatal(err)
	}
	// Explicit approximate mode falls back to the latest state once.
	fs, err := s.Fork(ctx, 1, "fork-1", ForkSessionOptions{BoundaryMode: "approximate"})
	if err != nil {
		t.Fatalf("fork with approximate fallback: %v", err)
	}
	if fs.RequestedBoundaryCursor() != 2 {
		t.Errorf("requested boundary = %d, want 2 (step 1's cursor)", fs.RequestedBoundaryCursor())
	}
	// The event-only parent has no state: the fallback fork is lineage-only
	// (not restorable) and must be reported as such.
	if r := fs.BoundaryRestorable(); r == nil || *r {
		t.Errorf("boundary restorable = %v, want false (no snapshot on parent)", r)
	}
	if fs.ForkReceipt() == nil {
		t.Error("fork receipt should be recorded on the session")
	}
	f.mu.Lock()
	meta := f.metadata["fork-1"]
	f.mu.Unlock()
	if meta == nil || meta["forked_at_step"] != float64(1) {
		t.Errorf("fork metadata = %v", meta)
	}
}

func TestSessionResumeCrashRecovery(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "crashed", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "step", StepOptions{Input: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(ctx, "boom"); err != nil {
		t.Fatal(err)
	}

	resumed, err := Resume(ctx, c, "crashed", ResumeOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status() != SessionStatusStarted {
		t.Errorf("status = %q", resumed.Status())
	}
	if resumed.StepCount() != 1 {
		t.Errorf("step count = %d", resumed.StepCount())
	}
	if cursors := resumed.Cursors(); len(cursors) != 1 {
		t.Errorf("cursors = %v", cursors)
	}
	// can continue stepping
	if _, err := resumed.Step(ctx, "step", StepOptions{Input: 2}); err != nil {
		t.Fatalf("step after resume: %v", err)
	}
	if resumed.StepCount() != 2 {
		t.Errorf("step count after resume = %d", resumed.StepCount())
	}
}

func TestSessionResumeMethod(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "method-resume", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "step", StepOptions{Input: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(ctx, "boom"); err != nil {
		t.Fatal(err)
	}

	// The method form must behave exactly like the package-level Resume.
	resumed, err := s.Resume(ctx, ResumeOptions{})
	if err != nil {
		t.Fatalf("s.Resume: %v", err)
	}
	if resumed.ChannelID() != "method-resume" || resumed.StepCount() != 1 {
		t.Errorf("bad resume: channel=%q steps=%d", resumed.ChannelID(), resumed.StepCount())
	}

	// Resuming an unstarted session has no channel to resolve.
	unstarted, _ := NewAgentSession(c, "never-started", AgentSessionOptions{})
	if _, err := unstarted.Resume(ctx, ResumeOptions{}); err == nil {
		t.Error("expected error when resuming an unstarted session")
	}
}

func TestSessionResumeCompletedRaises(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "done", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Resume(ctx, c, "done", ResumeOptions{}); err == nil {
		t.Fatal("resume of completed session should fail")
	} else {
		var completedErr *SessionCompletedError
		if !asSessionCompleted(err, &completedErr) {
			t.Errorf("got %T", err)
		}
	}
}

func asSessionCompleted(err error, out **SessionCompletedError) bool {
	if e, ok := err.(*SessionCompletedError); ok {
		*out = e
		return true
	}
	return false
}

func TestResumeReplayFallbackPaginates(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// A channel with 1500 step events preceded by the server's
	// fork.started lineage marker, and NO metadata cursors (as if it
	// were created outside AgentSession): step resolution must page
	// through the replay fallback (server cap is 1000 per call) and
	// exclude the system marker from position mapping.
	f.mu.Lock()
	f.events["big"] = []fakeEvent{{
		ID: "marker", ChannelID: "big", Type: "fork.started",
		Payload: map[string]any{}, Actor: "system", Cursor: 1, Timestamp: "t",
		Metadata: map[string]any{},
	}}
	for i := 0; i < 1500; i++ {
		f.events["big"] = append(f.events["big"], fakeEvent{
			ID:        fmt.Sprintf("evt-%d", i+1),
			ChannelID: "big",
			Type:      "step",
			Payload:   map[string]any{},
			Actor:     "x",
			Cursor:    int64(i + 2),
			Timestamp: "t",
			Metadata:  map[string]any{"step_number": float64(i + 1)},
		})
	}
	// Channel metadata exists but carries NO cursors list, so step→cursor
	// resolution must use the replay fallback (like a channel created
	// outside AgentSession).
	f.metadata["big"] = map[string]any{"status": "started", "step_count": float64(1500)}
	f.mu.Unlock()

	// fork-from-existing at the very last step (1500 > 1000) exercises the
	// second page.
	step := 1500
	sess, err := Resume(ctx, c, "big", ResumeOptions{ForkAtStep: &step, Name: "big-fork"})
	if err != nil {
		t.Fatalf("Resume (fork at step 1500): %v", err)
	}
	if sess.ChannelID() != "big-fork" {
		t.Errorf("channel = %q", sess.ChannelID())
	}

	// The fork must have happened at the cursor of step 1500 — the
	// fork.started marker must NOT shift positions (1500 steps over
	// cursors 2..1501 ⇒ step 1500 = cursor 1501).
	f.mu.Lock()
	meta := f.metadata["big-fork"]
	f.mu.Unlock()
	if meta == nil {
		t.Fatal("fork metadata missing")
	}
	if meta["forked_at_cursor"] != float64(1501) {
		t.Errorf("forked_at_cursor = %v, want 1501 (fork.started excluded)", meta["forked_at_cursor"])
	}

	// A step beyond the last event still errors cleanly.
	beyond := 1501
	if _, err := Resume(ctx, c, "big", ResumeOptions{ForkAtStep: &beyond, Name: "big-fork-2"}); err == nil {
		t.Error("expected error for step beyond the last event")
	}
}

func TestResumeForkExcludesSessionLifecycleMarkers(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// A real AgentSession channel: session.started, 9 user steps, then
	// session.completed — NO metadata cursors (forces the replay fallback).
	f.mu.Lock()
	f.events["sess"] = []fakeEvent{
		{ID: "s0", ChannelID: "sess", Type: "session.started", Payload: map[string]any{}, Actor: "system", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
	}
	for i := 0; i < 9; i++ {
		f.events["sess"] = append(f.events["sess"], fakeEvent{
			ID:        fmt.Sprintf("e%d", i+1),
			ChannelID: "sess",
			Type:      "step",
			Payload:   map[string]any{},
			Actor:     "x",
			Cursor:    int64(i + 2),
			Timestamp: "t",
			Metadata:  map[string]any{"step_number": float64(i + 1)},
		})
	}
	f.events["sess"] = append(f.events["sess"], fakeEvent{
		ID: "sx", ChannelID: "sess", Type: "session.completed",
		Payload: map[string]any{}, Actor: "system", Cursor: 11, Timestamp: "t", Metadata: map[string]any{},
	})
	// Metadata exists (a real AgentSession channel) but carries NO cursors,
	// so step→cursor resolution must use the replay fallback.
	f.metadata["sess"] = map[string]any{"status": "started", "step_count": float64(9)}
	f.mu.Unlock()

	// Fork at the LAST user step (9): must resolve to step 9's cursor (10),
	// NOT be shifted by session.started or session.completed.
	step := 9
	if _, err := Resume(ctx, c, "sess", ResumeOptions{ForkAtStep: &step, Name: "sess-fork"}); err != nil {
		t.Fatalf("Resume (fork at step 9): %v", err)
	}
	f.mu.Lock()
	meta := f.metadata["sess-fork"]
	f.mu.Unlock()
	if meta["forked_at_cursor"] != float64(10) {
		t.Errorf("forked_at_cursor = %v, want 10 (session lifecycle markers excluded)", meta["forked_at_cursor"])
	}

	// A step beyond the 9 user steps must error (session.completed must not
	// count as a step).
	beyond := 10
	if _, err := Resume(ctx, c, "sess", ResumeOptions{ForkAtStep: &beyond, Name: "sess-fork-2"}); err == nil {
		t.Error("expected error for step beyond the last user step (session.completed must not count)")
	}
}

func TestResumeForkInheritedStepWalksLineage(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Root ran 5 steps (cursors 1-5). A fork "f1" inherited steps 1-3
	// (forked_at_step=3) and recorded its OWN steps 4-5 (cursors 2-3 on its
	// channel, because fork.started=1, session.started... the fake server
	// does not add those markers on fork, so f1's own events start at 1).
	//
	// Forking f1 at step 2 (an INHERITED step) must walk up to the root and
	// use the root's step-2 cursor — NOT f1's own first event.
	f.mu.Lock()
	f.metadata["root"] = map[string]any{
		"status": "completed", "step_count": float64(5),
		"cursors": []any{float64(1), float64(2), float64(3), float64(4), float64(5)},
	}
	f.events["root"] = []fakeEvent{}
	for i := 0; i < 5; i++ {
		f.events["root"] = append(f.events["root"], fakeEvent{
			ID: fmt.Sprintf("r%d", i+1), ChannelID: "root", Type: "step",
			Payload: map[string]any{}, Actor: "x", Cursor: int64(i + 1), Timestamp: "t", Metadata: map[string]any{},
		})
	}
	// f1 inherited steps 1-3, then recorded its own steps 4-5.
	f.metadata["f1"] = map[string]any{
		"status": "started", "step_count": float64(5),
		"forked_at_step":    float64(3),
		"parent_channel_id": "root",
		"cursors":           []any{float64(0), float64(0), float64(0), float64(1), float64(2)},
	}
	f.events["f1"] = []fakeEvent{
		{ID: "f1a", ChannelID: "f1", Type: "fork.started", Payload: map[string]any{}, Actor: "system", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "f1b", ChannelID: "f1", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{"step_number": float64(4)}},
		{ID: "f1c", ChannelID: "f1", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{"step_number": float64(5)}},
	}
	f.mu.Unlock()

	step := 2
	// The fake fork handler copies parent state to the child; the child's
	// metadata cursor should be the ROOT's step-2 cursor (=2), because step 2
	// is inherited from root.
	if _, err := Resume(ctx, c, "f1", ResumeOptions{ForkAtStep: &step, Name: "f1-fork"}); err != nil {
		t.Fatalf("Resume (fork f1 at inherited step 2): %v", err)
	}
	f.mu.Lock()
	meta := f.metadata["f1-fork"]
	f.mu.Unlock()
	if meta == nil {
		t.Fatal("fork metadata missing")
	}
	// The fork request must have targeted the ROOT at cursor 2.
	f.mu.Lock()
	// fake fork handler stores experiment_metadata on the child; find which
	// source was used by checking the child's forked_from.
	src := meta["forked_from"]
	f.mu.Unlock()
	if src != "root" {
		t.Errorf("forked_from = %v, want root (inherited step resolves to ancestor)", src)
	}
	if meta["forked_at_cursor"] != float64(2) {
		t.Errorf("forked_at_cursor = %v, want 2 (root's step-2 cursor)", meta["forked_at_cursor"])
	}
}

func TestResumeForkStrictRaisesOnMissingBoundary(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Event-only channel (metadata without cursors) and no saved state
	// boundaries at any cursor: strict fork-from-existing must raise
	// NoRestorableCheckpointError instead of silently falling forward.
	f.mu.Lock()
	f.metadata["src"] = map[string]any{"status": "started", "step_count": float64(3)}
	f.events["src"] = []fakeEvent{
		{ID: "e1", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e2", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e3", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{}},
	}
	f.boundaryFails = true
	f.mu.Unlock()

	step := 2
	_, err := Resume(ctx, c, "src", ResumeOptions{ForkAtStep: &step, Name: "src-fork"})
	if _, ok := err.(*NoRestorableCheckpointError); !ok {
		t.Fatalf("expected NoRestorableCheckpointError, got %v", err)
	}
}

func TestResumeForkApproximateFallsBackToLatestState(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Event-only channel (metadata without cursors) and no saved state
	// boundaries at any cursor: explicit approximate mode falls back to the
	// latest state (at_cursor=0), exactly like AgentSession.Fork.
	f.mu.Lock()
	f.metadata["src"] = map[string]any{"status": "started", "step_count": float64(3)}
	f.events["src"] = []fakeEvent{
		{ID: "e1", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e2", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e3", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{}},
	}
	f.boundaryFails = true
	f.mu.Unlock()

	step := 2
	sess, err := Resume(ctx, c, "src", ResumeOptions{ForkAtStep: &step, Name: "src-fork", BoundaryMode: "approximate"})
	if err != nil {
		t.Fatalf("Resume fork with approximate fallback: %v", err)
	}
	if sess.ChannelID() != "src-fork" {
		t.Errorf("channel = %q", sess.ChannelID())
	}
	f.mu.Lock()
	meta := f.metadata["src-fork"]
	f.mu.Unlock()
	if meta == nil || meta["forked_at_step"] != float64(2) {
		t.Errorf("fork metadata = %v", meta)
	}
}

func TestResumeForkApproximateRefusesContaminatedState(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// The parent HAS a saved state snapshot at a LATER cursor (cursor 5) than
	// the requested fork boundary (step 2 → cursor 2). A strict fork has no
	// boundary snapshot at/before cursor 2, so approximate falls back to
	// at_cursor=0 → the server copies the LATEST snapshot (cursor 5) into the
	// child. Because the resolved boundary (5) is BEYOND the requested (2),
	// the inherited snapshot contains data from AFTER the fork point: the SDK
	// must refuse to prime with it — InheritedState() is nil.
	f.mu.Lock()
	f.metadata["src"] = map[string]any{
		"status": "started", "step_count": float64(3),
		"cursors": []any{float64(1), float64(2), float64(3)},
	}
	f.events["src"] = []fakeEvent{
		{ID: "e1", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e2", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e3", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{}},
	}
	// The only snapshot is at cursor 5 (after step 3): no boundary at cursor 2.
	f.states["src"] = []fakeState{
		{Version: 1, Cursor: 5, State: map[string]any{"s9_out": "latest"}, Timestamp: "t"},
	}
	f.boundaryFails = true
	f.mu.Unlock()

	step := 2
	sess, err := Resume(ctx, c, "src", ResumeOptions{
		ForkAtStep: &step, Name: "src-fork", BoundaryMode: "approximate",
	})
	if err != nil {
		t.Fatalf("Resume approximate fork: %v", err)
	}
	if sess.RequestedBoundaryCursor() != 2 {
		t.Errorf("requested boundary = %d, want 2", sess.RequestedBoundaryCursor())
	}
	if sess.ResolvedBoundaryCursor() != 5 {
		t.Errorf("resolved boundary = %d, want 5 (latest fallback)", sess.ResolvedBoundaryCursor())
	}
	// The fallback resolved BEYOND the request → the SDK must NOT expose the
	// contaminated state as steps 1..2's data.
	if inh := sess.InheritedState(); inh != nil {
		t.Errorf("InheritedState() = %v, want nil (contaminated state refused)", inh)
	}
}

func TestResumeForkApproximateExactBoundaryInherits(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Parent has a snapshot AT-OR-BEFORE the requested step-2 boundary
	// (cursor 2): the fallback has no drift, so the SDK inherits normally.
	f.mu.Lock()
	f.metadata["src"] = map[string]any{
		"status": "started", "step_count": float64(3),
		"cursors": []any{float64(1), float64(2), float64(3)},
	}
	f.events["src"] = []fakeEvent{
		{ID: "e1", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e2", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e3", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{}},
	}
	f.states["src"] = []fakeState{
		{Version: 1, Cursor: 2, State: map[string]any{"s1_out": "a", "s2_out": "b"}, Timestamp: "t"},
	}
	f.mu.Unlock()

	step := 2
	sess, err := Resume(ctx, c, "src", ResumeOptions{
		ForkAtStep: &step, Name: "src-fork", BoundaryMode: "approximate",
	})
	if err != nil {
		t.Fatalf("Resume approximate fork: %v", err)
	}
	if sess.ResolvedBoundaryCursor() != 2 {
		t.Errorf("resolved boundary = %d, want 2 (no drift)", sess.ResolvedBoundaryCursor())
	}
	inh := sess.InheritedState()
	if inh == nil || inh["s2_out"] != "b" {
		t.Errorf("InheritedState() = %v, want steps 1-2 data", inh)
	}
}

func TestResumeForkLineageOnlyNoStateCopy(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// Event-only channel (no saved snapshots). boundary_mode='lineage_only'
	// must fork WITHOUT a state copy: restorable=false, resolved cursor 0,
	// no inherited state, and the child records the parent lineage.
	f.mu.Lock()
	f.metadata["src"] = map[string]any{
		"status": "started", "step_count": float64(3),
		"cursors": []any{float64(1), float64(2), float64(3)},
	}
	f.events["src"] = []fakeEvent{
		{ID: "e1", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 1, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e2", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 2, Timestamp: "t", Metadata: map[string]any{}},
		{ID: "e3", ChannelID: "src", Type: "step", Payload: map[string]any{}, Actor: "x", Cursor: 3, Timestamp: "t", Metadata: map[string]any{}},
	}
	// No states → the fake's fork copies nothing (resolvedCursor stays 0).
	f.mu.Unlock()

	step := 2
	sess, err := Resume(ctx, c, "src", ResumeOptions{
		ForkAtStep: &step, Name: "src-lo", BoundaryMode: "lineage_only",
	})
	if err != nil {
		t.Fatalf("Resume lineage_only fork: %v", err)
	}
	if sess.StepCount() != 2 {
		t.Errorf("step count = %d, want 2", sess.StepCount())
	}
	if r := sess.BoundaryRestorable(); r == nil || *r {
		t.Errorf("boundary restorable = %v, want false", r)
	}
	if sess.ResolvedBoundaryCursor() != 0 {
		t.Errorf("resolved boundary = %d, want 0 (no state copy)", sess.ResolvedBoundaryCursor())
	}
	if inh := sess.InheritedState(); inh != nil {
		t.Errorf("InheritedState() = %v, want nil (no state copied)", inh)
	}
	f.mu.Lock()
	meta := f.metadata["src-lo"]
	f.mu.Unlock()
	if meta == nil || meta["forked_at_step"] != float64(2) {
		t.Errorf("fork metadata = %v", meta)
	}
}

func TestSessionResumeForkFromExisting(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "src", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Step(ctx, "step", StepOptions{Input: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Complete(ctx); err != nil {
		t.Fatal(err)
	}

	step := 2
	resumed, err := Resume(ctx, c, "src", ResumeOptions{ForkAtStep: &step, Name: "src-fork"})
	if err != nil {
		t.Fatalf("Resume fork: %v", err)
	}
	if resumed.ChannelID() != "src-fork" {
		t.Errorf("channel = %q", resumed.ChannelID())
	}
	f.mu.Lock()
	meta := f.metadata["src-fork"]
	f.mu.Unlock()
	// metadata cursors: [2, 3, 4]; step 2 → cursor 3
	if meta["forked_at_cursor"] != float64(3) {
		t.Errorf("forked_at_cursor = %v", meta["forked_at_cursor"])
	}
}

func TestSessionForkReplayFallback(t *testing.T) {
	// source channel created outside AgentSession: metadata exists but has
	// no cursors list → resolve step→cursor by replay.
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()
	f.mu.Lock()
	f.events["manual"] = []fakeEvent{
		{ID: "e1", ChannelID: "manual", Type: "a", Payload: map[string]any{}, Cursor: 1, Timestamp: "t"},
		{ID: "e2", ChannelID: "manual", Type: "b", Payload: map[string]any{}, Cursor: 2, Timestamp: "t"},
		{ID: "e3", ChannelID: "manual", Type: "c", Payload: map[string]any{}, Cursor: 3, Timestamp: "t"},
	}
	f.metadata["manual"] = map[string]any{"status": "started", "step_count": 3}
	f.mu.Unlock()

	step := 2
	resumed, err := Resume(ctx, c, "manual", ResumeOptions{ForkAtStep: &step, Name: "manual-fork"})
	if err != nil {
		t.Fatalf("Resume with replay fallback: %v", err)
	}
	f.mu.Lock()
	meta := f.metadata["manual-fork"]
	f.mu.Unlock()
	if meta["forked_at_cursor"] != float64(2) {
		t.Errorf("forked_at_cursor = %v, want 2", meta["forked_at_cursor"])
	}
	_ = resumed
}

func TestSessionForkNonMonotonicCursors(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()
	f.mu.Lock()
	f.events["weird"] = []fakeEvent{
		{ID: "e1", ChannelID: "weird", Type: "a", Cursor: 3, Timestamp: "t"},
		{ID: "e2", ChannelID: "weird", Type: "b", Cursor: 2, Timestamp: "t"},
	}
	f.mu.Unlock()

	step := 1
	if _, err := Resume(ctx, c, "weird", ResumeOptions{ForkAtStep: &step, Name: "w"}); err == nil {
		t.Fatal("non-monotonic cursors should fail")
	}
}

func TestSessionRetryOnTransientErrors(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	s, _ := NewAgentSession(c, "retry", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// step is retried (mirrors the Python SDK): 2 failures then success
	f.failNext("POST /api/v1/events/record", 2)
	start := time.Now()
	if _, err := s.Step(ctx, "s", StepOptions{}); err != nil {
		t.Fatalf("Step with retries: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < retryBaseDelay {
		t.Errorf("expected backoff delay, got %v", elapsed)
	}
	// retries were exhausted cleanly: events recorded = started + 1 step
	f.mu.Lock()
	evs := f.events["retry"]
	ops := f.operationIDs["retry"]
	f.mu.Unlock()
	if len(evs) != 2 {
		t.Errorf("expected 2 events after retries, got %d", len(evs))
	}
	// the retried step must reuse the SAME operation_id so the server can
	// replay the original event instead of recording a duplicate
	if len(ops) == 0 {
		t.Fatal("step did not send an operation_id")
	}
	for _, op := range ops {
		if op != ops[0] {
			t.Errorf("retried step changed operation_id: %v", ops)
		}
	}
}

func TestStepRetryNonRetryablePropagates(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	s, _ := NewAgentSession(c, "noretry", AgentSessionOptions{})
	if err := s.Start(ctxForTest()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.permanentFail["POST /api/v1/events/record"] = true
	f.mu.Unlock()
	start := time.Now()
	if _, err := s.Step(ctxForTest(), "s", StepOptions{}); err == nil {
		t.Fatal("expected error for permanent 400")
	} else if apiErr, ok := err.(*APIError); !ok || apiErr.StatusCode != 400 {
		t.Fatalf("got %T (%v), want APIError 400", err, err)
	}
	// non-retryable: no backoff delay applied (must be well under retryBaseDelay)
	if elapsed := time.Since(start); elapsed >= retryBaseDelay {
		t.Errorf("non-retryable error waited %v (backoff applied unexpectedly)", elapsed)
	}
	// exactly one attempt was made
	f.mu.Lock()
	evs := f.events["noretry"]
	f.mu.Unlock()
	if len(evs) != 1 { // only session.started
		t.Errorf("record was attempted %d times, want 1", len(evs))
	}
}

func ctxForTest() context.Context { return context.Background() }

func TestStateManagerLifecycle(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// seed an event so latest_cursor works
	f.mu.Lock()
	f.events["agent"] = []fakeEvent{{ID: "e1", ChannelID: "agent", Type: "x", Cursor: 1, Timestamp: "t"}}
	f.mu.Unlock()

	sm := NewStateManager(c, "agent")
	v, err := sm.Save(ctx, map[string]any{"mem": "one"})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if v != 1 {
		t.Errorf("version = %d", v)
	}
	v2, err := sm.Save(ctx, map[string]any{"mem": "two"})
	if err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	if v2 != 2 {
		t.Errorf("version 2 = %d", v2)
	}

	state, err := sm.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state["mem"] != "two" {
		t.Errorf("load = %v", state)
	}

	resumed, err := sm.Resume(ctx, map[string]any{"mem": "default"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed["mem"] != "two" {
		t.Errorf("resume = %v", resumed)
	}

	versions, err := sm.ListVersions(ctx)
	if err != nil || len(versions) != 2 {
		t.Fatalf("versions = %v %v", versions, err)
	}

	got, err := sm.GetVersion(ctx, 1)
	if err != nil || got.State["mem"] != "one" {
		t.Fatalf("get version 1 = %v %v", got, err)
	}

	if err := sm.DeleteVersion(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := sm.GetVersion(ctx, 1); err == nil {
		t.Fatal("deleted version should 404")
	}
}

func TestStateManagerResumeDefault(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	sm := NewStateManager(c, "empty")
	state, err := sm.Resume(context.Background(), map[string]any{"mem": "seed"})
	if err != nil {
		t.Fatal(err)
	}
	if state["mem"] != "seed" {
		t.Errorf("resume default = %v", state)
	}
	nilState, err := sm.Resume(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nilState) != 0 {
		t.Errorf("resume nil default = %v", nilState)
	}
	loaded, err := sm.Load(context.Background())
	if err != nil || loaded != nil {
		t.Errorf("load empty = %v %v", loaded, err)
	}
}

func TestStateManagerFork(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := context.Background()

	// seed an event so latest_cursor is non-zero
	f.mu.Lock()
	f.events["src"] = []fakeEvent{{ID: "e1", ChannelID: "src", Type: "x", Cursor: 1, Timestamp: "t"}}
	f.mu.Unlock()

	sm := NewStateManager(c, "src")
	if _, err := sm.Save(ctx, map[string]any{"step": 1, "accum": "base"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fork, err := sm.Fork(ctx, "child", StateManagerForkOptions{Reason: "refine"})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if fork.Channel() != "child" {
		t.Errorf("fork channel = %q", fork.Channel())
	}
	// Load on the fork returns the inherited state.
	inherited, err := fork.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inherited["accum"] != "base" {
		t.Errorf("fork inherited state = %v", inherited)
	}

	// The fork evolves independently; the source is unaffected.
	if _, err := fork.Save(ctx, map[string]any{"step": 2, "accum": "refined"}); err != nil {
		t.Fatalf("fork Save: %v", err)
	}
	srcState, err := sm.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if srcState["accum"] != "base" {
		t.Errorf("source state changed by fork save: %v", srcState)
	}
	forkState, err := fork.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if forkState["accum"] != "refined" {
		t.Errorf("fork state = %v", forkState)
	}
}

func TestStateManagerForkBoundaryFallback(t *testing.T) {
	f := newFakeActae(t)
	// Event + snapshot at cursor 1, but fork with at_cursor>0 is rejected.
	f.mu.Lock()
	f.events["src"] = []fakeEvent{{ID: "e1", ChannelID: "src", Type: "x", Cursor: 1, Timestamp: "t"}}
	f.states["src"] = []fakeState{{Version: 1, Cursor: 1, State: map[string]any{"accum": "base"}, Timestamp: "t"}}
	f.boundaryFails = true
	f.mu.Unlock()

	c := sessionTestClient(t, f)
	ctx := context.Background()

	sm := NewStateManager(c, "src")
	fork, err := sm.Fork(ctx, "child", StateManagerForkOptions{})
	if err != nil {
		t.Fatalf("Fork with boundary fallback: %v", err)
	}
	if fork.Channel() != "child" {
		t.Errorf("fork channel = %q", fork.Channel())
	}
	// The retry at at_cursor=0 must have succeeded and inherited the latest
	// snapshot (the only one, at cursor 1).
	inherited, err := fork.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inherited["accum"] != "base" {
		t.Errorf("fallback inherited state = %v", inherited)
	}
}

// TestStepDeterministicOperationID verifies that a step's operation id is
// derived deterministically from (channel, type, step number, content):
// a crash-recovery re-drive of the SAME step (same step_number, same
// payload/metadata) produces the SAME id, so the server replays the
// original event instead of duplicating it; different content derives a
// distinct id.
func TestStepDeterministicOperationID(t *testing.T) {
	f := newFakeActae(t)
	c := sessionTestClient(t, f)
	ctx := ctxForTest()

	// Fresh session on channel "det": step 1 with fixed content.
	s, _ := NewAgentSession(c, "det", AgentSessionOptions{})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(ctx, "s", StepOptions{Input: "p", Output: "r"}); err != nil {
		t.Fatal(err)
	}

	// Second session on the SAME channel re-drives the same step: same
	// step_number (1), same content -> same operation id (server replay).
	s2, _ := NewAgentSession(c, "det", AgentSessionOptions{})
	if err := s2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Step(ctx, "s", StepOptions{Input: "p", Output: "r"}); err != nil {
		t.Fatal(err)
	}

	f.mu.Lock()
	ops := f.operationIDs["det"]
	f.mu.Unlock()
	if len(ops) != 2 {
		t.Fatalf("expected 2 step operation ids, got %d", len(ops))
	}
	if ops[0] != ops[1] {
		t.Errorf("same step re-drive changed operation id: %q vs %q", ops[0], ops[1])
	}

	// A step with different content must derive a DIFFERENT id.
	s3, _ := NewAgentSession(c, "det", AgentSessionOptions{})
	if err := s3.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.Step(ctx, "s", StepOptions{Input: "p", Output: "DIFFERENT"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	ops = f.operationIDs["det"]
	f.mu.Unlock()
	if len(ops) != 3 {
		t.Fatalf("expected 3 step operation ids, got %d", len(ops))
	}
	if ops[2] == ops[0] {
		t.Errorf("different step content must derive a different operation id: %q", ops[2])
	}

	// The id is exactly the documented UUIDv5 derivation (cross-SDK parity).
	expected := DeterministicOperationKey(
		"agent-session", "s", "det", "1",
		CanonicalStepContent(
			map[string]any{"step_number": 1, "input": "p", "output": "r"},
			map[string]any{"step_number": 1},
		),
	)
	if ops[0] != expected {
		t.Errorf("derivation drift: got %q want %q", ops[0], expected)
	}
}
