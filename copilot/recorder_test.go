package copilot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	actae "github.com/BViganotti/actae-go"
	copilotsdk "github.com/github/copilot-sdk/go"
)

// fakeActae is a minimal in-memory Actae for copilot tests.
type fakeActae struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	events   map[string][]map[string]any // channel → recorded events
	metadata map[string]map[string]any
	nextCur  int64
}

func newFakeActae(t *testing.T) *fakeActae {
	f := &fakeActae{t: t, events: map[string][]map[string]any{}, metadata: map[string]map[string]any{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeActae) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/events/record":
		var body struct {
			ChannelID string         `json:"channel_id"`
			EventType string         `json:"event_type"`
			Payload   any            `json:"payload"`
			Metadata  map[string]any `json:"metadata"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.nextCur++
		ev := map[string]any{
			"id":         "e" + itoa(f.nextCur),
			"channel_id": body.ChannelID,
			"type":       body.EventType,
			"payload":    body.Payload,
			"actor":      body.Metadata["actor"],
			"cursor":     f.nextCur,
			"timestamp":  "2026-08-03T00:00:00Z",
			"metadata":   body.Metadata["metadata"],
		}
		f.events[body.ChannelID] = append(f.events[body.ChannelID], ev)
		json.NewEncoder(w).Encode(map[string]any{"event": ev})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/events/replay/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/events/replay/")
		evs := f.events[channel]
		if evs == nil {
			evs = []map[string]any{}
		}
		json.NewEncoder(w).Encode(map[string]any{"events": evs})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/events/cursor/"):
		channel := strings.TrimPrefix(r.URL.Path, "/api/v1/events/cursor/")
		evs := f.events[channel]
		cur := 0
		if len(evs) > 0 {
			cur = int(evs[len(evs)-1]["cursor"].(float64))
		}
		json.NewEncoder(w).Encode(map[string]any{"latest_cursor": cur})

	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/metadata"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/channels/"), "/metadata")
		meta, ok := f.metadata[channel]
		if !ok {
			w.WriteHeader(404)
			w.Write([]byte(`{"error": "not_found"}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"channel_id": channel, "display_name": channel, "created_at": "t",
			"experiment_metadata": meta,
		})

	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/metadata"):
		channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/channels/"), "/metadata")
		var body struct {
			ExperimentMetadata map[string]any `json:"experiment_metadata"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if f.metadata[channel] == nil {
			f.metadata[channel] = map[string]any{}
		}
		for k, v := range body.ExperimentMetadata {
			f.metadata[channel][k] = v
		}
		json.NewEncoder(w).Encode(map[string]any{"channel_id": channel})

	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/channels/fork":
		var body struct {
			SourceChannelID    string         `json:"source_channel_id"`
			NewChannelID       string         `json:"new_channel_id"`
			ExperimentMetadata map[string]any `json:"experiment_metadata"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.metadata[body.NewChannelID] = body.ExperimentMetadata
		if body.ExperimentMetadata == nil {
			f.metadata[body.NewChannelID] = map[string]any{}
		}
		json.NewEncoder(w).Encode(map[string]any{
			"channel_id": body.NewChannelID, "parent_channel_id": body.SourceChannelID,
			"experiment_metadata": f.metadata[body.NewChannelID],
		})

	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/state/"):
		var body struct {
			Cursor int64          `json:"cursor"`
			State  map[string]any `json:"state"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]any{"version": body.Cursor + 100, "state": body.State})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/state/"):
		json.NewEncoder(w).Encode(map[string]any{"cursor": 5, "state": map[string]any{"mem": "saved"}})

	default:
		http.Error(w, `{"error": "not found"}`, 404)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func newClient(t *testing.T, f *fakeActae) *actae.Client {
	t.Helper()
	c, err := actae.NewClient(actae.ClientOptions{APIKey: "sk-test", Endpoint: f.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newSession(id string) *copilotsdk.Session {
	return &copilotsdk.Session{SessionID: id}
}

func makeEvent(id, typ string, data copilotsdk.SessionEventData) copilotsdk.SessionEvent {
	parent := ""
	return copilotsdk.SessionEvent{
		ID:        id,
		ParentID:  &parent,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// waitFor polls until cond returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func (f *fakeActae) recorded(channel string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.events[channel]))
	copy(out, f.events[channel])
	return out
}

// ---------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------

func TestRecorderRecordsEvents(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer rec.Stop()

	rec.handleEvent(makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "hello"}))
	rec.handleEvent(makeEvent("evt-2", "tool.execution_start", &copilotsdk.ToolExecutionStartData{ToolName: "bash", ToolCallID: "tc-1"}))
	rec.handleEvent(makeEvent("evt-2", "tool.execution_start", &copilotsdk.ToolExecutionStartData{ToolName: "bash", ToolCallID: "tc-1"})) // dup

	if !waitFor(t, 2*time.Second, func() bool {
		return len(f.recorded("copilot:sess-1")) >= 3
	}) {
		t.Fatalf("expected 3 events (started + 2, deduped), got %d", len(f.recorded("copilot:sess-1")))
	}

	evs := f.recorded("copilot:sess-1")
	if len(evs) != 3 {
		t.Fatalf("expected exactly 3 events, got %d: %v", len(evs), evs)
	}
	if evs[0]["type"] != "copilot.session.started" {
		t.Errorf("first event = %v", evs[0]["type"])
	}
	var toolEvt map[string]any
	for _, e := range evs {
		if e["type"] == "copilot.tool.execution_start" {
			toolEvt = e
		}
	}
	if toolEvt == nil {
		t.Fatal("tool_execution.start not recorded")
	}
	payload := toolEvt["payload"].(map[string]any)
	if payload["event_type"] != "tool.execution_start" {
		t.Errorf("payload event_type = %v", payload["event_type"])
	}
	meta := toolEvt["metadata"].(map[string]any)
	if meta["event_id"] != "evt-2" {
		t.Errorf("metadata event_id = %v", meta["event_id"])
	}
	if cur, ok := rec.CursorForEvent("evt-2"); !ok || cur == 0 {
		t.Error("CursorForEvent missing/zero")
	}
	if rec.ChannelID() != "copilot:sess-1" {
		t.Errorf("ChannelID = %q", rec.ChannelID())
	}
}

func TestRecorderSkipsEphemeral(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	eph := true
	rec.handleEvent(copilotsdk.SessionEvent{ID: "evt-e", Timestamp: time.Now(), Ephemeral: &eph, Data: &copilotsdk.AssistantMessageDeltaData{}})
	// give the worker time to drain (it shouldn't record anything)
	time.Sleep(200 * time.Millisecond)
	if n := len(f.recorded("copilot:sess-1")); n != 1 {
		t.Errorf("ephemeral event recorded, got %d events", n)
	}

	// with IncludeEphemeral it is recorded
	f2 := newFakeActae(t)
	rec2 := NewRecorder(newClient(t, f2), RecorderOptions{IncludeEphemeral: true})
	if err := rec2.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec2.Stop()
	rec2.handleEvent(copilotsdk.SessionEvent{ID: "evt-e", Timestamp: time.Now(), Ephemeral: &eph, Data: &copilotsdk.AssistantMessageDeltaData{}})
	if !waitFor(t, 2*time.Second, func() bool {
		return len(f2.recorded("copilot:sess-1")) >= 2
	}) {
		t.Error("ephemeral event not recorded with IncludeEphemeral")
	}
}

func TestRecorderEventTypeFilter(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{EventTypes: []string{"user.message"}})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	rec.handleEvent(makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "hi"}))
	rec.handleEvent(makeEvent("evt-2", "assistant.message", &copilotsdk.AssistantMessageData{Content: "bye"}))
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-1") {
			if e["type"] == "copilot.user.message" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("filtered event never recorded")
	}
	time.Sleep(150 * time.Millisecond)
	for _, e := range f.recorded("copilot:sess-1") {
		typ := e["type"].(string)
		if typ != "copilot.session.started" && typ != "copilot.user.message" {
			t.Errorf("unexpected event type recorded: %s", typ)
		}
	}
}

func TestRecorderStopDrains(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	rec.handleEvent(makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "x"}))
	rec.Stop()
	// Stop returns after draining; the event must be persisted.
	found := false
	for _, e := range f.recorded("copilot:sess-1") {
		if e["type"] == "copilot.user.message" {
			found = true
		}
	}
	if !found {
		t.Error("queued event not drained on Stop")
	}
	rec.Stop() // double stop is safe
}

func TestRecorderRecordHook(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	rec.RecordHook("pre_tool_use", "sess-1", copilotsdk.PreToolUseHookInput{
		SessionID: "sess-1", ToolName: "bash", ToolArgs: map[string]any{"cmd": "ls"},
	})
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-1") {
			if e["type"] == "copilot.hook.pre_tool_use" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("hook event not recorded")
	}
}

func TestRecorderHookBeforeAttach(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	// hook queued before any session exists: worker starts lazily and derives
	// the channel from the invocation session ID
	rec.RecordHook("session_start", "pre-sess", copilotsdk.SessionStartHookInput{SessionID: "pre-sess"})
	if !waitFor(t, 2*time.Second, func() bool {
		return len(f.recorded("copilot:pre-sess")) >= 1
	}) {
		t.Fatal("pre-attach hook event not recorded")
	}
	evs := f.recorded("copilot:pre-sess")
	if len(evs) != 1 || evs[0]["type"] != "copilot.hook.session_start" {
		t.Errorf("pre-attach hook events = %v", evs)
	}
	rec.Stop()
}

func TestRecorderCustomChannel(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{ChannelID: "custom-chan"})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()
	rec.handleEvent(makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "x"}))
	if !waitFor(t, 2*time.Second, func() bool {
		return len(f.recorded("custom-chan")) >= 2
	}) {
		t.Fatal("custom channel events not recorded")
	}
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

func TestRecordingHooksChainsToUser(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	var userCalled bool
	userHooks := &copilotsdk.SessionHooks{
		OnUserPromptSubmitted: func(input copilotsdk.UserPromptSubmittedHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.UserPromptSubmittedHookOutput, error) {
			userCalled = true
			return &copilotsdk.UserPromptSubmittedHookOutput{ModifiedPrompt: "MODIFIED"}, nil
		},
	}
	hooks := RecordingHooks(rec, userHooks)

	out, err := hooks.OnUserPromptSubmitted(copilotsdk.UserPromptSubmittedHookInput{
		SessionID: "sess-1", Prompt: "p",
	}, copilotsdk.HookInvocation{SessionID: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !userCalled {
		t.Error("user hook not chained")
	}
	if out == nil || out.ModifiedPrompt != "MODIFIED" {
		t.Errorf("user output not preserved: %+v", out)
	}

	// recording happened
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-1") {
			if e["type"] == "copilot.hook.user_prompt_submitted" {
				return true
			}
		}
		return false
	}) {
		t.Error("hook event not recorded")
	}

	// nil next still works (record-only)
	hooks2 := RecordingHooks(rec, nil)
	_, err = hooks2.OnErrorOccurred(copilotsdk.ErrorOccurredHookInput{SessionID: "sess-1", Error: "e"}, copilotsdk.HookInvocation{SessionID: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	// all 8 hook types are wired
	for _, h := range []func() (any, error){
		func() (any, error) {
			return hooks.OnPreToolUse(copilotsdk.PreToolUseHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
		func() (any, error) {
			return hooks.OnPostToolUse(copilotsdk.PostToolUseHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
		func() (any, error) {
			return hooks.OnPostToolUseFailure(copilotsdk.PostToolUseFailureHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
		func() (any, error) {
			return hooks.OnSessionStart(copilotsdk.SessionStartHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
		func() (any, error) {
			return hooks.OnSessionEnd(copilotsdk.SessionEndHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
		func() (any, error) {
			return hooks.OnPreMCPToolCall(copilotsdk.PreMCPToolCallHookInput{SessionID: "s"}, copilotsdk.HookInvocation{})
		},
	} {
		if _, err := h(); err != nil {
			t.Fatalf("hook handler error: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

func TestManagerForkResolvesEventCursor(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})

	rec, err := mgr.Track(context.Background(), newSession("sess-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.StopAll()
	rec.handleEvent(makeEvent("evt-9", "user.message", &copilotsdk.UserMessageData{Content: "fork me"}))

	if !waitFor(t, 2*time.Second, func() bool {
		_, ok := rec.CursorForEvent("evt-9")
		return ok
	}) {
		t.Fatal("event cursor never tracked")
	}

	newChannel, err := mgr.Fork(context.Background(), "sess-1", "evt-9", ForkOptions{NewChannelID: "fork-1"})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if newChannel != "fork-1" {
		t.Errorf("channel = %q", newChannel)
	}
	f.mu.Lock()
	meta := f.metadata["fork-1"]
	f.mu.Unlock()
	if meta["forked_from"] != "copilot:sess-1" {
		t.Errorf("forked_from = %v", meta["forked_from"])
	}
	if meta["forked_at_event"] != "evt-9" {
		t.Errorf("forked_at_event = %v", meta["forked_at_event"])
	}
}

func TestManagerForkReplayFallback(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	// seed a channel with an event whose metadata carries the target event id
	f.mu.Lock()
	f.nextCur = 5
	f.events["copilot:sess-2"] = []map[string]any{{
		"id": "e1", "channel_id": "copilot:sess-2", "type": "copilot.user.message",
		"payload": map[string]any{}, "actor": "copilot", "cursor": float64(5),
		"timestamp": "t", "metadata": map[string]any{"event_id": "old-evt-1"},
	}}
	f.mu.Unlock()

	_, err := mgr.Track(context.Background(), newSession("sess-2"))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.StopAll()

	newChannel, err := mgr.Fork(context.Background(), "sess-2", "old-evt-1", ForkOptions{NewChannelID: "fork-2"})
	if err != nil {
		t.Fatalf("Fork with replay fallback: %v", err)
	}
	f.mu.Lock()
	meta := f.metadata[newChannel]
	f.mu.Unlock()
	if meta["forked_at_cursor"] != float64(5) {
		t.Errorf("forked_at_cursor = %v", meta["forked_at_cursor"])
	}
}

func TestManagerForkUnknownEvent(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	if _, err := mgr.Track(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer mgr.StopAll()
	if _, err := mgr.Fork(context.Background(), "sess-1", "does-not-exist", ForkOptions{NewChannelID: "f"}); err == nil {
		t.Fatal("expected error for unknown event")
	}
	if _, err := mgr.Fork(context.Background(), "untracked", "e", ForkOptions{NewChannelID: "f"}); err == nil {
		t.Fatal("expected error for untracked session")
	}
	if _, err := mgr.Fork(context.Background(), "sess-1", "e", ForkOptions{}); err == nil {
		t.Fatal("expected error when NewChannelID missing")
	}
}

func TestManagerSnapshotAndLoad(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	ctx := context.Background()

	ver, err := mgr.Snapshot(ctx, "sess-3", map[string]any{"mem": "one"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if ver == 0 {
		t.Error("version = 0")
	}
	state, err := mgr.LoadSnapshot(ctx, "sess-3")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if state["mem"] != "saved" {
		t.Errorf("state = %v", state)
	}
	if _, err := mgr.ListSnapshots(ctx, "sess-3"); err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
}

func TestManagerEndAndUntrack(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	_, err := mgr.Track(context.Background(), newSession("sess-4"))
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.EndSession(context.Background(), "sess-4", "complete"); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-4") {
			if e["type"] == "copilot.session.ended" {
				return true
			}
		}
		return false
	}) {
		t.Error("session.ended not recorded")
	}

	mgr.Untrack("sess-4")
	if mgr.RecorderFor("sess-4") != nil {
		t.Error("recorder still tracked after Untrack")
	}
	if err := mgr.EndSession(context.Background(), "sess-4", "x"); err == nil {
		t.Error("expected error for untracked session")
	}
}

func TestManagerTrackIsIdempotent(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	rec, err := mgr.Track(context.Background(), newSession("sess-5"))
	if err != nil {
		t.Fatal(err)
	}
	rec2, err := mgr.Track(context.Background(), newSession("sess-5"))
	if err != nil {
		t.Fatal(err)
	}
	if rec != rec2 {
		t.Error("Track not idempotent")
	}
	defer mgr.StopAll()
}

func TestManagerChannelFor(t *testing.T) {
	mgr := NewManager(nil, ManagerOptions{})
	if got := mgr.ChannelFor("abc"); got != "copilot:abc" {
		t.Errorf("ChannelFor = %q", got)
	}
	mgr2 := NewManager(nil, ManagerOptions{ChannelPrefix: "exp"})
	if got := mgr2.ChannelFor("abc"); got != "exp:abc" {
		t.Errorf("ChannelFor with prefix = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Session-end lifecycle
// ---------------------------------------------------------------------------

func TestRecordSessionEndedOnce(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	ctx := context.Background()
	if err := rec.RecordSessionEnded(ctx, "complete"); err != nil {
		t.Fatal(err)
	}
	// second call is a no-op (no duplicate events)
	if err := rec.RecordSessionEnded(ctx, "complete"); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-1") {
			if e["type"] == "copilot.session.ended" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("session.ended not recorded")
	}
	count := 0
	for _, e := range f.recorded("copilot:sess-1") {
		if e["type"] == "copilot.session.ended" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("session.ended recorded %d times, want 1", count)
	}
}

func TestSessionEndHookRecordsLifecycle(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	hooks := RecordingHooks(rec, nil)
	if _, err := hooks.OnSessionEnd(copilotsdk.SessionEndHookInput{
		SessionID: "sess-1", Reason: "complete",
	}, copilotsdk.HookInvocation{SessionID: "sess-1"}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		var hook, ended bool
		for _, e := range f.recorded("copilot:sess-1") {
			switch e["type"] {
			case "copilot.hook.session_end":
				hook = true
			case "copilot.session.ended":
				ended = true
			}
		}
		return hook && ended
	}) {
		t.Fatal("session_end hook + lifecycle event not recorded")
	}
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

// backfillSession wraps a fake GetEvents response.
type backfillSession struct {
	events []copilotsdk.SessionEvent
	err    error
}

func (bs *backfillSession) GetEvents(ctx context.Context) ([]copilotsdk.SessionEvent, error) {
	return bs.events, bs.err
}

func TestBackfillRecordsHistory(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	// simulate a session with 3 historical events, 1 of which was already
	// recorded live before the backfill
	rec.handleEvent(makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "live"}))
	history := []copilotsdk.SessionEvent{
		makeEvent("evt-1", "user.message", &copilotsdk.UserMessageData{Content: "live"}),
		makeEvent("evt-2", "user.message", &copilotsdk.UserMessageData{Content: "old"}),
		makeEvent("evt-3", "tool.execution_start", &copilotsdk.ToolExecutionStartData{ToolName: "bash", ToolCallID: "tc-3"}),
	}
	rec.history = &backfillSession{events: history}

	n, err := rec.Backfill(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// evt-1 already seen → skipped; evt-2 + evt-3 recorded
	if n != 2 {
		t.Errorf("backfilled %d events, want 2", n)
	}
	// both events must eventually be persisted (session.started + 3 user/tool
	// events, with evt-1 deduped)
	if !waitFor(t, 2*time.Second, func() bool {
		var userMsg, toolStart bool
		for _, e := range f.recorded("copilot:sess-1") {
			switch e["type"] {
			case "copilot.user.message":
				userMsg = true
			case "copilot.tool.execution_start":
				toolStart = true
			}
		}
		return userMsg && toolStart
	}) {
		t.Fatal("backfilled events not persisted")
	}
	// exactly one copy of evt-1's user.message (live + history deduped)
	count := 0
	for _, e := range f.recorded("copilot:sess-1") {
		if e["type"] == "copilot.user.message" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("user.message recorded %d times, want 2 (evt-1 deduped, evt-2 unique)", count)
	}
}

func TestBackfillErrors(t *testing.T) {
	f := newFakeActae(t)
	rec := NewRecorder(newClient(t, f), RecorderOptions{})
	// not attached → error
	if _, err := rec.Backfill(context.Background()); err == nil {
		t.Error("expected error for unattached recorder")
	}
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()
	rec.history = &backfillSession{err: NewError("history unavailable")}
	if _, err := rec.Backfill(context.Background()); err == nil {
		t.Error("expected error when GetEvents fails")
	}
}

// ---------------------------------------------------------------------------
// Session info metadata
// ---------------------------------------------------------------------------

// metadataClient is a minimal fake for the copilotsdk.Client surface used
// by recordSessionInfo.
type metadataClient struct {
	meta *copilotsdk.SessionMetadata
	err  error
}

func (mc *metadataClient) GetSessionMetadata(ctx context.Context, sessionID string) (*copilotsdk.SessionMetadata, error) {
	return mc.meta, mc.err
}

func TestRecordSessionInfo(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	rec := mgr.NewRecorder(RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()

	summary := "implemented the parser"
	mgr.recordSessionInfo(context.Background(), &metadataClient{meta: &copilotsdk.SessionMetadata{
		SessionID: "sess-1",
		StartTime: time.Now().Add(-time.Hour),
		Summary:   &summary,
		IsRemote:  true,
		Context: &copilotsdk.SessionContext{
			WorkingDirectory: "/repo",
			Repository:       "org/repo",
			Branch:         "main",
		},
	}}, rec)

	var infoEvt map[string]any
	if !waitFor(t, 2*time.Second, func() bool {
		for _, e := range f.recorded("copilot:sess-1") {
			if e["type"] == "copilot.session.info" {
				infoEvt = e
				return true
			}
		}
		return false
	}) {
		t.Fatal("copilot.session.info not recorded")
	}
	payload := infoEvt["payload"].(map[string]any)
	if payload["summary"] != "implemented the parser" {
		t.Errorf("summary = %v", payload["summary"])
	}
	if payload["is_remote"] != true {
		t.Errorf("is_remote = %v", payload["is_remote"])
	}
	if payload["context"] == nil {
		t.Error("context missing from session info")
	}
	// session_id never makes it into the payload twice
	if payload["session_id"] != "sess-1" {
		t.Errorf("session_id = %v", payload["session_id"])
	}
}

func TestRecordSessionInfoFailureIsBestEffort(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	rec := mgr.NewRecorder(RecorderOptions{})
	if err := rec.Attach(context.Background(), newSession("sess-1")); err != nil {
		t.Fatal(err)
	}
	defer rec.Stop()
	// metadata fetch failure must not panic or block; nothing recorded
	mgr.recordSessionInfo(context.Background(), &metadataClient{err: NewError("nope")}, rec)
	time.Sleep(100 * time.Millisecond)
	for _, e := range f.recorded("copilot:sess-1") {
		if e["type"] == "copilot.session.info" {
			t.Error("session.info recorded despite metadata failure")
		}
	}
}

func TestContinueSessionValidation(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	ctx := context.Background()

	// ForkChannelID is required (validated before any client use)
	if _, err := mgr.ContinueSession(ctx, nil, "sess-1", "evt-1", &copilotsdk.SessionConfig{}, ContinueSessionOptions{}); err == nil {
		t.Fatal("expected error when ForkChannelID missing")
	}
	// source session must be tracked (Fork runs before client use)
	if _, err := mgr.ContinueSession(ctx, nil, "untracked", "evt-1", &copilotsdk.SessionConfig{}, ContinueSessionOptions{ForkChannelID: "f"}); err == nil {
		t.Fatal("expected error for untracked source session")
	}
}

func TestForkMetadataShape(t *testing.T) {
	f := newFakeActae(t)
	mgr := NewManager(newClient(t, f), ManagerOptions{})
	rec, err := mgr.Track(context.Background(), newSession("sess-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.StopAll()
	rec.handleEvent(makeEvent("evt-5", "user.message", &copilotsdk.UserMessageData{Content: "x"}))
	if !waitFor(t, 2*time.Second, func() bool {
		_, ok := rec.CursorForEvent("evt-5")
		return ok
	}) {
		t.Fatal("cursor never tracked")
	}

	if _, err := mgr.Fork(context.Background(), "sess-1", "evt-5", ForkOptions{
		NewChannelID: "fork-meta", DisplayName: "Fork A", Reason: "experiment",
	}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	meta := f.metadata["fork-meta"]
	f.mu.Unlock()
	if meta["forked_from"] != "copilot:sess-1" {
		t.Errorf("forked_from = %v", meta["forked_from"])
	}
	if meta["forked_at_event"] != "evt-5" {
		t.Errorf("forked_at_event = %v", meta["forked_at_event"])
	}
	if meta["forked_at_cursor"] == nil {
		t.Error("forked_at_cursor missing")
	}
	if meta["status"] != "created" {
		t.Errorf("status = %v", meta["status"])
	}
	if meta["session_name"] != "fork-meta" {
		t.Errorf("session_name = %v", meta["session_name"])
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var _ = context.Background
