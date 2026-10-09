package actae

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// testServer wraps an httptest server with a scripted handler that records
// requests for assertions.
type testServer struct {
	t      *testing.T
	server *httptest.Server
	mu     sync.Mutex
	reqs   []httpReq
	// routes maps "METHOD path" → handler func (or a fixed response).
	routes map[string]routeHandler
	// autoKey asserts every request carries x-api-key.
}

type httpReq struct {
	method string
	path   string
	query  string
	body   map[string]any
	key    string
}

type routeHandler func(w http.ResponseWriter, r *http.Request, body map[string]any)

func newTestServer(t *testing.T, routes map[string]routeHandler) *testServer {
	t.Helper()
	ts := &testServer{t: t, routes: routes}
	ts.server = httptest.NewServer(http.HandlerFunc(ts.handle))
	t.Cleanup(ts.server.Close)
	return ts
}

func (ts *testServer) handle(w http.ResponseWriter, r *http.Request) {
	ts.mu.Lock()
	req := httpReq{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, key: r.Header.Get("x-api-key")}
	if r.Body != nil && r.Body != http.NoBody {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			req.body = body
		}
	}
	ts.reqs = append(ts.reqs, req)
	ts.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if h, ok := ts.routes[r.Method+" "+r.URL.Path]; ok {
		h(w, r, req.body)
		return
	}
	http.Error(w, `{"error": "not found", "status": "not_found"}`, http.StatusNotFound)
}

func (ts *testServer) requests() []httpReq {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]httpReq, len(ts.reqs))
	copy(out, ts.reqs)
	return out
}

func (ts *testServer) lastRequest() httpReq {
	reqs := ts.requests()
	if len(reqs) == 0 {
		ts.t.Fatal("no requests recorded")
	}
	return reqs[len(reqs)-1]
}

func jsonBody(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func newTestClient(t *testing.T, ts *testServer) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		APIKey:   "sk-test-123",
		Endpoint: ts.server.URL,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

const recordEventJSON = `{
	"event": {
		"id": "evt-1", "channel_id": "chan-1", "type": "agent.step",
		"payload": {"input": {"$sonic_rs::private::JsonNumber": "42"}},
		"actor": "agent", "cursor": 7, "channel_cursor": 3,
		"timestamp": "2026-08-03T00:00:00Z", "depends_on": null
	}
}`

func TestRecord(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["channel_id"] != "chan-1" || body["event_type"] != "agent.step" {
				t.Errorf("bad record body: %v", body)
			}
			if m, _ := body["metadata"].(map[string]any); m["actor"] != "agent" {
				t.Errorf("bad actor: %v", body["metadata"])
			}
			w.Write([]byte(recordEventJSON))
		},
	})
	c := newTestClient(t, ts)
	ev, err := c.Record(context.Background(), "chan-1", "agent.step",
		map[string]any{"input": "x"}, RecordOptions{Actor: "agent"})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if ev.ID != "evt-1" || ev.ChannelID != "chan-1" || ev.Cursor != 7 {
		t.Errorf("bad event: %+v", ev)
	}
	// sonic number wrapper must be unwrapped to int64
	payload := ev.Payload.(map[string]any)
	if payload["input"] != int64(42) {
		t.Errorf("sonic unwrap failed: %#v", payload["input"])
	}
	if ts.lastRequest().key != "sk-test-123" {
		t.Error("x-api-key header missing")
	}
}

func TestRecordOperationID(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/record": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["operation_id"] != "op-1" {
				t.Errorf("operation_id not forwarded: %v", body["operation_id"])
			}
			w.Write([]byte(recordEventJSON))
		},
	})
	c := newTestClient(t, ts)
	_, err := c.Record(context.Background(), "chan-1", "agent.step",
		map[string]any{"input": "x"}, RecordOptions{OperationID: Ptr("op-1")})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestNewClientFromEnv(t *testing.T) {
	t.Setenv("ACTAE_API_KEY", "sk-env-1")
	t.Setenv("ACTAE_URL", "http://env-host:9000")
	c, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	if c.apiKey != "sk-env-1" || c.endpoint != "http://env-host:9000" {
		t.Errorf("env not applied: %q %q", c.apiKey, c.endpoint)
	}
	if c.wsEndpoint != "ws://env-host:9000/ws" {
		t.Errorf("ws endpoint not derived: %q", c.wsEndpoint)
	}
	t.Setenv("ACTAE_API_KEY", "")
	if _, err := NewClientFromEnv(ClientOptions{}); err == nil {
		t.Error("expected error when ACTAE_API_KEY missing")
	}
}

func TestPtr(t *testing.T) {
	if got := *Ptr(42); got != 42 {
		t.Errorf("Ptr(int): %d", got)
	}
	if got := *Ptr("x"); got != "x" {
		t.Errorf("Ptr(string): %q", got)
	}
}

func TestAutoReconnectDefault(t *testing.T) {
	// Setting Timeout must never flip the reconnect default (the old
	// zero-value bool trap: AutoReconnect defaulted to true only when
	// Timeout was also zero).
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://x", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !c.autoReconnect {
		t.Error("setting Timeout must not disable auto-reconnect")
	}

	// Explicit pointer control works in both directions.
	c2, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://x", AutoReconnect: Ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	if c2.autoReconnect {
		t.Error("AutoReconnect: Ptr(false) must disable auto-reconnect")
	}
	c3, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://x", AutoReconnect: Ptr(true), Timeout: 1 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !c3.autoReconnect {
		t.Error("AutoReconnect: Ptr(true) must enable auto-reconnect")
	}
}

func TestTransportErrorWrapped(t *testing.T) {
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.Record(context.Background(), "c", "t", map[string]any{}, RecordOptions{})
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("transport error not wrapped in ConnectionError: %T %v", err, err)
	}
}

func TestMidBodyReadErrorWrapped(t *testing.T) {
	// A response that dies AFTER its header (valid status line + a
	// Content-Length larger than the body) must surface as a
	// ConnectionError, not a raw io error — the read phase is a transport
	// failure like the connect phase (https:// parity with TestTransportErrorWrapped
	// and with the documented "transport failures are ConnectionError"
	// contract that AgentSession/state.Store retry logic relies on).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("no hijacker")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"partial"))
		conn.Close()
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.LatestState(context.Background(), "c")
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("mid-body read error not wrapped in ConnectionError: %T %v", err, err)
	}
}

func TestCancelWakeupAlreadyFired(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"DELETE /api/v1/scheduler/wakeups/w1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "invalid_state", "error": "wakeup already fired"}`))
		},
	})
	c := newTestClient(t, ts)
	cancelled, err := c.CancelWakeup(context.Background(), "w1")
	if cancelled {
		t.Error("expected cancelled=false on 409")
	}
	if !errors.Is(err, ErrWakeupAlreadyFired) {
		t.Fatalf("expected ErrWakeupAlreadyFired, got %v", err)
	}
}

func TestAsAccessors(t *testing.T) {
	m := map[string]any{"i": int64(3), "f": float64(2.0), "s": "hi", "n": float64(2.5)}
	if got := AsInt64(m["i"], 0); got != 3 {
		t.Errorf("AsInt64 int64: %d", got)
	}
	if got := AsInt64(m["f"], 0); got != 2 {
		t.Errorf("AsInt64 float: %d", got)
	}
	if got := AsInt64(m["n"], 0); got != 0 {
		t.Errorf("AsInt64 non-integral should return default: %d", got)
	}
	if AsString(m["s"]) != "hi" || AsString(m["i"]) != "" {
		t.Error("AsString mismatch")
	}
	if got := AsMap(map[string]any{"a": 1}); AsInt64(got["a"], 0) != 1 {
		t.Errorf("AsMap: %v", got)
	}
	if got := AsList([]any{1, 2}); len(got) != 2 {
		t.Errorf("AsList: %v", got)
	}
}

func TestReplayWithOptions(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/events/replay/chan-1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			if r.URL.Query().Get("limit") != "500" || r.URL.Query().Get("cursor") != "10" ||
				r.URL.Query().Get("event_type") != "agent.step" {
				t.Errorf("bad query: %s", r.URL.RawQuery)
			}
			w.Write([]byte(`{"events": [{"id": "evt-1", "channel_id": "chan-1", "type": "agent.step", "payload": {}, "actor": "agent", "cursor": 7, "timestamp": "t"}]}`))
		},
	})
	c := newTestClient(t, ts)
	limit := 500
	cursor := int64(10)
	et := "agent.step"
	events, err := c.Replay(context.Background(), "chan-1", ReplayOptions{Limit: limit, Cursor: &cursor, EventType: &et})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(events) != 1 || events[0].Cursor != 7 {
		t.Errorf("bad replay: %+v", events)
	}
}

func TestQuery(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/query": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["channel_ids"] == nil || body["event_type"] != "tool_call" {
				t.Errorf("bad query body: %v", body)
			}
			w.Write([]byte(`{"events": [{"id": "e2", "channel_id": "chan-1", "type": "tool_call", "payload": {"ok": true}, "actor": "a", "cursor": 3, "timestamp": "t", "agent_id": "ag"}]}`))
		},
	})
	c := newTestClient(t, ts)
	et := "tool_call"
	events, err := c.Query(context.Background(), QueryOptions{ChannelIDs: []string{"chan-1"}, EventType: &et})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(events) != 1 || events[0].AgentID == nil || *events[0].AgentID != "ag" {
		t.Errorf("bad query result: %+v", events)
	}
}

func TestGetCursor(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/events/cursor/chan-1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"latest_cursor": 42}`))
		},
	})
	c := newTestClient(t, ts)
	cur, err := c.GetCursor(context.Background(), "chan-1")
	if err != nil || cur == nil || *cur != 42 {
		t.Fatalf("GetCursor: %v %v", cur, err)
	}
}

func TestTransition(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/events/transition": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["type"] != "state.change" || body["expected_version"] != float64(3) {
				t.Errorf("bad transition body: %v", body)
			}
			w.Write([]byte(`{"event": {"id": "e", "channel_id": "c", "type": "state.change", "payload": {}, "actor": "a", "cursor": 5, "timestamp": "t"}, "state_version": 4}`))
		},
	})
	c := newTestClient(t, ts)
	ev := int64(3)
	res, err := c.Transition(context.Background(), "chan-1", "state.change",
		map[string]any{"x": 1}, map[string]any{"y": 2}, TransitionOptions{ExpectedVersion: &ev})
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if res.StateVersion != 4 || res.Event.Cursor != 5 {
		t.Errorf("bad transition result: %+v", res)
	}
}

func TestStateLifecycle(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/state/chan-1": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["cursor"] != float64(9) {
				t.Errorf("bad save body: %v", body)
			}
			w.Write([]byte(`{"version": 2}`))
		},
		"GET /api/v1/state/chan-1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"cursor": 9, "state": {"mem": "x"}}`))
		},
		"GET /api/v1/state/chan-1/versions": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"versions": [{"version": 1, "cursor": 3, "timestamp": "t"}]}`))
		},
		"GET /api/v1/state/chan-1/version/1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"version": 1, "cursor": 3, "state": {"mem": "old"}}`))
		},
		"DELETE /api/v1/state/chan-1/version/1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"deleted": true}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	if v, err := c.SaveState(ctx, "chan-1", 9, map[string]any{"mem": "x"}, SaveStateOptions{}); err != nil || v != 2 {
		t.Fatalf("SaveState: %v %v", v, err)
	}
	latest, err := c.LatestState(ctx, "chan-1")
	if err != nil || latest.State["mem"] != "x" || latest.Cursor != 9 {
		t.Fatalf("LatestState: %v %v", latest, err)
	}
	versions, err := c.ListStates(ctx, "chan-1", ListStatesOptions{})
	if err != nil || len(versions) != 1 || versions[0].Version != 1 || versions[0].Cursor != 3 {
		t.Fatalf("ListStates: %v %v", versions, err)
	}
	got, err := c.GetState(ctx, "chan-1", 1)
	if err != nil || got.State["mem"] != "old" || got.Version != 1 {
		t.Fatalf("GetState: %v %v", got, err)
	}
	if err := c.DeleteState(ctx, "chan-1", 1); err != nil {
		t.Fatalf("DeleteState: %v", err)
	}
}

func TestLatestStateEmpty(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/state/chan-1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"message": "no state yet"}`))
		},
	})
	c := newTestClient(t, ts)
	got, err := c.LatestState(context.Background(), "chan-1")
	if err != nil || got != nil {
		t.Fatalf("expected nil, got %v %v", got, err)
	}
}

func TestChannelsForkMetadataForks(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"channels": ["a", "b"]}`))
		},
		"POST /api/v1/channels/fork": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["source_channel_id"] != "a" || body["new_channel_id"] != "b" || body["at_cursor"] != float64(5) {
				t.Errorf("bad fork body: %v", body)
			}
			if body["operation_id"] == "" {
				t.Error("missing operation_id")
			}
			w.Write([]byte(`{"fork_id": "b", "source_channel_id": "a", "child_channel_id": "b", "requested_cursor": 5, "resolved_cursor": 5, "restorable": true, "replayed": false, "manifest": null, "reproducibility": "state_exact"}`))
		},
		"GET /api/v1/channels/b/metadata": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"channel_id": "b", "display_name": "B", "created_at": "t", "experiment_metadata": {"k": "v"}}`))
		},
		"GET /api/v1/channels/b/forks": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"forks": [{"channel_id": "c"}]}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	channels, err := c.ListChannels(ctx)
	if err != nil || len(channels) != 2 {
		t.Fatalf("ListChannels: %v %v", channels, err)
	}
	meta, err := c.Fork(ctx, "a", "b", 5, ForkOptions{OperationID: ptr("op-1")})
	if err != nil || meta.ChildChannelID != "b" || meta.ResolvedCursor != 5 {
		t.Fatalf("Fork: %v %v", meta, err)
	}
	got, err := c.GetChannelMetadata(ctx, "b")
	if err != nil || got == nil || got.ExperimentMetadata["k"] != "v" {
		t.Fatalf("GetChannelMetadata: %v %v", got, err)
	}
	forks, err := c.ListForks(ctx, "b")
	if err != nil || len(forks) != 1 || forks[0].ChannelID != "c" {
		t.Fatalf("ListForks: %v %v", forks, err)
	}
}

func ptr[T any](v T) *T { return &v }

func TestGetChannelMetadataNotFound(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{})
	c := newTestClient(t, ts)
	got, err := c.GetChannelMetadata(context.Background(), "missing")
	if err != nil || got != nil {
		t.Fatalf("expected (nil, nil) for 404, got %v %v", got, err)
	}
}

func TestGetForkTree(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels/root/fork-tree": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"channel_id": "root", "children": [{"channel_id": "child"}]}`))
		},
	})
	c := newTestClient(t, ts)
	tree, err := c.GetForkTree(context.Background(), "root")
	if err != nil || tree == nil || tree.ChannelID != "root" || len(tree.Children) != 1 {
		t.Fatalf("GetForkTree: %v %v", tree, err)
	}
}

func TestDiffStates(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels/diff": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			if r.URL.Query().Get("left") != "fix-a" || r.URL.Query().Get("right") != "fix-b" {
				t.Errorf("bad query: %v", r.URL.RawQuery)
			}
			w.Write([]byte(`{
				"left_channel_id": "fix-a", "right_channel_id": "fix-b",
				"left": {"channel_id": "fix-a", "cursor": 1, "state": {"step": 2, "x": true}},
				"right": {"channel_id": "fix-b", "cursor": 1, "state": {"step": 3, "y": true}},
				"common": {"channel_id": "root", "cursor": 1, "state": {"step": 1}},
				"left_diverged_at_cursor": 1, "right_diverged_at_cursor": 1, "truncated": false, "entry_count_total": 3, "max_entries": 500,
				"entries": [
					{"path": ["step"], "kind": "changed", "left": 2, "right": 3},
					{"path": ["x"], "kind": "removed", "left": true, "right": null},
					{"path": ["y"], "kind": "added", "left": null, "right": true}
				]
			}`))
		},
	})
	c := newTestClient(t, ts)
	diff, err := c.DiffStates(context.Background(), "fix-a", "fix-b")
	if err != nil {
		t.Fatalf("DiffStates: %v", err)
	}
	if diff.LeftChannelID != "fix-a" || diff.RightChannelID != "fix-b" {
		t.Errorf("bad channels: %+v", diff)
	}
	if diff.Common == nil || diff.Common.ChannelID != "root" {
		t.Errorf("bad common: %+v", diff.Common)
	}
	if diff.LeftDivergedAtCursor == nil || *diff.LeftDivergedAtCursor != 1 {
		t.Errorf("bad left diverged cursor: %v", diff.LeftDivergedAtCursor)
	}
	if diff.RightDivergedAtCursor == nil || *diff.RightDivergedAtCursor != 1 {
		t.Errorf("bad right diverged cursor: %v", diff.RightDivergedAtCursor)
	}
	if diff.Truncated {
		t.Error("expected truncated=false parse")
	}
	if diff.EntryCountTotal != 3 {
		t.Errorf("entry_count_total = %d, want 3", diff.EntryCountTotal)
	}
	if len(diff.Entries) != 3 {
		t.Fatalf("bad entries: %+v", diff.Entries)
	}
	if diff.Entries[0].Kind != "changed" || diff.Entries[0].Left != int64(2) || diff.Entries[0].Right != int64(3) {
		t.Errorf("bad changed entry: %+v", diff.Entries[0])
	}
	if diff.Entries[1].Kind != "removed" || diff.Entries[2].Kind != "added" {
		t.Errorf("bad kinds: %+v %+v", diff.Entries[1], diff.Entries[2])
	}
}

func TestDiffStatesNoCommon(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels/diff": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{
				"left_channel_id": "a", "right_channel_id": "b",
				"left": {"channel_id": "a", "cursor": 1, "state": {}},
				"right": {"channel_id": "b", "cursor": 1, "state": {}},
				"common": null, "left_diverged_at_cursor": null, "right_diverged_at_cursor": null, "truncated": false, "entry_count_total": 0, "max_entries": 500, "entries": []
			}`))
		},
	})
	c := newTestClient(t, ts)
	diff, err := c.DiffStates(context.Background(), "a", "b")
	if err != nil {
		t.Fatalf("DiffStates: %v", err)
	}
	if diff.Common != nil {
		t.Errorf("common should be nil, got %+v", diff.Common)
	}
	if len(diff.Entries) != 0 {
		t.Errorf("expected no entries, got %+v", diff.Entries)
	}
}

func TestDecisionTrail(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels/fork-c/trail": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{
				"channel_id": "fork-c", "origin_run_id": "root",
				"ancestry": [
					{"channel_id": "fork-c", "parent_channel_id": "fork-a", "forked_at_cursor": 5, "display_name": "C", "reason": null},
					{"channel_id": "fork-a", "parent_channel_id": "root", "forked_at_cursor": 3, "display_name": "A", "reason": "first"},
					{"channel_id": "root", "parent_channel_id": null, "forked_at_cursor": 0, "display_name": null, "reason": null}
				],
				"boundary": {"channel_id": "fork-a", "cursor": 3, "state": {"step": 3}},
				"executions": [
					{"id": "ex-1", "channel_id": "fork-c", "key_name": "calc", "tool_name": "calculator",
					 "status": "completed", "params": {"expr": "2+2"}, "result": {"ok": true}, "error": null,
					 "started_cursor": 6, "completed_cursor": 7}
				]
			}`))
		},
	})
	c := newTestClient(t, ts)
	trail, err := c.DecisionTrail(context.Background(), "fork-c")
	if err != nil {
		t.Fatalf("DecisionTrail: %v", err)
	}
	if trail.ChannelID != "fork-c" || trail.OriginRunID != "root" {
		t.Errorf("bad trail identity: %+v", trail)
	}
	if len(trail.Ancestry) != 3 {
		t.Fatalf("bad ancestry: %+v", trail.Ancestry)
	}
	if trail.Ancestry[0].ChannelID != "fork-c" || trail.Ancestry[2].ChannelID != "root" {
		t.Errorf("bad ancestry chain: %+v", trail.Ancestry)
	}
	if trail.Boundary == nil || trail.Boundary.ChannelID != "fork-a" || trail.Boundary.Cursor != 3 {
		t.Errorf("bad boundary: %+v", trail.Boundary)
	}
	if len(trail.Executions) != 1 || trail.Executions[0].KeyName != "calc" {
		t.Errorf("bad executions: %+v", trail.Executions)
	}
	if st := trail.Executions[0].StartedCursor; st == nil || *st != 6 {
		t.Errorf("bad started_cursor: %v", trail.Executions[0].StartedCursor)
	}
}

func TestUpdateMetadata(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"PUT /api/v1/channels/c/metadata": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if body["display_name"] != "New" {
				t.Errorf("bad body: %v", body)
			}
			w.Write([]byte(`{"channel_id": "c", "display_name": "New", "created_at": "t"}`))
		},
	})
	c := newTestClient(t, ts)
	name := "New"
	meta, err := c.UpdateMetadata(context.Background(), "c", UpdateMetadataOptions{
		DisplayName:        &name,
		ExperimentMetadata: map[string]any{"x": 1},
	})
	if err != nil || meta.DisplayName == nil || *meta.DisplayName != "New" {
		t.Fatalf("UpdateMetadata: %v %v", meta, err)
	}
}

func TestHealthMetrics(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /healthz": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"status": "ok", "timestamp": "t", "instance_id": "i1", "check_duration_ms": 5, "components": {"postgres": {"status": "ok", "latency_ms": 2}}}`))
		},
		"GET /readyz": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"status": "ready", "timestamp": "t", "instance_id": "i1", "readiness_checks": {"database_ready": true, "capacity_available": true, "uptime_seconds": 100, "connection_utilization": 0.5, "active_connections": 10, "max_connections": 50}}`))
		},
		"GET /metrics": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			w.Write([]byte("# TYPE actae_events_total counter\nactae_events_total 12\n"))
		},
		"GET /metrics.json": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"status": "ok", "websocket": {"connections": 3, "topics": 10}}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	hs, err := c.HealthCheck(ctx)
	if err != nil || hs.Status != "ok" || hs.Components["postgres"].Status != "ok" {
		t.Fatalf("HealthCheck: %+v %v", hs, err)
	}
	rr, err := c.ReadinessCheck(ctx)
	if err != nil || !rr.DatabaseReady || rr.ActiveConnections != 10 {
		t.Fatalf("ReadinessCheck: %+v %v", rr, err)
	}
	text, err := c.GetMetricsText(ctx)
	if err != nil || !strings.Contains(text, "actae_events_total") {
		t.Fatalf("GetMetricsText: %q %v", text, err)
	}
	mj, err := c.GetMetricsJSON(ctx)
	if err != nil || mj.WebsocketConnections != 3 {
		t.Fatalf("GetMetricsJSON: %+v %v", mj, err)
	}
}

func TestAuthFlow(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/auth/signup": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"token": "jwt-1", "user": {"id": "u1", "email": "a@b.c"}}`))
		},
		"POST /api/v1/auth/login": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"token": "jwt-2", "user": {"id": "u1", "email": "a@b.c"}}`))
		},
		"POST /api/v1/auth/logout": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			if r.Header.Get("authorization") != "Bearer jwt-2" {
				t.Error("missing bearer token")
			}
			w.Write([]byte(`{"success": true}`))
		},
		"GET /api/v1/auth/me": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"user": {"id": "u1", "email": "a@b.c", "role": "user"}}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	signup, err := c.Signup(ctx, SignupOptions{Email: "a@b.c", Password: "pw"})
	if err != nil || signup.Token != "jwt-1" || signup.User.Email != "a@b.c" {
		t.Fatalf("Signup: %+v %v", signup, err)
	}
	login, err := c.Login(ctx, "a@b.c", "pw")
	if err != nil || login.Token != "jwt-2" {
		t.Fatalf("Login: %+v %v", login, err)
	}
	if err := c.Logout(ctx, "jwt-2"); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	me, err := c.GetMe(ctx, "jwt-2")
	if err != nil || me.ID != "u1" {
		t.Fatalf("GetMe: %+v %v", me, err)
	}
}

func TestConsumerGroups(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/groups": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"group_id": "g1", "channel_id": "c1", "created_at": "t"}`))
		},
		"GET /api/v1/groups": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"groups": [{"group_id": "g1", "channel_id": "c1"}]}`))
		},
		"DELETE /api/v1/groups/g1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"deleted": true}`))
		},
		"POST /api/v1/groups/g1/join": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"group_id": "g1", "consumer_id": "consumer-1", "last_cursor": 4, "claimed_cursor": 4, "updated_at": "t"}`))
		},
		"POST /api/v1/groups/g1/work": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"group_id": "g1", "consumer_id": "consumer-1", "lease_until": "t", "events": [{"id": "e", "channel_id": "c1", "type": "t", "payload": {"x": 1}, "actor": "a", "cursor": 5, "timestamp": "t"}]}`))
		},
		"POST /api/v1/groups/g1/ack": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"acknowledged": true}`))
		},
		"POST /api/v1/groups/g1/heartbeat": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"ok": true}`))
		},
		"GET /api/v1/groups/g1/offsets": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"offsets": [{"group_id": "g1", "consumer_id": "consumer-1", "last_cursor": 4, "claimed_cursor": 4, "updated_at": "t"}]}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	g, err := c.CreateGroup(ctx, "g1", "c1", nil)
	if err != nil || g.GroupID != "g1" {
		t.Fatalf("CreateGroup: %+v %v", g, err)
	}
	groups, err := c.ListGroups(ctx, ListGroupsOptions{})
	if err != nil || len(groups) != 1 {
		t.Fatalf("ListGroups: %v %v", groups, err)
	}
	if err := c.DeleteGroup(ctx, "g1"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	off, err := c.JoinGroup(ctx, "g1", "consumer-1", 60)
	if err != nil || off.ClaimedCursor != 4 {
		t.Fatalf("JoinGroup: %+v %v", off, err)
	}
	work, err := c.ClaimWork(ctx, "g1", "consumer-1", 10)
	if err != nil || len(work.Events) != 1 || work.Events[0].Cursor != 5 {
		t.Fatalf("ClaimWork: %+v %v", work, err)
	}
	if err := c.AckWork(ctx, "g1", "consumer-1", 5); err != nil {
		t.Fatalf("AckWork: %v", err)
	}
	if err := c.Heartbeat(ctx, "g1", "consumer-1", 60); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	offsets, err := c.GroupOffsets(ctx, "g1")
	if err != nil || len(offsets) != 1 || offsets[0].ConsumerID != "consumer-1" {
		t.Fatalf("GroupOffsets: %v %v", offsets, err)
	}
}

func TestWakeups(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/scheduler/wakeups": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"id": "w1", "channel_id": "c1", "run_at": "2026-08-04T00:00:00Z", "status": "scheduled", "payload": {"x": 1}, "created_at": "t"}`))
		},
		"GET /api/v1/scheduler/wakeups": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"wakeups": [{"id": "w1", "channel_id": "c1", "run_at": "t", "status": "scheduled", "created_at": "t"}]}`))
		},
		"GET /api/v1/scheduler/wakeups/w1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"id": "w1", "channel_id": "c1", "run_at": "t", "status": "scheduled", "payload": {"x": 1}, "created_at": "t"}`))
		},
		"DELETE /api/v1/scheduler/wakeups/w1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"cancelled": true}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	w, err := c.ScheduleWakeup(ctx, "c1", "2026-08-04T00:00:00Z", map[string]any{"x": 1})
	if err != nil || w.ID != "w1" {
		t.Fatalf("ScheduleWakeup: %+v %v", w, err)
	}
	list, err := c.ListWakeups(ctx, ListWakeupsOptions{})
	if err != nil || len(list) != 1 {
		t.Fatalf("ListWakeups: %v %v", list, err)
	}
	got, err := c.GetWakeup(ctx, "w1")
	if err != nil || got == nil || got.Payload["x"] != int64(1) {
		t.Fatalf("GetWakeup: %+v %v", got, err)
	}
	cancelled, err := c.CancelWakeup(ctx, "w1")
	if err != nil || !cancelled {
		t.Fatalf("CancelWakeup: %v %v", cancelled, err)
	}
}

func TestExecutions(t *testing.T) {
	execJSON := `{"id": "ex1", "channel_id": "c1", "key_name": "k", "tool_name": "t", "status": "claimed", "attempts": 1, "params": {"p": 1}, "created_at": "t", "updated_at": "t"}`
	wrapped := func(inner string) string { return `{"execution": ` + inner + `}` }
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/executions/claim": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(`{"status": "claimed", "claim_token": "ct-1", "execution": ` + execJSON + `}`))
		},
		"POST /api/v1/executions/ex1/complete": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(wrapped(strings.Replace(execJSON, `"status": "claimed"`, `"status": "completed"`, 1) + `, "result": {"r": 2}`)))
		},
		"POST /api/v1/executions/ex1/fail": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(wrapped(strings.Replace(execJSON, `"status": "claimed"`, `"status": "failed"`, 1) + `, "error": {"message": "oops"}`)))
		},
		"POST /api/v1/executions/ex1/heartbeat": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(wrapped(execJSON)))
		},
		"POST /api/v1/executions/ex1/cancel": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			w.Write([]byte(wrapped(strings.Replace(execJSON, `"status": "claimed"`, `"status": "cancelled"`, 1))))
		},
		"GET /api/v1/executions/ex1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(wrapped(execJSON)))
		},
		"GET /api/v1/executions": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"executions": [` + execJSON + `]}`))
		},
		"DELETE /api/v1/executions/ex1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"deleted": true}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	claim, err := c.ClaimExecution(ctx, "c1", "k", "t", map[string]any{"p": 1}, ClaimExecutionOptions{})
	if err != nil || claim.Execution.ID != "ex1" || claim.ClaimToken == nil || *claim.ClaimToken != "ct-1" {
		t.Fatalf("ClaimExecution: %+v %v", claim, err)
	}
	tok := ptr("ct-1")
	done, err := c.CompleteExecution(ctx, "ex1", tok, map[string]any{"r": 2})
	if err != nil || done.Status != "completed" {
		t.Fatalf("CompleteExecution: %+v %v", done, err)
	}
	failed, err := c.FailExecution(ctx, "ex1", "oops", FailExecutionOptions{})
	if err != nil || failed.Status != "failed" {
		t.Fatalf("FailExecution: %+v %v", failed, err)
	}
	hb, err := c.HeartbeatExecution(ctx, "ex1", tok, nil)
	if err != nil || hb.Status != "claimed" {
		t.Fatalf("HeartbeatExecution: %+v %v", hb, err)
	}
	cc, err := c.CancelExecution(ctx, "ex1", tok)
	if err != nil || cc.Status != "cancelled" {
		t.Fatalf("CancelExecution: %+v %v", cc, err)
	}
	got, err := c.GetExecution(ctx, "ex1")
	if err != nil || got.ID != "ex1" {
		t.Fatalf("GetExecution: %+v %v", got, err)
	}
	list, err := c.ListExecutions(ctx, "c1", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListExecutions: %v %v", list, err)
	}
	if err := c.DeleteExecution(ctx, "ex1"); err != nil {
		t.Fatalf("DeleteExecution: %v", err)
	}
}

func TestErrorMapping(t *testing.T) {
	routes := map[string]routeHandler{
		"GET /rate-limited": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(429)
			w.Write([]byte(`{"error": "slow down", "retry_after_seconds": 17}`))
		},
		"GET /unauthorized": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(401)
			w.Write([]byte(`{"error": "invalid api key"}`))
		},
		"GET /snapshot-boundary": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "snapshot_boundary_required", "error": "no boundary"}`))
		},
		"GET /version-conflict": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "version_conflict", "error": "stale"}`))
		},
		"GET /consumer-not-found": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "consumer_not_found", "error": "gone"}`))
		},
		"GET /idempotency-mismatch": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "idempotency_key_mismatch", "error": "dup"}`))
		},
		"GET /execution-not-owned": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(409)
			w.Write([]byte(`{"status": "execution_not_owned", "error": "stale token"}`))
		},
		"GET /execution-not-found": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(404)
			w.Write([]byte(`{"status": "execution_not_found", "error": "execution_not_found"}`))
		},
		"GET /generic-500": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(500)
			w.Write([]byte(`{"error": "internal"}`))
		},
		"GET /plain-error": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(403)
			w.Write([]byte(`not json`))
		},
	}
	ts := newTestServer(t, routes)
	c := newTestClient(t, ts)

	check := func(path string, wantType func(err error) bool) {
		t.Helper()
		_, err := c.do(context.Background(), http.MethodGet, path, nil, nil, nil)
		if err == nil {
			t.Fatalf("%s: expected error", path)
		}
		if !wantType(err) {
			t.Fatalf("%s: got %T (%v), want typed error", path, err, err)
		}
	}

	check("/rate-limited", func(e error) bool { _, ok := e.(*RateLimitError); return ok })
	check("/unauthorized", func(e error) bool { _, ok := e.(*AuthError); return ok })
	check("/snapshot-boundary", func(e error) bool { _, ok := e.(*SnapshotBoundaryError); return ok })
	check("/version-conflict", func(e error) bool { _, ok := e.(*VersionConflictError); return ok })
	check("/consumer-not-found", func(e error) bool { _, ok := e.(*ConsumerError); return ok })
	check("/idempotency-mismatch", func(e error) bool { _, ok := e.(*IdempotencyKeyMismatchError); return ok })
	check("/execution-not-owned", func(e error) bool { _, ok := e.(*ExecutionNotOwnedError); return ok })
	check("/execution-not-found", func(e error) bool { _, ok := e.(*ExecutionNotFoundError); return ok })
	check("/generic-500", func(e error) bool {
		ae, ok := e.(*APIError)
		return ok && ae.StatusCode == 500
	})

	// rate limit carries retry-after
	_, err := c.do(context.Background(), http.MethodGet, "/rate-limited", nil, nil, nil)
	if rl, ok := err.(*RateLimitError); !ok || rl.RetryAfterSeconds != 17 {
		t.Errorf("RateLimitError retry_after = %v", err)
	}

	// plain-text 4xx maps to APIError with the raw body
	_, err = c.do(context.Background(), http.MethodGet, "/plain-error", nil, nil, nil)
	if ae, ok := err.(*APIError); !ok || ae.StatusCode != 403 || ae.Error() == "" {
		t.Errorf("plain error mapping failed: %v", err)
	}
}

func TestRateLimitDefaultRetryAfter(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /rate-limited": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.WriteHeader(429)
			w.Write([]byte(`{"error": "slow down"}`))
		},
	})
	c := newTestClient(t, ts)
	_, err := c.do(context.Background(), http.MethodGet, "/rate-limited", nil, nil, nil)
	if rl, ok := err.(*RateLimitError); !ok || rl.RetryAfterSeconds != 60 {
		t.Errorf("expected default retry_after 60, got %v", err)
	}
}

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(ClientOptions{}); err == nil {
		t.Error("expected error when APIKey missing")
	}
	if _, err := NewClient(ClientOptions{APIKey: "k"}); err == nil {
		t.Error("expected error when no endpoint")
	}
}

func TestEndpointDerivation(t *testing.T) {
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "https://example.com:8002"})
	if err != nil {
		t.Fatal(err)
	}
	if c.WSEndpoint() != "wss://example.com:8002/ws" {
		t.Errorf("WSEndpoint = %q", c.WSEndpoint())
	}
	c2, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://localhost:8002"})
	if err != nil {
		t.Fatal(err)
	}
	if c2.WSEndpoint() != "ws://localhost:8002/ws" {
		t.Errorf("WSEndpoint = %q", c2.WSEndpoint())
	}
	c3, err := NewClient(ClientOptions{APIKey: "k", WSEndpoint: "ws://custom:9999/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if c3.WSEndpoint() != "ws://custom:9999/ws" {
		t.Errorf("explicit WSEndpoint = %q", c3.WSEndpoint())
	}
}

func TestForkAutoOperationID(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"POST /api/v1/channels/fork": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
			op, _ := body["operation_id"].(string)
			if len(op) != 36 {
				t.Errorf("operation_id = %q, want UUID v4 length 36", op)
			}
			w.Write([]byte(`{"channel_id": "b"}`))
		},
	})
	c := newTestClient(t, ts)
	if _, err := c.Fork(context.Background(), "a", "b", 1, ForkOptions{}); err != nil {
		t.Fatalf("Fork: %v", err)
	}
}

func TestNewUUIDFormat(t *testing.T) {
	id := newUUID()
	if len(id) != 36 {
		t.Errorf("newUUID() = %q, want 36 chars", id)
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[4]) != 12 {
		t.Errorf("newUUID() = %q, bad format", id)
	}
	if parts[2][0] != '4' {
		t.Errorf("newUUID() = %q, version byte not 4", id)
	}
}

func TestErrorForJSONBody(t *testing.T) {
	// 4xx with JSON body should map through mapHTTPError
	err := mapHTTPError(409, map[string]any{"status": "execution_not_owned", "error": "nope"})
	if _, ok := err.(*ExecutionNotOwnedError); !ok {
		t.Errorf("mapHTTPError = %T", err)
	}
}

var _ = fmt.Sprintf

func TestValidateChannelID(t *testing.T) {
	valid := []string{"a", "channel-1", "ch_1", "project.a:step-2", "x" + strings.Repeat("y", 255)}
	for _, id := range valid {
		if err := validateChannelID(id); err != nil {
			t.Errorf("valid id %q rejected: %v", id, err)
		}
	}
	invalid := []string{"", "a/b", "a b", "a\tb", "a\"b", "a\\b", "a%", "x" + strings.Repeat("y", 256)}
	for _, id := range invalid {
		if err := validateChannelID(id); err == nil {
			t.Errorf("invalid id %q accepted", id)
		}
	}
}

func TestCapabilities(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/auth/capabilities": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"auth_method": "api_key", "key_source": "local", "permissions": {"read": ["project-a.*"], "write": ["*"], "delete": [], "admin": []}}`))
		},
	})
	c := newTestClient(t, ts)
	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !caps.CanRead("project-a.ch") {
		t.Error("should read project-a")
	}
	if caps.CanRead("project-b.ch") {
		t.Error("should NOT read project-b")
	}
	if !caps.CanWrite("x") {
		t.Error("should write *")
	}
	if !caps.CanPublish("x") || !caps.CanFork("x") {
		t.Error("write:* should imply publish/fork")
	}
	if caps.can("delete", "x") {
		t.Error("should NOT delete")
	}
}
