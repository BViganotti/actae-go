package state_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"

	"github.com/BViganotti/actae-go"
	"github.com/BViganotti/actae-go/state"
)

// sha256Hex mirrors the store's commandDigest so tests can assert the exact
// derived operation id.
func sha256Hex(m map[string]any) string {
	raw, _ := json.Marshal(m)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ------------------------------------------------------------------ //
// fake server harness (state package tests are standalone; they cannot
// reuse actae's internal testServer from the parent package's tests)
// ------------------------------------------------------------------ //

type fakeCall struct {
	method string
	path   string
	body   map[string]any
}

type fakeServer struct {
	t     *testing.T
	srv   *httptest.Server
	mu    sync.Mutex
	calls []fakeCall
	// snapshots is the state-version history (newest first).
	snapshots []stateVersion
	// transitions is the scripted outcome queue for POST /events/transition.
	// A 200 outcome evolves snapshots like the real server (new version +
	// cursor, state from the request).
	transitions []transitionOutcome
	// loadHijacks > 0 makes the next load-phase request die mid-connection.
	loadHijacks int
}

type stateVersion struct {
	version  int64
	cursor   int64
	state    map[string]any
	hasState bool
}

type transitionOutcome struct {
	status int
	// If true, conn is hijacked and closed with garbage (transport error).
	hijack bool
	body   map[string]any
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	fs := &fakeServer{t: t}
	fs.srv = httptest.NewServer(http.HandlerFunc(fs.handle))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	call := fakeCall{method: r.Method, path: r.URL.Path}
	if r.Body != nil && r.Body != http.NoBody {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			call.body = body
		}
	}
	fs.calls = append(fs.calls, call)
	fs.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	switch {
	case call.method == "GET" && call.path == "/api/v1/state/chan/versions":
		fs.mu.Lock()
		if fs.loadHijacks > 0 {
			fs.loadHijacks--
			fs.mu.Unlock()
			hijackAndKill(w, fs.t, true)
			return
		}
		var out []map[string]any
		for _, sv := range fs.snapshots {
			out = append(out, map[string]any{
				"version":   sv.version,
				"cursor":    sv.cursor,
				"timestamp": "2026-08-18T00:00:00Z",
			})
		}
		fs.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"versions": out})

	case call.method == "GET" && pathVersion(call.path) >= 0:
		fs.mu.Lock()
		defer fs.mu.Unlock()
		want := pathVersion(call.path)
		var snap *stateVersion
		for i := range fs.snapshots {
			if fs.snapshots[i].version == want {
				snap = &fs.snapshots[i]
				break
			}
		}
		if snap == nil || !snap.hasState {
			// Server contract for a missing blob: 404.
			http.Error(w, `{"error": "version not found"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"channel_id": "chan",
			"version":    snap.version,
			"cursor":     snap.cursor,
			"timestamp":  "2026-08-18T00:00:00Z",
			"state":      snap.state,
		})

	case call.method == "POST" && call.path == "/api/v1/events/transition":
		fs.mu.Lock()
		if len(fs.transitions) == 0 {
			fs.mu.Unlock()
			http.Error(w, `{"error":"unexpected transition","status":"not_found"}`, http.StatusNotFound)
			return
		}
		out := fs.transitions[0]
		fs.transitions = fs.transitions[1:]
		fs.mu.Unlock()

		if out.hijack {
			hijackAndKill(w, fs.t, false)
			return
		}
		w.WriteHeader(out.status)
		json.NewEncoder(w).Encode(out.body)

		if out.status == 200 {
			// The event committed: record the snapshot the request carried,
			// mirroring the real server's state evolution.
			fs.mu.Lock()
			state, _ := call.body["state"].(map[string]any)
			cursor := int64(len(fs.snapshots)) + 1
			fs.snapshots = append([]stateVersion{{version: cursor, cursor: cursor, state: state, hasState: true}}, fs.snapshots...)
			fs.mu.Unlock()
		}

	default:
		http.Error(w, `{"error": "not found", "status": "not_found"}`, http.StatusNotFound)
	}
}

// hijackAndKill hijacks the connection and kills it mid-response. With
// midBody=false it sends non-HTTP garbage (the client's transport parse
// fails at Do()); with midBody=true it sends a valid header with a larger
// Content-Length than it writes (the body read fails after the header).
func hijackAndKill(w http.ResponseWriter, t *testing.T, midBody bool) {
	t.Helper()
	hj, ok := w.(http.Hijacker)
	if !ok {
		t.Fatal("server does not support hijacking")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	if midBody {
		conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"vers"))
	} else {
		conn.Write([]byte("\x00\x01\x02\xffbroken"))
	}
	conn.Close()
}

// pathVersion parses the trailing version from
// /api/v1/state/chan/version/<n>; -1 when the path is not a version route.
func pathVersion(path string) int64 {
	const prefix = "/api/v1/state/chan/version/"
	if len(path) <= len(prefix) || path[:len(prefix)] != prefix {
		return -1
	}
	var v int64
	_, err := fmt.Sscanf(path[len(prefix):], "%d", &v)
	if err != nil {
		return -1
	}
	return v
}

func (fs *fakeServer) allCalls() []fakeCall {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]fakeCall, len(fs.calls))
	copy(out, fs.calls)
	return out
}

func (fs *fakeServer) transitionCalls() []fakeCall {
	var out []fakeCall
	for _, c := range fs.allCalls() {
		if c.method == "POST" && c.path == "/api/v1/events/transition" {
			out = append(out, c)
		}
	}
	return out
}

func (fs *fakeServer) client() *actae.Client {
	fs.t.Helper()
	c, err := actae.NewClient(actae.ClientOptions{
		Endpoint: fs.srv.URL,
		APIKey:   "sk-test-123",
	})
	if err != nil {
		fs.t.Fatal(err)
	}
	return c
}

func okTransition(stateVersion int64) transitionOutcome {
	return transitionOutcome{status: 200, body: map[string]any{
		"event": map[string]any{
			"id":         "evt-1",
			"channel_id": "chan",
			"type":       "counter.incremented",
			"payload":    map[string]any{},
			"cursor":     float64(4),
			"timestamp":  "2026-08-18T00:00:00Z",
		},
		"state_version": stateVersion,
	}}
}

func conflict() transitionOutcome {
	return transitionOutcome{status: 409, body: map[string]any{
		"error":  "conflict",
		"status": "version_conflict",
	}}
}

func idempotencyConflict() transitionOutcome {
	return transitionOutcome{status: 409, body: map[string]any{
		"error":  "key reuse",
		"status": "idempotency_conflict",
	}}
}

// counter is the fixture T: a deterministic incrementor.
type counter struct {
	Count int64 `json:"count"`
}

func incrementor() func(cur *counter) (state.Command, error) {
	return func(cur *counter) (state.Command, error) {
		cur.Count++
		return state.Command{
			EventType:    "counter.incremented",
			OperationKey: "increment",
			Payload:      map[string]any{"to": cur.Count},
			Actor:        "unit-test",
		}, nil
	}
}

// ------------------------------------------------------------------ //

func TestDeterministicOperationKey(t *testing.T) {
	idRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	a := actae.DeterministicOperationKey("state.commit", "counter.incremented", "chan", "increment")
	b := actae.DeterministicOperationKey("state.commit", "counter.incremented", "chan", "increment")
	if a != b {
		t.Fatalf("same inputs must produce the same key: %q vs %q", a, b)
	}
	if !idRe.MatchString(a) {
		t.Fatalf("key %q is not a RFC 4122 version-5 UUID", a)
	}

	otherAction := actae.DeterministicOperationKey("state.commit", "counter.decremented", "chan", "increment")
	otherIdentity := actae.DeterministicOperationKey("state.commit", "counter.incremented", "chan", "other")
	otherScope := actae.DeterministicOperationKey("other.scope", "counter.incremented", "chan", "increment")
	if a == otherAction || a == otherIdentity || a == otherScope {
		t.Fatal("different inputs must produce different keys")
	}

	// NUL-joined identity parts must not collide across boundaries:
	// ("a","b:c", nil) vs ("a","b","c").
	paired := actae.DeterministicOperationKey("s", "a", "b:c")
	tripled := actae.DeterministicOperationKey("s", "a", "b", "c")
	if paired == tripled {
		t.Fatal("identity part boundary collision")
	}

	// Long inputs stay valid UUIDs (truncation must not corrupt the format).
	long := actae.DeterministicOperationKey("s", "a", fmt.Sprintf("%01024d", 0))
	if !idRe.MatchString(long) {
		t.Fatalf("long input produced invalid key %q", long)
	}
}

func TestFirstCommitSendsZeroGuards(t *testing.T) {
	fs := newFakeServer(t)
	fs.transitions = []transitionOutcome{okTransition(1)}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tr := fs.transitionCalls()
	if len(tr) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(tr))
	}
	body := tr[0].body
	if v := body["expected_version"]; v != float64(0) {
		t.Fatalf("expected_version = %v, want 0 (no snapshot yet)", v)
	}
	if v := body["expected_cursor"]; v != float64(0) {
		t.Fatalf("expected_cursor = %v, want 0", v)
	}
	digest := sha256Hex(map[string]any{
		"type":     "counter.incremented",
		"payload":  map[string]any{"to": int64(1)},
		"state":    map[string]any{"count": int64(1)},
		"metadata": nil,
		"actor":    "unit-test",
	})
	wantOp := actae.DeterministicOperationKey("state.commit", "counter.incremented", "chan", "increment", digest)
	if got := body["operation_id"]; got != wantOp {
		t.Fatalf("operation_id = %v, want %s", got, wantOp)
	}
	if v := body["type"]; v != "counter.incremented" {
		t.Fatalf("event type = %v", v)
	}
	stateBody, _ := body["state"].(map[string]any)
	if stateBody["count"] != float64(1) {
		t.Fatalf("posted state = %v, want count=1", stateBody)
	}
	if res.StateVersion != 1 || res.State.Count != 1 || res.Event.Cursor != 4 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestCommitReloadsLatestSnapshot(t *testing.T) {
	fs := newFakeServer(t)
	fs.snapshots = []stateVersion{{version: 3, cursor: 9, state: map[string]any{"count": float64(3)}, hasState: true}}
	fs.transitions = []transitionOutcome{okTransition(4)}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tr := fs.transitionCalls()
	if len(tr) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(tr))
	}
	body := tr[0].body
	if v := body["expected_version"]; v != float64(3) {
		t.Fatalf("expected_version = %v, want 3", v)
	}
	if v := body["expected_cursor"]; v != float64(9) {
		t.Fatalf("expected_cursor = %v, want 9", v)
	}
	stateBody, _ := body["state"].(map[string]any)
	if stateBody["count"] != float64(4) {
		t.Fatalf("mutated state not posted: %v", stateBody)
	}
	if res.StateVersion != 4 || res.State.Count != 4 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestCommitFixedKeySequentialCommitsProduceDistinctOperationIDs(t *testing.T) {
	// Regression: the store previously derived the operation id from the
	// OperationKey alone, so two SEQUENTIAL commits from an incrementor
	// with a fixed key ("increment") reused the same id. The server silently
	// replays an existing operation id — the second commit then returned the
	// FIRST event (same cursor!) and wrote a duplicate snapshot at cursor 1,
	// corrupting the ledger (found live against the dev instance: versions 8
	// and 9 at the same cursor). The id now folds in a content digest, so
	// changed content is its own transition.
	fs := newFakeServer(t)
	fs.transitions = []transitionOutcome{okTransition(1), okTransition(2)}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	res2, err := s.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res2.State.Count != 2 || res2.StateVersion != 2 {
		t.Fatalf("second commit must be its own transition, got %+v", res2)
	}

	tr := fs.transitionCalls()
	if len(tr) != 2 {
		t.Fatalf("expected 2 transitions, got %d", len(tr))
	}
	if tr[0].body["operation_id"] == tr[1].body["operation_id"] {
		t.Fatalf("changed content must produce a new operation id, but both used %v", tr[0].body["operation_id"])
	}
	// The fake server evolved its snapshots like the real one: the second
	// commit loaded version 1 and guarded against it.
	if v := tr[1].body["expected_version"]; v != float64(1) {
		t.Fatalf("second commit expected_version = %v, want 1", v)
	}
	if v := tr[1].body["expected_cursor"]; v != float64(1) {
		t.Fatalf("second commit expected_cursor = %v, want 1", v)
	}
	// And the replayed state was the mutated one (never the stale first).
	stateBody, _ := tr[1].body["state"].(map[string]any)
	if stateBody["count"] != float64(2) {
		t.Fatalf("second commit posted state = %v, want count=2", stateBody)
	}
}

func TestCommitRetriesOnVersionConflict(t *testing.T) {
	fs := newFakeServer(t)
	fs.snapshots = []stateVersion{{version: 3, cursor: 9, state: map[string]any{"count": float64(3)}, hasState: true}}
	// First attempt conflicts; the retry (fresh load -> same snapshot) wins.
	fs.transitions = []transitionOutcome{conflict(), okTransition(4)}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tr := fs.transitionCalls()
	if len(tr) != 2 {
		t.Fatalf("expected 2 transitions, got %d", len(tr))
	}
	// Deterministic operation id: the retry reuses the exact same id.
	if tr[0].body["operation_id"] != tr[1].body["operation_id"] {
		t.Fatal("retry must reuse the same operation_id")
	}
	// The conflict path re-applied Mutate from a fresh load.
	stateBody, _ := tr[1].body["state"].(map[string]any)
	if stateBody["count"] != float64(4) {
		t.Fatalf("retry posted state = %v, want count=4 (Mutate re-applied)", stateBody)
	}
	if res.StateVersion != 4 || res.State.Count != 4 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestCommitExhaustsConflictRetries(t *testing.T) {
	fs := newFakeServer(t)
	fs.snapshots = []stateVersion{{version: 1, cursor: 2, state: map[string]any{"count": float64(1)}, hasState: true}}
	for i := 0; i < 20; i++ {
		fs.transitions = append(fs.transitions, conflict())
	}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(context.Background())
	var vc *actae.VersionConflictError
	if !errors.As(err, &vc) {
		t.Fatalf("expected VersionConflictError, got %v", err)
	}
	if n := len(fs.transitionCalls()); n != 5 {
		t.Fatalf("expected 5 attempts, got %d", n)
	}
}

func TestCommitRetriesTransportErrorWithSameOperationID(t *testing.T) {
	fs := newFakeServer(t)
	// First transition connection dies mid-request; retry succeeds.
	fs.transitions = []transitionOutcome{{status: 0, hijack: true}, okTransition(2)}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit should have recovered from the transport error: %v", err)
	}

	tr := fs.transitionCalls()
	if len(tr) != 2 {
		t.Fatalf("expected 2 transitions (1 failed + 1 retry), got %d", len(tr))
	}
	if tr[0].body["operation_id"] != tr[1].body["operation_id"] {
		t.Fatal("transport retry must reuse the same operation_id")
	}
	if res.StateVersion != 2 || res.State.Count != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestCommitPropagatesIdempotencyConflict(t *testing.T) {
	fs := newFakeServer(t)
	fs.transitions = []transitionOutcome{idempotencyConflict()}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(context.Background())
	var ic *actae.IdempotencyConflictError
	if !errors.As(err, &ic) {
		t.Fatalf("expected IdempotencyConflictError, got %v", err)
	}
	if n := len(fs.transitionCalls()); n != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", n)
	}
}

func TestCommitPropagatesLoadPhaseTransportError(t *testing.T) {
	// The load phase (ListStates/GetState) is idempotent by nature but is
	// NOT retried — a transport failure there must surface as a
	// ConnectionError without any transition attempt.
	fs := newFakeServer(t)
	fs.transitions = []transitionOutcome{okTransition(1)}
	fs.loadHijacks = 1

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(context.Background())
	var connErr *actae.ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("expected ConnectionError, got %v", err)
	}
	if n := len(fs.transitionCalls()); n != 0 {
		t.Fatalf("no transition must fire when the load fails, got %d", n)
	}
}

func TestCommitPropagatesMutateErrorWithoutTransition(t *testing.T) {
	fs := newFakeServer(t)
	sentinel := errors.New("mutate boom")
	s, err := state.New(fs.client(), "chan", func(cur *counter) (state.Command, error) {
		return state.Command{}, sentinel
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if n := len(fs.transitionCalls()); n != 0 {
		t.Fatalf("no transition must fire when Mutate fails, got %d", n)
	}
}

func TestNewValidation(t *testing.T) {
	fs := newFakeServer(t)
	if _, err := state.New(nil, "chan", incrementor()); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := state.New(fs.client(), "", incrementor()); err == nil {
		t.Fatal("empty channel must fail")
	}
	if _, err := state.New[*counter](fs.client(), "chan", nil); err == nil {
		t.Fatal("nil mutate must fail")
	}
}

func TestCommitRejectsInvalidCommands(t *testing.T) {
	fs := newFakeServer(t)
	fs.transitions = []transitionOutcome{okTransition(1)}
	cases := []struct {
		name string
		cmd  state.Command
	}{
		{"empty event type", state.Command{OperationKey: "k"}},
		{"empty operation key", state.Command{EventType: "e"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := state.New(fs.client(), "chan", func(cur *counter) (state.Command, error) { return tc.cmd, nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Commit(context.Background()); err == nil {
				t.Fatal("expected an error for an invalid command")
			}
		})
	}
}

func TestLoad(t *testing.T) {
	fs := newFakeServer(t)
	fs.snapshots = []stateVersion{{version: 3, cursor: 9, state: map[string]any{"count": float64(3)}, hasState: true}}

	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	cur, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cur == nil || cur.Count != 3 {
		t.Fatalf("Load = %+v, want count=3", cur)
	}

	v3, err := s.LoadVersion(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if v3 == nil || v3.Count != 3 {
		t.Fatalf("LoadVersion(3) = %+v", v3)
	}

	// Missing version -> 404 surfaces as an *actae.APIError (server
	// contract: HTTP 404 "version not found").
	_, err = s.LoadVersion(context.Background(), 99)
	var apiErr *actae.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("LoadVersion(99) = %v, want APIError 404", err)
	}
	if _, err := s.LoadVersion(context.Background(), 0); err == nil {
		t.Fatal("version 0 must be rejected")
	}
}

func TestLoadEmptyChannel(t *testing.T) {
	fs := newFakeServer(t)
	s, err := state.New(fs.client(), "chan", incrementor())
	if err != nil {
		t.Fatal(err)
	}
	cur, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cur != nil {
		t.Fatalf("Load on an empty channel = %+v, want nil", cur)
	}
}
