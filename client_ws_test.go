package actae

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsTestServer is a scripted WebSocket server speaking the Actae protocol.
type wsTestServer struct {
	t      *testing.T
	server *httptest.Server
	url    string

	mu           sync.Mutex
	authenticate bool
	authDelay    time.Duration
	onMessage    func(msg map[string]any, conn *websocket.Conn)
	// manualAcks disables the automatic broadcast Ack so a test can script
	// acks itself (reordering, stale delivery).
	manualAcks bool
	received   []map[string]any
	conns      []*websocket.Conn
	connWrites map[*websocket.Conn]*sync.Mutex
}

func (ts *wsTestServer) firstConn() *websocket.Conn {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.conns) == 0 {
		return nil
	}
	return ts.conns[0]
}

func newWSTestServer(t *testing.T) *wsTestServer {
	ts := &wsTestServer{t: t, authenticate: true, connWrites: map[*websocket.Conn]*sync.Mutex{}}
	upgrader := websocket.Upgrader{}
	ts.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ts.mu.Lock()
		ts.conns = append(ts.conns, conn)
		ts.connWrites[conn] = &sync.Mutex{}
		ts.mu.Unlock()
		go ts.handleConn(conn)
	}))
	ts.url = "ws" + strings.TrimPrefix(ts.server.URL, "http")
	t.Cleanup(ts.server.Close)
	return ts
}

// sendAll pushes a server→client frame to every connected client.
func (ts *wsTestServer) sendAll(raw string) {
	ts.mu.Lock()
	conns := make([]*websocket.Conn, len(ts.conns))
	copy(conns, ts.conns)
	ts.mu.Unlock()
	for _, conn := range conns {
		ts.send(conn, raw)
	}
}

// closeAll closes every tracked client connection (server-initiated close).
func (ts *wsTestServer) closeAll() {
	ts.mu.Lock()
	conns := make([]*websocket.Conn, len(ts.conns))
	copy(conns, ts.conns)
	ts.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (ts *wsTestServer) handleConn(conn *websocket.Conn) {
	defer conn.Close()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		ts.mu.Lock()
		ts.received = append(ts.received, msg)
		onMsg := ts.onMessage
		auth := ts.authenticate
		ts.mu.Unlock()

		if onMsg != nil {
			onMsg(msg, conn)
		}

		typ, _ := msg["type"].(string)
		switch typ {
		case "auth":
			if auth {
				time.Sleep(ts.authDelay)
				ts.send(conn, `{"Connection": {"event": "authenticated", "payload": {"connection_id": "conn-1"}}}`)
			}
		case "subscribe":
			topic, _ := msg["topic"].(string)
			cursor := 0
			if c, ok := msg["cursor"].(float64); ok {
				cursor = int(c)
			}
			ts.send(conn, `{"Subscription": {"topic": "`+topic+`", "event": "subscribed", "cursor": `+strconv.Itoa(cursor)+`, "payload": {"topic": "`+topic+`", "cursor": `+strconv.Itoa(cursor)+`}}}`)
		case "unsubscribe":
			topic, _ := msg["topic"].(string)
			ts.send(conn, `{"Subscription": {"event": "unsubscribed", "payload": {"topic": "`+topic+`"}}}`)
		case "broadcast":
			topic, _ := msg["topic"].(string)
			ts.mu.Lock()
			manual := ts.manualAcks
			ts.mu.Unlock()
			if manual {
				continue
			}
			payload, _ := msg["payload"]
			pb, _ := json.Marshal(payload)
			rid, _ := msg["request_id"].(string)
			ridField := ""
			if rid != "" {
				ridField = `, "request_id": "` + rid + `"`
			}
			ts.send(conn, `{"Ack": {"id": "ack-1", "channel_id": "`+topic+`", "event_type": "broadcast", "payload": `+string(pb)+`, "actor": "test", "cursor": 11, "channel_cursor": 5, "timestamp": "2026-08-03T00:00:00Z"`+ridField+`}}`)
		case "connection":
			if event, _ := msg["event"].(string); event == "ping" {
				ts.send(conn, `{"Connection": {"event": "pong", "payload": {}}}`)
			}
		}
	}
}

func (ts *wsTestServer) send(conn *websocket.Conn, raw string) {
	ts.mu.Lock()
	wmu := ts.connWrites[conn]
	ts.mu.Unlock()
	if wmu == nil {
		return
	}
	wmu.Lock()
	defer wmu.Unlock()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
		ts.t.Logf("ws server send failed: %v", err)
	}
}

func (ts *wsTestServer) receivedMessages() []map[string]any {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]map[string]any, len(ts.received))
	copy(out, ts.received)
	return out
}

func (ts *wsTestServer) setOnMessage(fn func(msg map[string]any, conn *websocket.Conn)) {
	ts.mu.Lock()
	ts.onMessage = fn
	ts.mu.Unlock()
}

func newWSClient(t *testing.T, ts *wsTestServer) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 0,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestNormalizeMsg(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
		want string
	}{
		{"connection", map[string]any{"Connection": map[string]any{"event": "connected"}}, "connection"},
		{"broadcast", map[string]any{"Broadcast": map[string]any{"topic": "t"}}, "broadcast"},
		{"subscription", map[string]any{"Subscription": map[string]any{"event": "subscribed"}}, "subscription"},
		{"ack", map[string]any{"Ack": map[string]any{"id": "1"}}, "ack"},
		{"cursor_sync", map[string]any{"CursorSync": map[string]any{"topic": "t"}}, "cursor_sync"},
		{"error", map[string]any{"Error": map[string]any{"message": "boom"}}, "error"},
		{"nack", map[string]any{"Nack": map[string]any{"id": "1"}}, "nack"},
		{"flat passthrough", map[string]any{"type": "broadcast"}, "broadcast"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeMsg(c.in)
			if got["type"] != c.want {
				t.Errorf("type = %v, want %v", got["type"], c.want)
			}
		})
	}
}

func TestConnectAndAuth(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !c.IsConnected() || !c.IsAuthenticated() {
		t.Fatal("expected connected + authenticated")
	}
	if c.ConnectionID() != "conn-1" {
		t.Errorf("ConnectionID = %q", c.ConnectionID())
	}
	// auth message was sent
	msgs := ts.receivedMessages()
	if len(msgs) != 1 || msgs[0]["type"] != "auth" || msgs[0]["api_key"] != "sk-test-123" {
		t.Errorf("auth message not sent: %v", msgs)
	}
	// connect twice is a no-op
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	if len(ts.receivedMessages()) != 1 {
		t.Error("second Connect should not re-send auth")
	}
}

func TestAuthRejected(t *testing.T) {
	ts := newWSTestServer(t)
	ts.mu.Lock()
	ts.authenticate = false
	ts.mu.Unlock()
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 0,
		Timeout:           300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	err = c.Connect(context.Background())
	var authErr *AuthError
	if err == nil {
		t.Fatal("expected AuthError")
	}
	if !asAuthError(err, &authErr) {
		t.Fatalf("got %T, want AuthError", err)
	}
}

func TestAuthTimeout(t *testing.T) {
	ts := newWSTestServer(t)
	ts.mu.Lock()
	ts.authenticate = false
	ts.mu.Unlock()
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 0,
		Timeout:           100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
	err = c.Connect(context.Background())
	if !asAuthError(err, new(*AuthError)) {
		t.Fatalf("got %T, want AuthError (timeout)", err)
	}
}

func asAuthError(err error, out **AuthError) bool {
	if ae, ok := err.(*AuthError); ok {
		*out = ae
		return true
	}
	return false
}

func TestSubscribeWaitAndUnsubscribe(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	var subscribedCb sync.Map
	c.OnSubscribed(func(topic string, cursor *int64) {
		subscribedCb.Store(topic, cursor)
	})

	cursor := int64(42)
	if err := c.Subscribe(context.Background(), "chan-1", &cursor, true); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	topics := c.SubscribedTopics()
	if topics["chan-1"] != 42 {
		t.Errorf("subscribed map = %v", topics)
	}
	if v, ok := subscribedCb.Load("chan-1"); !ok || v.(*int64) == nil || *v.(*int64) != 42 {
		t.Error("onSubscribed not called with cursor")
	}

	if err := c.Unsubscribe(context.Background(), "chan-1"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	// wait for the unsubscribed event to land
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := c.SubscribedTopics()["chan-1"]; !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := c.SubscribedTopics()["chan-1"]; ok {
		t.Error("topic still subscribed after unsubscribe")
	}

	msgs := ts.receivedMessages()
	if len(msgs) != 3 {
		t.Fatalf("expected auth+subscribe+unsubscribe, got %v", msgs)
	}
	if msgs[1]["type"] != "subscribe" || msgs[1]["cursor"] != float64(42) {
		t.Errorf("bad subscribe msg: %v", msgs[1])
	}
	if msgs[2]["type"] != "unsubscribe" {
		t.Errorf("bad unsubscribe msg: %v", msgs[2])
	}
}

func TestSubscribeNotConnected(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	err := c.Subscribe(context.Background(), "t", nil, false)
	if !isConnectionError(err) {
		t.Fatalf("got %T, want ConnectionError", err)
	}
	_, err = c.Publish(context.Background(), "t", map[string]any{"x": 1})
	if !isConnectionError(err) {
		t.Fatalf("got %T, want ConnectionError", err)
	}
}

func isConnectionError(err error) bool {
	_, ok := err.(*ConnectionError)
	return ok
}

func TestSubscribeTimeout(t *testing.T) {
	ts := newWSTestServer(t)
	ts.mu.Lock()
	ts.authenticate = false
	ts.mu.Unlock()
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Skipf("auth needed for this test: %v", err)
	}
	// server never confirms subscriptions
	ts.mu.Lock()
	ts.authenticate = true
	ts.onMessage = func(msg map[string]any, conn *websocket.Conn) {}
	ts.mu.Unlock()
	oldTimeout := subscribeAckTimeout
	subscribeAckTimeout = 100 * time.Millisecond
	defer func() { subscribeAckTimeout = oldTimeout }()

	err := c.Subscribe(context.Background(), "t", nil, true)
	if !isConnectionError(err) {
		t.Fatalf("got %T, want ConnectionError", err)
	}
}

func TestPublishAck(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev, err := c.Publish(context.Background(), "chan-1", map[string]any{"n": 1})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.ID != "ack-1" || ev.ChannelID != "chan-1" || ev.Cursor != 11 || ev.EventType != "broadcast" {
		t.Errorf("bad ack event: %+v", ev)
	}
	payload := ev.Payload.(map[string]any)
	if payload["n"] != int64(1) {
		t.Errorf("payload = %#v", ev.Payload)
	}
	msgs := ts.receivedMessages()
	if len(msgs) != 2 || msgs[1]["type"] != "broadcast" || msgs[1]["topic"] != "chan-1" {
		t.Errorf("broadcast not sent: %v", msgs)
	}
}

func TestPublishOperationID(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	op := "3f2f4a72-8e6a-4d1c-9b5e-1a2b3c4d5e6f"
	if _, err := c.Publish(context.Background(), "chan-1", map[string]any{"n": 1},
		PublishOptions{OperationID: &op}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msgs := ts.receivedMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected auth + broadcast, got %d messages", len(msgs))
	}
	if msgs[1]["operation_id"] != op {
		t.Errorf("operation_id not sent: %v", msgs[1])
	}
	// Without options the field is absent.
	if _, err := c.Publish(context.Background(), "chan-2", map[string]any{"n": 2}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msgs = ts.receivedMessages()
	if _, ok := msgs[2]["operation_id"]; ok {
		t.Errorf("operation_id sent without options: %v", msgs[2])
	}
}

func TestPublishConcurrentReorderedAcks(t *testing.T) {
	ts := newWSTestServer(t)
	ts.manualAcks = true
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Capture both publishes' request_ids and reply with acks in REVERSE
	// order — ack for publish B first, then ack for publish A. Each Publish
	// must return its OWN event (audit item 7); with a shared FIFO queue
	// they would swap acknowledgements.
	ridCh := make(chan string, 2)
	bothSeen := make(chan struct{})
	ts.setOnMessage(func(msg map[string]any, conn *websocket.Conn) {
		if msg["type"] == "broadcast" {
			rid, _ := msg["request_id"].(string)
			ridCh <- rid
			if len(ridCh) == 2 {
				close(bothSeen)
			}
		}
	})

	results := make(chan Event, 2)
	errs := make(chan error, 2)
	go func() {
		ev, err := c.Publish(context.Background(), "chan-a", map[string]any{"pub": "A"})
		if err != nil {
			errs <- err
			return
		}
		results <- ev
	}()
	go func() {
		ev, err := c.Publish(context.Background(), "chan-b", map[string]any{"pub": "B"})
		if err != nil {
			errs <- err
			return
		}
		results <- ev
	}()

	// Both publishes reached the server (and registered their waiters).
	select {
	case <-bothSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("both publishes never reached the server")
	}
	ridA := <-ridCh
	ridB := <-ridCh

	ts.send(ts.firstConn(), `{"Ack": {"id": "ack-B", "channel_id": "chan-b", "event_type": "broadcast", "payload": {"pub":"B"}, "actor": "t", "cursor": 2, "channel_cursor": 2, "timestamp": "t", "request_id": "`+ridB+`"}}`)
	ts.send(ts.firstConn(), `{"Ack": {"id": "ack-A", "channel_id": "chan-a", "event_type": "broadcast", "payload": {"pub":"A"}, "actor": "t", "cursor": 1, "channel_cursor": 1, "timestamp": "t", "request_id": "`+ridA+`"}}`)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			t.Fatalf("Publish failed: %v", err)
		case ev := <-results:
			switch ev.ChannelID {
			case "chan-a":
				if ev.ID != "ack-A" {
					t.Fatalf("chan-a publish got wrong ack: id=%s", ev.ID)
				}
				seen["A"] = true
			case "chan-b":
				if ev.ID != "ack-B" {
					t.Fatalf("chan-b publish got wrong ack: id=%s", ev.ID)
				}
				seen["B"] = true
			default:
				t.Fatalf("unexpected event channel: %s", ev.ChannelID)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for publish results: %v", seen)
		}
	}
	if !seen["A"] || !seen["B"] {
		t.Fatalf("both publishes must succeed, got %v", seen)
	}
}

func TestPublishStaleAckNotConsumed(t *testing.T) {
	ts := newWSTestServer(t)
	ts.manualAcks = true
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A short timeout makes publish A time out before its ack is delivered.
	c.timeout = 200 * time.Millisecond

	ridCh := make(chan string, 2)
	seen := make(chan struct{})
	var seenOnce sync.Once
	ts.setOnMessage(func(msg map[string]any, conn *websocket.Conn) {
		if msg["type"] == "broadcast" {
			rid, _ := msg["request_id"].(string)
			ridCh <- rid
			seenOnce.Do(func() { close(seen) })
		}
	})

	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		if _, err := c.Publish(context.Background(), "chan-a", map[string]any{"pub": "A"}); err == nil {
			t.Error("expected publish A to time out (no ack delivered)")
		}
	}()
	<-seen  // A's broadcast reached the server
	<-aDone // A has timed out
	ridA := <-ridCh

	// Deliver A's ack AFTER A timed out — it must be discarded, not stolen
	// by the next publish.
	ts.send(ts.firstConn(), `{"Ack": {"id": "ack-A", "channel_id": "chan-a", "event_type": "broadcast", "payload": {"pub":"A"}, "actor": "t", "cursor": 1, "channel_cursor": 1, "timestamp": "t", "request_id": "`+ridA+`"}}`)

	evCh := make(chan Event, 1)
	go func() {
		ev, err := c.Publish(context.Background(), "chan-b", map[string]any{"pub": "B"})
		if err != nil {
			t.Errorf("publish B failed: %v", err)
			return
		}
		evCh <- ev
	}()
	// Wait for B's broadcast, then ack it.
	ridB := <-ridCh
	ts.send(ts.firstConn(), `{"Ack": {"id": "ack-B", "channel_id": "chan-b", "event_type": "broadcast", "payload": {"pub":"B"}, "actor": "t", "cursor": 2, "channel_cursor": 2, "timestamp": "t", "request_id": "`+ridB+`"}}`)

	select {
	case ev := <-evCh:
		if ev.ChannelID != "chan-b" || ev.ID != "ack-B" {
			t.Fatalf("publish B consumed a stale ack: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publish B timed out waiting for its ack")
	}
}

func TestPublishEchoSelf(t *testing.T) {
	ts := newWSTestServer(t)
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 0,
		EchoSelf:          true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	echoed := make(chan Event, 1)
	c.OnMessage(func(topic string, event Event) {
		if topic == "chan-echo" {
			echoed <- event
		}
	})
	if _, err := c.Publish(context.Background(), "chan-echo", map[string]any{"n": 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case ev := <-echoed:
		if ev.Cursor != 11 || ev.ChannelID != "chan-echo" {
			t.Errorf("bad echoed event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("own publish not echoed to local callback")
	}
}

func TestOnMessageMultipleCallbacks(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 2)
	c.OnMessage(func(topic string, event Event) { got <- "first:" + topic })
	c.OnMessage(func(topic string, event Event) { got <- "second:" + topic })
	ts.sendAll(`{"Broadcast": {"topic": "chan-1", "payload": {"event": {"id": "e1", "channel_id": "chan-1", "event_type": "x", "payload": {}, "actor": "a", "cursor": 1, "timestamp": "t"}}}}`)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case s := <-got:
			seen[s] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for callbacks (%d/2): %v", i, seen)
		}
	}
	if !seen["first:chan-1"] || !seen["second:chan-1"] {
		t.Errorf("both callbacks must fire, got %v", seen)
	}
}

func TestSubscribeAndWait(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.SubscribeAndWait(context.Background(), "t", nil); err != nil {
		t.Fatalf("SubscribeAndWait: %v", err)
	}
	if cur, ok := c.SubscribedTopics()["t"]; !ok || cur != 0 {
		t.Errorf("topic not tracked: %v", c.SubscribedTopics())
	}
}

func TestStream(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := c.Stream(ctx, "chan-stream", nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// events for the streamed topic arrive on the channel
	ts.sendAll(`{"Broadcast": {"topic": "chan-stream", "payload": {"event": {"id": "e1", "channel_id": "chan-stream", "event_type": "x", "payload": {"n": 1}, "actor": "a", "cursor": 5, "timestamp": "t"}}}}`)
	// events for other topics are filtered out
	ts.sendAll(`{"Broadcast": {"topic": "other", "payload": {"event": {"id": "e2", "channel_id": "other", "event_type": "x", "payload": {}, "actor": "a", "cursor": 6, "timestamp": "t"}}}}`)
	select {
	case ev := <-events:
		if ev.Cursor != 5 || ev.ChannelID != "chan-stream" {
			t.Errorf("bad streamed event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no stream event delivered")
	}

	// a disconnect closes the stream channel (Python stream() parity)
	ts.closeAll()
	select {
	case _, ok := <-events:
		if ok {
			t.Error("stream channel must be closed after disconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream not closed after disconnect")
	}
}

func TestStreamNotConnected(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if _, err := c.Stream(context.Background(), "t", nil); !isConnectionError(err) {
		t.Fatalf("got %T, want ConnectionError", err)
	}
}

func TestBroadcastDelivery(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	type delivered struct {
		topic string
		event Event
	}
	ch := make(chan delivered, 4)
	c.OnMessage(func(topic string, event Event) {
		ch <- delivered{topic, event}
	})

	// tagged broadcast with nested event
	ts.sendAll(`{"Broadcast": {"topic": "chan-1", "payload": {"event": {"id": "e1", "channel_id": "chan-1", "event_type": "agent.step", "payload": {"k": "v"}, "actor": "a", "cursor": 1, "timestamp": "t", "delivery_id": "d1"}}}}`)
	// flat broadcast with inline payload
	ts.sendAll(`{"type": "broadcast", "topic": "chan-2", "payload": {"raw": true}}`)
	// flat broadcast with nested event
	ts.sendAll(`{"type": "broadcast", "topic": "chan-3", "payload": {"event": {"id": "e3", "channel_id": "chan-3", "type": "x", "payload": {"n": 2}, "actor": "b", "cursor": 3, "timestamp": "t"}}}`)

	for i := 0; i < 3; i++ {
		select {
		case d := <-ch:
			switch d.topic {
			case "chan-1":
				if d.event.ID != "e1" || d.event.EventType != "agent.step" || d.event.DeliveryID == nil || *d.event.DeliveryID != "d1" {
					t.Errorf("chan-1 event: %+v", d.event)
				}
				if p := d.event.Payload.(map[string]any); p["k"] != "v" {
					t.Errorf("chan-1 payload: %#v", d.event.Payload)
				}
			case "chan-2":
				if d.event.EventType != "broadcast" || d.event.ID != "" {
					t.Errorf("chan-2 event: %+v", d.event)
				}
				if p := d.event.Payload.(map[string]any); p["raw"] != true {
					t.Errorf("chan-2 payload: %#v", d.event.Payload)
				}
			case "chan-3":
				if d.event.EventType != "x" || d.event.Cursor != 3 {
					t.Errorf("chan-3 event: %+v", d.event)
				}
			default:
				t.Errorf("unexpected topic %q", d.topic)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for broadcast %d", i)
		}
	}
}

func TestErrorFrame(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan string, 2)
	c.OnError(func(msg string) { errCh <- msg })

	ts.sendAll(`{"Error": {"message": "rate limited"}}`)
	select {
	case m := <-errCh:
		if m != "rate limited" {
			t.Errorf("error message = %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onError not called")
	}

	// subscription error event
	ts.sendAll(`{"Subscription": {"event": "error", "payload": {"error": "sub failed"}}}`)
	select {
	case m := <-errCh:
		if m != "sub failed" {
			t.Errorf("sub error = %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onError not called for subscription error")
	}
}

func TestPingKeepAlive(t *testing.T) {
	ts := newWSTestServer(t)
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range ts.receivedMessages() {
			if m["type"] == "connection" && m["event"] == "ping" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no ping sent")
}

func TestDisconnect(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if c.IsConnected() {
		t.Fatal("still connected after Disconnect")
	}
	// auto-reconnect must be disabled
	if c.autoReconnect {
		t.Fatal("autoReconnect should be false after Disconnect")
	}
	if _, err := c.Publish(context.Background(), "t", map[string]any{}); !isConnectionError(err) {
		t.Fatalf("publish after disconnect: %v", err)
	}
}

func TestReconnectResubscribes(t *testing.T) {
	ts := newWSTestServer(t)
	ts.mu.Lock()
	ts.authenticate = false // will be enabled on reconnect
	ts.mu.Unlock()
	c, err := NewClient(ClientOptions{
		APIKey:            "sk-test-123",
		WSEndpoint:        ts.url,
		KeepAliveInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	// enable auth so connect succeeds
	ts.mu.Lock()
	ts.authenticate = true
	ts.mu.Unlock()

	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	cursor := int64(9)
	if err := c.Subscribe(context.Background(), "chan-1", &cursor, true); err != nil {
		t.Fatal(err)
	}

	reconnected := make(chan struct{}, 1)
	c.OnReconnect(func() { reconnected <- struct{}{} })

	// kill the server side of the connection
	ts.closeAll()

	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect happened")
	}
	if !c.IsConnected() {
		t.Fatal("not connected after reconnect")
	}
	if topics := c.SubscribedTopics(); topics["chan-1"] != 9 {
		t.Errorf("resubscribed cursors = %v, want chan-1=9", topics)
	}
}

func TestCursorSync(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ts.sendAll(`{"CursorSync": {"topic": "chan-1", "cursor": 77}}`)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if topics := c.SubscribedTopics(); topics["chan-1"] == 77 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cursor_sync not applied: %v", c.SubscribedTopics())
}

func TestCallbackPanicSafety(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.OnMessage(func(topic string, event Event) { panic("boom") })
	c.OnError(func(msg string) { panic("boom") })

	ts.sendAll(`{"Broadcast": {"topic": "t", "payload": {"raw": 1}}}`)
	ts.sendAll(`{"Error": {"message": "x"}}`)
	// if panics were not recovered, the reader goroutine would die; verify the
	// connection still works by publishing.
	time.Sleep(100 * time.Millisecond)
	if _, err := c.Publish(context.Background(), "t", map[string]any{"ok": true}); err != nil {
		t.Fatalf("connection dead after callback panic: %v", err)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var _ = atomic.Int64{}
