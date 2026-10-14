package actae

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageHandler receives (topic, event) for every broadcast message
// published on a subscribed topic.
type MessageHandler func(topic string, event Event)

// ErrorHandler receives a server-side error message string. Fires on
// subscription errors and server-side error frames.
type ErrorHandler func(message string)

// SubscribedHandler receives (topic, cursor) when a subscription is
// confirmed by the server. cursor is the topic cursor at subscription time
// (nil when unknown).
type SubscribedHandler func(topic string, cursor *int64)

// DisconnectedHandler fires when the WebSocket connection drops (before
// auto-reconnect).
type DisconnectedHandler func()

// ReconnectHandler fires after auto-reconnect completes and all topics have
// been resubscribed.
type ReconnectHandler func()

// wsTypeMap maps the server's externally-tagged enum keys to flat SDK
// message types.
var wsTypeMap = map[string]string{
	"Connection":   "connection",
	"Broadcast":    "broadcast",
	"Subscription": "subscription",
	"Ack":          "ack",
	"CursorSync":   "cursor_sync",
	"Error":        "error",
	"Nack":         "nack",
}

// subscribeAckTimeout bounds how long Subscribe(wait=true) waits for the
// server's confirmation (mirrors the Python SDK's 10s).
var subscribeAckTimeout = 10 * time.Second

// normalizeMsg converts externally-tagged Actae messages (e.g.
// {"Broadcast": {"topic": ..., "payload": ...}}) into the flat SDK format
// ({"type": "broadcast", ...}). Messages already in flat format are returned
// unchanged.
func normalizeMsg(data map[string]any) map[string]any {
	for key, typeName := range wsTypeMap {
		inner, ok := data[key].(map[string]any)
		if ok {
			inner["type"] = typeName
			return inner
		}
	}
	return data
}

// ---------------------------------------------------------------------------
// Callback registration
// ---------------------------------------------------------------------------

// OnMessage registers a broadcast callback. Callbacks accumulate: every
// registered callback is invoked (in registration order) for each
// broadcast, and all registered callbacks also receive events echoed by
// EchoSelf. To stop receiving messages, register a callback that checks a
// flag you can close.
func (c *Client) OnMessage(cb MessageHandler) {
	c.cbMu.Lock()
	c.onMessage = append(c.onMessage, cb)
	c.cbMu.Unlock()
}

// addMessageHandler registers a broadcast callback and returns an
// unregister function. Index-based removal: the handler list is
// append-only, so registration indices stay stable.
func (c *Client) addMessageHandler(cb MessageHandler) func() {
	c.cbMu.Lock()
	idx := len(c.onMessage)
	c.onMessage = append(c.onMessage, cb)
	c.cbMu.Unlock()
	return func() {
		c.cbMu.Lock()
		defer c.cbMu.Unlock()
		if idx >= len(c.onMessage) {
			return
		}
		c.onMessage = append(c.onMessage[:idx], c.onMessage[idx+1:]...)
	}
}

// addDisconnectedHandler registers an internal disconnect callback
// (single use per registration) and returns an unregister function. These
// are fired by the reader when a connected WebSocket drops, before
// auto-reconnect — used by Stream to end its channel.
func (c *Client) addDisconnectedHandler(cb func()) func() {
	c.cbMu.Lock()
	idx := len(c.onDisconnectedInternal)
	c.onDisconnectedInternal = append(c.onDisconnectedInternal, cb)
	c.cbMu.Unlock()
	return func() {
		c.cbMu.Lock()
		defer c.cbMu.Unlock()
		if idx >= len(c.onDisconnectedInternal) {
			return
		}
		c.onDisconnectedInternal = append(c.onDisconnectedInternal[:idx], c.onDisconnectedInternal[idx+1:]...)
	}
}

// OnError registers the WebSocket error callback (single; replaces previous).
func (c *Client) OnError(cb ErrorHandler) {
	c.cbMu.Lock()
	c.onError = cb
	c.cbMu.Unlock()
}

// OnSubscribed registers the subscription-confirmation callback (single).
func (c *Client) OnSubscribed(cb SubscribedHandler) {
	c.cbMu.Lock()
	c.onSubscribed = cb
	c.cbMu.Unlock()
}

// OnDisconnected registers the disconnection callback (single). Fires when
// the connection drops, before auto-reconnect.
func (c *Client) OnDisconnected(cb DisconnectedHandler) {
	c.cbMu.Lock()
	c.onDisconnected = cb
	c.cbMu.Unlock()
}

// OnReconnect registers the reconnection callback (single). Fires after
// auto-reconnect completes and all topics have been resubscribed.
func (c *Client) OnReconnect(cb ReconnectHandler) {
	c.cbMu.Lock()
	c.onReconnect = cb
	c.cbMu.Unlock()
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

// Connect opens the WebSocket connection to the Actae server and
// authenticates with the configured API key. Safe to call multiple times —
// subsequent calls are no-ops while already connected.
//
// Returns ConnectionError when the handshake fails and AuthError when the
// API key is rejected or authentication times out.
func (c *Client) Connect(ctx context.Context) error {
	c.wsMu.Lock()
	if c.connected {
		c.wsMu.Unlock()
		return nil
	}
	c.wsMu.Unlock()
	return c.doConnect(ctx)
}

func (c *Client) doConnect(ctx context.Context) error {
	dialer := websocket.Dialer{
		TLSClientConfig:  c.tlsConfig,
		HandshakeTimeout: c.timeout,
	}
	conn, _, err := dialer.DialContext(ctx, c.wsEndpoint, nil)
	if err != nil {
		return NewConnectionError("WebSocket connection failed: " + err.Error())
	}

	c.wsMu.Lock()
	oldConn := c.wsConn
	c.wsConn = conn
	c.wsGen++
	gen := c.wsGen
	c.connected = false
	c.authenticated = false
	c.connectionID = ""
	c.authCh = make(chan struct{})
	c.wsMu.Unlock()
	if oldConn != nil {
		_ = oldConn.Close()
	}

	go c.reader(gen)

	if err := c.sendJSON(map[string]any{"type": "auth", "api_key": c.apiKey}); err != nil {
		c.teardownConn()
		return err
	}

	authCh := c.currentAuthCh()
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	select {
	case <-authCh:
	case <-ctx.Done():
		c.teardownConn()
		return NewConnectionError("WebSocket connection failed: " + ctx.Err().Error())
	case <-timer.C:
		c.teardownConn()
		return NewAuthError("Authentication timed out")
	}

	c.wsMu.Lock()
	c.connected = true
	c.wsMu.Unlock()
	go c.keepAliveLoop()
	return nil
}

// Disconnect closes the WebSocket connection and stops auto-reconnect.
// Safe to call multiple times. After Disconnect, auto-reconnect stays
// disabled (mirrors the Python SDK).
func (c *Client) Disconnect() error {
	c.wsMu.Lock()
	c.autoReconnect = false
	c.wsMu.Unlock()
	c.teardownConn()
	return nil
}

func (c *Client) teardownConn() {
	c.wsMu.Lock()
	conn := c.wsConn
	c.wsConn = nil
	c.connected = false
	c.authenticated = false
	// Drop any in-flight publish waiters (audit item 7).
	c.pendingAcks = make(map[string]chan map[string]any)
	c.wsMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// IsConnected reports whether the WebSocket is connected and authenticated.
func (c *Client) IsConnected() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.connected
}

// IsAuthenticated reports whether WebSocket authentication completed.
func (c *Client) IsAuthenticated() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.authenticated
}

// ConnectionID returns the server-assigned connection ID, or "" before
// authentication completes.
func (c *Client) ConnectionID() string {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.connectionID
}

// SubscribedTopics returns a copy of the topic → cursor map for all active
// subscriptions.
func (c *Client) SubscribedTopics() map[string]int64 {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	out := make(map[string]int64, len(c.subscribed))
	for t, cur := range c.subscribed {
		out[t] = cur
	}
	return out
}

// ---------------------------------------------------------------------------
// Subscribe / publish
// ---------------------------------------------------------------------------

// Subscribe subscribes to a topic for real-time events. When cursor is
// non-nil, events from that cursor onward are replayed before live events.
// When wait is true, blocks until the server confirms (up to 10s).
func (c *Client) Subscribe(ctx context.Context, topic string, cursor *int64, wait bool) error {
	if err := validateChannelID(topic); err != nil {
		return err
	}
	c.wsMu.Lock()
	if !c.connected || c.wsConn == nil {
		c.wsMu.Unlock()
		return NewConnectionError("Not connected")
	}
	msg := map[string]any{"type": "subscribe", "topic": topic}
	if cursor != nil {
		msg["cursor"] = *cursor
	}
	var waiter chan struct{}
	if wait {
		waiter = make(chan struct{})
		c.subWaiters[topic] = waiter
	}
	c.wsMu.Unlock()

	if err := c.sendJSON(msg); err != nil {
		if waiter != nil {
			c.clearWaiter(topic, waiter)
		}
		return err
	}
	if !wait {
		return nil
	}
	timer := time.NewTimer(subscribeAckTimeout)
	defer timer.Stop()
	select {
	case <-waiter:
		return nil
	case <-timer.C:
		c.clearWaiter(topic, waiter)
		return NewConnectionError("Subscription confirmation for " + topic + " timed out")
	case <-ctx.Done():
		c.clearWaiter(topic, waiter)
		return ctx.Err()
	}
}

func (c *Client) clearWaiter(topic string, waiter chan struct{}) {
	c.wsMu.Lock()
	if cur, ok := c.subWaiters[topic]; ok && cur == waiter {
		delete(c.subWaiters, topic)
	}
	c.wsMu.Unlock()
}

// SubscribeAndWait subscribes to a topic and blocks until the server
// confirms (up to 10s) — the common case, without the positional wait
// bool. When cursor is non-nil, events from that cursor onward are
// replayed before live events.
func (c *Client) SubscribeAndWait(ctx context.Context, topic string, cursor *int64) error {
	return c.Subscribe(ctx, topic, cursor, true)
}

// Stream subscribes to a topic and returns a channel of live events
// (Python SDK `stream()` parity). When cursor is non-nil, events with a
// cursor strictly greater than it are replayed before live events.
//
// The channel is buffered (256 events); when the buffer is full events
// are dropped with a log line rather than blocking the reader. The
// channel is closed when the WebSocket disconnects or ctx is cancelled —
// drain until closed, or cancel ctx to stop early. Registering a stream
// does not interfere with OnMessage callbacks; both receive broadcasts.
//
// Returns ConnectionError when the client is not connected.
func (c *Client) Stream(ctx context.Context, topic string, cursor *int64) (<-chan Event, error) {
	c.wsMu.Lock()
	connected := c.connected && c.wsConn != nil
	c.wsMu.Unlock()
	if !connected {
		return nil, NewConnectionError("Not connected")
	}

	ch := make(chan Event, 256)
	done := make(chan struct{})
	var once sync.Once
	closeCh := func() {
		once.Do(func() {
			close(ch)
			close(done)
		})
	}

	unsubMsg := c.addMessageHandler(func(t string, event Event) {
		if t != topic {
			return
		}
		select {
		case ch <- event:
		default:
			log.Printf("actae: stream queue full for %s, dropping event", topic)
		}
	})
	unsubDisc := c.addDisconnectedHandler(closeCh)

	if err := c.Subscribe(ctx, topic, cursor, true); err != nil {
		unsubMsg()
		unsubDisc()
		return nil, err
	}

	go func() {
		defer unsubMsg()
		defer unsubDisc()
		select {
		case <-ctx.Done():
		case <-done:
		}
		closeCh()
	}()
	return ch, nil
}

// Unsubscribe stops delivery of events for a topic.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	c.wsMu.Lock()
	connected := c.connected && c.wsConn != nil
	c.wsMu.Unlock()
	if !connected {
		return NewConnectionError("Not connected")
	}
	return c.sendJSON(map[string]any{"type": "unsubscribe", "topic": topic})
}

// PublishOptions configures Publish.
type PublishOptions struct {
	// OperationID makes the publish idempotent: retrying with the same
	// topic + operation_id replays the original persisted event instead
	// of duplicating. The WS path otherwise has no idempotency key — a
	// timed-out publish retried without one can duplicate the event.
	OperationID *string
}

// Publish broadcasts an event to a topic over WebSocket. The server
// persists the event and sends back an Ack carrying the full event (cursor,
// channel_cursor (alias of the per-channel cursor), timestamp, payload); Publish returns that persisted Event
// without an extra HTTP round-trip.
//
// The server does not echo a broadcast back to the publishing connection;
// with ClientOptions.EchoSelf set, the returned event is also delivered to
// the locally registered OnMessage callbacks.
//
// Optional PublishOptions may be passed; set OperationID (a stable UUID) to
// make retries idempotent — a retry with the same (topic, operation_id)
// replays the original event instead of inserting a duplicate.
//
// Returns APIError when no Ack arrives within the configured timeout.
func (c *Client) Publish(ctx context.Context, topic string, payload any, opts ...PublishOptions) (Event, error) {
	if err := validateChannelID(topic); err != nil {
		return Event{}, err
	}
	c.wsMu.Lock()
	connected := c.connected && c.wsConn != nil
	c.wsMu.Unlock()
	if !connected {
		return Event{}, NewConnectionError("Not connected")
	}

	// Correlate the ack to THIS publish via a fresh request_id. The server
	// echoes it verbatim in the Ack, so concurrent publishes (and a delayed
	// ack arriving after a timeout) can never be consumed by the wrong
	// caller (audit item 7). Without correlation, two concurrent publishes
	// could each receive the other's acknowledgement.
	requestID := newUUID()
	waiter := make(chan map[string]any, 1)
	c.wsMu.Lock()
	if _, dup := c.pendingAcks[requestID]; dup {
		c.wsMu.Unlock()
		return Event{}, NewAPIError(409, "duplicate request_id in flight")
	}
	c.pendingAcks[requestID] = waiter
	c.wsMu.Unlock()
	defer func() {
		c.wsMu.Lock()
		delete(c.pendingAcks, requestID)
		c.wsMu.Unlock()
	}()

	msg := map[string]any{
		"type":       "broadcast",
		"topic":      topic,
		"payload":    payload,
		"request_id": requestID,
	}
	if len(opts) > 0 && opts[0].OperationID != nil {
		msg["operation_id"] = *opts[0].OperationID
	}
	if err := c.sendJSON(msg); err != nil {
		return Event{}, err
	}
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	select {
	case ack := <-waiter:
		ev := eventFromAck(ack, topic, payload)
		if c.echoSelf {
			c.fireMessage(topic, ev)
		}
		return ev, nil
	case <-timer.C:
		return Event{}, NewAPIError(500, "No Ack received from server after publish")
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

// fireMessage invokes every registered OnMessage callback for an event.
// Panics inside callbacks are recovered (safeCall).
func (c *Client) fireMessage(topic string, event Event) {
	c.cbMu.RLock()
	cbs := make([]MessageHandler, len(c.onMessage))
	copy(cbs, c.onMessage)
	c.cbMu.RUnlock()
	for _, cb := range cbs {
		safeCall(func() { cb(topic, event) })
	}
}

func eventFromAck(m map[string]any, topic string, payload any) Event {
	channelID := m["channel_id"]
	if channelID == nil {
		channelID = topic
	}
	eventType := m["event_type"]
	if eventType == nil {
		eventType = "broadcast"
	}
	ackPayload := m["payload"]
	if ackPayload == nil {
		ackPayload = payload
	}
	return Event{
		ID:            str(m, "id"),
		ChannelID:     channelID.(string),
		EventType:     eventType.(string),
		Payload:       unwrapSonic(ackPayload),
		Actor:         str(m, "actor"),
		Cursor:        intOf(m, "cursor", 0),
		ChannelCursor: int64ptr(m, "channel_cursor"),
		Timestamp:     str(m, "timestamp"),
	}
}

// ---------------------------------------------------------------------------
// Reader and dispatch
// ---------------------------------------------------------------------------

func (c *Client) reader(gen int) {
	c.wsMu.Lock()
	conn := c.wsConn
	c.wsMu.Unlock()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		m, err := decodeValue(data)
		if err != nil {
			continue
		}
		msgMap, ok := m.(map[string]any)
		if !ok {
			continue
		}
		c.dispatch(msgMap)
	}
	c.handleDisconnect(gen)
}

func (c *Client) handleDisconnect(gen int) {
	c.wsMu.Lock()
	if gen != c.wsGen {
		// Stale reader from a previous connection; the current connection is
		// unaffected.
		c.wsMu.Unlock()
		return
	}
	wasConnected := c.connected
	c.connected = false
	c.authenticated = false
	c.wsConn = nil
	// Any publishes still awaiting an ack on the dead connection will
	// time out; drop their waiters so they can never match a future
	// connection's ack (audit item 7).
	c.pendingAcks = make(map[string]chan map[string]any)
	autoReconnect := c.autoReconnect
	c.wsMu.Unlock()

	if wasConnected {
		c.cbMu.RLock()
		onDisc := c.onDisconnected
		internals := make([]func(), len(c.onDisconnectedInternal))
		copy(internals, c.onDisconnectedInternal)
		c.cbMu.RUnlock()
		if onDisc != nil {
			safeCall(func() { onDisc() })
		}
		for _, cb := range internals {
			safeCall(cb)
		}
		if autoReconnect {
			go c.reconnectLoop()
		}
	}
}

func (c *Client) dispatch(data map[string]any) {
	data = normalizeMsg(data)
	switch msgType := str(data, "type"); msgType {
	case "connection":
		event := str(data, "event")
		payload := mapOf(data, "payload")
		switch event {
		case "connected":
			c.wsMu.Lock()
			c.connectionID = str(payload, "connection_id")
			c.wsMu.Unlock()
		case "authenticated":
			c.wsMu.Lock()
			c.authenticated = true
			if id := str(payload, "connection_id"); id != "" {
				c.connectionID = id
			}
			ch := c.authCh
			c.wsMu.Unlock()
			if ch != nil {
				close(ch)
			}
		case "pong":
		}

	case "broadcast":
		topic := str(data, "topic")
		var event Event
		if inner, ok := data["payload"].(map[string]any); ok {
			if innerEvent, ok := inner["event"].(map[string]any); ok {
				event = eventFromBroadcast(innerEvent)
			} else {
				event = eventFromBroadcast(map[string]any{"id": "", "payload": data["payload"], "type": "broadcast"})
			}
		} else {
			event = eventFromBroadcast(map[string]any{"id": "", "payload": data["payload"], "type": "broadcast"})
		}
		c.fireMessage(topic, event)

	case "subscription":
		event := str(data, "event")
		payload := mapOf(data, "payload")
		switch event {
		case "subscribed":
			topic := str(data, "topic")
			if topic == "" {
				topic = str(payload, "topic")
			}
			cursor := int64ptr(data, "cursor")
			if cursor == nil {
				cursor = int64ptr(payload, "cursor")
			}
			c.wsMu.Lock()
			if cursor != nil {
				c.subscribed[topic] = *cursor
			} else {
				c.subscribed[topic] = 0
			}
			waiter := c.subWaiters[topic]
			delete(c.subWaiters, topic)
			c.wsMu.Unlock()
			c.cbMu.RLock()
			cb := c.onSubscribed
			c.cbMu.RUnlock()
			if cb != nil {
				safeCall(func() { cb(topic, cursor) })
			}
			// Wake Subscribe only after the callback has run. This preserves the
			// documented ordering guarantee for callers that wait=true.
			if waiter != nil {
				close(waiter)
			}
		case "unsubscribed":
			topic := str(payload, "topic")
			if topic == "" {
				topic = str(data, "topic")
			}
			c.wsMu.Lock()
			delete(c.subscribed, topic)
			c.wsMu.Unlock()
		case "error":
			msg := str(payload, "error")
			if msg == "" {
				msg = "Subscription failed"
			}
			c.fireError(msg)
		}

	case "cursor_sync":
		topic := str(data, "topic")
		c.wsMu.Lock()
		c.subscribed[topic] = intOf(data, "cursor", 0)
		c.wsMu.Unlock()

	case "ack":
		// Correlate by request_id (audit item 7): a Publish registers a
		// waiter under its request_id and only its own ack is delivered to
		// it. Acks without a request_id (legacy servers) fall back to the
		// shared FIFO queue; stale acks for a request that already timed out
		// are dropped.
		if rid := str(data, "request_id"); rid != "" {
			c.wsMu.Lock()
			waiter := c.pendingAcks[rid]
			delete(c.pendingAcks, rid)
			c.wsMu.Unlock()
			if waiter != nil {
				select {
				case waiter <- data:
				default:
					log.Printf("actae: ack %s dropped (waiter not reading)", rid)
				}
			}
			return
		}
		select {
		case c.ackCh <- data:
		default:
			log.Printf("actae: ack queue full, dropping ack")
		}

	case "error":
		msg := str(data, "message")
		if msg == "" {
			msg = "Unknown WebSocket error"
		}
		c.fireError(msg)
	}
}

func (c *Client) fireError(msg string) {
	c.cbMu.RLock()
	cb := c.onError
	c.cbMu.RUnlock()
	if cb != nil {
		safeCall(func() { cb(msg) })
	}
}

// ---------------------------------------------------------------------------
// Reconnection and keepalive
// ---------------------------------------------------------------------------

func (c *Client) reconnectLoop() {
	delay := 500 * time.Millisecond
	maxDelay := 30 * time.Second
	for {
		time.Sleep(delay)

		c.wsMu.Lock()
		if c.connected || !c.autoReconnect {
			c.wsMu.Unlock()
			return
		}
		c.wsMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		err := c.doConnect(ctx)
		cancel()
		if err != nil {
			c.wsMu.Lock()
			c.reconnectFailures++
			failures := c.reconnectFailures
			c.wsMu.Unlock()
			if failures >= c.maxReconnectFailures {
				c.wsMu.Lock()
				c.autoReconnect = false
				c.wsMu.Unlock()
				log.Printf("actae: reconnect failed %d times (max %d), giving up", failures, c.maxReconnectFailures)
				return
			}
			if delay*2 > maxDelay {
				delay = maxDelay
			} else {
				delay *= 2
			}
			continue
		}

		c.wsMu.Lock()
		topics := make(map[string]int64, len(c.subscribed))
		for t, cur := range c.subscribed {
			topics[t] = cur
		}
		c.wsMu.Unlock()

		for topic, cur := range topics {
			var cursor *int64
			if cur > 0 {
				cur := cur
				cursor = &cur
			}
			if err := c.Subscribe(context.Background(), topic, cursor, true); err != nil {
				log.Printf("actae: resubscribe to %s failed: %v", topic, err)
			}
		}

		c.wsMu.Lock()
		c.reconnectFailures = 0
		c.wsMu.Unlock()

		c.cbMu.RLock()
		onReconn := c.onReconnect
		c.cbMu.RUnlock()
		if onReconn != nil {
			safeCall(func() { onReconn() })
		}
		return
	}
}

// keepAliveLoop sends JSON connection pings every KeepAliveInterval seconds
// (server drops idle connections after 300s). Exits when the connection is
// gone.
func (c *Client) keepAliveLoop() {
	interval := c.keepAlive
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := c.sendJSON(map[string]any{"type": "connection", "event": "ping", "payload": map[string]any{}}); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// sendJSON writes a message to the WebSocket. Returns ConnectionError when
// not connected. Writes are serialized with wsWriteMu — gorilla's Conn is not
// safe for concurrent writers, and concurrent Publish/keepalive must not race
// on SetWriteDeadline/WriteJSON.
func (c *Client) sendJSON(v any) error {
	c.wsMu.Lock()
	conn := c.wsConn
	c.wsMu.Unlock()
	if conn == nil {
		return NewConnectionError("Not connected")
	}
	c.wsWriteMu.Lock()
	defer c.wsWriteMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return conn.WriteJSON(v)
}

func (c *Client) currentAuthCh() chan struct{} {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.authCh
}

func safeCall(f func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("actae: callback panicked: %v", r)
		}
	}()
	f()
}
