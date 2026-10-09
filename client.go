package actae

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ClientOptions configures an ActaeClient. Endpoint (HTTP base URL) or
// WSEndpoint must be provided.
type ClientOptions struct {
	// APIKey is the API key for authentication. Required.
	APIKey string
	// Endpoint is the HTTP API base URL (e.g. "http://localhost:8002").
	Endpoint string
	// WSEndpoint is the WebSocket URL. Auto-derived from Endpoint when
	// omitted (https:// → wss://, http:// → ws://, /ws appended).
	WSEndpoint string
	// TLSConfig is a pre-built TLS configuration (custom CAs, mTLS).
	// Applied to both the HTTP transport and the WebSocket dialer.
	TLSConfig *tls.Config
	// ClientCert / ClientKey are mTLS client certificate and key file paths.
	ClientCert string
	// ClientKey is the mTLS client private key file path.
	ClientKey string
	// CACert is a custom CA certificate file path (self-signed certs).
	CACert string
	// Timeout is the timeout for HTTP and WebSocket operations
	// (default 30s).
	Timeout time.Duration
	// AutoReconnect controls WebSocket auto-reconnect on drop. nil means
	// true (reconnect enabled); set to a non-nil value to override. It is
	// a pointer so that setting Timeout can never accidentally change the
	// default.
	AutoReconnect *bool
	// MaxReconnectFailures is the number of consecutive failed reconnect
	// attempts before giving up (default 10).
	MaxReconnectFailures int
	// KeepAliveInterval controls the JSON ping cadence sent to keep the
	// WebSocket alive (server drops connections idle > 300s). Zero uses the
	// 30s default; a negative value disables client pings.
	KeepAliveInterval time.Duration
	// EchoSelf delivers your own Publish'd events to the locally registered
	// OnMessage callbacks. The server does not echo a broadcast back to the
	// connection that published it, so without this flag a single client
	// that subscribes AND publishes never sees its own events. Default
	// false (wire parity with the Python SDK); the server still persists
	// and delivers to all other connections.
	EchoSelf bool
	// HTTPClient overrides the underlying HTTP client (default: shared
	// client with Timeout). When set, Timeout is ignored for HTTP calls
	// and the TLS options (TLSConfig/CACert/ClientCert/ClientKey) are NOT
	// applied — configure the override's own Transport for TLS.
	HTTPClient *http.Client
}

// Client is the Actae client. It provides the full HTTP API and an optional
// WebSocket connection for real-time subscribe/publish. Safe for concurrent
// use by multiple goroutines.
type Client struct {
	apiKey        string
	endpoint      string
	wsEndpoint    string
	timeout       time.Duration
	autoReconnect bool
	keepAlive     time.Duration
	echoSelf      bool
	httpClient    *http.Client
	tlsConfig     *tls.Config

	// WebSocket state (guarded by wsMu).
	wsMu                 sync.Mutex
	wsConn               *websocket.Conn
	wsWriteMu            sync.Mutex // serializes writes to the underlying conn
	wsGen                int
	connected            bool
	authenticated        bool
	connectionID         string
	subscribed           map[string]int64
	subWaiters           map[string]chan struct{}
	authCh               chan struct{}
	ackCh                chan map[string]any
	pendingAcks          map[string]chan map[string]any
	reconnectFailures    int
	maxReconnectFailures int

	// Callbacks (guarded by cbMu).
	cbMu                   sync.RWMutex
	onMessage              []MessageHandler
	onError                ErrorHandler
	onSubscribed           SubscribedHandler
	onDisconnected         DisconnectedHandler
	onReconnect            ReconnectHandler
	onDisconnectedInternal []func()
}

// NewClient creates an Actae client. It panics-free; validation errors are
// returned.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.APIKey == "" {
		return nil, fmt.Errorf("api_key is required")
	}
	if opts.Endpoint == "" && opts.WSEndpoint == "" {
		return nil, fmt.Errorf("at least one of endpoint or ws_endpoint is required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	autoReconnect := true
	if opts.AutoReconnect != nil {
		autoReconnect = *opts.AutoReconnect
	}
	keepAlive := opts.KeepAliveInterval
	if keepAlive < 0 {
		keepAlive = 0
	} else if keepAlive == 0 {
		keepAlive = 30 * time.Second
	}
	maxReconnect := opts.MaxReconnectFailures
	if maxReconnect <= 0 {
		maxReconnect = 10
	}

	tlsCfg, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
		if tlsCfg != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = tlsCfg
			httpClient.Transport = transport
		}
	}

	endpoint := strings.TrimSuffix(opts.Endpoint, "/")
	wsEndpoint := opts.WSEndpoint
	if wsEndpoint == "" && endpoint != "" {
		derived := strings.Replace(strings.Replace(endpoint, "https://", "wss://", 1), "http://", "ws://", 1)
		if strings.HasSuffix(derived, "/ws") {
			wsEndpoint = derived
		} else {
			wsEndpoint = derived + "/ws"
		}
	} else if wsEndpoint != "" {
		wsEndpoint = strings.TrimSuffix(wsEndpoint, "/")
	}

	return &Client{
		apiKey:               opts.APIKey,
		endpoint:             endpoint,
		wsEndpoint:           wsEndpoint,
		timeout:              timeout,
		autoReconnect:        autoReconnect,
		keepAlive:            keepAlive,
		echoSelf:             opts.EchoSelf,
		httpClient:           httpClient,
		tlsConfig:            tlsCfg,
		subscribed:           make(map[string]int64),
		subWaiters:           make(map[string]chan struct{}),
		ackCh:                make(chan map[string]any, 256),
		pendingAcks:          make(map[string]chan map[string]any),
		maxReconnectFailures: maxReconnect,
	}, nil
}

func buildTLSConfig(opts ClientOptions) (*tls.Config, error) {
	cfg := opts.TLSConfig
	if opts.ClientCert == "" && opts.ClientKey == "" && opts.CACert == "" {
		return cfg, nil
	}
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if opts.CACert != "" {
		pem, err := os.ReadFile(opts.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca_cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_cert: no certificates found in %s", opts.CACert)
		}
		cfg.RootCAs = pool
	}
	if opts.ClientCert != "" {
		cert, err := tls.LoadX509KeyPair(opts.ClientCert, opts.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// Endpoint returns the configured HTTP base URL.
func (c *Client) Endpoint() string { return c.endpoint }

// WSEndpoint returns the configured WebSocket URL.
func (c *Client) WSEndpoint() string { return c.wsEndpoint }

// NewClientFromEnv builds a client from the standard Actae environment
// variables. When an env var is set it overrides the corresponding
// ClientOptions field:
//
//	ACTAE_URL      — HTTP base URL (e.g. "http://localhost:8002")
//	ACTAE_WS_URL   — WebSocket URL (optional; derived from ACTAE_URL)
//	ACTAE_API_KEY  — API key (required)
//
// Returns an error when ACTAE_API_KEY is missing and no opts.APIKey was
// given. This is the convenient entry point for services deployed with
// env-config.
func NewClientFromEnv(opts ClientOptions) (*Client, error) {
	env := func(key string) string { return os.Getenv(key) }
	if v := env("ACTAE_API_KEY"); v != "" {
		opts.APIKey = v
	}
	if v := env("ACTAE_URL"); v != "" {
		opts.Endpoint = v
	}
	if v := env("ACTAE_WS_URL"); v != "" {
		opts.WSEndpoint = v
	}
	if opts.APIKey == "" {
		return nil, fmt.Errorf("api_key is required (set ACTAE_API_KEY or pass ClientOptions.APIKey)")
	}
	return NewClient(opts)
}

// ------------------------------------------------------------------ //
// validateChannelID enforces the shared channel-id grammar (audit item 10):
// 1..=256 bytes of [A-Za-z0-9._:-], no slashes or whitespace. Fails fast on
// the client so a channel that can never be addressed over HTTP/WS is never
// created. Human-readable names belong in metadata, not the id.
func validateChannelID(id string) error {
	if id == "" {
		return fmt.Errorf("channel id is required")
	}
	if len(id) > 256 {
		return fmt.Errorf("channel id exceeds maximum length of 256")
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == ':' || r == '-' {
			continue
		}
		return fmt.Errorf("invalid channel id %q — use [A-Za-z0-9._:-] only (no slashes or whitespace)", id)
	}
	return nil
}

// validateChannelIDs validates a slice of channel ids, returning the first
// offending id.
func validateChannelIDs(ids []string) error {
	for _, id := range ids {
		if err := validateChannelID(id); err != nil {
			return err
		}
	}
	return nil
}

// HTTP request helper
// ------------------------------------------------------------------ //

// do performs an HTTP request and returns the parsed body. JSON bodies are
// returned as map[string]any (numbers normalized to int64/float64, sonic-rs
// markers unwrapped); text/* responses are returned as string.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, headers http.Header) (any, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}

	u := c.endpoint + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	// Percent-encode each path segment so channel IDs containing spaces,
	// slashes, unicode, etc. survive transport (mirrors aiohttp, which
	// encodes automatically).
	u = escapePath(u)

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if headers == nil {
		headers = http.Header{}
	}
	if headers.Get("x-api-key") == "" {
		headers.Set("x-api-key", c.apiKey)
	}
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Transport-level failures (server down, refused, TLS handshake)
		// are ConnectionError so callers and the AgentSession retry logic
		// can handle them uniformly with the WebSocket path. Context
		// cancellation/deadline pass through unwrapped so errors.Is works.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, NewConnectionError("HTTP request failed: " + err.Error())
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		// Mid-body connection drops are transport failures too — wrap them
		// like the Do() path so ConnectionError handling is uniform. Context
		// cancellation/deadline still pass through unwrapped.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, NewConnectionError("HTTP response read failed: " + err.Error())
	}

	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/") || strings.Contains(contentType, "application/text") {
		bodyText := string(raw)
		if resp.StatusCode >= 400 {
			return nil, NewAPIError(resp.StatusCode, bodyText)
		}
		return bodyText, nil
	}

	var parsed any
	if len(bytes.TrimSpace(raw)) > 0 {
		parsed, err = decodeValue(raw)
		if err != nil {
			// Non-JSON body: on error responses fall back to the raw text so
			// they still map cleanly; on success, hand back the raw string.
			if resp.StatusCode >= 400 {
				parsed = map[string]any{"message": strings.TrimSpace(string(raw))}
			} else {
				parsed = string(raw)
			}
		}
	}

	if resp.StatusCode >= 400 {
		return nil, mapHTTPError(resp.StatusCode, parsed)
	}
	return parsed, nil
}

// mapHTTPError converts a non-2xx status into the matching typed error,
// mirroring the Python SDK's _request error mapping.
// escapePath percent-encodes every path segment of an absolute URL, leaving
// the scheme/host and any query string untouched. Pre-existing percent
// escapes are preserved (double-encoding avoided).
func escapePath(u string) string {
	idx := strings.Index(u, "://")
	if idx < 0 {
		return u
	}
	rest := u[idx+3:]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return u
	}
	hostPort := rest[:slash]
	tail := rest[slash:]
	if q := strings.IndexByte(tail, '?'); q >= 0 {
		tail = tail[:q]
	}
	segs := strings.Split(tail, "/")
	for i, s := range segs {
		if s == "" || strings.ContainsAny(s, "%") {
			continue
		}
		segs[i] = url.PathEscape(s)
	}
	return u[:idx+3] + hostPort + strings.Join(segs, "/") + querySuffix(u)
}

func querySuffix(u string) string {
	if q := strings.IndexByte(u, '?'); q >= 0 {
		return u[q:]
	}
	return ""
}

func mapHTTPError(status int, parsed any) error {
	m, _ := parsed.(map[string]any)
	statusCode := str(m, "status")
	errorMsg := str(m, "error")
	if errorMsg == "" {
		errorMsg = str(m, "message")
	}
	if status == 429 {
		retryAfter := intOf(m, "retry_after_seconds", 60)
		return NewRateLimitError(int(retryAfter), errorMsg)
	}
	if status == 401 {
		// Python SDK parity: wrong/expired credentials surface as
		// AuthError on the HTTP path too (not APIError), so
		// `errors.As(err, &AuthError)` catches all auth failures.
		return NewAuthError(errorMsg)
	}
	if status == 409 {
		switch statusCode {
		case "snapshot_boundary_required":
			return NewSnapshotBoundaryError(errorMsg)
		case "version_conflict":
			return NewVersionConflictError(errorMsg)
		case "consumer_not_found":
			return NewConsumerError(errorMsg)
		case "idempotency_key_mismatch":
			return NewIdempotencyKeyMismatchError(errorMsg)
		case "execution_not_owned":
			return NewExecutionNotOwnedError(errorMsg)
		case "idempotency_conflict":
			return NewIdempotencyConflictError(errorMsg)
		case "channel_conflict":
			return NewChannelConflictError(errorMsg)
		case "counterfactual_blocked":
			return NewCounterfactualBlockedError(errorMsg)
		case "fork_tool_blocked":
			return NewForkToolBlockedError(errorMsg)
		}
	}
	if status == 404 && statusCode == "execution_not_found" {
		return NewExecutionNotFoundError(errorMsg)
	}
	return NewAPIError(status, errorMsg)
}

// ------------------------------------------------------------------ //
// Events — HTTP API
// ------------------------------------------------------------------ //

// RecordOptions configures Record.
type RecordOptions struct {
	Actor    string
	AgentID  *string
	UserID   *string
	Metadata map[string]any
	// OperationID makes the record idempotent: retrying with the same
	// channel_id + operation_id returns the original persisted event
	// instead of inserting a duplicate. Generate a fresh UUID per logical
	// record attempt (see newUUID or actae.Ptr).
	OperationID *string
	// StepNumber records a durable step -> cursor mapping in the server-owned
	// step index, making step→cursor resolution and fork_at_step exact
	// server-side.
	StepNumber   *int64
	Dependencies []EventDependency
}

func dependencyPayload(values []EventDependency) []map[string]any {
	out := make([]map[string]any, 0, len(values))
	for _, value := range values {
		kind := value.DependencyType
		if kind == "" {
			kind = "causal"
		}
		out = append(out, map[string]any{"event_id": value.EventID, "dependency_type": kind})
	}
	return out
}

// Record records a new event on a channel.
// POST /api/v1/events/record. Returns the persisted Event with its
// server-assigned cursor and ID. When OperationID is set and an event with
// the same channel_id + operation_id already exists, the original event is
// returned (idempotent replay, safe for retries).
//
// The returned Event's Metadata/AgentID/UserID are filled from opts (the
// record response omits them); all other fields come from the server.
func (c *Client) Record(ctx context.Context, channelID, eventType string, payload any, opts RecordOptions) (Event, error) {
	if err := validateChannelID(channelID); err != nil {
		return Event{}, err
	}
	meta := map[string]any{
		"actor":    opts.Actor,
		"agent_id": opts.AgentID,
		"user_id":  opts.UserID,
		"metadata": opts.Metadata,
	}
	body := map[string]any{
		"channel_id": channelID,
		"event_type": eventType,
		"payload":    payload,
		"metadata":   meta,
	}
	if opts.OperationID != nil {
		body["operation_id"] = *opts.OperationID
	}
	if opts.StepNumber != nil {
		body["step_number"] = *opts.StepNumber
	}
	if len(opts.Dependencies) > 0 {
		body["dependencies"] = dependencyPayload(opts.Dependencies)
	}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/events/record", nil, body, nil)
	if err != nil {
		return Event{}, err
	}
	m, _ := result.(map[string]any)
	ev := eventFromRecord(mapOf(m, "event"))
	ev.Metadata = opts.Metadata
	ev.AgentID = opts.AgentID
	ev.UserID = opts.UserID
	return ev, nil
}

// ReplayOptions configures Replay.
type ReplayOptions struct {
	Cursor    *int64
	Limit     int
	EventType *string
}

// Replay replays events from a channel, starting at an optional cursor.
// GET /api/v1/events/replay/{channel_id}. Limit defaults to 100 (server
// max 1000 — page with Cursor for more).
func (c *Client) Replay(ctx context.Context, channelID string, opts ReplayOptions) ([]Event, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	query := url.Values{}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	query.Set("limit", fmt.Sprintf("%d", limit))
	if opts.Cursor != nil {
		query.Set("cursor", fmt.Sprintf("%d", *opts.Cursor))
	}
	if opts.EventType != nil {
		query.Set("event_type", *opts.EventType)
	}
	result, err := c.do(ctx, http.MethodGet, "/api/v1/events/replay/"+channelID, query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	events := []Event{}
	for _, raw := range listOf(m, "events") {
		if ev, ok := raw.(map[string]any); ok {
			events = append(events, eventFromReplay(ev))
		}
	}
	return events, nil
}

// CausalGraphTyped returns the bounded transitive multi-parent ancestry of an event.
func (c *Client) CausalGraphTyped(ctx context.Context, eventID string, maxNodes int64) (CausalGraph, error) {
	if maxNodes <= 0 {
		maxNodes = 1000
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/events/"+eventID+"/causal-graph", url.Values{"max_nodes": []string{fmt.Sprintf("%d", maxNodes)}}, nil, nil)
	if err != nil {
		return CausalGraph{}, err
	}
	m, _ := data.(map[string]any)
	graph := CausalGraph{RootEventID: str(m, "root_event_id"), Truncated: boolOf(m, "truncated", false)}
	for _, raw := range listOf(m, "events") {
		if value, ok := raw.(map[string]any); ok {
			graph.Events = append(graph.Events, eventFromReplay(value))
		}
	}
	for _, raw := range listOf(m, "dependencies") {
		if value, ok := raw.(map[string]any); ok {
			graph.Dependencies = append(graph.Dependencies, EventDependency{EventID: str(value, "event_id"), DependsOnEventID: str(value, "depends_on_event_id"), DependencyType: str(value, "dependency_type")})
		}
	}
	return graph, nil
}

// CausalGraph returns the bounded transitive causal ancestry of an event.
func (c *Client) CausalGraph(ctx context.Context, eventID string, maxNodes int) (map[string]any, error) {
	if maxNodes <= 0 {
		maxNodes = 1000
	}
	query := url.Values{"max_nodes": []string{fmt.Sprintf("%d", maxNodes)}}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/events/"+eventID+"/causal-graph", query, nil, nil)
	if err != nil {
		return nil, err
	}
	result, _ := data.(map[string]any)
	return result, nil
}

// QueryOptions configures Query.
type QueryOptions struct {
	ChannelIDs  []string
	EventType   *string
	Actor       *string
	CursorStart *int64
	CursorEnd   *int64
	FromTime    *string // ISO 8601 lower bound
	ToTime      *string // ISO 8601 upper bound
	Limit       int
	Offset      *int64
}

// Query searches events across channels with flexible filters.
// POST /api/v1/events/query. Limit defaults to 100 (server max 1000).
func (c *Client) Query(ctx context.Context, opts QueryOptions) ([]Event, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	body := map[string]any{"limit": limit}
	if opts.ChannelIDs != nil {
		body["channel_ids"] = opts.ChannelIDs
	}
	if opts.EventType != nil {
		body["event_type"] = *opts.EventType
	}
	if opts.Actor != nil {
		body["actor"] = *opts.Actor
	}
	if opts.CursorStart != nil {
		body["cursor_start"] = *opts.CursorStart
	}
	if opts.CursorEnd != nil {
		body["cursor_end"] = *opts.CursorEnd
	}
	if opts.FromTime != nil {
		body["from"] = *opts.FromTime
	}
	if opts.ToTime != nil {
		body["to"] = *opts.ToTime
	}
	if opts.Offset != nil {
		body["offset"] = *opts.Offset
	}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/events/query", nil, body, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	events := []Event{}
	for _, raw := range listOf(m, "events") {
		if ev, ok := raw.(map[string]any); ok {
			events = append(events, eventFromQuery(ev))
		}
	}
	return events, nil
}

// GetCursor returns the latest cursor value for a channel, or nil when the
// channel has no events. GET /api/v1/events/cursor/{channel_id}.
func (c *Client) GetCursor(ctx context.Context, channelID string) (*int64, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	result, err := c.do(ctx, http.MethodGet, "/api/v1/events/cursor/"+channelID, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	return int64ptr(m, "latest_cursor"), nil
}

// LatestCursor is an alias for GetCursor.
func (c *Client) LatestCursor(ctx context.Context, channelID string) (*int64, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	return c.GetCursor(ctx, channelID)
}

// TransitionOptions configures Transition.
type TransitionOptions struct {
	Actor           string
	AgentID         *string
	UserID          *string
	Metadata        map[string]any
	OperationID     *string
	ExpectedVersion *int64
	ExpectedCursor  *int64
	Dependencies    []EventDependency
}

// Transition atomically persists an event AND its resulting state snapshot in
// one server-side transaction. POST /api/v1/events/transition. Returns the
// persisted Event and the new state version. Raises VersionConflictError on
// guard mismatch.
func (c *Client) Transition(ctx context.Context, channelID, eventType string, payload, state any, opts TransitionOptions) (TransitionResult, error) {
	if err := validateChannelID(channelID); err != nil {
		return TransitionResult{}, err
	}
	meta := map[string]any{"actor": opts.Actor}
	if opts.AgentID != nil {
		meta["agent_id"] = *opts.AgentID
	}
	if opts.UserID != nil {
		meta["user_id"] = *opts.UserID
	}
	if opts.Metadata != nil {
		meta["metadata"] = opts.Metadata
	}
	body := map[string]any{
		"channel_id": channelID,
		"type":       eventType,
		"payload":    payload,
		"state":      state,
		"metadata":   meta,
	}
	if opts.OperationID != nil {
		body["operation_id"] = *opts.OperationID
	}
	if len(opts.Dependencies) > 0 {
		body["dependencies"] = dependencyPayload(opts.Dependencies)
	}
	if opts.ExpectedVersion != nil {
		body["expected_version"] = *opts.ExpectedVersion
	}
	if opts.ExpectedCursor != nil {
		body["expected_cursor"] = *opts.ExpectedCursor
	}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/events/transition", nil, body, nil)
	if err != nil {
		return TransitionResult{}, err
	}
	m, _ := result.(map[string]any)
	return TransitionResult{
		Event:        eventFromRecord(mapOf(m, "event")),
		StateVersion: intOf(m, "state_version", 0),
	}, nil
}

// SaveStateOptions configures SaveState with optimistic-concurrency guards.
type SaveStateOptions struct {
	ExpectedVersion *int64
	ExpectedCursor  *int64
}

// SaveState saves a cursor-aligned context snapshot for agent resume. Each
// call creates a new immutable version. POST /api/v1/state/{channel_id}.
// Returns the assigned version number.
func (c *Client) SaveState(ctx context.Context, channelID string, cursor int64, state any, opts SaveStateOptions) (int64, error) {
	if err := validateChannelID(channelID); err != nil {
		return 0, err
	}
	body := map[string]any{
		"channel_id": channelID,
		"cursor":     cursor,
		"state":      state,
	}
	if opts.ExpectedVersion != nil {
		body["expected_version"] = *opts.ExpectedVersion
	}
	if opts.ExpectedCursor != nil {
		body["expected_cursor"] = *opts.ExpectedCursor
	}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/state/"+channelID, nil, body, nil)
	if err != nil {
		return 0, err
	}
	m, _ := result.(map[string]any)
	return intOf(m, "version", 0), nil
}

// LatestState returns the latest saved state snapshot for a channel, or
// nil when no state has been saved. GET /api/v1/state/{channel_id}.
func (c *Client) LatestState(ctx context.Context, channelID string) (*StateSnapshot, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	result, err := c.do(ctx, http.MethodGet, "/api/v1/state/"+channelID, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	return stateSnapshotFrom(m), nil
}

// ListStatesOptions configures ListStates pagination.
type ListStatesOptions struct {
	Limit  int
	Offset int
}

// ListStates lists state version history for a channel (metadata only, no
// blobs). GET /api/v1/state/{channel_id}/versions. Entries are ordered
// newest-first.
func (c *Client) ListStates(ctx context.Context, channelID string, opts ListStatesOptions) ([]StateVersionInfo, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	query := url.Values{
		"limit":  {fmt.Sprintf("%d", limit)},
		"offset": {fmt.Sprintf("%d", opts.Offset)},
	}
	result, err := c.do(ctx, http.MethodGet, "/api/v1/state/"+channelID+"/versions", query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	out := []StateVersionInfo{}
	for _, raw := range listOf(m, "versions") {
		if v, ok := raw.(map[string]any); ok {
			out = append(out, StateVersionInfo{
				Version:   intOf(v, "version", 0),
				Cursor:    intOf(v, "cursor", 0),
				Timestamp: str(v, "timestamp"),
			})
		}
	}
	return out, nil
}

// GetState returns a specific state snapshot by version, or nil when the
// version does not exist. GET /api/v1/state/{channel_id}/version/{version}.
func (c *Client) GetState(ctx context.Context, channelID string, version int64) (*StateSnapshot, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	result, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/state/%s/version/%d", channelID, version), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	return stateSnapshotFrom(m), nil
}

// DeleteState deletes a specific state snapshot version.
// DELETE /api/v1/state/{channel_id}/version/{version}.
func (c *Client) DeleteState(ctx context.Context, channelID string, version int64) error {
	if err := validateChannelID(channelID); err != nil {
		return err
	}
	_, err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/state/%s/version/%d", channelID, version), nil, nil, nil)
	return err
}

// ListChannels lists all active channel IDs known to Actae.
// GET /api/v1/channels.
func (c *Client) ListChannels(ctx context.Context) ([]string, error) {
	result, err := c.do(ctx, http.MethodGet, "/api/v1/channels", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	out := []string{}
	for _, raw := range listOf(m, "channels") {
		if s, ok := raw.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// ForkOptions configures Fork.
type ForkOptions struct {
	DisplayName        *string
	Reason             *string
	ExperimentMetadata map[string]any
	OperationID        *string
	// Manifest is the immutable fork manifest (model, prompts, tools,
	// environment fingerprint, dependencies, seed) echoed in the receipt.
	Manifest map[string]any
	// ToolPolicies is the child channel's side-effect policy map
	// ({"tool": "replay"|"block"|"live"|"auto", "*": default}). Omitted/empty
	// means the server default (auto): an exact inherited tool result is
	// replayed; new or changed work runs live.
	ToolPolicies map[string]any
	// ExpectedVersion / ExpectedCursor make "fork latest" mean "latest as of
	// the state I observed"; the fork is rejected with VersionConflictError
	// when the source state advanced.
	ExpectedVersion *int64
	ExpectedCursor  *int64
}

// Fork forks a channel at a cursor into a new experiment fork.
// POST /api/v1/channels/fork.
//
// Boundary semantics: at_cursor=0 forks from the channel's latest saved
// state; at_cursor>0 forks from the latest snapshot whose cursor <=
// at_cursor (SnapshotBoundaryError when none exists). Passing the same
// OperationID replays the fork idempotently (returns the ORIGINAL child even
// when the retried request names a different child id; a reused key with a
// different request is an IdempotencyConflictError; a child id created under
// a different definition is a ChannelConflictError).
//
// Returns the immutable ForkReceipt (requested/resolved boundary, source
// state version + SHA-256 fingerprint, restorability, reproducibility grade).
func (c *Client) Fork(ctx context.Context, sourceChannelID, newChannelID string, atCursor int64, opts ForkOptions) (ForkReceipt, error) {
	if err := validateChannelID(sourceChannelID); err != nil {
		return ForkReceipt{}, err
	}
	if err := validateChannelID(newChannelID); err != nil {
		return ForkReceipt{}, err
	}
	operationID := opts.OperationID
	if operationID == nil {
		id := newUUID()
		operationID = &id
	}
	payload := map[string]any{
		"source_channel_id": sourceChannelID,
		"new_channel_id":    newChannelID,
		"at_cursor":         atCursor,
		"operation_id":      *operationID,
	}
	if opts.DisplayName != nil && *opts.DisplayName != "" {
		payload["display_name"] = *opts.DisplayName
	}
	if opts.Reason != nil && *opts.Reason != "" {
		payload["reason"] = *opts.Reason
	}
	if opts.ExperimentMetadata != nil {
		payload["experiment_metadata"] = opts.ExperimentMetadata
	}
	if opts.Manifest != nil {
		payload["manifest"] = opts.Manifest
	}
	if len(opts.ToolPolicies) > 0 {
		payload["tool_policies"] = opts.ToolPolicies
	}
	if opts.ExpectedVersion != nil {
		payload["expected_version"] = *opts.ExpectedVersion
	}
	if opts.ExpectedCursor != nil {
		payload["expected_cursor"] = *opts.ExpectedCursor
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/channels/fork", nil, payload, nil)
	if err != nil {
		return ForkReceipt{}, err
	}
	m, _ := data.(map[string]any)
	return forkReceiptFromDict(m), nil
}

// GetForkReceipt fetches the immutable fork receipt for a channel.
// GET /api/v1/channels/{channel_id}/receipt.
func (c *Client) GetForkReceipt(ctx context.Context, channelID string) (ForkReceipt, error) {
	if err := validateChannelID(channelID); err != nil {
		return ForkReceipt{}, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/"+channelID+"/receipt", nil, nil, nil)
	if err != nil {
		return ForkReceipt{}, err
	}
	m, _ := data.(map[string]any)
	return forkReceiptFromDict(m), nil
}

// StepResolution is the result of resolving a step number against the
// server-owned step index.
type StepResolution struct {
	ChannelID  string
	StepNumber int64
	Cursor     int64
	EventID    *string
}

// ResolveStep resolves a step number to the channel that OWNS it and its
// cursor via the server-owned step index.
// GET /api/v1/channels/{channel_id}/steps/{step_number}.
//
// Walks the fork lineage server-side: a fork inherits its first
// forked_at_step steps from its parent, so step N of a fork resolves against
// the ancestor that recorded it. Returns (nil, nil) when the step is not
// indexed (channels created before the index — callers fall back to
// replay-based resolution).
func (c *Client) ResolveStep(ctx context.Context, channelID string, stepNumber int64) (*StepResolution, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/channels/%s/steps/%d", channelID, stepNumber), nil, nil, nil)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	m, _ := data.(map[string]any)
	return &StepResolution{
		ChannelID:  str(m, "channel_id"),
		StepNumber: intOf(m, "step_number", stepNumber),
		Cursor:     intOf(m, "cursor", 0),
		EventID:    strptr(m, "event_id"),
	}, nil
}

// LatestStepNumber returns the highest step number recorded on a channel
// (0 = none), via the server-owned step index.
// GET /api/v1/channels/{channel_id}/steps.
//
// O(1) crash-recovery probe used by Resume. Returns (nil, nil) when the
// endpoint is unavailable (older server), so callers fall back to a
// replay-based recovery.
func (c *Client) LatestStepNumber(ctx context.Context, channelID string) (*int64, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/channels/%s/steps", channelID), nil, nil, nil)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	m, _ := data.(map[string]any)
	last := intOf(m, "last_step_number", 0)
	return &last, nil
}

// CreateExperiment creates an experiment group (baseline + variants).
// POST /api/v1/experiments.
func (c *Client) CreateExperiment(ctx context.Context, name string, opts ExperimentOptions) (map[string]any, error) {
	payload := map[string]any{"name": name}
	if opts.Description != "" {
		payload["description"] = opts.Description
	}
	if opts.BaselineChannelID != "" {
		payload["baseline_channel_id"] = opts.BaselineChannelID
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/experiments", nil, payload, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ExperimentOptions configures CreateExperiment.
type ExperimentOptions struct {
	Description       string
	BaselineChannelID string
}

// ListExperiments lists experiment groups. GET /api/v1/experiments.
func (c *Client) ListExperiments(ctx context.Context) ([]any, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/experiments", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if list, ok := data.([]any); ok {
		return list, nil
	}
	if m, ok := data.(map[string]any); ok {
		return listOf(m, "experiments"), nil
	}
	return []any{}, nil
}

// AddExperimentMember adds a fork to an experiment group.
// POST /api/v1/experiments/{group_id}/members.
func (c *Client) AddExperimentMember(ctx context.Context, groupID, channelID string, opts ExperimentMemberOptions) (map[string]any, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	payload := map[string]any{"channel_id": channelID, "role": opts.Role}
	if opts.Role == "" {
		payload["role"] = "variant"
	}
	if opts.DeclaredDelta != nil {
		payload["declared_delta"] = opts.DeclaredDelta
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/experiments/"+groupID+"/members", nil, payload, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ExperimentMemberOptions configures AddExperimentMember.
type ExperimentMemberOptions struct {
	Role          string
	DeclaredDelta map[string]any
}

// RankExperiment ranks an experiment's members by result score.
// GET /api/v1/experiments/{group_id}/rank.
func (c *Client) RankExperiment(ctx context.Context, groupID string) (map[string]any, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/experiments/"+groupID+"/rank", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// SetOutcome records a fork outcome + optional score.
// PATCH /api/v1/channels/{channel_id}/outcome.
func (c *Client) SetOutcome(ctx context.Context, channelID, outcome string, score *float64) (map[string]any, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	payload := map[string]any{"outcome": outcome}
	if score != nil {
		payload["score"] = *score
	}
	data, err := c.do(ctx, http.MethodPatch, "/api/v1/channels/"+channelID+"/outcome", nil, payload, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// PromoteChannel promotes a winning fork into its parent (append-only
// merge). POST /api/v1/channels/{channel_id}/promote.
func (c *Client) PromoteChannel(ctx context.Context, channelID string) (map[string]any, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/channels/"+channelID+"/promote", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteChannel soft-deletes a channel (optionally the whole subtree) and
// purges its rows. DELETE /api/v1/channels/{channel_id}.
func (c *Client) DeleteChannel(ctx context.Context, channelID string, recursive bool) (map[string]any, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	q := ""
	if recursive {
		q = "?recursive=true"
	}
	data, err := c.do(ctx, http.MethodDelete, "/api/v1/channels/"+channelID+q, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// CompareChannels fetches the combined "what changed?" comparison between
// two forks (state diff + manifest diff + tool diff + output + metrics).
// GET /api/v1/channels/compare?left=&right=.
func (c *Client) CompareChannels(ctx context.Context, left, right string) (map[string]any, error) {
	if err := validateChannelID(left); err != nil {
		return nil, err
	}
	if err := validateChannelID(right); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/compare?left="+left+"&right="+right, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// GetChannelMetadata returns metadata for a channel, or nil on HTTP 404.
// GET /api/v1/channels/{channel_id}/metadata.
func (c *Client) GetChannelMetadata(ctx context.Context, channelID string) (*ChannelMetadata, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/"+channelID+"/metadata", nil, nil, nil)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	m, _ := data.(map[string]any)
	meta := channelMetadataFromDict(m)
	return &meta, nil
}

// ListForks lists direct children (forks) of a channel.
// GET /api/v1/channels/{channel_id}/forks.
func (c *Client) ListForks(ctx context.Context, channelID string) ([]ChannelMetadata, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/"+channelID+"/forks", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []ChannelMetadata{}
	for _, raw := range listOf(m, "forks") {
		if b, ok := raw.(map[string]any); ok {
			out = append(out, channelMetadataFromDict(b))
		}
	}
	return out, nil
}

// GetForkTree returns the full execution tree rooted at a channel.
// GET /api/v1/channels/{root_channel_id}/fork-tree.
func (c *Client) GetForkTree(ctx context.Context, rootChannelID string) (*ForkInfo, error) {
	if err := validateChannelID(rootChannelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/"+rootChannelID+"/fork-tree", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return forkInfoFromDict(m), nil
}

// DiffStates structurally diffs two channels' latest saved states — the
// counterfactual-debugging primitive: fork two fixes from a failing run,
// then diff their states to see which one fixed it.
//
// GET /api/v1/channels/diff?left={left}&right={right}.
//
// Kind convention on Entries: "added" = right-only, "removed" = left-only,
// "changed" = present on both with different values. Common is the shared
// ancestor state when both channels descend from the same origin_run_id
// (sibling forks), else nil.
func (c *Client) DiffStates(ctx context.Context, leftChannelID, rightChannelID string) (*StateDiff, error) {
	if err := validateChannelID(leftChannelID); err != nil {
		return nil, err
	}
	if err := validateChannelID(rightChannelID); err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("left", leftChannelID)
	query.Set("right", rightChannelID)
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/diff", query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	diff := stateDiffFrom(m)
	return &diff, nil
}

// DecisionTrail returns the full lineage-as-audit view for a channel: the
// chain back to the origin_run_id root, the boundary state it forked from,
// and the idempotent tool-execution ledger rows that happened on this
// channel. Every decision in a run traces back to a fork point, with the
// tool calls that produced each state.
//
// GET /api/v1/channels/{channel_id}/trail.
func (c *Client) DecisionTrail(ctx context.Context, channelID string) (*DecisionTrail, error) {
	if err := validateChannelID(channelID); err != nil {
		return nil, err
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/channels/"+channelID+"/trail", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	trail := decisionTrailFrom(m)
	return &trail, nil
}

// UpdateMetadataOptions configures UpdateMetadata (partial update — omitted
// fields retain their existing values).
type UpdateMetadataOptions struct {
	DisplayName        *string
	Reason             *string
	ExperimentMetadata map[string]any
}

// UpdateMetadata updates channel metadata.
// PUT /api/v1/channels/{channel_id}/metadata.
func (c *Client) UpdateMetadata(ctx context.Context, channelID string, opts UpdateMetadataOptions) (ChannelMetadata, error) {
	if err := validateChannelID(channelID); err != nil {
		return ChannelMetadata{}, err
	}
	payload := map[string]any{"channel_id": channelID}
	if opts.DisplayName != nil {
		payload["display_name"] = *opts.DisplayName
	}
	if opts.Reason != nil {
		payload["reason"] = *opts.Reason
	}
	if opts.ExperimentMetadata != nil {
		payload["experiment_metadata"] = opts.ExperimentMetadata
	}
	data, err := c.do(ctx, http.MethodPut, "/api/v1/channels/"+channelID+"/metadata", nil, payload, nil)
	if err != nil {
		return ChannelMetadata{}, err
	}
	m, _ := data.(map[string]any)
	return channelMetadataFromDict(m), nil
}

// ------------------------------------------------------------------ //
// Health & readiness
// ------------------------------------------------------------------ //

// HealthCheck checks the health of the Actae server. GET /healthz.
func (c *Client) HealthCheck(ctx context.Context) (HealthStatus, error) {
	result, err := c.do(ctx, http.MethodGet, "/healthz", nil, nil, nil)
	if err != nil {
		return HealthStatus{}, err
	}
	m, _ := result.(map[string]any)
	return healthStatusFromResponse(m), nil
}

// ReadinessCheck checks whether the Actae server is ready to accept traffic.
// GET /readyz.
func (c *Client) ReadinessCheck(ctx context.Context) (ReadinessResult, error) {
	result, err := c.do(ctx, http.MethodGet, "/readyz", nil, nil, nil)
	if err != nil {
		return ReadinessResult{}, err
	}
	m, _ := result.(map[string]any)
	return readinessResultFromResponse(m), nil
}

// GetMetricsText returns Prometheus-format metrics. GET /metrics.
func (c *Client) GetMetricsText(ctx context.Context) (string, error) {
	result, err := c.do(ctx, http.MethodGet, "/metrics", nil, nil, nil)
	if err != nil {
		return "", err
	}
	s, _ := result.(string)
	return s, nil
}

// GetMetricsJSON returns structured JSON metrics. GET /metrics.json.
func (c *Client) GetMetricsJSON(ctx context.Context) (MetricsSnapshot, error) {
	result, err := c.do(ctx, http.MethodGet, "/metrics.json", nil, nil, nil)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	m, _ := result.(map[string]any)
	return metricsSnapshotFromResponse(m), nil
}

// ------------------------------------------------------------------ //
// Auth API
// ------------------------------------------------------------------ //

// SignupOptions configures Signup.
type SignupOptions struct {
	Email    string
	Password string
	Name     *string
}

// Signup creates a new user account. POST /api/v1/auth/signup.
func (c *Client) Signup(ctx context.Context, opts SignupOptions) (AuthResult, error) {
	body := map[string]any{"email": opts.Email, "password": opts.Password}
	if opts.Name != nil && *opts.Name != "" {
		body["name"] = *opts.Name
	}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/auth/signup", nil, body, nil)
	if err != nil {
		return AuthResult{}, err
	}
	m, _ := result.(map[string]any)
	return authResultFromResponse(m), nil
}

// Login authenticates with email and password. POST /api/v1/auth/login.
func (c *Client) Login(ctx context.Context, email, password string) (AuthResult, error) {
	body := map[string]any{"email": email, "password": password}
	result, err := c.do(ctx, http.MethodPost, "/api/v1/auth/login", nil, body, nil)
	if err != nil {
		return AuthResult{}, err
	}
	m, _ := result.(map[string]any)
	return authResultFromResponse(m), nil
}

func bearerHeaders(token string) http.Header {
	return http.Header{"authorization": {token}}
}

// Logout invalidates a JWT token server-side. POST /api/v1/auth/logout.
func (c *Client) Logout(ctx context.Context, jwtToken string) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/auth/logout", nil, nil, bearerHeaders("Bearer "+jwtToken))
	return err
}

// GetMe returns the current user's profile for a JWT token.
// GET /api/v1/auth/me.
func (c *Client) GetMe(ctx context.Context, jwtToken string) (UserInfo, error) {
	result, err := c.do(ctx, http.MethodGet, "/api/v1/auth/me", nil, nil, bearerHeaders("Bearer "+jwtToken))
	if err != nil {
		return UserInfo{}, err
	}
	m, _ := result.(map[string]any)
	return userInfoFromDict(mapOf(m, "user")), nil
}

// Capabilities returns the caller's parsed key scopes so a least-privilege
// client can ask before acting (selling point 5).
// GET /api/v1/auth/capabilities.
func (c *Client) Capabilities(ctx context.Context) (Capabilities, error) {
	result, err := c.do(ctx, http.MethodGet, "/api/v1/auth/capabilities", nil, nil, nil)
	if err != nil {
		return Capabilities{}, err
	}
	m, _ := result.(map[string]any)
	return capabilitiesFromDict(m), nil
}

// ------------------------------------------------------------------ //
// Consumer groups
// ------------------------------------------------------------------ //

// CreateGroup creates a durable consumer group bound to a channel.
// POST /api/v1/groups.
func (c *Client) CreateGroup(ctx context.Context, groupID, channelID string, metadata map[string]any) (GroupInfo, error) {
	if err := validateChannelID(channelID); err != nil {
		return GroupInfo{}, err
	}
	body := map[string]any{"group_id": groupID, "channel_id": channelID}
	if metadata != nil {
		body["metadata"] = metadata
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/groups", nil, body, nil)
	if err != nil {
		return GroupInfo{}, err
	}
	m, _ := data.(map[string]any)
	return groupInfoFromResponse(m), nil
}

// ListGroupsOptions configures ListGroups.
type ListGroupsOptions struct {
	// ChannelID filters groups to a single channel. Empty lists all
	// groups.
	ChannelID string
}

// ListGroups lists consumer groups, optionally filtered by channel.
// GET /api/v1/groups?channel_id=...
func (c *Client) ListGroups(ctx context.Context, opts ListGroupsOptions) ([]GroupInfo, error) {
	query := url.Values{}
	if opts.ChannelID != "" {
		query.Set("channel_id", opts.ChannelID)
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/groups", query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []GroupInfo{}
	for _, raw := range listOf(m, "groups") {
		if g, ok := raw.(map[string]any); ok {
			out = append(out, groupInfoFromResponse(g))
		}
	}
	return out, nil
}

// DeleteGroup deletes a consumer group (cascades consumers and offsets).
// DELETE /api/v1/groups/{group_id}.
func (c *Client) DeleteGroup(ctx context.Context, groupID string) error {
	_, err := c.do(ctx, http.MethodDelete, "/api/v1/groups/"+groupID, nil, nil, nil)
	return err
}

// JoinGroup registers (or renews) a consumer's lease and durable offset row.
// POST /api/v1/groups/{group_id}/join. leaseSeconds defaults to 60.
func (c *Client) JoinGroup(ctx context.Context, groupID, consumerID string, leaseSeconds int) (GroupOffset, error) {
	if leaseSeconds <= 0 {
		leaseSeconds = 60
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/groups/"+groupID+"/join", nil,
		map[string]any{"consumer_id": consumerID, "lease_seconds": leaseSeconds}, nil)
	if err != nil {
		return GroupOffset{}, err
	}
	m, _ := data.(map[string]any)
	return groupOffsetFromResponse(m), nil
}

// ClaimWork claims a batch of events for a consumer (at-least-once semantics).
// POST /api/v1/groups/{group_id}/work. limit defaults to 100.
func (c *Client) ClaimWork(ctx context.Context, groupID, consumerID string, limit int) (ClaimedWork, error) {
	if limit <= 0 {
		limit = 100
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/groups/"+groupID+"/work", nil,
		map[string]any{"consumer_id": consumerID, "limit": limit}, nil)
	if err != nil {
		return ClaimedWork{}, err
	}
	m, _ := data.(map[string]any)
	return claimedWorkFromResponse(m), nil
}

// AckWork acknowledges processed work up to and including cursor.
// POST /api/v1/groups/{group_id}/ack.
//
// cursor is a CHANNEL cursor (Event.ChannelCursor), not the global cursor,
// and commits a watermark: pass the highest contiguously processed channel
// cursor. Acking higher than you actually processed skips the intervening
// events permanently — they will not be redelivered.
func (c *Client) AckWork(ctx context.Context, groupID, consumerID string, cursor int64) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/groups/"+groupID+"/ack", nil,
		map[string]any{"consumer_id": consumerID, "cursor": cursor}, nil)
	return err
}

// Heartbeat extends a consumer's lease. POST /api/v1/groups/{group_id}/heartbeat.
func (c *Client) Heartbeat(ctx context.Context, groupID, consumerID string, leaseSeconds int) error {
	if leaseSeconds <= 0 {
		leaseSeconds = 60
	}
	_, err := c.do(ctx, http.MethodPost, "/api/v1/groups/"+groupID+"/heartbeat", nil,
		map[string]any{"consumer_id": consumerID, "lease_seconds": leaseSeconds}, nil)
	return err
}

// GroupOffsets returns per-consumer durable offsets for a group.
// GET /api/v1/groups/{group_id}/offsets.
func (c *Client) GroupOffsets(ctx context.Context, groupID string) ([]GroupOffset, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/groups/"+groupID+"/offsets", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []GroupOffset{}
	for _, raw := range listOf(m, "offsets") {
		if o, ok := raw.(map[string]any); ok {
			out = append(out, groupOffsetFromResponse(o))
		}
	}
	return out, nil
}

// CreateExecutionGroup creates a durable distributed-agent coordination group.
func (c *Client) CreateExecutionGroup(ctx context.Context, groupID string, metadata map[string]any) (ExecutionGroup, error) {
	if err := validateChannelID(groupID); err != nil {
		return ExecutionGroup{}, err
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups", nil, map[string]any{"group_id": groupID, "metadata": metadata}, nil)
	if err != nil {
		return ExecutionGroup{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["group"].(map[string]any)
	return executionGroupFrom(raw), nil
}
func (c *Client) AddExecutionGroupMember(ctx context.Context, groupID, memberID, channelID, role string, metadata map[string]any) (ExecutionGroupMember, error) {
	if err := validateChannelID(memberID); err != nil {
		return ExecutionGroupMember{}, err
	}
	if err := validateChannelID(channelID); err != nil {
		return ExecutionGroupMember{}, err
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups/"+groupID+"/members", nil, map[string]any{"member_id": memberID, "channel_id": channelID, "role": role, "metadata": metadata}, nil)
	if err != nil {
		return ExecutionGroupMember{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["member"].(map[string]any)
	return executionGroupMemberFrom(raw), nil
}
func (c *Client) ExecutionGroupMembers(ctx context.Context, groupID string) ([]ExecutionGroupMember, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/execution-groups/"+groupID+"/members", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []ExecutionGroupMember{}
	for _, raw := range listOf(m, "members") {
		if value, ok := raw.(map[string]any); ok {
			out = append(out, executionGroupMemberFrom(value))
		}
	}
	return out, nil
}
func (c *Client) ClaimMember(ctx context.Context, groupID, memberID, ownerID string, leaseSeconds int) (MemberLease, error) {
	if leaseSeconds <= 0 {
		leaseSeconds = 60
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups/"+groupID+"/members/"+memberID+"/claim", nil, map[string]any{"owner_id": ownerID, "lease_seconds": leaseSeconds}, nil)
	if err != nil {
		return MemberLease{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["lease"].(map[string]any)
	return memberLeaseFrom(raw), nil
}
func (c *Client) HeartbeatMember(ctx context.Context, groupID, memberID, ownerID string, generation int64, leaseSeconds int) (MemberLease, error) {
	if leaseSeconds <= 0 {
		leaseSeconds = 60
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups/"+groupID+"/members/"+memberID+"/heartbeat", nil, map[string]any{"owner_id": ownerID, "generation": generation, "lease_seconds": leaseSeconds}, nil)
	if err != nil {
		return MemberLease{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["lease"].(map[string]any)
	return memberLeaseFrom(raw), nil
}
func (c *Client) SendGroupMessage(ctx context.Context, groupID, fromMemberID, toMemberID, messageType string, payload any, causalContext map[string]any, operationID *string) (GroupMessage, error) {
	body := map[string]any{"from_member_id": fromMemberID, "to_member_id": toMemberID, "type": messageType, "payload": payload}
	if causalContext != nil {
		body["causal_context"] = causalContext
	}
	if operationID != nil {
		body["operation_id"] = *operationID
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups/"+groupID+"/messages", nil, body, nil)
	if err != nil {
		return GroupMessage{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["message"].(map[string]any)
	return groupMessageFrom(raw), nil
}
func (c *Client) GroupMessages(ctx context.Context, groupID, memberID string, after *string, limit int) ([]GroupMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	q := url.Values{}
	q.Set("limit", fmt.Sprintf("%d", limit))
	if after != nil {
		q.Set("after", *after)
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/execution-groups/"+groupID+"/members/"+memberID+"/messages", q, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []GroupMessage{}
	for _, raw := range listOf(m, "messages") {
		if v, ok := raw.(map[string]any); ok {
			out = append(out, groupMessageFrom(v))
		}
	}
	return out, nil
}
func (c *Client) AcknowledgeGroupMessage(ctx context.Context, messageID, ownerID string, generation int64) (GroupMessage, error) {
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-group-messages/"+messageID+"/ack", nil, map[string]any{"owner_id": ownerID, "generation": generation}, nil)
	if err != nil {
		return GroupMessage{}, err
	}
	m, _ := data.(map[string]any)
	raw, _ := m["message"].(map[string]any)
	return groupMessageFrom(raw), nil
}
func (c *Client) ReleaseMember(ctx context.Context, groupID, memberID, ownerID string, generation int64) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/execution-groups/"+groupID+"/members/"+memberID+"/release", nil, map[string]any{"owner_id": ownerID, "generation": generation}, nil)
	return err
}

// ForkExecutionGroup creates an immutable counterfactual group receipt.
func (c *Client) ForkExecutionGroup(ctx context.Context, forkGroupID, sourceGroupID, interventionMemberID string, interventionCursor int64, memberPolicies, toolPolicies map[string]any) (map[string]any, error) {
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-group-forks", nil, map[string]any{"fork_group_id": forkGroupID, "source_group_id": sourceGroupID, "intervention_member_id": interventionMemberID, "intervention_cursor": interventionCursor, "member_policies": memberPolicies, "tool_policies": toolPolicies}, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return mapOf(m, "fork"), nil
}
func (c *Client) GetExecutionGroupFork(ctx context.Context, forkGroupID string) (map[string]any, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/execution-group-forks/"+forkGroupID, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return mapOf(m, "fork"), nil
}
func (c *Client) PromoteExecutionGroupForkMember(ctx context.Context, forkGroupID, memberID string) (map[string]any, error) {
	data, err := c.do(ctx, http.MethodPost, "/api/v1/execution-group-forks/"+forkGroupID+"/members/"+memberID+"/promote", nil, map[string]any{}, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return mapOf(m, "fork"), nil
}

// ------------------------------------------------------------------ //
// Persisted wake-ups
// ------------------------------------------------------------------ //

// ScheduleWakeup schedules a wake-up for a channel. At or after runAt
// (RFC 3339) the server fires a scheduler.wakeup event on the channel.
// POST /api/v1/scheduler/wakeups.
func (c *Client) ScheduleWakeup(ctx context.Context, channelID, runAt string, payload map[string]any) (Wakeup, error) {
	body := map[string]any{"channel_id": channelID, "run_at": runAt}
	if payload != nil {
		body["payload"] = payload
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/scheduler/wakeups", nil, body, nil)
	if err != nil {
		return Wakeup{}, err
	}
	m, _ := data.(map[string]any)
	return wakeupFromResponse(m), nil
}

// ListWakeupsOptions configures ListWakeups.
type ListWakeupsOptions struct {
	// ChannelID filters wake-ups to a single channel. Empty lists all.
	ChannelID string
	// Status filters by status (e.g. "scheduled", "fired", "cancelled").
	// Empty lists all statuses.
	Status string
}

// ListWakeups lists wake-ups, optionally filtered by channel and status.
// GET /api/v1/scheduler/wakeups.
func (c *Client) ListWakeups(ctx context.Context, opts ListWakeupsOptions) ([]Wakeup, error) {
	query := url.Values{}
	if opts.ChannelID != "" {
		query.Set("channel_id", opts.ChannelID)
	}
	if opts.Status != "" {
		query.Set("status", opts.Status)
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/scheduler/wakeups", query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []Wakeup{}
	for _, raw := range listOf(m, "wakeups") {
		if w, ok := raw.(map[string]any); ok {
			out = append(out, wakeupFromResponse(w))
		}
	}
	return out, nil
}

// GetWakeup fetches a single wake-up, or nil on HTTP 404.
// GET /api/v1/scheduler/wakeups/{id}.
func (c *Client) GetWakeup(ctx context.Context, wakeupID string) (*Wakeup, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/scheduler/wakeups/"+wakeupID, nil, nil, nil)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	m, _ := data.(map[string]any)
	w := wakeupFromResponse(m)
	return &w, nil
}

// CancelWakeup cancels a pending wake-up. Returns true when the wake-up was
// cancelled. When the wake-up was already fired, failed, or cancelled (HTTP
// 409), returns (false, ErrWakeupAlreadyFired) — use errors.Is to treat
// that as a benign no-op.
// DELETE /api/v1/scheduler/wakeups/{id}.
func (c *Client) CancelWakeup(ctx context.Context, wakeupID string) (bool, error) {
	_, err := c.do(ctx, http.MethodDelete, "/api/v1/scheduler/wakeups/"+wakeupID, nil, nil, nil)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == 409 {
			return false, ErrWakeupAlreadyFired
		}
		return false, err
	}
	return true, nil
}

// ------------------------------------------------------------------ //
// Idempotent tool executions
// ------------------------------------------------------------------ //

// ClaimExecutionOptions configures ClaimExecution.
type ClaimExecutionOptions struct {
	DedupFields     []string
	LeaseSeconds    *int
	EmitReplayEvent bool
	Actor           *string
}

// ClaimExecution claims an idempotent tool execution (create-or-replay).
// POST /api/v1/executions/claim. The idempotency unit is
// (channel_id, key_name); a completed execution with a matching request hash
// replays its persisted result. A mismatch raises IdempotencyKeyMismatchError.
func (c *Client) ClaimExecution(ctx context.Context, channelID, keyName, toolName string, params any, opts ClaimExecutionOptions) (ExecutionClaim, error) {
	body := map[string]any{
		"channel_id": channelID,
		"key_name":   keyName,
		"tool_name":  toolName,
	}
	if params != nil {
		body["params"] = params
	}
	if opts.DedupFields != nil {
		body["dedup_fields"] = opts.DedupFields
	}
	if opts.LeaseSeconds != nil {
		body["lease_seconds"] = *opts.LeaseSeconds
	}
	if opts.EmitReplayEvent {
		body["emit_replay_event"] = true
	}
	if opts.Actor != nil {
		body["actor"] = *opts.Actor
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/executions/claim", nil, body, nil)
	if err != nil {
		return ExecutionClaim{}, err
	}
	m, _ := data.(map[string]any)
	return executionClaimFromResponse(m), nil
}

// CompleteExecution completes a claimed execution with its persisted result.
// POST /api/v1/executions/{id}/complete. A stale claim token raises
// ExecutionNotOwnedError; duplicate completes are idempotent.
func (c *Client) CompleteExecution(ctx context.Context, executionID string, claimToken *string, result any) (ExecutionInfo, error) {
	body := map[string]any{}
	if claimToken != nil {
		body["claim_token"] = *claimToken
	}
	if result != nil {
		body["result"] = result
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/executions/"+executionID+"/complete", nil, body, nil)
	if err != nil {
		return ExecutionInfo{}, err
	}
	m, _ := data.(map[string]any)
	return executionInfoFromResponse(mapOf(m, "execution")), nil
}

// FailExecutionOptions configures FailExecution.
type FailExecutionOptions struct {
	ClaimToken *string
	ErrorType  *string
	Stack      *string
}

// FailExecution marks a claimed execution failed with a structured error.
// POST /api/v1/executions/{id}/fail.
func (c *Client) FailExecution(ctx context.Context, executionID, message string, opts FailExecutionOptions) (ExecutionInfo, error) {
	body := map[string]any{"message": message}
	if opts.ClaimToken != nil {
		body["claim_token"] = *opts.ClaimToken
	}
	if opts.ErrorType != nil {
		body["error_type"] = *opts.ErrorType
	}
	if opts.Stack != nil {
		body["stack"] = *opts.Stack
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/executions/"+executionID+"/fail", nil, body, nil)
	if err != nil {
		return ExecutionInfo{}, err
	}
	m, _ := data.(map[string]any)
	return executionInfoFromResponse(mapOf(m, "execution")), nil
}

// HeartbeatExecution extends a running execution's lease.
// POST /api/v1/executions/{id}/heartbeat.
func (c *Client) HeartbeatExecution(ctx context.Context, executionID string, claimToken *string, leaseSeconds *int) (ExecutionInfo, error) {
	body := map[string]any{}
	if claimToken != nil {
		body["claim_token"] = *claimToken
	}
	if leaseSeconds != nil {
		body["lease_seconds"] = *leaseSeconds
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/executions/"+executionID+"/heartbeat", nil, body, nil)
	if err != nil {
		return ExecutionInfo{}, err
	}
	m, _ := data.(map[string]any)
	return executionInfoFromResponse(mapOf(m, "execution")), nil
}

// CancelExecution cancels a running execution without storing a result.
// POST /api/v1/executions/{id}/cancel.
func (c *Client) CancelExecution(ctx context.Context, executionID string, claimToken *string) (ExecutionInfo, error) {
	body := map[string]any{}
	if claimToken != nil {
		body["claim_token"] = *claimToken
	}
	data, err := c.do(ctx, http.MethodPost, "/api/v1/executions/"+executionID+"/cancel", nil, body, nil)
	if err != nil {
		return ExecutionInfo{}, err
	}
	m, _ := data.(map[string]any)
	return executionInfoFromResponse(mapOf(m, "execution")), nil
}

// GetExecution fetches a single execution by id. Raises
// ExecutionNotFoundError when it does not exist. GET /api/v1/executions/{id}.
func (c *Client) GetExecution(ctx context.Context, executionID string) (ExecutionInfo, error) {
	data, err := c.do(ctx, http.MethodGet, "/api/v1/executions/"+executionID, nil, nil, nil)
	if err != nil {
		return ExecutionInfo{}, err
	}
	m, _ := data.(map[string]any)
	return executionInfoFromResponse(mapOf(m, "execution")), nil
}

// ListExecutions lists executions for a channel, newest first.
// GET /api/v1/executions?channel_id=...&limit=... Limit must be 1–100.
func (c *Client) ListExecutions(ctx context.Context, channelID string, limit int) ([]ExecutionInfo, error) {
	if limit <= 0 {
		limit = 50
	}
	query := url.Values{
		"channel_id": {channelID},
		"limit":      {fmt.Sprintf("%d", limit)},
	}
	data, err := c.do(ctx, http.MethodGet, "/api/v1/executions", query, nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	out := []ExecutionInfo{}
	for _, raw := range listOf(m, "executions") {
		if e, ok := raw.(map[string]any); ok {
			out = append(out, executionInfoFromResponse(e))
		}
	}
	return out, nil
}

// DeleteExecution deletes an execution record. DELETE /api/v1/executions/{id}.
// Raises ExecutionNotFoundError when it did not exist.
func (c *Client) DeleteExecution(ctx context.Context, executionID string) error {
	_, err := c.do(ctx, http.MethodDelete, "/api/v1/executions/"+executionID, nil, nil, nil)
	return err
}
