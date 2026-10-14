package actae

// FleetClient routes the canonical data-plane API through an organization
// fleet gateway. It intentionally mirrors direct operation names; the gateway
// owns dispatch and authorization, while instances own event/state semantics.
import (
	"bufio"
	"bytes"
	"context"
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

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func doJSON(ctx context.Context, c *http.Client, method, target string, body any, decorate func(http.Header)) (any, error) {
	var r io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return nil, e
		}
		r = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, target, r)
	if e != nil {
		return nil, e
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	decorate(req.Header)
	resp, e := c.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(resp.Body)
	if e != nil {
		return nil, e
	}
	var v any
	if len(bytes.TrimSpace(raw)) > 0 {
		if e = json.Unmarshal(raw, &v); e != nil {
			v = string(raw)
		}
	}
	if resp.StatusCode >= 400 {
		return nil, NewAPIError(resp.StatusCode, fmt.Sprint(v))
	}
	return v, nil
}

type FleetClientOptions struct {
	OrganizationToken string
	OrganizationID    string
	Endpoint          string
	TokenEndpoint     string
	HTTPClient        *http.Client
	// RequestedScopes (PR-006) is the explicit scope set requested from the
	// SaaS token exchange. It is intersected with the organization token's
	// own grants. Leave nil/empty for the least-privilege default (the data
	// read+write surface).
	RequestedScopes []string
}

// DefaultFleetScopes is the least-privilege default requested from the SaaS
// token exchange when FleetClientOptions.RequestedScopes is empty.
var DefaultFleetScopes = []string{
	"events:read", "events:write",
	"state:read", "state:write",
	"channels:read", "channels:write",
	"forks:create",
}

// InstanceError is a safe per-instance error from a multi-instance fleet read.
type InstanceError struct {
	InstanceID string `json:"instance_id"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable"`
}

// FleetPage preserves the server-issued opaque keyset token.
type FleetPage struct {
	Items         []any  `json:"items"`
	NextPageToken string `json:"next_page_token,omitempty"`
	// PageTokens (PR-008) carries the full per-instance continuation map for
	// multi-instance reads (instance_id -> opaque token). Never collapsed.
	PageTokens map[string]string `json:"page_tokens,omitempty"`
}

// PartialResult deliberately separates successful items from failed instances.
type PartialResult struct {
	FleetPage
	Errors []InstanceError `json:"errors"`
}

func (r PartialResult) RequireComplete() ([]any, error) {
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("partial fleet result: %s: %s", r.Errors[0].InstanceID, r.Errors[0].Code)
	}
	return r.Items, nil
}

type FleetClient struct {
	endpoint, tokenEndpoint, orgToken, fleetToken, organizationID string
	fleetTokenExpiresAt                                           time.Time
	httpClient                                                    *http.Client
	mu                                                            sync.Mutex
	capabilities                                                  map[string]map[string]any
	requestedScopes                                               []string
}

// FleetStreamOptions controls the hosted gateway WebSocket protocol. Tickets
// are single-use; TicketProvider is called for every reconnect and receives
// the last observed cursor so the issuer can mint an appropriately scoped
// resume ticket.
type FleetStreamOptions struct {
	InstanceID           string
	Ticket               string
	TicketProvider       func(context.Context, int64) (string, error)
	ResumeCursor         int64
	MaxReconnectAttempts int
	ReconnectDelay       time.Duration
	// Origin is the Origin header sent on the WS handshake. The gateway
	// verifies it against the stream ticket's bound origin, so callers must
	// set it to the same origin the ticket was minted with. Empty = the
	// websocket library default (derived from the gateway host).
	Origin string
}

// StreamWithReconnect consumes a ticketed gateway stream. Events are
// delivered at least once and deduplicated by (instance_id,event_id); control
// frames are forwarded unchanged. The channels close when ctx is cancelled or
// the retry budget is exhausted.
func (f *FleetClient) StreamWithReconnect(ctx context.Context, opts FleetStreamOptions) (<-chan map[string]any, <-chan error, error) {
	if opts.Ticket == "" && opts.TicketProvider == nil {
		return nil, nil, fmt.Errorf("ticket or ticket provider is required")
	}
	if opts.MaxReconnectAttempts < 0 {
		opts.MaxReconnectAttempts = 0
	}
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = 250 * time.Millisecond
	}
	out := make(chan map[string]any, 256)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		seen := map[string]struct{}{}
		cursor := opts.ResumeCursor
		ticket := opts.Ticket
		attempts := 0
		for {
			if ticket == "" {
				if opts.TicketProvider == nil {
					errs <- fmt.Errorf("gateway stream ticket expired; TicketProvider is required for reconnect")
					return
				}
				var err error
				ticket, err = opts.TicketProvider(ctx, cursor)
				if err != nil {
					errs <- err
					return
				}
			}
			u, err := url.Parse(f.endpoint + f.gatewayPath("/events/stream"))
			if err != nil {
				errs <- err
				return
			}
			q := u.Query()
			q.Set("ticket", ticket)
			if cursor != 0 {
				q.Set("resume_cursor", fmt.Sprint(cursor))
			}
			u.RawQuery = q.Encode()
			wsURL := u.String()
			if strings.HasPrefix(wsURL, "https://") {
				wsURL = "wss://" + strings.TrimPrefix(wsURL, "https://")
			} else if strings.HasPrefix(wsURL, "http://") {
				wsURL = "ws://" + strings.TrimPrefix(wsURL, "http://")
			}
			dialer := websocket.DefaultDialer
			var header http.Header
			if opts.Origin != "" {
				header = http.Header{"Origin": {opts.Origin}}
			}
			conn, _, err := dialer.DialContext(ctx, wsURL, header)
			if err != nil {
				if attempts >= opts.MaxReconnectAttempts {
					errs <- err
					return
				}
				attempts++
				if !sleepContext(ctx, opts.ReconnectDelay, attempts) {
					return
				}
				ticket = ""
				continue
			}
			ticket = ""
			attempts = 0
			for {
				_, raw, readErr := conn.ReadMessage()
				if readErr != nil {
					conn.Close()
					if ctx.Err() != nil {
						return
					}
					break
				}
				var item map[string]any
				if json.Unmarshal(raw, &item) != nil {
					continue
				}
				event := item
				if b, ok := item["Broadcast"].(map[string]any); ok {
					if p, ok := b["payload"].(map[string]any); ok {
						if e, ok := p["event"].(map[string]any); ok {
							event = e
						}
					}
				} else if e, ok := item["event"].(map[string]any); ok {
					event = e
				}
				if n, ok := event["cursor"].(float64); ok {
					cursor = int64(n)
				}
				id, ok := event["event_id"]
				if !ok {
					id, ok = event["id"]
				}
				if ok {
					key := fmt.Sprintf("%s:%v", opts.InstanceID, id)
					if _, dup := seen[key]; dup {
						continue
					}
					seen[key] = struct{}{}
					if len(seen) > 10000 {
						seen = map[string]struct{}{}
					}
				}
				select {
				case out <- item:
				case <-ctx.Done():
					conn.Close()
					return
				}
			}
			if attempts >= opts.MaxReconnectAttempts {
				return
			}
			attempts++
			if !sleepContext(ctx, opts.ReconnectDelay, attempts) {
				return
			}
		}
	}()
	return out, errs, nil
}

func sleepContext(ctx context.Context, base time.Duration, attempt int) bool {
	d := base * time.Duration(1<<(minInt(attempt-1, 10)))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// FleetSessionTransport adapts one fleet instance to the same backend used by
// AgentSession. It contains no lifecycle or fork-resolution logic.
type FleetSessionTransport struct {
	fleet      *FleetClient
	instanceID string
}

// Stream consumes the fleet SSE/NDJSON stream with a bounded buffer. Closing
// the caller's context closes the response and terminates both channels.
func (f *FleetClient) Stream(ctx context.Context, instance string, q url.Values) (<-chan map[string]any, <-chan error, error) {
	tok, err := f.token(ctx)
	if err != nil {
		return nil, nil, err
	}
	path := "/api/v1/fleet/stream"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.endpoint+path, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "text/event-stream")
	if instance != "" {
		req.Header.Set("X-Actae-Instance-Id", instance)
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, nil, NewAPIError(resp.StatusCode, "fleet stream unavailable")
	}
	out := make(chan map[string]any, 256)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		defer resp.Body.Close()
		seen := map[string]struct{}{}
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "data:") {
				line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
			if line == "" {
				continue
			}
			var item map[string]any
			if json.Unmarshal([]byte(line), &item) != nil {
				continue
			}
			if id, ok := item["event_id"]; ok {
				key := fmt.Sprintf("%v:%v", item["instance_id"], id)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				if len(seen) > 10000 {
					seen = map[string]struct{}{}
				}
			}
			select {
			case out <- item:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			errs <- err
		}
	}()
	return out, errs, nil
}

var _ sessionBackend = (*FleetSessionTransport)(nil)

func (f *FleetClient) ForInstance(instanceID string) (*FleetSessionTransport, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("instance_id is required")
	}
	return &FleetSessionTransport{fleet: f, instanceID: instanceID}, nil
}
func (s *FleetSessionTransport) Record(ctx context.Context, channelID, eventType string, payload any, opts RecordOptions) (Event, error) {
	meta := map[string]any{"actor": opts.Actor, "agent_id": opts.AgentID, "user_id": opts.UserID, "metadata": opts.Metadata}
	body := map[string]any{"channel_id": channelID, "event_type": eventType, "payload": payload, "metadata": meta}
	if opts.OperationID != nil {
		body["operation_id"] = *opts.OperationID
	}
	if opts.StepNumber != nil {
		body["step_number"] = *opts.StepNumber
	}
	if len(opts.Dependencies) > 0 {
		body["dependencies"] = dependencyPayload(opts.Dependencies)
	}
	key := ""
	if opts.OperationID != nil {
		key = *opts.OperationID
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.events.record", s.instanceID, nil, nil, body, key, "")
	if e != nil {
		return Event{}, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	ev := eventFromRecord(mapOf(m, "event"))
	ev.Metadata = opts.Metadata
	ev.AgentID = opts.AgentID
	ev.UserID = opts.UserID
	return ev, nil
}
func (s *FleetSessionTransport) Replay(ctx context.Context, channelID string, opts ReplayOptions) ([]Event, error) {
	q := url.Values{}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	q.Set("limit", fmt.Sprint(limit))
	if opts.Cursor != nil {
		q.Set("cursor", fmt.Sprint(*opts.Cursor))
	}
	if opts.EventType != nil {
		q.Set("event_type", *opts.EventType)
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.events.replay", s.instanceID, map[string]string{"channel_id": channelID}, q, nil, "", "")
	if e != nil {
		return nil, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	out := []Event{}
	for _, raw := range listOf(m, "events") {
		if item, ok := raw.(map[string]any); ok {
			out = append(out, eventFromReplay(item))
		}
	}
	return out, nil
}
func (s *FleetSessionTransport) SaveState(ctx context.Context, channelID string, cursor int64, state any, opts SaveStateOptions) (int64, error) {
	b := map[string]any{"channel_id": channelID, "cursor": cursor, "state": state}
	if opts.ExpectedVersion != nil {
		b["expected_version"] = *opts.ExpectedVersion
	}
	if opts.ExpectedCursor != nil {
		b["expected_cursor"] = *opts.ExpectedCursor
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.state.save", s.instanceID,
		map[string]string{"channel_id": channelID}, nil, b, "", "")
	if e != nil {
		return 0, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	return intOf(m, "version", 0), nil
}
func (s *FleetSessionTransport) LatestState(ctx context.Context, channelID string) (*StateSnapshot, error) {
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.state.load", s.instanceID, map[string]string{"channel_id": channelID}, nil, nil, "", "")
	if e != nil {
		return nil, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	return stateSnapshotFrom(m), nil
}
func (s *FleetSessionTransport) Fork(ctx context.Context, source, child string, atCursor int64, opts ForkOptions) (ForkReceipt, error) {
	b := map[string]any{"source_channel_id": source, "new_channel_id": child, "at_cursor": atCursor}
	if opts.DisplayName != nil {
		b["display_name"] = *opts.DisplayName
	}
	if opts.Reason != nil {
		b["reason"] = *opts.Reason
	}
	if opts.ExperimentMetadata != nil {
		b["experiment_metadata"] = opts.ExperimentMetadata
	}
	if opts.OperationID != nil {
		b["operation_id"] = *opts.OperationID
	}
	if opts.Manifest != nil {
		b["manifest"] = opts.Manifest
	}
	if opts.ExpectedVersion != nil {
		b["expected_version"] = *opts.ExpectedVersion
	}
	if opts.ExpectedCursor != nil {
		b["expected_cursor"] = *opts.ExpectedCursor
	}
	key := ""
	if opts.OperationID != nil {
		key = *opts.OperationID
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.forks.create", s.instanceID,
		nil, nil, b, key, "")
	if e != nil {
		return ForkReceipt{}, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	return forkReceiptFromDict(m), nil
}
func (s *FleetSessionTransport) GetChannelMetadata(ctx context.Context, channelID string) (*ChannelMetadata, error) {
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.channels.metadata.get", s.instanceID, map[string]string{"channel_id": channelID}, nil, nil, "", "")
	if e != nil {
		if a, ok := e.(*APIError); ok && a.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	x := channelMetadataFromDict(m)
	return &x, nil
}
func (s *FleetSessionTransport) UpdateMetadata(ctx context.Context, channelID string, opts UpdateMetadataOptions) (ChannelMetadata, error) {
	b := map[string]any{"channel_id": channelID}
	if opts.DisplayName != nil {
		b["display_name"] = *opts.DisplayName
	}
	if opts.Reason != nil {
		b["reason"] = *opts.Reason
	}
	if opts.ExperimentMetadata != nil {
		b["experiment_metadata"] = opts.ExperimentMetadata
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.channels.metadata.put", s.instanceID, map[string]string{"channel_id": channelID}, nil, b, "", "")
	if e != nil {
		return ChannelMetadata{}, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	return channelMetadataFromDict(m), nil
}
func (s *FleetSessionTransport) ResolveStep(ctx context.Context, channelID string, step int64) (*StepResolution, error) {
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.channels.steps.resolve", s.instanceID, map[string]string{"channel_id": channelID, "step_number": fmt.Sprint(step)}, nil, nil, "", "")
	if e != nil {
		if a, ok := e.(*APIError); ok && a.StatusCode == 404 {
			return nil, nil
		}
		return nil, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	return &StepResolution{ChannelID: str(m, "channel_id"), StepNumber: intOf(m, "step_number", step), Cursor: intOf(m, "cursor", 0), EventID: strptr(m, "event_id")}, nil
}

// LatestStepNumber returns the highest step number recorded on a channel
// (0 = none) via the federated manifest; (nil, nil) when unavailable.
func (s *FleetSessionTransport) LatestStepNumber(ctx context.Context, channelID string) (*int64, error) {
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.channels.steps.latest", s.instanceID, map[string]string{"channel_id": channelID}, nil, nil, "", "")
	if e != nil {
		if a, ok := e.(*APIError); ok && a.StatusCode == 404 {
			return nil, nil
		}
		return nil, e
	}
	m, _ := v.(map[string]any)
	if data, ok := m["data"].(map[string]any); ok {
		m = data
	}
	last := intOf(m, "last_step_number", 0)
	return &last, nil
}

func (s *FleetSessionTransport) SetOutcome(ctx context.Context, channelID, outcome string, score *float64) (map[string]any, error) {
	b := map[string]any{"outcome": outcome}
	if score != nil {
		b["score"] = *score
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.channels.outcome.set", s.instanceID,
		map[string]string{"channel_id": channelID}, nil, b, "", "")
	m, _ := v.(map[string]any)
	return m, e
}
func (s *FleetSessionTransport) PromoteChannel(ctx context.Context, channelID string) (map[string]any, error) {
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.forks.promote", s.instanceID,
		map[string]string{"channel_id": channelID}, nil, nil, "", "")
	m, _ := v.(map[string]any)
	return m, e
}
func (s *FleetSessionTransport) CreateExperiment(ctx context.Context, name string, opts ExperimentOptions) (map[string]any, error) {
	b := map[string]any{"name": name}
	if opts.Description != "" {
		b["description"] = opts.Description
	}
	if opts.BaselineChannelID != "" {
		b["baseline_channel_id"] = opts.BaselineChannelID
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.experiments.create", s.instanceID,
		nil, nil, b, "", "")
	m, _ := v.(map[string]any)
	return m, e
}
func (s *FleetSessionTransport) AddExperimentMember(ctx context.Context, groupID, channelID string, opts ExperimentMemberOptions) (map[string]any, error) {
	b := map[string]any{"channel_id": channelID, "role": opts.Role}
	if b["role"] == "" {
		b["role"] = "variant"
	}
	if opts.DeclaredDelta != nil {
		b["declared_delta"] = opts.DeclaredDelta
	}
	v, e := s.fleet.CallManifest(ctx, "actae.api.v1.experiments.members.add", s.instanceID,
		map[string]string{"group_id": groupID}, nil, b, "", "")
	m, _ := v.(map[string]any)
	return m, e
}

func NewFleetClient(opts FleetClientOptions) (*FleetClient, error) {
	if opts.OrganizationToken == "" || opts.Endpoint == "" {
		return nil, fmt.Errorf("organization_token and endpoint are required")
	}
	h := opts.HTTPClient
	if h == nil {
		h = http.DefaultClient
	}
	orgID := opts.OrganizationID
	if orgID == "" {
		orgID = "default"
	}
	tokenEndpoint := opts.TokenEndpoint
	if tokenEndpoint == "" {
		tokenEndpoint = opts.Endpoint
	}
	scopes := opts.RequestedScopes
	if len(scopes) == 0 {
		scopes = DefaultFleetScopes
	}
	return &FleetClient{endpoint: trimEndpoint(opts.Endpoint), tokenEndpoint: trimEndpoint(tokenEndpoint), orgToken: opts.OrganizationToken, organizationID: orgID, httpClient: h, capabilities: map[string]map[string]any{}, requestedScopes: scopes}, nil
}

// NewFleetClientFromEnv loads ACTAE_FLEET_URL, ACTAE_ORG_TOKEN, and
// ACTAE_ORG_ID, with explicit options taking precedence.
func NewFleetClientFromEnv(opts FleetClientOptions) (*FleetClient, error) {
	if opts.Endpoint == "" {
		opts.Endpoint = os.Getenv("ACTAE_FLEET_URL")
	}
	if opts.OrganizationToken == "" {
		opts.OrganizationToken = os.Getenv("ACTAE_ORG_TOKEN")
	}
	if opts.OrganizationID == "" {
		opts.OrganizationID = os.Getenv("ACTAE_ORG_ID")
	}
	if opts.TokenEndpoint == "" {
		opts.TokenEndpoint = os.Getenv("ACTAE_FLEET_TOKEN_URL")
	}
	return NewFleetClient(opts)
}
func (f *FleetClient) gatewayPath(suffix string) string {
	return "/v1/organizations/" + url.PathEscape(f.organizationID) + suffix
}
func trimEndpoint(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
func (f *FleetClient) token(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fleetToken != "" && time.Now().Before(f.fleetTokenExpiresAt) {
		return f.fleetToken, nil
	}
	// PR-006: the canonical exchange body requires purpose, organization_id
	// and the requested scopes — the real SaaS returns 422 without them.
	body := map[string]any{
		"purpose":         "fleet_access",
		"organization_id": f.organizationID,
		"scopes":          f.requestedScopes,
	}
	v, e := f.requestBase(ctx, http.MethodPost, f.tokenEndpoint, "/api/v1/fleet/token", "", body, map[string]string{"Authorization": "Bearer " + f.orgToken})
	if e != nil {
		return "", e
	}
	m, _ := v.(map[string]any)
	if t, ok := m["access_token"].(string); ok {
		f.fleetToken = t
		f.fleetTokenExpiresAt = tokenExpiry(m)
		return t, nil
	}
	if t, ok := m["token"].(string); ok {
		f.fleetToken = t
		f.fleetTokenExpiresAt = tokenExpiry(m)
		return t, nil
	}
	return "", fmt.Errorf("fleet token exchange returned no access token")
}
func tokenExpiry(m map[string]any) time.Time {
	seconds := 300.0
	if n, ok := m["expires_in"].(float64); ok {
		seconds = n
	}
	if seconds < 15 {
		seconds = 0
	}
	return time.Now().Add(time.Duration(seconds-15) * time.Second)
}
func (f *FleetClient) request(ctx context.Context, method, path, instance string, body any, headers map[string]string) (any, error) {
	return f.requestBase(ctx, method, f.endpoint, path, instance, body, headers)
}
func (f *FleetClient) requestBase(ctx context.Context, method, base, path, instance string, body any, headers map[string]string) (any, error) {
	var tok string
	var e error
	if path != "/api/v1/fleet/token" {
		tok, e = f.token(ctx)
		if e != nil {
			return nil, e
		}
	}
	v, e := doJSON(ctx, f.httpClient, method, base+path, body, func(h http.Header) {
		if tok != "" {
			h.Set("Authorization", "Bearer "+tok)
		}
		if instance != "" {
			h.Set("X-Actae-Instance-Id", instance)
		}
		for k, v := range headers {
			h.Set(k, v)
		}
	})
	if e != nil {
		var apiErr *APIError
		if path != "/api/v1/fleet/token" && errors.As(e, &apiErr) && apiErr.StatusCode == http.StatusUnauthorized {
			f.mu.Lock()
			f.fleetToken = ""
			f.fleetTokenExpiresAt = time.Time{}
			f.mu.Unlock()
			// One retry is safe here: the gateway rejected the access token before dispatch.
			return f.requestAfterRefresh(ctx, method, path, instance, body, headers)
		}
		return nil, e
	}
	return v, nil
}
func (f *FleetClient) requestAfterRefresh(ctx context.Context, method, path, instance string, body any, headers map[string]string) (any, error) {
	tok, err := f.token(ctx)
	if err != nil {
		return nil, err
	}
	return doJSON(ctx, f.httpClient, method, f.endpoint+path, body, func(h http.Header) {
		h.Set("Authorization", "Bearer "+tok)
		if instance != "" {
			h.Set("X-Actae-Instance-Id", instance)
		}
		for k, v := range headers {
			h.Set(k, v)
		}
	})
}
func (f *FleetClient) Instances(ctx context.Context) (any, error) {
	return f.request(ctx, http.MethodGet, f.gatewayPath("/instances"), "", nil, nil)
}

func (f *FleetClient) Capabilities(ctx context.Context, instance string) (map[string]any, error) {
	f.mu.Lock()
	cached := f.capabilities[instance]
	f.mu.Unlock()
	if cached != nil {
		return cached, nil
	}
	// PR-017: use the gateway's typed capability endpoint, not a generic
	// /api/v1/auth/capabilities proxy (which the gateway does not expose).
	v, err := f.request(ctx, http.MethodGet, f.gatewayPath("/instances/"+url.PathEscape(instance)+"/capabilities"), "", nil, nil)
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string]any)
	f.mu.Lock()
	f.capabilities[instance] = m
	f.mu.Unlock()
	return m, nil
}

func (f *FleetClient) EnsureOperation(ctx context.Context, operationID, instance string) error {
	m, err := f.Capabilities(ctx, instance)
	if err != nil {
		return err
	}
	// PR-017: the gateway advertises supported fleet operations; also accept
	// the canonical instance's operations list wrapped under `capabilities`.
	raw := m["supported_fleet_operations"]
	if raw == nil {
		if c, ok := m["capabilities"].(map[string]any); ok {
			raw = c["operations"]
			if raw == nil {
				raw = c["operation_ids"]
			}
		}
	}
	if raw == nil {
		raw = m["operations"]
	}
	if raw == nil {
		raw = m["operation_ids"]
	}
	if raw == nil {
		raw = m["supported_operations"]
	}
	if raw == nil {
		return nil
	}
	if list, ok := raw.([]any); ok {
		for _, value := range list {
			if fmt.Sprint(value) == operationID {
				return nil
			}
		}
		return NewAPIError(http.StatusUpgradeRequired, "unsupported API operation: "+operationID)
	}
	return nil
}

// Call invokes an operation from spec/api-parity/v1/manifest.json. Prefer
// the named helpers; Call keeps newly-added manifest operations usable without
// waiting for a client release.
func (f *FleetClient) Call(ctx context.Context, method, path, instance string, query url.Values, body any, operationID, confirmTarget string) (any, error) {
	if (method == http.MethodDelete || strings.HasSuffix(strings.TrimSuffix(path, "/"), "/cancel")) && confirmTarget == "" {
		return nil, fmt.Errorf("confirm target is required for destructive fleet operations")
	}
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	h := map[string]string{}
	if operationID != "" {
		h["Idempotency-Key"] = operationID
	}
	if confirmTarget != "" {
		h["X-Actae-Confirm-Target"] = confirmTarget
	}
	return f.request(ctx, method, path, instance, body, h)
}

// CallManifest invokes a checked-in manifest operation. Path parameters are
// rendered by exact placeholder name; missing and unknown parameters fail
// locally so a typo can never become a customer-controlled proxy path.
func (f *FleetClient) CallManifest(ctx context.Context, operationID, instance string, pathParams map[string]string, query url.Values, body any, idempotencyKey, confirmTarget string) (any, error) {
	op, ok := ManifestOperationByID(operationID)
	if !ok {
		return nil, fmt.Errorf("unknown API manifest operation: %s", operationID)
	}
	path := op.Path
	for name, value := range pathParams {
		placeholder := "{" + name + "}"
		if !strings.Contains(path, placeholder) {
			return nil, fmt.Errorf("unknown path parameter for %s: %s", operationID, name)
		}
		path = strings.Replace(path, placeholder, url.PathEscape(value), 1)
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '{' {
			return nil, fmt.Errorf("missing path parameter for %s", operationID)
		}
	}
	if instance == "" {
		return nil, fmt.Errorf("instance_id is required for fleet operation %s", operationID)
	}
	// PR-016: auth/session/dashboard operations are NOT fleet commands. Fail
	// locally before any network I/O.
	if !op.FleetSupported {
		return nil, NewAPIError(http.StatusUpgradeRequired, "unsupported API operation: "+operationID)
	}
	// Confirmation is manifest-driven (not hardcoded to DELETE).
	if op.Confirmation && confirmTarget == "" {
		return nil, fmt.Errorf("confirm target is required for %s", operationID)
	}
	pathParamsEnvelope := map[string]string{}
	for k, v := range pathParams {
		pathParamsEnvelope[k] = v
	}
	queryEnvelope := map[string]any{}
	for k, values := range query {
		if len(values) == 1 {
			queryEnvelope[k] = values[0]
		} else {
			queryEnvelope[k] = values
		}
	}
	envelope := map[string]any{"path_params": pathParamsEnvelope, "query": queryEnvelope, "body": body}
	if body == nil {
		envelope["body"] = map[string]any{}
	}
	if op.Action != "read" && idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	return f.request(ctx, http.MethodPost, f.gatewayPath("/instances/"+url.PathEscape(instance)+"/ops/"+url.PathEscape(operationID)), "", envelope, map[string]string{"Idempotency-Key": idempotencyKey, "X-Actae-Confirm-Target": confirmTarget})
}

func (f *FleetClient) CallOperation(ctx context.Context, operationID, method, path, instance string, query url.Values, body any, idempotencyKey, confirmTarget string) (any, error) {
	if err := f.EnsureOperation(ctx, operationID, instance); err != nil {
		return nil, err
	}
	return f.Call(ctx, method, path, instance, query, body, idempotencyKey, confirmTarget)
}
func (f *FleetClient) Record(ctx context.Context, instance, channel, event string, payload any, operationID string) (any, error) {
	b := map[string]any{"channel_id": channel, "event_type": event, "payload": payload}
	if operationID != "" {
		b["operation_id"] = operationID
	}
	return f.request(ctx, http.MethodPost, f.gatewayPath("/instances/"+url.PathEscape(instance)+"/events"), "", b, nil)
}
func (f *FleetClient) Query(ctx context.Context, instance string, q url.Values) (any, error) {
	return f.QueryWithToken(ctx, instance, q, "")
}

// QueryWithToken is Query with an explicit opaque keyset continuation token.
// PR-008: the token travels as the top-level `page_tokens` map, never inside
// the canonical `query` object.
func (f *FleetClient) QueryWithToken(ctx context.Context, instance string, q url.Values, pageToken string) (any, error) {
	body := map[string]any{}
	for key, values := range q {
		if len(values) == 1 {
			body[key] = values[0]
		} else {
			body[key] = values
		}
	}
	// PR-007: keyset pagination must be explicit so even a tokenless FIRST
	// page returns a continuation token (legacy offset mode cannot start).
	if _, ok := body["pagination"]; !ok {
		body["pagination"] = "keyset"
	}
	envelope := map[string]any{"instances": []string{instance}, "query": body}
	if pageToken != "" {
		envelope["page_tokens"] = map[string]string{instance: pageToken}
	}
	return f.request(ctx, http.MethodPost, f.gatewayPath("/events/query"), "", envelope, nil)
}

// QueryPages iterates opaque keyset pages without assuming per-channel
// numbering. The callback runs synchronously and may stop early by returning
// an error; cancellation is propagated through ctx. Continuation tokens are
// passed through the gateway's top-level `page_tokens` map (PR-008).
func (f *FleetClient) QueryPages(ctx context.Context, instance string, q url.Values, visit func(FleetPage) error) error {
	if q == nil {
		q = url.Values{}
	}
	var token string
	for {
		value, err := f.QueryWithToken(ctx, instance, q, token)
		if err != nil {
			return err
		}
		raw, _ := value.(map[string]any)
		page := FleetPage{}
		if events, ok := raw["events"].([]any); ok {
			page.Items = events
		} else if items, ok := raw["items"].([]any); ok {
			page.Items = items
		}
		if p, ok := raw["page"].(map[string]any); ok {
			if tokens, ok := p["next_page_tokens"].(map[string]any); ok {
				page.NextPageToken, _ = tokens[instance].(string)
				page.PageTokens = stringMap(tokens)
			}
		}
		if page.NextPageToken == "" {
			page.NextPageToken, _ = raw["next_page_token"].(string)
		}
		if err := visit(page); err != nil {
			return err
		}
		if page.NextPageToken == "" {
			return nil
		}
		token = page.NextPageToken
	}
}

func stringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func (f *FleetClient) QueryMany(ctx context.Context, instances []string, body map[string]any) (PartialResult, error) {
	return f.QueryManyWithTokens(ctx, instances, body, nil)
}

// QueryManyWithTokens is QueryMany with an explicit per-instance continuation
// map. PR-008: selectors are `instances`, filters are `query`, continuation
// is the top-level `page_tokens` map; the full `next_page_tokens` map is
// preserved in the result (never collapsed to a single token).
func (f *FleetClient) QueryManyWithTokens(ctx context.Context, instances []string, body map[string]any, pageTokens map[string]string) (PartialResult, error) {
	if body == nil {
		body = map[string]any{}
	}
	// PR-007: keyset pagination must be explicit so even a tokenless FIRST
	// page returns a continuation token.
	if _, ok := body["pagination"]; !ok {
		body["pagination"] = "keyset"
	}
	envelope := map[string]any{"instances": instances, "query": body}
	if len(pageTokens) > 0 {
		envelope["page_tokens"] = pageTokens
	}
	v, err := f.request(ctx, http.MethodPost, f.gatewayPath("/events/query"), "", envelope, nil)
	if err != nil {
		return PartialResult{}, err
	}
	raw, _ := v.(map[string]any)
	result := PartialResult{}
	if items, ok := raw["events"].([]any); ok {
		result.Items = items
	} else if items, ok := raw["items"].([]any); ok {
		result.Items = items
	}
	if token, ok := raw["next_page_token"].(string); ok {
		result.NextPageToken = token
	}
	if page, ok := raw["page"].(map[string]any); ok {
		if tokens, ok := page["next_page_tokens"].(map[string]any); ok {
			result.PageTokens = stringMap(tokens)
			if len(instances) == 1 {
				result.NextPageToken, _ = tokens[instances[0]].(string)
			}
		}
	}
	if errs, ok := raw["errors"].([]any); ok {
		for _, entry := range errs {
			if e, ok := entry.(map[string]any); ok {
				msg := fmt.Sprint(e["error"])
				if msg == "<nil>" {
					msg = fmt.Sprint(e["message"])
				}
				result.Errors = append(result.Errors, InstanceError{InstanceID: fmt.Sprint(e["instance_id"]), Code: msg, Message: msg, Retryable: e["retryable"] == true})
			}
		}
	}
	return result, nil
}
func (f *FleetClient) Replay(ctx context.Context, instance, channel string, q url.Values) (any, error) {
	if q == nil {
		q = url.Values{}
	}
	return f.CallManifest(ctx, "actae.api.v1.events.replay", instance,
		map[string]string{"channel_id": channel}, q, nil, "", "")
}
func (f *FleetClient) Fork(ctx context.Context, instance, source, child string, atCursor int64, operationID string) (any, error) {
	b := map[string]any{"source_channel_id": source, "new_channel_id": child, "at_cursor": atCursor}
	if operationID != "" {
		b["operation_id"] = operationID
	}
	return f.request(ctx, http.MethodPost, f.gatewayPath("/instances/"+url.PathEscape(instance)+"/forks"), "", b, nil)
}
func (f *FleetClient) SaveState(ctx context.Context, instance, channel string, cursor int64, state any) (any, error) {
	return f.request(ctx, http.MethodPut, f.gatewayPath("/instances/"+url.PathEscape(instance)+"/state/"+url.PathEscape(channel)), "", map[string]any{"channel_id": channel, "cursor": cursor, "state": state}, nil)
}
func (f *FleetClient) LatestState(ctx context.Context, instance, channel string) (any, error) {
	return f.CallManifest(ctx, "actae.api.v1.state.load", instance,
		map[string]string{"channel_id": channel}, nil, nil, "", "")
}
