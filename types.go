package actae

// Event is a single event persisted in an Actae channel. Events are the core
// unit of data in Actae: every mutation is recorded as an event with a
// monotonic cursor that determines ordering.
type Event struct {
	// ID is the server-assigned event UUID.
	ID string
	// ChannelID is the channel this event belongs to (a channel is the
	// same concept as a WebSocket "topic").
	ChannelID string
	// EventType is the user-defined type label (e.g. "agent.step").
	EventType string
	// Payload is the event data. JSON numbers arrive as int64 when
	// integral and float64 otherwise; use the As* accessors or marshal
	// into your own struct.
	Payload any
	// Actor is who/what created the event.
	Actor string
	// Cursor is the gapless, per-channel event index (always ≥ 1, never
	// skips). Pass it to Replay or Subscribe as the resume position.
	Cursor int64
	// ChannelCursor is a compatibility alias for Cursor: the server emits
	// the same value under both names so older clients keep working. Prefer
	// Cursor.
	ChannelCursor *int64
	// Timestamp is the server-assigned RFC 3339 timestamp.
	Timestamp string
	// AgentID is the agent identifier recorded via RecordOptions.AgentID
	// (only present on replay/query/broadcast paths).
	AgentID *string
	// UserID is the user identifier recorded via RecordOptions.UserID
	// (only present on replay/query/broadcast paths).
	UserID *string
	// Metadata is the metadata dict recorded via RecordOptions.Metadata
	// (only present on replay/query/broadcast paths).
	Metadata map[string]any
	// DependsOn is the ID of the parent event this event causally depends
	// on (set for fork lineage markers and tool-execution lifecycle
	// events).
	DependsOn *string
	// DeliveryID is the per-subscriber delivery tracking ID assigned by
	// the server for a broadcast (WebSocket delivery only).
	DeliveryID *string
}

type EventDependency struct {
	EventID          string
	DependsOnEventID string
	DependencyType   string
}
type CausalGraph struct {
	RootEventID  string
	Events       []Event
	Dependencies []EventDependency
	Truncated    bool
}

// ExecutionGroup is a durable coordination namespace for independently
// executing agents. It is distinct from consumer GroupInfo.
type ExecutionGroup struct {
	GroupID   string
	Status    string
	Metadata  map[string]any
	CreatedAt string
	UpdatedAt string
}
type ExecutionGroupMember struct {
	GroupID, MemberID, ChannelID string
	Role, OwnerID                *string
	Metadata                     map[string]any
	Generation                   int64
	LeaseUntil                   *string
}
type MemberLease struct {
	GroupID, MemberID, OwnerID string
	Generation                 int64
	LeaseUntil                 string
}
type GroupMessage struct {
	MessageID, GroupID, FromMemberID, ToMemberID, MessageType string
	Payload                                                   any
	CausalContext                                             map[string]any
	SourceEventID, DeliveryEventID, Status                    string
	AcknowledgedAt                                            *string
}

func executionGroupFrom(m map[string]any) ExecutionGroup {
	return ExecutionGroup{GroupID: str(m, "group_id"), Status: str(m, "status"), Metadata: mapOf(m, "metadata"), CreatedAt: str(m, "created_at"), UpdatedAt: str(m, "updated_at")}
}
func executionGroupMemberFrom(m map[string]any) ExecutionGroupMember {
	return ExecutionGroupMember{GroupID: str(m, "group_id"), MemberID: str(m, "member_id"), ChannelID: str(m, "channel_id"), Role: strptr(m, "role"), OwnerID: strptr(m, "owner_id"), Metadata: mapOf(m, "metadata"), Generation: intOf(m, "generation", 0), LeaseUntil: strptr(m, "lease_until")}
}
func memberLeaseFrom(m map[string]any) MemberLease {
	return MemberLease{GroupID: str(m, "group_id"), MemberID: str(m, "member_id"), OwnerID: str(m, "owner_id"), Generation: intOf(m, "generation", 0), LeaseUntil: str(m, "lease_until")}
}
func groupMessageFrom(m map[string]any) GroupMessage {
	return GroupMessage{MessageID: str(m, "message_id"), GroupID: str(m, "group_id"), FromMemberID: str(m, "from_member_id"), ToMemberID: str(m, "to_member_id"), MessageType: str(m, "type"), Payload: unwrapSonic(m["payload"]), CausalContext: mapOf(m, "causal_context"), SourceEventID: str(m, "source_event_id"), DeliveryEventID: str(m, "delivery_event_id"), Status: str(m, "status"), AcknowledgedAt: strptr(m, "acknowledged_at")}
}

func eventFromRecord(m map[string]any) Event {
	return Event{
		ID:            str(m, "id"),
		ChannelID:     str(m, "channel_id"),
		EventType:     str(m, "type"),
		Payload:       unwrapSonic(m["payload"]),
		Actor:         str(m, "actor"),
		Cursor:        intOf(m, "cursor", 0),
		ChannelCursor: int64ptr(m, "channel_cursor"),
		Timestamp:     str(m, "timestamp"),
		DependsOn:     strptr(m, "depends_on"),
	}
}

func eventFromReplay(m map[string]any) Event {
	e := eventFromRecord(m)
	e.AgentID = strptr(m, "agent_id")
	e.Metadata = mapOf(m, "metadata")
	return e
}

func eventFromQuery(m map[string]any) Event {
	e := eventFromRecord(m)
	e.AgentID = strptr(m, "agent_id")
	return e
}

func eventFromBroadcast(m map[string]any) Event {
	typ := str(m, "event_type")
	if typ == "" {
		typ = str(m, "type")
	}
	if typ == "" {
		typ = "broadcast"
	}
	return Event{
		ID:            str(m, "id"),
		ChannelID:     str(m, "channel_id"),
		EventType:     typ,
		Payload:       unwrapSonic(m["payload"]),
		Actor:         str(m, "actor"),
		Cursor:        intOf(m, "cursor", 0),
		ChannelCursor: int64ptr(m, "channel_cursor"),
		Timestamp:     str(m, "timestamp"),
		Metadata:      mapOf(m, "metadata"),
		DependsOn:     strptr(m, "depends_on"),
		DeliveryID:    strptr(m, "delivery_id"),
	}
}

// HealthComponent is the status of a single component within a health check.
type HealthComponent struct {
	Status  string
	Details map[string]any
}

// HealthStatus is the result of GET /healthz.
type HealthStatus struct {
	Status          string
	Timestamp       string
	InstanceID      string
	Components      map[string]HealthComponent
	CheckDurationMS int64
}

func healthStatusFromResponse(m map[string]any) HealthStatus {
	components := map[string]HealthComponent{}
	for name, raw := range mapOf(m, "components") {
		comp, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		details := map[string]any{}
		for k, v := range comp {
			if k != "status" {
				details[k] = v
			}
		}
		components[name] = HealthComponent{Status: str(comp, "status"), Details: details}
	}
	return HealthStatus{
		Status:          str(m, "status"),
		Timestamp:       str(m, "timestamp"),
		InstanceID:      str(m, "instance_id"),
		Components:      components,
		CheckDurationMS: intOf(m, "check_duration_ms", 0),
	}
}

// ReadinessResult is the result of GET /readyz.
type ReadinessResult struct {
	Status                string
	Timestamp             string
	InstanceID            string
	CheckDurationMS       int64
	DatabaseReady         bool
	CapacityAvailable     bool
	UptimeSeconds         int64
	ConnectionUtilization float64
	ActiveConnections     int64
	MaxConnections        int64
}

func readinessResultFromResponse(m map[string]any) ReadinessResult {
	checks := mapOf(m, "readiness_checks")
	return ReadinessResult{
		Status:                str(m, "status"),
		Timestamp:             str(m, "timestamp"),
		InstanceID:            str(m, "instance_id"),
		CheckDurationMS:       intOf(m, "check_duration_ms", 0),
		DatabaseReady:         boolOf(checks, "database_ready", false),
		CapacityAvailable:     boolOf(checks, "capacity_available", false),
		UptimeSeconds:         intOf(checks, "uptime_seconds", 0),
		ConnectionUtilization: floatOf(checks, "connection_utilization", 0),
		ActiveConnections:     intOf(checks, "active_connections", 0),
		MaxConnections:        intOf(checks, "max_connections", 0),
	}
}

func floatOf(m map[string]any, key string, def float64) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return def
}

// MetricsSnapshot is the result of GET /metrics.json.
type MetricsSnapshot struct {
	Status               string
	Timestamp            string
	UptimeSeconds        int64
	WebsocketConnections int64
	TopicCount           int64
	MessagesSent         int64
	MessagesReceived     int64
	TotalMessages        int64
	BackPressure         map[string]any
}

func metricsSnapshotFromResponse(m map[string]any) MetricsSnapshot {
	ws := mapOf(m, "websocket")
	bp := mapOf(ws, "back_pressure")
	if bp == nil {
		bp = map[string]any{}
	}
	return MetricsSnapshot{
		Status:               str(m, "status"),
		Timestamp:            str(m, "timestamp"),
		UptimeSeconds:        intOf(m, "uptime_seconds", 0),
		WebsocketConnections: intOf(ws, "connections", 0),
		TopicCount:           intOf(ws, "topics", 0),
		MessagesSent:         intOf(ws, "messages_sent", 0),
		MessagesReceived:     intOf(ws, "messages_received", 0),
		TotalMessages:        intOf(ws, "total_messages", 0),
		BackPressure:         bp,
	}
}

// UserInfo is a user profile returned by the auth endpoints.
type UserInfo struct {
	ID            string
	Email         string
	EmailVerified bool
	Name          *string
	Image         *string
	CreatedAt     *string
}

func userInfoFromDict(m map[string]any) UserInfo {
	return UserInfo{
		ID:            str(m, "id"),
		Email:         str(m, "email"),
		EmailVerified: boolOf(m, "email_verified", false),
		Name:          strptr(m, "name"),
		Image:         strptr(m, "image"),
		CreatedAt:     strptr(m, "created_at"),
	}
}

// AuthResult is returned by Signup and Login.
type AuthResult struct {
	User  UserInfo
	Token string
}

func authResultFromResponse(m map[string]any) AuthResult {
	return AuthResult{
		User:  userInfoFromDict(mapOf(m, "user")),
		Token: str(m, "token"),
	}
}

// ChannelMetadata is metadata for a channel or fork in the Actae execution
// tree, returned by Fork, GetChannelMetadata and ListForks.
type ChannelMetadata struct {
	ChannelID       string
	ParentChannelID *string
	OriginRunID     string
	// ForkedAtCursor is the RESOLVED fork boundary: the snapshot cursor the
	// child actually inherited (equals ResolvedStateCursor; 0 for roots).
	ForkedAtCursor     int64
	ForkedAt           string
	DisplayName        *string
	Reason             *string
	ExperimentMetadata map[string]any
	// RequestedAtCursor is the boundary the caller asked for (0 = latest).
	RequestedAtCursor int64
	// ResolvedStateCursor is the snapshot cursor the child actually inherited.
	ResolvedStateCursor int64
	// ResolvedCursor is an alias of ResolvedStateCursor.
	ResolvedCursor int64
	// ResolvedEventID is the parent boundary event the fork.started depends on.
	ResolvedEventID *string
	// SourceStateVersion is the source snapshot version copied (0 = none).
	SourceStateVersion int64
	// SourceStateSHA256 is the SHA-256 fingerprint of the copied state.
	SourceStateSHA256 *string
	// Restorable is true when a state snapshot was copied (restorable checkpoint).
	Restorable bool
	// Manifest is the immutable fork manifest supplied at fork time.
	Manifest map[string]any
	// Reproducibility is the grade: state_exact | context_exact |
	// execution_replayable | best_effort.
	Reproducibility *string
	// ToolPolicies is a forked channel's side-effect policy map (nil = auto).
	ToolPolicies map[string]any
	// Outcome: promoted | rejected | inconclusive | crashed.
	Outcome *string
	// ResultScore recorded for ranking (higher is better).
	ResultScore *float64
	// DeletedAt marks a soft-deleted channel (retention/GC).
	DeletedAt *string
}

func channelMetadataFromDict(m map[string]any) ChannelMetadata {
	return ChannelMetadata{
		ChannelID:           str(m, "channel_id"),
		ParentChannelID:     strptr(m, "parent_channel_id"),
		OriginRunID:         str(m, "origin_run_id"),
		ForkedAtCursor:      intOf(m, "forked_at_cursor", 0),
		ForkedAt:            str(m, "forked_at"),
		DisplayName:         strptr(m, "display_name"),
		Reason:              strptr(m, "reason"),
		ExperimentMetadata:  sonicMap(m, "experiment_metadata"),
		RequestedAtCursor:   intOf(m, "requested_at_cursor", 0),
		ResolvedStateCursor: intOf(m, "resolved_state_cursor", 0),
		ResolvedCursor:      intOf(m, "resolved_cursor", intOf(m, "resolved_state_cursor", 0)),
		ResolvedEventID:     strptr(m, "resolved_event_id"),
		SourceStateVersion:  intOf(m, "source_state_version", 0),
		SourceStateSHA256:   strptr(m, "source_state_sha256"),
		Restorable:          boolOf(m, "restorable", false),
		Manifest:            sonicMap(m, "manifest"),
		Reproducibility:     strptr(m, "reproducibility"),
		ToolPolicies:        sonicMap(m, "tool_policies"),
		Outcome:             strptr(m, "outcome"),
		ResultScore:         float64ptr(m, "result_score"),
		DeletedAt:           strptr(m, "deleted_at"),
	}
}

// ForkReceipt is the immutable fork receipt returned at creation and
// queryable via GetForkReceipt. This is the provenance record the dashboard
// and audit narrative are built around.
type ForkReceipt struct {
	ForkID             string
	SourceChannelID    *string
	ChildChannelID     string
	RequestedCursor    int64
	ResolvedCursor     int64
	ResolvedEventID    *string
	SourceStateVersion int64
	SourceStateSHA256  *string
	Restorable         bool
	Replayed           bool
	Manifest           map[string]any
	Reproducibility    *string
	// ToolPolicies is the child channel's side-effect policy map
	// ({"tool": "replay"|"block"|"live"|"auto", "*": default}); nil means the
	// server default (auto).
	ToolPolicies map[string]any
}

func forkReceiptFromDict(m map[string]any) ForkReceipt {
	forkID := str(m, "fork_id")
	if forkID == "" {
		forkID = str(m, "channel_id")
	}
	childID := str(m, "child_channel_id")
	if childID == "" {
		childID = str(m, "channel_id")
	}
	requested := intOf(m, "requested_cursor", intOf(m, "requested_at_cursor", 0))
	resolved := intOf(m, "resolved_cursor", intOf(m, "resolved_state_cursor", 0))
	return ForkReceipt{
		ForkID:             forkID,
		SourceChannelID:    strptr(m, "source_channel_id"),
		ChildChannelID:     childID,
		RequestedCursor:    requested,
		ResolvedCursor:     resolved,
		ResolvedEventID:    strptr(m, "resolved_event_id"),
		SourceStateVersion: intOf(m, "source_state_version", 0),
		SourceStateSHA256:  strptr(m, "source_state_sha256"),
		Restorable:         boolOf(m, "restorable", false),
		Replayed:           boolOf(m, "replayed", false),
		Manifest:           sonicMap(m, "manifest"),
		Reproducibility:    strptr(m, "reproducibility"),
		ToolPolicies:       sonicMap(m, "tool_policies"),
	}
}

// ForkInfo is a recursive execution tree node returned by
// GetForkTree.
type ForkInfo struct {
	ChannelID      string
	DisplayName    *string
	Reason         *string
	ForkedAtCursor int64
	EventCount     int64
	LatestCursor   *int64
	Children       []*ForkInfo
}

func forkInfoFromDict(m map[string]any) *ForkInfo {
	b := &ForkInfo{
		ChannelID:      str(m, "channel_id"),
		DisplayName:    strptr(m, "display_name"),
		Reason:         strptr(m, "reason"),
		ForkedAtCursor: intOf(m, "forked_at_cursor", 0),
		EventCount:     intOf(m, "event_count", 0),
		LatestCursor:   int64ptr(m, "latest_cursor"),
	}
	for _, raw := range listOf(m, "children") {
		if child, ok := raw.(map[string]any); ok {
			b.Children = append(b.Children, forkInfoFromDict(child))
		}
	}
	return b
}

// TransitionResult is the result of an atomic event+state transition.
type TransitionResult struct {
	Event        Event
	StateVersion int64
}

// StateSnapshot is a cursor-aligned versioned state snapshot returned by
// LatestState and GetState.
type StateSnapshot struct {
	// Cursor is the channel cursor the snapshot is aligned to.
	Cursor int64
	// Version is the immutable version number. The latest-state response
	// does not carry it (0); versioned lookups do.
	Version int64
	// Timestamp is the snapshot time (RFC 3339); empty on LatestState.
	Timestamp string
	// State is the state dict.
	State map[string]any
}

// StateVersionInfo is version-history metadata for a channel (no state
// blob), returned by ListStates.
type StateVersionInfo struct {
	Version   int64
	Cursor    int64
	Timestamp string
}

// StatePoint is a point-in-time state snapshot referenced by a diff or
// decision trail. State is a generic map; use Actae As* helpers or
// re-marshal into your own struct as needed.
type StatePoint struct {
	ChannelID string
	Cursor    int64
	State     map[string]any
}

// StateDiffEntry is one structural difference between two fork states at a
// JSON path. Kind is "added" (right-only), "removed" (left-only), or
// "changed" (both differ). Left/Right carry the value on each side (nil for
// added/removed).
type StateDiffEntry struct {
	Path  []string
	Kind  string
	Left  any
	Right any
}

// StateDiff is the structural diff of two channels' latest saved states.
// Common is the shared ancestor state when both channels descend from the
// same origin_run_id (sibling forks), else nil.
type StateDiff struct {
	LeftChannelID         string
	RightChannelID        string
	Left                  StatePoint
	Right                 StatePoint
	Common                *StatePoint
	LeftDivergedAtCursor  *int64
	RightDivergedAtCursor *int64
	Entries               []StateDiffEntry
	Truncated             bool
	EntryCountTotal       int
	MaxEntries            int
}

// LineageHop is one hop in a channel's lineage chain back to its root run.
type LineageHop struct {
	ChannelID       string
	ParentChannelID *string
	ForkedAtCursor  int64
	DisplayName     *string
	Reason          *string
}

// ExecutionLedgerEntry is one idempotent tool-execution ledger row surfaced
// by DecisionTrail.
type ExecutionLedgerEntry struct {
	ID              string
	ChannelID       string
	KeyName         string
	ToolName        string
	Status          string
	Params          any
	Result          any
	Error           any
	StartedCursor   *int64
	CompletedCursor *int64
}

// DecisionTrail is the full lineage-as-audit view for a channel: the chain
// back to the origin_run_id root, the boundary state it forked from, and the
// tool-execution ledger rows that happened on this channel.
type DecisionTrail struct {
	ChannelID   string
	OriginRunID string
	Ancestry    []LineageHop
	Boundary    *StatePoint
	Executions  []ExecutionLedgerEntry
}

func statePointFrom(m map[string]any) *StatePoint {
	if m == nil {
		return nil
	}
	return &StatePoint{
		ChannelID: str(m, "channel_id"),
		Cursor:    intOf(m, "cursor", 0),
		State:     mapOf(m, "state"),
	}
}

func stateDiffEntryFrom(m map[string]any) StateDiffEntry {
	return StateDiffEntry{
		Path:  stringListOf(m, "path"),
		Kind:  str(m, "kind"),
		Left:  m["left"],
		Right: m["right"],
	}
}

func stateDiffFrom(m map[string]any) StateDiff {
	out := StateDiff{
		LeftChannelID:  str(m, "left_channel_id"),
		RightChannelID: str(m, "right_channel_id"),
		Left:           StatePoint{},
		Right:          StatePoint{},
	}
	if lp := statePointFrom(mapOf(m, "left")); lp != nil {
		out.Left = *lp
	}
	if rp := statePointFrom(mapOf(m, "right")); rp != nil {
		out.Right = *rp
	}
	if c := mapOf(m, "common"); c != nil {
		out.Common = statePointFrom(c)
	}
	out.LeftDivergedAtCursor = int64ptr(m, "left_diverged_at_cursor")
	out.RightDivergedAtCursor = int64ptr(m, "right_diverged_at_cursor")
	out.Truncated = boolOf(m, "truncated", false)
	out.EntryCountTotal = int(intOf(m, "entry_count_total", 0))
	out.MaxEntries = int(intOf(m, "max_entries", 500))
	for _, raw := range listOf(m, "entries") {
		if e, ok := raw.(map[string]any); ok {
			out.Entries = append(out.Entries, stateDiffEntryFrom(e))
		}
	}
	return out
}

func lineageHopFrom(m map[string]any) LineageHop {
	return LineageHop{
		ChannelID:       str(m, "channel_id"),
		ParentChannelID: strptr(m, "parent_channel_id"),
		ForkedAtCursor:  intOf(m, "forked_at_cursor", 0),
		DisplayName:     strptr(m, "display_name"),
		Reason:          strptr(m, "reason"),
	}
}

func executionLedgerEntryFrom(m map[string]any) ExecutionLedgerEntry {
	return ExecutionLedgerEntry{
		ID:              str(m, "id"),
		ChannelID:       str(m, "channel_id"),
		KeyName:         str(m, "key_name"),
		ToolName:        str(m, "tool_name"),
		Status:          str(m, "status"),
		Params:          m["params"],
		Result:          m["result"],
		Error:           m["error"],
		StartedCursor:   int64ptr(m, "started_cursor"),
		CompletedCursor: int64ptr(m, "completed_cursor"),
	}
}

func decisionTrailFrom(m map[string]any) DecisionTrail {
	out := DecisionTrail{
		ChannelID:   str(m, "channel_id"),
		OriginRunID: str(m, "origin_run_id"),
	}
	if b := mapOf(m, "boundary"); b != nil {
		out.Boundary = statePointFrom(b)
	}
	for _, raw := range listOf(m, "ancestry") {
		if h, ok := raw.(map[string]any); ok {
			out.Ancestry = append(out.Ancestry, lineageHopFrom(h))
		}
	}
	for _, raw := range listOf(m, "executions") {
		if e, ok := raw.(map[string]any); ok {
			out.Executions = append(out.Executions, executionLedgerEntryFrom(e))
		}
	}
	return out
}

// stringListOf extracts a []string for a key (used for diff entry paths).
func stringListOf(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// stateSnapshotFrom builds a *StateSnapshot from a state endpoint response
// body. Returns nil when the body has no cursor (no state saved / missing
// version).
func stateSnapshotFrom(m map[string]any) *StateSnapshot {
	if int64ptr(m, "cursor") == nil {
		return nil
	}
	s := &StateSnapshot{
		Cursor:    intOf(m, "cursor", 0),
		Version:   intOf(m, "version", 0),
		Timestamp: str(m, "timestamp"),
	}
	if st, ok := unwrapSonic(m["state"]).(map[string]any); ok {
		s.State = st
	}
	return s
}

// GroupInfo is a durable consumer group bound to a channel.
type GroupInfo struct {
	GroupID   string
	ChannelID string
	CreatedAt string
	Metadata  map[string]any
}

func groupInfoFromResponse(m map[string]any) GroupInfo {
	return GroupInfo{
		GroupID:   str(m, "group_id"),
		ChannelID: str(m, "channel_id"),
		CreatedAt: str(m, "created_at"),
		Metadata:  sonicMap(m, "metadata"),
	}
}

// GroupOffset is a consumer's durable offset state within a group.
type GroupOffset struct {
	GroupID       string
	ConsumerID    string
	LastCursor    int64
	ClaimedCursor int64
	UpdatedAt     string
}

func groupOffsetFromResponse(m map[string]any) GroupOffset {
	return GroupOffset{
		GroupID:       str(m, "group_id"),
		ConsumerID:    str(m, "consumer_id"),
		LastCursor:    intOf(m, "last_cursor", 0),
		ClaimedCursor: intOf(m, "claimed_cursor", 0),
		UpdatedAt:     str(m, "updated_at"),
	}
}

// ClaimedWork is a work batch claimed by a consumer.
type ClaimedWork struct {
	GroupID    string
	ConsumerID string
	Events     []Event
	LeaseUntil string
}

func claimedWorkFromResponse(m map[string]any) ClaimedWork {
	w := ClaimedWork{
		GroupID:    str(m, "group_id"),
		ConsumerID: str(m, "consumer_id"),
		LeaseUntil: str(m, "lease_until"),
	}
	for _, raw := range listOf(m, "events") {
		if ev, ok := raw.(map[string]any); ok {
			w.Events = append(w.Events, eventFromReplay(ev))
		}
	}
	return w
}

// Wakeup is a persisted scheduled wake-up.
type Wakeup struct {
	ID         string
	ChannelID  string
	RunAt      string
	Status     string
	Payload    map[string]any
	CreatedAt  string
	ExecutedAt *string
	Attempts   int64
	Error      *string
}

func wakeupFromResponse(m map[string]any) Wakeup {
	return Wakeup{
		ID:         str(m, "id"),
		ChannelID:  str(m, "channel_id"),
		RunAt:      str(m, "run_at"),
		Status:     str(m, "status"),
		Payload:    sonicMap(m, "payload"),
		CreatedAt:  str(m, "created_at"),
		ExecutedAt: strptr(m, "executed_at"),
		Attempts:   intOf(m, "attempts", 0),
		Error:      strptr(m, "error"),
	}
}

// ExecutionInfo is a single idempotent tool execution ledger record.
// Status is one of running | completed | failed | interrupted | cancelled |
// expired.
type ExecutionInfo struct {
	ID               string
	ChannelID        string
	KeyName          string
	ToolName         string
	Status           string
	Attempts         int64
	LeaseUntil       *string
	StartedCursor    *int64
	CompletedCursor  *int64
	StartedEventID   *string
	CompletedEventID *string
	ReplayEmitted    bool
	Params           map[string]any
	Result           any
	Error            any
	CreatedAt        string
	UpdatedAt        string
	CompletedAt      *string
}

func executionInfoFromResponse(m map[string]any) ExecutionInfo {
	return ExecutionInfo{
		ID:               str(m, "id"),
		ChannelID:        str(m, "channel_id"),
		KeyName:          str(m, "key_name"),
		ToolName:         str(m, "tool_name"),
		Status:           str(m, "status"),
		Attempts:         intOf(m, "attempts", 0),
		LeaseUntil:       strptr(m, "lease_until"),
		StartedCursor:    int64ptr(m, "started_cursor"),
		CompletedCursor:  int64ptr(m, "completed_cursor"),
		StartedEventID:   strptr(m, "started_event_id"),
		CompletedEventID: strptr(m, "completed_event_id"),
		ReplayEmitted:    boolOf(m, "replay_emitted", false),
		Params:           sonicMap(m, "params"),
		Result:           unwrapSonic(m["result"]),
		Error:            unwrapSonic(m["error"]),
		CreatedAt:        str(m, "created_at"),
		UpdatedAt:        str(m, "updated_at"),
		CompletedAt:      strptr(m, "completed_at"),
	}
}

// ExecutionClaim is the outcome of claiming an idempotent tool execution.
// Status is one of claimed | replayed | reclaimed | in_progress.
type ExecutionClaim struct {
	Status     string
	Execution  ExecutionInfo
	Result     any
	ClaimToken *string
}

func executionClaimFromResponse(m map[string]any) ExecutionClaim {
	return ExecutionClaim{
		Status:     str(m, "status"),
		Execution:  executionInfoFromResponse(mapOf(m, "execution")),
		Result:     unwrapSonic(m["result"]),
		ClaimToken: strptr(m, "claim_token"),
	}
}

// Capabilities is the parsed result of GET /api/v1/auth/capabilities: the
// caller's key scopes mirroring the server's ApiKeyPermissions pattern
// matching. Use CanRead/CanWrite/CanPublish/CanFork to ask before acting
// (selling point 5).
type Capabilities struct {
	AuthMethod string
	KeyID      string
	KeySource  string
	ExpiresAt  string
	Read       []string
	Write      []string
	Delete     []string
	Admin      []string
}

func capabilitiesFromDict(m map[string]any) Capabilities {
	perms := mapOf(m, "permissions")
	return Capabilities{
		AuthMethod: str(m, "auth_method"),
		KeyID:      str(m, "key_id"),
		KeySource:  str(m, "key_source"),
		ExpiresAt:  str(m, "expires_at"),
		Read:       listOfStrings(perms, "read"),
		Write:      listOfStrings(perms, "write"),
		Delete:     listOfStrings(perms, "delete"),
		Admin:      listOfStrings(perms, "admin"),
	}
}

func listOfStrings(m map[string]any, key string) []string {
	out := []string{}
	for _, v := range listOf(m, key) {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// matchesPattern mirrors the server's ApiKeyPermissions matching: "*" matches
// everything, a trailing "*" is a prefix match, otherwise exact.
func matchesPattern(pattern, resource string) bool {
	if pattern == "*" {
		return true
	}
	if idx := len(pattern) - 1; idx >= 0 && pattern[idx] == '*' {
		return len(resource) >= idx && resource[:idx] == pattern[:idx]
	}
	return pattern == resource
}

func (c Capabilities) can(action string, resource string) bool {
	var list []string
	switch action {
	case "read":
		list = c.Read
	case "write":
		list = c.Write
	case "delete":
		list = c.Delete
	case "admin":
		list = c.Admin
	default:
		return false
	}
	for _, p := range list {
		if matchesPattern(p, resource) {
			return true
		}
	}
	// Admin patterns grant every action.
	for _, p := range c.Admin {
		if matchesPattern(p, resource) {
			return true
		}
	}
	return false
}

// CanRead reports whether the principal may read the channel.
func (c Capabilities) CanRead(channel string) bool { return c.can("read", channel) }

// CanWrite reports whether the principal may write to the channel.
func (c Capabilities) CanWrite(channel string) bool { return c.can("write", channel) }

// CanPublish reports whether the principal may publish to the channel.
func (c Capabilities) CanPublish(channel string) bool { return c.can("write", channel) }

// CanFork reports whether the principal may fork the channel.
func (c Capabilities) CanFork(channel string) bool { return c.can("write", channel) }
