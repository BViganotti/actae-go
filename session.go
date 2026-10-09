package actae

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// sdkVersion is stamped into the fork manifest's environment fingerprint.
const sdkVersion = "0.1.0"

// AgentSession lifecycle statuses.
const (
	SessionStatusCreated   = "created"
	SessionStatusStarted   = "started"
	SessionStatusStepping  = "stepping"
	SessionStatusCompleted = "completed"
	SessionStatusCrashed   = "crashed"
)

const (
	// maxStoredCursors caps the cursors list persisted in channel metadata
	// (for fork resolution). Beyond this, only the most recent cursors are
	// stored; older steps use the replay fallback path in resume.
	maxStoredCursors = 1000
	// maxLineageDepth caps the fork-lineage walk when resolving an inherited
	// step (mirrors the server's execution-tree depth cap).
	maxLineageDepth = 10
	// maxSessionRetries is the number of retries (N+1 total attempts) for
	// transient failures.
	maxSessionRetries = 3
	// retryBaseDelay / retryMaxDelay bound the exponential backoff.
	retryBaseDelay = 500 * time.Millisecond
	retryMaxDelay  = 5 * time.Second
)

// StateFn returns the current agent state dict. Called every
// SnapshotInterval steps; returning nil skips the snapshot.
type StateFn func() map[string]any

// sessionBackend is the transport-neutral surface used by AgentSession.
// Direct Client and FleetSessionTransport implement this exact contract;
// lifecycle, retries, deterministic IDs and fork boundary rules remain here.
type sessionBackend interface {
	Record(context.Context, string, string, any, RecordOptions) (Event, error)
	Replay(context.Context, string, ReplayOptions) ([]Event, error)
	SaveState(context.Context, string, int64, any, SaveStateOptions) (int64, error)
	LatestState(context.Context, string) (*StateSnapshot, error)
	Fork(context.Context, string, string, int64, ForkOptions) (ForkReceipt, error)
	GetChannelMetadata(context.Context, string) (*ChannelMetadata, error)
	UpdateMetadata(context.Context, string, UpdateMetadataOptions) (ChannelMetadata, error)
	ResolveStep(context.Context, string, int64) (*StepResolution, error)
	LatestStepNumber(context.Context, string) (*int64, error)
	SetOutcome(context.Context, string, string, *float64) (map[string]any, error)
	PromoteChannel(context.Context, string) (map[string]any, error)
	CreateExperiment(context.Context, string, ExperimentOptions) (map[string]any, error)
	AddExperimentMember(context.Context, string, string, ExperimentMemberOptions) (map[string]any, error)
}

// AgentSessionOptions configures a new AgentSession.
type AgentSessionOptions struct {
	// DisplayName is an optional human-readable name for the dashboard.
	// Falls back to Name when empty.
	DisplayName string
	// StateFn returns the current agent state; snapshots are saved every
	// SnapshotInterval steps.
	StateFn StateFn
	// SnapshotInterval saves a state snapshot every N steps (default 1).
	SnapshotInterval int
	// Params is an arbitrary parameters dict stored in metadata.
	Params map[string]any
}

// AgentSession wraps an Actae channel and records agent steps as events.
// It tracks a step → cursor mapping so you can fork at a user-facing step
// number rather than a raw server cursor, and saves state snapshots every
// SnapshotInterval steps via an optional StateFn.
//
// Lifecycle: created → started → stepping → completed | crashed.
//
// Not goroutine-safe for concurrent Step/Fork/Resume; a mutex serialises
// Step calls but Fork and Resume should not overlap with Step.
type AgentSession struct {
	actae            sessionBackend
	name             string
	displayName      string
	stateFn          StateFn
	snapshotInterval int
	params           map[string]any

	channelID string
	stepCount int
	cursors   []int64
	startedAt string
	status    string
	resumed   bool

	// inheritedState is the state snapshot this fork inherited from its
	// parent at the fork boundary (steps 1..N's data), loaded from the fork
	// channel after the server copied it. nil for fresh sessions.
	inheritedState map[string]any

	// Fork boundary provenance (set after Fork / Resume(ForkAtStep)):
	// restorability, requested vs resolved boundary cursors, source state
	// version/hash and the reproducibility grade.
	boundaryRestorable *bool
	requestedBoundary  int64
	resolvedBoundary   int64
	sourceStateVersion int64
	sourceStateSHA256  string
	reproducibility    string
	forkReceipt        *ForkReceipt

	// Fork ergonomics: the opaque intervention descriptor recorded on the
	// fork's metadata (the APPLICATION reads it and applies the change; Actae
	// stores it, never interprets or executes it) and the side-effect tool
	// policy applied to the child channel.
	intervention map[string]any
	toolPolicies map[string]any

	mu sync.Mutex
}

// NewAgentSession creates a new AgentSession. Use Start() (or the lifecycle
// methods) to begin recording; use Resume() for crash recovery or
// fork-from-existing.
func NewAgentSession(actae sessionBackend, name string, opts AgentSessionOptions) (*AgentSession, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	interval := opts.SnapshotInterval
	if interval < 1 {
		interval = 1
	}
	params := opts.Params
	if params == nil {
		params = map[string]any{}
	}
	return &AgentSession{
		actae:            actae,
		name:             name,
		displayName:      opts.DisplayName,
		stateFn:          opts.StateFn,
		snapshotInterval: interval,
		params:           params,
		status:           SessionStatusCreated,
	}, nil
}

// ChannelID returns the Actae channel backing this session ("" before
// Start).
func (s *AgentSession) ChannelID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.channelID
}

// Name returns the session name (used as channel identifier).
func (s *AgentSession) Name() string { return s.name }

// StepCount returns the number of steps recorded so far.
func (s *AgentSession) StepCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stepCount
}

// Status returns the current lifecycle status: "created", "started",
// "stepping", "completed", or "crashed".
func (s *AgentSession) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Cursors returns a copy of the cursor for each recorded step
// (index == step - 1).
func (s *AgentSession) Cursors() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.cursors))
	copy(out, s.cursors)
	return out
}

// Params returns a copy of the session parameters dict.
func (s *AgentSession) Params() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.params))
	for k, v := range s.params {
		out[k] = v
	}
	return out
}

// BoundaryRestorable reports whether this fork session inherited a
// restorable state checkpoint. nil for non-fork sessions; false for
// lineage-only forks.
func (s *AgentSession) BoundaryRestorable() *bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boundaryRestorable
}

// RequestedBoundaryCursor is the fork boundary the caller asked for
// (0 = latest).
func (s *AgentSession) RequestedBoundaryCursor() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestedBoundary
}

// ResolvedBoundaryCursor is the snapshot cursor the fork actually inherited.
// It exceeds RequestedBoundaryCursor when an approximate fallback inherited
// later state.
func (s *AgentSession) ResolvedBoundaryCursor() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolvedBoundary
}

// SourceStateVersion is the source snapshot version copied at fork time.
func (s *AgentSession) SourceStateVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sourceStateVersion
}

// SourceStateSHA256 is the SHA-256 fingerprint of the copied state.
func (s *AgentSession) SourceStateSHA256() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sourceStateSHA256
}

// Reproducibility is the server-computed grade for this fork.
func (s *AgentSession) Reproducibility() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reproducibility
}

// ForkReceipt returns the immutable fork receipt returned at creation, or nil
// for non-fork sessions.
// Intervention returns a copy of the opaque intervention descriptor recorded
// on this fork (e.g. {"model": "new-model"}). The application must read it and
// apply the change — Actae stores it but never interprets or executes it.
func (s *AgentSession) Intervention() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMap(s.intervention)
}

// ToolPolicies returns a copy of the side-effect tool policy applied to this
// fork's channel, or nil for the server default (auto).
func (s *AgentSession) ToolPolicies() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolPolicies == nil {
		return nil
	}
	return cloneMap(s.toolPolicies)
}

func (s *AgentSession) ForkReceipt() *ForkReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.forkReceipt
}

// Start begins recording on the session channel: records a
// session.started event and persists session metadata.
func (s *AgentSession) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.resumed {
		s.status = SessionStatusStarted
		s.mu.Unlock()
		return s.updateMetadataStatus(ctx, SessionStatusStarted, nil)
	}
	s.mu.Unlock()
	return s.start(ctx)
}

func (s *AgentSession) start(ctx context.Context) error {
	ev, err := s.actae.Record(ctx, s.name, SessionStarted, map[string]any{
		"session_name": s.name,
		"params":       s.params,
	}, RecordOptions{
		Actor: "agent_session",
		Metadata: map[string]any{
			"session_name":      s.name,
			"snapshot_interval": s.snapshotInterval,
			"has_state_fn":      s.stateFn != nil,
		},
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.channelID = ev.ChannelID
	s.startedAt = ev.Timestamp
	s.status = SessionStatusStarted
	s.mu.Unlock()
	return s.updateMetadataStatus(ctx, SessionStatusStarted, nil)
}

// Complete marks the session as completed: records a session.completed
// event and persists the final metadata. Safe to call multiple times; a
// completed or crashed session is a no-op.
func (s *AgentSession) Complete(ctx context.Context) error {
	s.mu.Lock()
	if s.status == SessionStatusCompleted || s.status == SessionStatusCrashed {
		s.mu.Unlock()
		return nil
	}
	stepCount := s.stepCount
	channelID := s.channelID
	s.mu.Unlock()

	if channelID != "" {
		if _, err := s.actae.Record(ctx, channelID, SessionCompleted, map[string]any{
			"session_name": s.name,
			"total_steps":  stepCount,
		}, RecordOptions{
			Actor:    "agent_session",
			Metadata: map[string]any{"total_steps": stepCount},
		}); err != nil {
			log.Printf("actae: failed to record session.completed for '%s': %v", s.name, err)
		}
	}

	s.mu.Lock()
	s.status = SessionStatusCompleted
	s.mu.Unlock()
	return s.updateMetadataStatus(ctx, SessionStatusCompleted, nil)
}

// Crash marks the session as crashed with the given reason (persisted in
// metadata). Recording failures are logged, not fatal.
func (s *AgentSession) Crash(ctx context.Context, reason string) error {
	s.mu.Lock()
	s.status = SessionStatusCrashed
	s.mu.Unlock()
	log.Printf("actae: session '%s' crashed: %s", s.name, reason)
	if err := s.updateMetadataStatus(ctx, SessionStatusCrashed, map[string]any{"crash_reason": reason}); err != nil {
		log.Printf("actae: failed to update metadata after crash for '%s': %v", s.name, err)
	}
	return nil
}

func (s *AgentSession) ensureChannel() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channelID == "" {
		return "", NewSessionError(fmt.Sprintf("Session '%s' has no channel — call Start first", s.name))
	}
	return s.channelID, nil
}

func (s *AgentSession) requireStatus(allowed ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range allowed {
		if s.status == st {
			return nil
		}
	}
	return NewSessionError(fmt.Sprintf("Session '%s' is %s, cannot perform operation.", s.name, s.status))
}

// updateMetadataStatus persists session metadata with retry on transient
// failures. It merges over existing metadata (e.g. from a fork) so fork keys
// are preserved.
func (s *AgentSession) updateMetadataStatus(ctx context.Context, status string, extra map[string]any) error {
	channelID, err := s.ensureChannel()
	if err != nil {
		return err
	}

	update := func(ctx context.Context) error {
		var existing map[string]any
		meta, err := s.actae.GetChannelMetadata(ctx, channelID)
		if err != nil {
			return err
		}
		if meta != nil && meta.ExperimentMetadata != nil {
			existing = meta.ExperimentMetadata
		}

		s.mu.Lock()
		stepCount := s.stepCount
		cursors := make([]int64, len(s.cursors))
		copy(cursors, s.cursors)
		params := make(map[string]any, len(s.params))
		for k, v := range s.params {
			params[k] = v
		}
		startedAt := s.startedAt
		name := s.name
		s.mu.Unlock()

		stored := cursors
		if len(stored) > maxStoredCursors {
			stored = stored[len(stored)-maxStoredCursors:]
		}
		metaMap := map[string]any{
			"session_name":   name,
			"params":         params,
			"status":         status,
			"started_at":     startedAt,
			"step_count":     stepCount,
			"cursors":        stored,
			"cursors_stored": len(stored),
		}
		if len(cursors) > maxStoredCursors {
			metaMap["cursors_warning"] = fmt.Sprintf(
				"Only last %d cursors stored; total steps: %d", maxStoredCursors, stepCount)
		}

		merged := make(map[string]any, len(existing)+len(metaMap)+len(extra))
		for k, v := range existing {
			merged[k] = v
		}
		for k, v := range metaMap {
			merged[k] = v
		}
		for k, v := range extra {
			merged[k] = v
		}

		displayName := s.displayName
		if displayName == "" {
			displayName = name
		}
		_, err = s.actae.UpdateMetadata(ctx, channelID, UpdateMetadataOptions{
			DisplayName:        &displayName,
			ExperimentMetadata: merged,
		})
		return err
	}

	return retryTransient(ctx, "update_metadata", update)
}

// StepOptions configures a single Step.
type StepOptions struct {
	// Input is the input data for the step.
	Input any
	// Output is the output data from the step.
	Output any
	// Context is the delta context generated at this step (stored in the
	// event payload so the dashboard can reconstruct cumulative state by
	// merging deltas across the cursor-linked event chain).
	Context any
	// Metadata is optional key-value metadata attached to the event.
	Metadata map[string]any
}

// CanonicalStepContent serializes a step's payload + metadata to the
// canonical JSON form used in the deterministic step operation id.
// json.Marshal sorts map keys recursively and emits the payload field
// FIRST (struct field order), so re-runs of the same step produce
// byte-identical content. NaN/Infinity fail marshaling here (canonical
// becomes "") and then fail the subsequent Record, which marshals the
// same payload — a loud failure, never a silently divergent id. The
// Python SDK (`_canonical_step_content`, Go-exact float formatting +
// escaping) and the TS SDK (`canonicalStepContent`) mirror this output
// byte-for-byte, so the derived UUIDv5 is identical across all three
// SDKs (parity vectors pinned in each SDK's test suite).
func CanonicalStepContent(payload, metadata map[string]any) string {
	b, err := json.Marshal(struct {
		Payload  map[string]any `json:"payload"`
		Metadata map[string]any `json:"metadata"`
	}{normalizeCanonicalObject(payload), normalizeCanonicalObject(metadata)})
	if err != nil {
		return ""
	}
	return string(b)
}

// normalizeCanonicalObject aligns numeric edge cases across language JSON
// encoders. encoding/json emits -0 for a negative-zero float, while the
// canonical Actae/Python/TypeScript representation is the JSON number 0.
// Apply the normalization recursively so nested values are covered too.
func normalizeCanonicalObject(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = normalizeCanonicalValue(value)
	}
	return out
}

func normalizeCanonicalValue(value any) any {
	switch v := value.(type) {
	case float64:
		if v == 0 && math.Signbit(v) {
			return float64(0)
		}
		return v
	case float32:
		if v == 0 && math.Signbit(float64(v)) {
			return float32(0)
		}
		return v
	case map[string]any:
		return normalizeCanonicalObject(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = normalizeCanonicalValue(item)
		}
		return out
	default:
		return value
	}
}

// Step records a single execution step as an event, with retry on transient
// failures, and returns the persisted Event (inspect Cursor for fork
// resolution).
func (s *AgentSession) Step(ctx context.Context, stepType string, opts StepOptions) (Event, error) {
	if err := s.requireStatus(SessionStatusStarted, SessionStatusStepping); err != nil {
		return Event{}, err
	}
	s.mu.Lock()
	channelID := s.channelID
	stepNum := s.stepCount + 1
	previousStatus := s.status
	s.status = SessionStatusStepping
	s.mu.Unlock()

	if channelID == "" {
		return Event{}, NewSessionError(fmt.Sprintf("Session '%s' has no channel — call Start first", s.name))
	}

	eventMeta := map[string]any{"step_number": stepNum}
	for k, v := range opts.Metadata {
		eventMeta[k] = v
	}

	payload := map[string]any{
		"step_number": stepNum,
		"input":       opts.Input,
		"output":      opts.Output,
	}
	if opts.Context != nil {
		payload["context"] = opts.Context
	}

	// A deterministic operation id per step makes the record idempotent:
	// if the first attempt persisted but its response was lost, the retry
	// (or a crash-recovery re-drive of the same step) returns the original
	// event instead of recording a duplicate. The id is a UUIDv5 over the
	// step's (scope, type, channel, number, canonical content), so two
	// identical step events — same channel, same step_number, same
	// payload/metadata — derive the SAME id and the server replays; a
	// step whose content differs derives a distinct id and records anew.
	// The derivation is byte-identical across the Python/Go/TS SDKs.
	operationID := DeterministicOperationKey(
		"agent-session", stepType, channelID,
		strconv.FormatInt(int64(stepNum), 10),
		CanonicalStepContent(payload, eventMeta),
	)
	stepNumber := int64(stepNum)
	record := func(ctx context.Context) (Event, error) {
		return s.actae.Record(ctx, channelID, stepType, payload, RecordOptions{
			Actor:       "agent_session",
			Metadata:    eventMeta,
			OperationID: &operationID,
			StepNumber:  &stepNumber,
		})
	}

	ev, err := retryTransientEvent(ctx, "step", record)
	if err != nil {
		return Event{}, err
	}

	s.mu.Lock()
	s.stepCount = stepNum
	s.cursors = append(s.cursors, ev.Cursor)
	stepCount := s.stepCount
	s.mu.Unlock()

	if s.stateFn != nil && stepCount%s.snapshotInterval == 0 {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("actae: state_fn panicked on step %d — skipping snapshot: %v", stepCount, r)
				}
			}()
			state := s.stateFn()
			if state != nil {
				if _, err := s.actae.SaveState(ctx, channelID, ev.Cursor, state, SaveStateOptions{}); err != nil {
					log.Printf("actae: save_state failed on step %d: %v", stepCount, err)
				}
			}
		}()
	}

	if stepCount%s.snapshotInterval == 0 && previousStatus != SessionStatusStepping {
		if err := s.updateMetadataStatus(ctx, SessionStatusStepping, nil); err != nil {
			return Event{}, err
		}
	}

	return ev, nil
}

// ForkSessionOptions configures Fork.
type ForkSessionOptions struct {
	DisplayName string
	Params      map[string]any
	StateFn     StateFn
	Reason      string
	// BoundaryMode is "exact" (default), "approximate" or "lineage_only".
	// See the Fork docstring for the strict/approximate/lineage_only policy.
	BoundaryMode string
	// Manifest is the immutable fork manifest echoed in the receipt.
	Manifest map[string]any
	// Intervention is an opaque descriptor of the intended change (e.g.
	// {"model": "new-model"}) recorded on the fork metadata. The application
	// reads session.Intervention() and applies it; Actae never interprets it.
	Intervention map[string]any
	// ToolPolicies is the child's side-effect policy map
	// ({"tool": "replay"|"block"|"live"|"auto", "*": default}); nil = auto.
	ToolPolicies map[string]any
}

// Fork forks this session at a given step into a new experiment fork.
// Returns an *unstarted* AgentSession — call Start() on it to begin
// recording events on the fork channel.
func (s *AgentSession) Fork(ctx context.Context, atStep int, name string, opts ForkSessionOptions) (*AgentSession, error) {
	if err := s.requireStatus(SessionStatusStarted, SessionStatusStepping, SessionStatusCompleted, SessionStatusCrashed); err != nil {
		return nil, err
	}
	if s.channelID == "" {
		return nil, NewSessionError("Session has no channel — was it started?")
	}
	if name == "" {
		return nil, fmt.Errorf("fork name is required")
	}

	// Resolve the owning channel + cursor. A live session's in-memory cursors
	// are exact for its own steps; inherited steps (cursor 0) of a fork
	// session must walk the lineage to the ancestor that recorded them.
	sourceChannelID := s.channelID
	cursor, err := s.cursorForStep(atStep)
	if err != nil {
		return nil, err
	}
	if cursor == 0 {
		sourceChannelID, cursor, err = s.resolveForkSource(ctx, s.channelID, atStep)
		if err != nil {
			return nil, err
		}
	}

	mergedParams := make(map[string]any, len(s.params)+len(opts.Params))
	for k, v := range s.params {
		mergedParams[k] = v
	}
	for k, v := range opts.Params {
		mergedParams[k] = v
	}

	experimentMetadata := map[string]any{
		"session_name":        name,
		"params":              mergedParams,
		"forked_from":         sourceChannelID,
		"forked_from_channel": s.channelID,
		"forked_at_step":      atStep,
		"forked_at_cursor":    cursor,
		"status":              SessionStatusCreated,
	}
	if len(opts.Intervention) > 0 {
		experimentMetadata["intervention"] = opts.Intervention
	}

	displayName := opts.DisplayName
	if displayName == "" {
		displayName = name
	}
	reason := opts.Reason
	if reason == "" {
		reason = fmt.Sprintf("Forked from %s at step %d", s.name, atStep)
	}

	// Strict-by-default boundary policy: the server forks from the latest
	// saved state at-or-before `cursor`. When no snapshot exists there, the
	// default "exact" mode raises NoRestorableCheckpointError instead of
	// silently inheriting later state. "approximate" falls back to the latest
	// state and reports the drift; "lineage_only" creates the fork with no
	// state copy.
	boundaryMode := opts.BoundaryMode
	if boundaryMode == "" {
		boundaryMode = "exact"
	}
	receipt, err := s.performFork(ctx, sourceChannelID, name, cursor, atStep, displayName, reason, experimentMetadata, boundaryMode, opts.Manifest, opts.ToolPolicies)
	if err != nil {
		return nil, err
	}
	s.applyForkReceipt(receipt)

	stateFn := opts.StateFn
	if stateFn == nil {
		stateFn = s.stateFn
	}
	session, err := NewAgentSession(s.actae, name, AgentSessionOptions{
		DisplayName:      opts.DisplayName,
		StateFn:          stateFn,
		SnapshotInterval: s.snapshotInterval,
		Params:           mergedParams,
	})
	if err != nil {
		return nil, err
	}
	session.channelID = name
	session.applyForkReceipt(receipt)
	session.intervention = cloneMap(opts.Intervention)
	if err := session.primeForkSession(ctx, s.channelID, atStep); err != nil {
		return nil, err
	}
	return session, nil
}

// performFork issues the server fork, applying the strict/approximate/
// lineage_only boundary policy. It returns the immutable fork receipt and
// records it on the receiver session.
func (s *AgentSession) performFork(ctx context.Context, sourceChannel, childName string, cursor int64, atStep int, displayName, reason string, experimentMetadata map[string]any, boundaryMode string, manifest map[string]any, toolPolicies map[string]any) (ForkReceipt, error) {
	switch boundaryMode {
	case "exact", "approximate", "lineage_only":
	default:
		return ForkReceipt{}, NewSessionError(fmt.Sprintf(
			"invalid boundary_mode %q (expected exact|approximate|lineage_only)", boundaryMode))
	}

	doFork := func(c int64) (ForkReceipt, error) {
		return s.actae.Fork(ctx, sourceChannel, childName, c, ForkOptions{
			DisplayName:        &displayName,
			Reason:             &reason,
			ExperimentMetadata: experimentMetadata,
			Manifest:           buildManifest(manifest),
			ToolPolicies:       toolPolicies,
		})
	}

	receipt, err := doFork(cursor)
	if err != nil {
		if _, ok := err.(*SnapshotBoundaryError); !ok {
			return ForkReceipt{}, err
		}
		if boundaryMode == "exact" {
			return ForkReceipt{}, NewNoRestorableCheckpointError(fmt.Sprintf(
				"No restorable checkpoint at step %d (cursor %d) on channel %q: no saved snapshot at or before the boundary. Save state on every step (StateFn + SnapshotInterval=1) or fork with BoundaryMode=approximate / lineage_only.",
				atStep, cursor, sourceChannel))
		}
		log.Printf("actae: no saved state at step %d cursor %d on %q — boundary_mode=%s forking from the latest state (inherited state is NOT an exact checkpoint)",
			atStep, cursor, sourceChannel, boundaryMode)
		receipt, err = doFork(0)
		if err != nil {
			return ForkReceipt{}, err
		}
		// The server recorded the fallback as a "latest" request; surface the
		// user's true intended boundary as the requested cursor so session
		// provenance reflects the drift honestly.
		receipt.RequestedCursor = cursor
	}

	if receipt.RequestedCursor > 0 && receipt.ResolvedCursor > receipt.RequestedCursor {
		log.Printf("actae: fork %q resolved to cursor %d which is BEYOND the requested %d — inherited state is not an exact checkpoint (temporal contamination risk). Inspect session.ResolvedBoundaryCursor().",
			childName, receipt.ResolvedCursor, receipt.RequestedCursor)
	}
	return receipt, nil
}

// applyForkReceipt stores the immutable fork provenance on the session.
func (s *AgentSession) applyForkReceipt(r ForkReceipt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	restorable := r.Restorable
	s.boundaryRestorable = &restorable
	s.requestedBoundary = r.RequestedCursor
	s.resolvedBoundary = r.ResolvedCursor
	s.sourceStateVersion = r.SourceStateVersion
	s.sourceStateSHA256 = strOrEmpty(r.SourceStateSHA256)
	s.reproducibility = strOrEmpty(r.Reproducibility)
	s.toolPolicies = cloneMap(r.ToolPolicies)
	if len(s.toolPolicies) == 0 {
		s.toolPolicies = nil
	}
	rCopy := r
	s.forkReceipt = &rCopy
}

func strOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// buildManifest merges caller-supplied manifest fields over an
// auto-detected environment fingerprint (runtime version + SDK version).
// The server grades `context_exact` only when model + environment +
// dependencies + seed are all present, so callers must supply those to claim
// context-exact reproducibility.
func buildManifest(caller map[string]any) map[string]any {
	out := map[string]any{
		"environment": map[string]any{
			"go":    runtime.Version(),
			"os":    runtime.GOOS + "/" + runtime.GOARCH,
			"actae": sdkVersion,
		},
	}
	for k, v := range caller {
		out[k] = v
	}
	return out
}

// SetOutcome records this fork's outcome (promoted | rejected |
// inconclusive | crashed) and an optional numeric result score.
func (s *AgentSession) SetOutcome(ctx context.Context, outcome string, score *float64) error {
	s.mu.Lock()
	channelID := s.channelID
	s.mu.Unlock()
	if channelID == "" {
		return NewSessionError(fmt.Sprintf("Session '%s' has no channel — call Start first", s.name))
	}
	_, err := s.actae.SetOutcome(ctx, channelID, outcome, score)
	return err
}

// Promote promotes this winning fork into its parent (append-only merge).
func (s *AgentSession) Promote(ctx context.Context) (map[string]any, error) {
	s.mu.Lock()
	channelID := s.channelID
	s.mu.Unlock()
	if channelID == "" {
		return nil, NewSessionError(fmt.Sprintf("Session '%s' has no channel — call Start first", s.name))
	}
	return s.actae.PromoteChannel(ctx, channelID)
}

// ResumeOptions configures Resume.
type ResumeOptions struct {
	// ForkAtStep, when non-nil, forks the source channel at this step into
	// a new channel (Name required). When nil, resumes the channel in place
	// (crash recovery).
	ForkAtStep *int
	// Name is required when ForkAtStep is set (the new fork channel).
	Name string
	// Params merged over the stored session params.
	Params map[string]any
	// StateFn is the state callback for the resumed session.
	StateFn StateFn
	// SnapshotInterval for the resumed session (default 1).
	SnapshotInterval int
	// BoundaryMode is "exact" (default), "approximate" or "lineage_only".
	BoundaryMode string
	// Manifest is the immutable fork manifest echoed in the receipt.
	Manifest map[string]any
	// Intervention is an opaque descriptor of the intended change recorded on
	// the fork metadata (application-applied; Actae never interprets it).
	Intervention map[string]any
	// ToolPolicies is the child's side-effect policy map (nil = auto).
	ToolPolicies map[string]any
}

// Resume resumes a previously-run (or crashed) session.
//
// Crash recovery (no ForkAtStep): resumes from the last recorded step;
// raises SessionCompletedError when the channel is already completed.
//
// Fork-from-existing (ForkAtStep set): forks the source channel at the
// given step into a fresh channel with Name.
func Resume(ctx context.Context, actae sessionBackend, channelID string, opts ResumeOptions) (*AgentSession, error) {
	meta, err := actae.GetChannelMetadata(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, NewSessionError(fmt.Sprintf("Channel '%s' not found (no metadata)", channelID))
	}

	expMeta := meta.ExperimentMetadata
	if expMeta == nil {
		expMeta = map[string]any{}
	}
	status := str(expMeta, "status")
	if status == "" {
		status = "unknown"
	}

	if opts.ForkAtStep != nil {
		if opts.Name == "" {
			return nil, NewSessionError("'Name' is required when ForkAtStep is set")
		}
		displayName := ""
		if meta.DisplayName != nil {
			displayName = *meta.DisplayName
		}
		session, err := NewAgentSession(actae, opts.Name, AgentSessionOptions{
			DisplayName:      displayName,
			StateFn:          opts.StateFn,
			SnapshotInterval: opts.SnapshotInterval,
			Params:           opts.Params,
		})
		if err != nil {
			return nil, err
		}
		session.channelID = opts.Name
		session.intervention = cloneMap(opts.Intervention)
		if err := session.forkFromChannel(ctx, channelID, *opts.ForkAtStep, opts.BoundaryMode, opts.Manifest, opts.Intervention, opts.ToolPolicies); err != nil {
			return nil, err
		}
		return session, nil
	}

	if status == SessionStatusCompleted {
		return nil, NewSessionCompletedError(fmt.Sprintf(
			"Session '%s' is already completed. Pass ForkAtStep to fork from it.", channelID))
	} else if status == SessionStatusCrashed {
		log.Printf("actae: resuming crashed session '%s' (last step: %s)",
			channelID, anyString(expMeta["step_count"]))
	}

	sessionParams := map[string]any{}
	if stored, ok := expMeta["params"].(map[string]any); ok {
		for k, v := range stored {
			sessionParams[k] = v
		}
	}
	for k, v := range opts.Params {
		sessionParams[k] = v
	}

	cursors := []int64{}
	if raw, ok := expMeta["cursors"].([]any); ok {
		for _, c := range raw {
			if i, ok := c.(int64); ok {
				cursors = append(cursors, i)
			} else if f, ok := c.(float64); ok {
				cursors = append(cursors, int64(f))
			}
		}
	}
	stepCount := int(intOf(expMeta, "step_count", 0))
	// The durable event log is authoritative: a hard process death skips the
	// metadata write, so reconstruct real progress rather than resuming a
	// hard-killed run back at step 1.
	// Recover true progress from the server-owned step index (O(1)); fall back
	// to a replay scan when the endpoint is unavailable (older server). Only
	// override metadata when the recovered value is AHEAD of it — retention
	// can purge old events, and a resume must never go backwards.
	if last, err := actae.LatestStepNumber(ctx, channelID); err == nil && last != nil {
		if int(*last) > stepCount {
			stepCount = int(*last)
			for len(cursors) < stepCount {
				cursors = append(cursors, 0)
			}
		}
	} else if recovered, ok := recoverStepProgress(ctx, actae, channelID); ok && len(recovered) > stepCount {
		cursors = recovered
		stepCount = len(recovered)
	}

	displayName := channelID
	if meta.DisplayName != nil && *meta.DisplayName != "" {
		displayName = *meta.DisplayName
	}

	session, err := NewAgentSession(actae, displayName, AgentSessionOptions{
		DisplayName:      displayName,
		StateFn:          opts.StateFn,
		SnapshotInterval: opts.SnapshotInterval,
		Params:           sessionParams,
	})
	if err != nil {
		return nil, err
	}
	session.channelID = channelID
	session.stepCount = stepCount
	session.cursors = cursors
	session.startedAt = str(expMeta, "started_at")
	session.status = SessionStatusStarted
	session.resumed = true
	// Preserve a fork's intervention descriptor across crash recovery.
	if stored, ok := expMeta["intervention"].(map[string]any); ok {
		session.intervention = cloneMap(stored)
	}
	// Crash recovery: expose the last saved state so the caller can continue
	// from where the run stopped.
	if snap, err := actae.LatestState(ctx, channelID); err == nil && snap != nil {
		session.inheritedState = snap.State
	}
	return session, nil
}

// anyInt64 extracts an integral JSON number from an opaque payload value.
func anyInt64(value any) (int64, bool) {
	switch n := value.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// recoverStepProgress reconstructs the per-step cursor list from the durable
// event log. Session metadata is written on lifecycle transitions, so a hard
// process death (os.Exit, SIGKILL, power loss) can leave it lagging; every
// Step writes "step_number" into its payload in the same transaction as the
// event, making the log authoritative. Returns (cursors, true) or (nil, false)
// when no step events exist (caller falls back to metadata).
func recoverStepProgress(ctx context.Context, actae sessionBackend, channelID string) ([]int64, bool) {
	const maxEvents = 50000
	byStep := map[int64]int64{}
	var cursor *int64
	seen := 0
	for seen < maxEvents {
		events, err := actae.Replay(ctx, channelID, ReplayOptions{Cursor: cursor, Limit: 1000})
		if err != nil || len(events) == 0 {
			break
		}
		for _, ev := range events {
			payload, ok := ev.Payload.(map[string]any)
			if !ok {
				continue
			}
			if stepNumber, ok := anyInt64(payload["step_number"]); ok && stepNumber >= 1 {
				if _, exists := byStep[stepNumber]; !exists {
					byStep[stepNumber] = ev.Cursor
				}
			}
		}
		seen += len(events)
		last := events[len(events)-1].Cursor
		if cursor != nil && last <= *cursor {
			break
		}
		c := last
		cursor = &c
	}
	if len(byStep) == 0 {
		return nil, false
	}
	var maxStep int64
	for step := range byStep {
		if step > maxStep {
			maxStep = step
		}
	}
	cursors := make([]int64, 0, maxStep)
	for i := int64(1); i <= maxStep; i++ {
		cursors = append(cursors, byStep[i])
	}
	return cursors, true
}

// ResumeFromFork resumes the exact child identified by a fork receipt. It
// deliberately does not perform a "latest fork" lookup, which is ambiguous
// under concurrent fork creation.
func ResumeFromFork(ctx context.Context, actae sessionBackend, receipt ForkReceipt, opts ResumeOptions) (*AgentSession, error) {
	if receipt.ChildChannelID == "" {
		return nil, NewSessionError("fork receipt has no child channel id")
	}
	return Resume(ctx, actae, receipt.ChildChannelID, opts)
}

// resolveForkSource resolves the channel that OWNS a step and its cursor.
//
// It walks up the fork lineage: a fork inherits its first `forked_at_step`
// steps from its parent, so forking a fork at an inherited step must resolve
// against the ancestor that recorded it — otherwise the fork's own event log
// (which only has its own steps) would map the position wrongly. The cursor
// comes from the owning channel's metadata `cursors` list, falling back to a
// position-based replay resolution when no cursors metadata exists.
func (s *AgentSession) resolveForkSource(ctx context.Context, channelID string, atStep int) (string, int64, error) {
	if atStep < 1 {
		return "", 0, NewSessionError(fmt.Sprintf("fork_at_step must be >= 1, got %d", atStep))
	}

	// Server-owned step index first: exact, durable and lineage-aware (the
	// server walks parent_channel_id using the recorded forked_at_step).
	// Falls back to the legacy client-side resolution when the step isn't
	// indexed (channels created before the index).
	if resolved, err := s.actae.ResolveStep(ctx, channelID, int64(atStep)); err != nil {
		return "", 0, err
	} else if resolved != nil {
		return resolved.ChannelID, resolved.Cursor, nil
	}

	// Walk up to the owning channel.
	owner := channelID
	for i := 0; i < maxLineageDepth; i++ {
		meta, err := s.actae.GetChannelMetadata(ctx, owner)
		if err != nil || meta == nil {
			break
		}
		forkedAtStep := int(0)
		if meta.ExperimentMetadata != nil {
			forkedAtStep = int(intOf(meta.ExperimentMetadata, "forked_at_step", 0))
		}
		if forkedAtStep == 0 || atStep > forkedAtStep {
			break // this channel recorded step `atStep` itself
		}
		parent := ""
		if meta.ParentChannelID != nil {
			parent = *meta.ParentChannelID
		}
		if parent == "" || parent == owner {
			break // no parent to delegate to
		}
		owner = parent
	}

	// Fast path: cursors list stored in the owner's metadata.
	var cursor *int64
	if meta, err := s.actae.GetChannelMetadata(ctx, owner); err == nil && meta != nil && meta.ExperimentMetadata != nil {
		if raw, ok := meta.ExperimentMetadata["cursors"].([]any); ok {
			if atStep <= len(raw) {
				if i, ok := raw[atStep-1].(int64); ok {
					cursor = &i
				} else if f, ok := raw[atStep-1].(float64); ok {
					i := int64(f)
					cursor = &i
				}
			}
		}
	}

	// Fallback: replay the owner's events and resolve by position.
	if cursor == nil {
		var sourceEvents []Event
		var pageCursor *int64
		for {
			batch, err := s.actae.Replay(ctx, owner, ReplayOptions{Cursor: pageCursor, Limit: 1000})
			if err != nil {
				if _, ok := err.(*APIError); !ok {
					return "", 0, err
				}
				break
			}
			sourceEvents = append(sourceEvents, batch...)
			if len(batch) == 0 || atStep <= len(sourceEvents) {
				break
			}
			last := batch[len(batch)-1]
			if pageCursor != nil && last.Cursor <= *pageCursor {
				break // no progress — avoid an infinite loop on a broken server
			}
			pageCursor = &last.Cursor
		}
		if len(sourceEvents) == 0 {
			return "", 0, NewSessionError(fmt.Sprintf(
				"Channel '%s' has no events — cannot resolve step %d", owner, atStep))
		}
		// Exclude system markers (fork.started / session lifecycle).
		filtered := sourceEvents[:0]
		for _, ev := range sourceEvents {
			switch ev.EventType {
			case "fork.started", "session.started", "session.completed":
				continue
			}
			filtered = append(filtered, ev)
		}
		sourceEvents = filtered
		if len(sourceEvents) == 0 {
			return "", 0, NewSessionError(fmt.Sprintf(
				"Channel '%s' has no user events — cannot resolve step %d", owner, atStep))
		}
		if atStep > len(sourceEvents) {
			return "", 0, NewSessionError(fmt.Sprintf(
				"Step %d exceeds the %d steps recorded on channel '%s'", atStep, len(sourceEvents), owner))
		}
		// Validate cursor monotonicity before trusting position-based mapping.
		for i := 1; i < len(sourceEvents); i++ {
			if sourceEvents[i].Cursor <= sourceEvents[i-1].Cursor {
				return "", 0, NewSessionError(fmt.Sprintf(
					"Channel '%s' has non-monotonic cursors (event %d: cursor %d <= cursor %d). Position-based step resolution is unreliable for this channel. Fork at a raw cursor instead.",
					owner, i, sourceEvents[i].Cursor, sourceEvents[i-1].Cursor))
			}
		}
		c := sourceEvents[atStep-1].Cursor
		cursor = &c
	}

	return owner, *cursor, nil
}

// forkFromChannel forks an existing channel's state into this session's
// channel. Resolves the step → cursor mapping via the source channel's
// metadata cursors list (fast path), falling back to replay when no cursors
// list exists (source created outside AgentSession). The replay fallback
// validates cursor monotonicity before trusting position-based mapping.
func (s *AgentSession) forkFromChannel(ctx context.Context, sourceChannelID string, atStep int, boundaryMode string, manifest map[string]any, intervention map[string]any, toolPolicies map[string]any) error {
	if atStep < 1 {
		return NewSessionError(fmt.Sprintf("fork_at_step must be >= 1, got %d", atStep))
	}

	// Resolve the owning channel + cursor, walking the lineage for inherited
	// steps and falling back to replay when no cursors metadata exists.
	owner, cursor, err := s.resolveForkSource(ctx, sourceChannelID, atStep)
	if err != nil {
		return err
	}
	sourceChannelID = owner

	if boundaryMode == "" {
		boundaryMode = "exact"
	}
	reason := fmt.Sprintf("Forked from %s at step %d (cursor %d)", sourceChannelID, atStep, cursor)
	forkMetadata := map[string]any{
		"session_name":     s.name,
		"params":           s.params,
		"forked_from":      sourceChannelID,
		"forked_at_step":   atStep,
		"forked_at_cursor": cursor,
		"status":           SessionStatusCreated,
	}
	if len(intervention) > 0 {
		forkMetadata["intervention"] = intervention
	}
	receipt, err := s.performFork(ctx, sourceChannelID, s.name, cursor, atStep, s.name, reason, forkMetadata, boundaryMode, manifest, toolPolicies)
	if err != nil {
		return err
	}
	s.applyForkReceipt(receipt)
	s.intervention = cloneMap(intervention)
	return s.primeForkSession(ctx, sourceChannelID, atStep)
}

// primeForkSession primes a fork session so it continues at `atStep + 1`
// with the parent's inherited state — the heart of "fork at step 5, refine
// step 6" without re-running steps 1-5. Sets stepCount, restores the parent's
// cursors (up to the fork step) for nested forks, loads the fork channel's
// inherited state snapshot, and makes StateFn return it.
func (s *AgentSession) primeForkSession(ctx context.Context, sourceChannelID string, atStep int) error {
	s.mu.Lock()
	s.stepCount = atStep
	if meta, err := s.actae.GetChannelMetadata(ctx, sourceChannelID); err == nil && meta != nil && meta.ExperimentMetadata != nil {
		if raw, ok := meta.ExperimentMetadata["cursors"].([]any); ok {
			cursors := make([]int64, 0, atStep)
			for i, c := range raw {
				if i >= atStep {
					break
				}
				if v, ok := c.(int64); ok {
					cursors = append(cursors, v)
				} else if f, ok := c.(float64); ok {
					cursors = append(cursors, int64(f))
				}
			}
			s.cursors = cursors
		}
	}
	s.mu.Unlock()

	// Load the fork's inherited state snapshot from the fork channel (the
	// server copied the parent's state at the boundary into the child).
	var inherited map[string]any
	if snap, err := s.actae.LatestState(ctx, s.channelID); err == nil && snap != nil {
		inherited = snap.State
	}

	// Never silently prime a fork with contaminated state: when the fork
	// resolved BEYOND the requested boundary (an approximate fallback to the
	// latest state), the inherited snapshot contains data from after the fork
	// point. Refuse to expose it as steps 1..N's data. (requested 0 = "latest"
	// is self-consistent and never contaminated.)
	if s.requestedBoundary > 0 && s.resolvedBoundary > s.requestedBoundary {
		log.Printf("actae: fork %q inherited cursor %d but requested %d — NOT priming with the contaminated inherited state. inherited_state is nil; set BoundaryMode=approximate explicitly if you intend to continue from the latest state.",
			s.name, s.resolvedBoundary, s.requestedBoundary)
		inherited = nil
	}

	s.mu.Lock()
	s.inheritedState = inherited
	// The caller's StateFn is intentionally NOT wrapped: it must reflect the
	// fork's evolving live state, and `inherited_state` is the seed the caller
	// folds into that live state once. (Python parity — wrapping it to always
	// return the inherited prefix would make the fork channel persist only the
	// inherited steps, never the fork's own output.)
	s.mu.Unlock()
	return nil
}

// cursorForStep maps a user-facing step number (1-indexed) to a server
// cursor.
func (s *AgentSession) cursorForStep(stepNumber int) (int64, error) {
	if stepNumber < 1 {
		return 0, NewSessionError(fmt.Sprintf("step_number must be >= 1, got %d", stepNumber))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cursors) == 0 {
		return 0, NewSessionError(fmt.Sprintf(
			"Session '%s' has no recorded steps yet — cannot resolve step %d", s.name, stepNumber))
	}
	if stepNumber > len(s.cursors) {
		return 0, NewSessionError(fmt.Sprintf(
			"Step %d exceeds the %d steps recorded on session '%s'", stepNumber, len(s.cursors), s.name))
	}
	return s.cursors[stepNumber-1], nil
}

// Resume resumes this session's channel (crash recovery or
// fork-from-existing). Convenience wrapper over the package-level Resume —
// equivalent to Resume(ctx, session.actae, session.ChannelID(), opts). The
// session must have been started (or already resumed).
func (s *AgentSession) Resume(ctx context.Context, opts ResumeOptions) (*AgentSession, error) {
	s.mu.Lock()
	channelID := s.channelID
	s.mu.Unlock()
	if channelID == "" {
		return nil, NewSessionError("Session has no channel — was it started?")
	}
	return Resume(ctx, s.actae, channelID, opts)
}

// String renders a human-readable session summary.
func (s *AgentSession) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("AgentSession(name=%q, channel=%q, status=%q, steps=%d)",
		s.name, s.channelID, s.status, s.stepCount)
}

// InheritedState returns the state snapshot this fork inherited from its
// parent at the fork boundary (steps 1..N's data), or nil for a fresh
// session. Only populated after Fork or Resume with ForkAtStep — the server
// copies the parent's saved state at the fork cursor into the fork channel,
// and this returns it so the fork's first step can run against the parent's
// accumulated data.
func (s *AgentSession) InheritedState() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inheritedState == nil {
		return nil
	}
	out := make(map[string]any, len(s.inheritedState))
	for k, v := range s.inheritedState {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// Retry helper
// ---------------------------------------------------------------------------

// isRetryable reports whether err is a transient failure that benefits from
// a retry: ConnectionError (including HTTP transport failures, which the
// client wraps), APIError with status >= 500 or 429, or a
// deadline-exceeded context error.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*ConnectionError); ok {
		return true
	}
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.StatusCode >= 500 || apiErr.StatusCode == 429
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// retryTransient calls fn with exponential backoff on transient failures:
// up to maxSessionRetries retries with delays 0.5s → 1s → 2s (capped 5s).
func retryTransient(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 1; attempt <= maxSessionRetries+1; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) || attempt > maxSessionRetries {
			return err
		}
		delay := retryBaseDelay << (attempt - 1)
		if delay > retryMaxDelay {
			delay = retryMaxDelay
		}
		log.Printf("actae: %s attempt %d/%d failed — retrying in %s: %v",
			op, attempt, maxSessionRetries+1, delay, err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// retryTransientEvent is retryTransient for operations returning (Event, error).
func retryTransientEvent(ctx context.Context, op string, fn func(ctx context.Context) (Event, error)) (Event, error) {
	var lastErr error
	for attempt := 1; attempt <= maxSessionRetries+1; attempt++ {
		ev, err := fn(ctx)
		if err == nil {
			return ev, nil
		}
		lastErr = err
		if !isRetryable(err) || attempt > maxSessionRetries {
			return Event{}, err
		}
		delay := retryBaseDelay << (attempt - 1)
		if delay > retryMaxDelay {
			delay = retryMaxDelay
		}
		log.Printf("actae: %s attempt %d/%d failed — retrying in %s: %v",
			op, attempt, maxSessionRetries+1, delay, err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
	}
	return Event{}, lastErr
}

func anyString(v any) string {
	if v == nil {
		return "?"
	}
	return fmt.Sprintf("%v", v)
}
