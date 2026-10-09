package actae

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// CheckpointSchema is shared byte-for-byte with the Python and TypeScript SDKs.
const CheckpointSchema = "actae.framework-checkpoint/v1"

// CheckpointMetadataKey is reserved inside framework-owned snapshot objects.
const CheckpointMetadataKey = "_actae_adapter"

type ActaeFeature string

const (
	FeatureEvents          ActaeFeature = "events"
	FeatureReplay          ActaeFeature = "replay"
	FeatureCheckpoints     ActaeFeature = "checkpoints"
	FeatureResume          ActaeFeature = "resume"
	FeatureForks           ActaeFeature = "forks"
	FeatureToolExecutions  ActaeFeature = "tool_executions"
	FeatureExperiments     ActaeFeature = "experiments"
	FeatureExecutionGroups ActaeFeature = "execution_groups"
	FeatureWakeups         ActaeFeature = "wakeups"
	FeatureCausalLineage   ActaeFeature = "causal_lineage"
)

var FullFeatureSet = []ActaeFeature{
	FeatureEvents, FeatureReplay, FeatureCheckpoints, FeatureResume, FeatureForks,
	FeatureToolExecutions, FeatureExperiments, FeatureExecutionGroups,
	FeatureWakeups, FeatureCausalLineage,
}

type AdapterSupportLevel string

const (
	AdapterCertified     AdapterSupportLevel = "certified"
	AdapterPreview       AdapterSupportLevel = "preview"
	AdapterObservability AdapterSupportLevel = "observability"
)

type ResumeFidelity string

const (
	ResumeCheckpointExact ResumeFidelity = "checkpoint_exact"
	ResumeSessionNative   ResumeFidelity = "session_native"
	ResumeReconstructed   ResumeFidelity = "reconstructed"
	ResumeContextSeeded   ResumeFidelity = "context_seeded"
	ResumeObserveOnly     ResumeFidelity = "observe_only"
)

// AdapterCapabilities is a machine-readable, testable support claim.
type AdapterCapabilities struct {
	Framework        string              `json:"framework"`
	AdapterVersion   string              `json:"adapter_version"`
	SupportLevel     AdapterSupportLevel `json:"support_level"`
	ResumeFidelity   ResumeFidelity      `json:"resume_fidelity"`
	Features         []ActaeFeature      `json:"features"`
	NativeCheckpoint bool                `json:"native_checkpoint"`
	Notes            string              `json:"notes,omitempty"`
}

func (c AdapterCapabilities) Validate() error {
	if c.Framework == "" || c.AdapterVersion == "" {
		return errors.New("framework and adapter version are required")
	}
	if c.Supports(FeatureResume) && !c.Supports(FeatureCheckpoints) {
		return errors.New("resume support requires checkpoint support")
	}
	if c.Supports(FeatureForks) && !c.Supports(FeatureCheckpoints) {
		return errors.New("fork support requires checkpoint support")
	}
	if c.NativeCheckpoint && !c.Supports(FeatureCheckpoints) {
		return errors.New("native checkpoint requires checkpoint support")
	}
	return nil
}

func (c AdapterCapabilities) Supports(feature ActaeFeature) bool {
	for _, candidate := range c.Features {
		if candidate == feature {
			return true
		}
	}
	return false
}

func (c AdapterCapabilities) MissingFeatures() []ActaeFeature {
	missing := make([]ActaeFeature, 0)
	for _, feature := range FullFeatureSet {
		if !c.Supports(feature) {
			missing = append(missing, feature)
		}
	}
	return missing
}

func (c AdapterCapabilities) FullParity() bool { return len(c.MissingFeatures()) == 0 }

// CopilotCapabilities describes the native Go adapter. The complete service
// surface is available beside it through RunContext.
var CopilotCapabilities = AdapterCapabilities{
	Framework: "github-copilot-sdk", AdapterVersion: "2",
	SupportLevel: AdapterCertified, ResumeFidelity: ResumeSessionNative,
	Features:         []ActaeFeature{FeatureEvents, FeatureReplay, FeatureCheckpoints, FeatureResume, FeatureForks, FeatureCausalLineage},
	NativeCheckpoint: true,
	Notes:            "Native Copilot session events, durable state and session fork integration.",
}

func CommonSurfaceCapabilities(native AdapterCapabilities) AdapterCapabilities {
	result := native
	result.Features = append([]ActaeFeature(nil), FullFeatureSet...)
	result.Notes += " Full Actae service surface is available through RunContext."
	return result
}

// CheckpointEnvelope stores portable state beside a framework-native opaque checkpoint.
type CheckpointEnvelope struct {
	Schema           string         `json:"schema"`
	Framework        string         `json:"framework"`
	FrameworkVersion *string        `json:"framework_version"`
	AdapterVersion   string         `json:"adapter_version"`
	ChannelID        string         `json:"channel_id"`
	EventCursor      *int64         `json:"event_cursor"`
	EventID          *string        `json:"event_id"`
	CreatedAt        string         `json:"created_at"`
	PortableState    map[string]any `json:"portable_state"`
	NativeCheckpoint any            `json:"native_checkpoint"`
	ApplicationState map[string]any `json:"application_state"`
	PendingWork      map[string]any `json:"pending_work"`
	Manifest         map[string]any `json:"manifest"`
}

// FrameworkEvent is the portable event vocabulary used by RunContext.Record.
type FrameworkEvent struct {
	Schema      string         `json:"schema"`
	Kind        string         `json:"kind"`
	Framework   string         `json:"framework"`
	RunID       string         `json:"run_id"`
	ParentRunID *string        `json:"parent_run_id"`
	AgentID     *string        `json:"agent_id"`
	OccurredAt  string         `json:"occurred_at"`
	Data        map[string]any `json:"data"`
}

func NewCheckpointEnvelope(framework, adapterVersion, channelID string) CheckpointEnvelope {
	return CheckpointEnvelope{
		Schema: CheckpointSchema, Framework: framework, AdapterVersion: adapterVersion,
		ChannelID: channelID, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		PortableState: map[string]any{}, ApplicationState: map[string]any{},
		PendingWork: map[string]any{}, Manifest: map[string]any{},
	}
}

func (e CheckpointEnvelope) Validate() error {
	if e.Schema != CheckpointSchema {
		return fmt.Errorf("unsupported checkpoint schema: %s", e.Schema)
	}
	if e.Framework == "" || e.AdapterVersion == "" || e.ChannelID == "" {
		return errors.New("framework, adapter version and channel ID are required")
	}
	if e.EventCursor != nil && *e.EventCursor < 0 {
		return errors.New("event cursor must be non-negative")
	}
	_, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("checkpoint envelope must be JSON-serializable: %w", err)
	}
	return nil
}

func EmbedCheckpointMetadata(state map[string]any, envelope CheckpointEnvelope) (map[string]any, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	result := make(map[string]any, len(state)+1)
	for key, value := range state {
		result[key] = value
	}
	encoded, _ := json.Marshal(envelope)
	var metadata map[string]any
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		return nil, err
	}
	result[CheckpointMetadataKey] = metadata
	if _, err := json.Marshal(result); err != nil {
		return nil, fmt.Errorf("framework state must be JSON-serializable: %w", err)
	}
	return result, nil
}

func ExtractCheckpointEnvelope(state map[string]any) (*CheckpointEnvelope, error) {
	raw, ok := state[CheckpointMetadataKey]
	if !ok || raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var envelope CheckpointEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return nil, fmt.Errorf("invalid checkpoint metadata: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	return &envelope, nil
}

func StripCheckpointMetadata(state map[string]any) map[string]any {
	result := make(map[string]any, len(state))
	for key, value := range state {
		if key != CheckpointMetadataKey {
			result[key] = value
		}
	}
	return result
}

type ToolExecutionInProgressError struct {
	ExecutionID string
	KeyName     string
}

func (e *ToolExecutionInProgressError) Error() string {
	return fmt.Sprintf("tool execution %q is already in progress (execution_id=%s)", e.KeyName, e.ExecutionID)
}

type toolExecutionClient interface {
	ClaimExecution(context.Context, string, string, string, any, ClaimExecutionOptions) (ExecutionClaim, error)
	CompleteExecution(context.Context, string, *string, any) (ExecutionInfo, error)
	FailExecution(context.Context, string, string, FailExecutionOptions) (ExecutionInfo, error)
	HeartbeatExecution(context.Context, string, *string, *int) (ExecutionInfo, error)
	CancelExecution(context.Context, string, *string) (ExecutionInfo, error)
}

type ToolExecutionOptions struct {
	DedupFields       []string
	LeaseSeconds      int
	HeartbeatInterval time.Duration
	EmitReplayEvent   *bool
}

type ToolFunc func(context.Context) (any, error)

// ToolExecutor mediates framework tool effects through Actae's execution ledger.
type ToolExecutor struct {
	client    toolExecutionClient
	ChannelID string
	Actor     string
}

func NewToolExecutor(client *Client, channelID string) *ToolExecutor {
	return newToolExecutor(client, channelID)
}

func newToolExecutor(client toolExecutionClient, channelID string) *ToolExecutor {
	return &ToolExecutor{client: client, ChannelID: channelID, Actor: "framework-tool"}
}

func (e *ToolExecutor) Execute(ctx context.Context, keyName, toolName string, params any, invoke ToolFunc, opts ToolExecutionOptions) (any, error) {
	if e.client == nil || e.ChannelID == "" || keyName == "" || toolName == "" || invoke == nil {
		return nil, errors.New("client, channel ID, key name, tool name and invoke are required")
	}
	lease := opts.LeaseSeconds
	if lease == 0 {
		lease = 60
	}
	if lease < 1 {
		return nil, errors.New("lease seconds must be positive")
	}
	emitReplay := true
	if opts.EmitReplayEvent != nil {
		emitReplay = *opts.EmitReplayEvent
	}
	actor := e.Actor
	claim, err := e.client.ClaimExecution(ctx, e.ChannelID, keyName, toolName, params, ClaimExecutionOptions{
		DedupFields: opts.DedupFields, LeaseSeconds: &lease,
		EmitReplayEvent: emitReplay, Actor: &actor,
	})
	if err != nil {
		return nil, err
	}
	switch claim.Status {
	case "replayed":
		return claim.Result, nil
	case "in_progress":
		return nil, &ToolExecutionInProgressError{ExecutionID: claim.Execution.ID, KeyName: keyName}
	case "claimed", "reclaimed":
	default:
		return nil, fmt.Errorf("unknown execution claim status: %s", claim.Status)
	}
	if claim.ClaimToken == nil {
		return nil, errors.New("owned execution claim is missing claim token")
	}

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := opts.HeartbeatInterval
	if interval <= 0 {
		interval = time.Duration(lease) * time.Second / 3
		if interval > 30*time.Second {
			interval = 30 * time.Second
		}
	}
	stopHeartbeat := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(stopHeartbeat) }) }
	defer stop()
	heartbeatErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-execCtx.Done():
				return
			case <-ticker.C:
				if _, hbErr := e.client.HeartbeatExecution(execCtx, claim.Execution.ID, claim.ClaimToken, &lease); hbErr != nil {
					select {
					case heartbeatErr <- hbErr:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	type outcome struct {
		value any
		err   error
	}
	result := make(chan outcome, 1)
	go func() { value, invokeErr := invoke(execCtx); result <- outcome{value, invokeErr} }()

	var out outcome
	select {
	case out = <-result:
		stop()
	case hbErr := <-heartbeatErr:
		e.bestEffortFail(claim, hbErr)
		return nil, hbErr
	case <-ctx.Done():
		e.bestEffortCancel(claim)
		return nil, ctx.Err()
	}
	if out.err != nil {
		e.bestEffortFail(claim, out.err)
		return nil, out.err
	}
	if _, err := json.Marshal(out.value); err != nil {
		validationErr := fmt.Errorf("tool result must be JSON-serializable: %w", err)
		e.bestEffortFail(claim, validationErr)
		return nil, validationErr
	}
	if _, err := e.client.CompleteExecution(ctx, claim.Execution.ID, claim.ClaimToken, out.value); err != nil {
		return nil, err
	}
	return out.value, nil
}

func (e *ToolExecutor) bestEffortCancel(claim ExecutionClaim) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = e.client.CancelExecution(ctx, claim.Execution.ID, claim.ClaimToken)
}

func (e *ToolExecutor) bestEffortFail(claim ExecutionClaim, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typeName := fmt.Sprintf("%T", cause)
	stack := string(debug.Stack())
	_, _ = e.client.FailExecution(ctx, claim.Execution.ID, cause.Error(), FailExecutionOptions{
		ClaimToken: claim.ClaimToken, ErrorType: &typeName, Stack: &stack,
	})
}

// RunContext exposes the same Actae service surface beside every Go adapter.
type RunContext struct {
	Client      *Client
	ChannelID   string
	Framework   string
	RunID       string
	ParentRunID string
	Actor       string
}

func NewRunContext(client *Client, channelID, framework, runID string) (RunContext, error) {
	run := RunContext{Client: client, ChannelID: channelID, Framework: framework, RunID: runID}
	return run, run.Validate()
}

func (r RunContext) Validate() error {
	if r.Client == nil || r.ChannelID == "" || r.Framework == "" || r.RunID == "" {
		return errors.New("client, channel ID, framework and run ID are required")
	}
	return nil
}

func (r RunContext) Tools() *ToolExecutor {
	executor := NewToolExecutor(r.Client, r.ChannelID)
	if r.Actor != "" {
		executor.Actor = r.Actor
	}
	return executor
}

func (r RunContext) Record(ctx context.Context, kind string, payload map[string]any) (Event, error) {
	return r.RecordWithOptions(ctx, kind, payload, FrameworkRecordOptions{})
}

type FrameworkRecordOptions struct {
	OperationID *string
}

// RecordWithOptions records a normalized framework event with optional
// idempotency. Runtime lifecycle events use this to remain stable per attempt.
func (r RunContext) RecordWithOptions(ctx context.Context, kind string, payload map[string]any, opts FrameworkRecordOptions) (Event, error) {
	if err := r.Validate(); err != nil {
		return Event{}, err
	}
	if kind == "" {
		return Event{}, errors.New("event kind is required")
	}
	var parent *string
	if r.ParentRunID != "" {
		parent = &r.ParentRunID
	}
	event := FrameworkEvent{
		Schema: "actae.framework-event/v1", Kind: kind, Framework: r.Framework,
		RunID: r.RunID, ParentRunID: parent,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: payload,
	}
	actor := r.Actor
	if actor == "" {
		actor = r.Framework
	}
	return r.Client.Record(ctx, r.ChannelID, "framework."+kind, event, RecordOptions{Actor: actor, OperationID: opts.OperationID})
}

func (r RunContext) Replay(ctx context.Context, opts ReplayOptions) ([]Event, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r.Client.Replay(ctx, r.ChannelID, opts)
}

func (r RunContext) SaveCheckpoint(ctx context.Context, envelope CheckpointEnvelope) (int64, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	if envelope.ChannelID != r.ChannelID || envelope.Framework != r.Framework {
		return 0, errors.New("checkpoint does not belong to this run context")
	}
	if err := envelope.Validate(); err != nil {
		return 0, err
	}
	cursor := int64(0)
	if envelope.EventCursor != nil {
		cursor = *envelope.EventCursor
	} else {
		latest, err := r.Client.LatestCursor(ctx, r.ChannelID)
		if err != nil {
			return 0, err
		}
		if latest != nil {
			cursor = *latest
		}
	}
	return r.Client.SaveState(ctx, r.ChannelID, cursor, envelope, SaveStateOptions{})
}

func (r RunContext) LoadCheckpoint(ctx context.Context) (*CheckpointEnvelope, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	snapshot, err := r.Client.LatestState(ctx, r.ChannelID)
	if err != nil || snapshot == nil {
		return nil, err
	}
	if schema, _ := snapshot.State["schema"].(string); schema == CheckpointSchema {
		encoded, marshalErr := json.Marshal(snapshot.State)
		if marshalErr != nil {
			return nil, marshalErr
		}
		var envelope CheckpointEnvelope
		if unmarshalErr := json.Unmarshal(encoded, &envelope); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		if validateErr := envelope.Validate(); validateErr != nil {
			return nil, validateErr
		}
		return &envelope, nil
	}
	return ExtractCheckpointEnvelope(snapshot.State)
}

type RunForkOptions struct {
	AtCursor    int64
	Reason      string
	Manifest    map[string]any
	OperationID *string
}

func (r RunContext) Fork(ctx context.Context, newChannelID string, opts RunForkOptions) (RunContext, ForkReceipt, error) {
	if err := r.Validate(); err != nil {
		return RunContext{}, ForkReceipt{}, err
	}
	displayName := newChannelID
	reason := opts.Reason
	receipt, err := r.Client.Fork(ctx, r.ChannelID, newChannelID, opts.AtCursor, ForkOptions{
		DisplayName: &displayName, Reason: &reason, Manifest: opts.Manifest, OperationID: opts.OperationID,
	})
	if err != nil {
		return RunContext{}, ForkReceipt{}, err
	}
	child := r
	child.ChannelID = newChannelID
	child.ParentRunID = r.RunID
	child.RunID = newChannelID
	return child, receipt, nil
}

func (r RunContext) CreateExperiment(ctx context.Context, name, description string) (map[string]any, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r.Client.CreateExperiment(ctx, name, ExperimentOptions{Description: description, BaselineChannelID: r.ChannelID})
}

func (r RunContext) AddToExperiment(ctx context.Context, groupID, role string, declaredDelta map[string]any) (map[string]any, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r.Client.AddExperimentMember(ctx, groupID, r.ChannelID, ExperimentMemberOptions{Role: role, DeclaredDelta: declaredDelta})
}

func (r RunContext) CreateExecutionGroup(ctx context.Context, groupID string, metadata map[string]any) (ExecutionGroup, error) {
	if err := r.Validate(); err != nil {
		return ExecutionGroup{}, err
	}
	return r.Client.CreateExecutionGroup(ctx, groupID, metadata)
}

func (r RunContext) ScheduleWakeup(ctx context.Context, runAt string, payload map[string]any) (Wakeup, error) {
	if err := r.Validate(); err != nil {
		return Wakeup{}, err
	}
	return r.Client.ScheduleWakeup(ctx, r.ChannelID, runAt, payload)
}

// SortedFeatures is useful in support tables and deterministic test output.
func SortedFeatures(features []ActaeFeature) []ActaeFeature {
	result := append([]ActaeFeature(nil), features...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
