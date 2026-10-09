package actae

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
)

// LifecycleErrorMode controls whether run lifecycle persistence is durable
// (raise) or telemetry-only (warn). Effect ownership is always fail-closed.
type LifecycleErrorMode string

const (
	LifecycleErrorsRaise LifecycleErrorMode = "raise"
	LifecycleErrorsWarn  LifecycleErrorMode = "warn"
)

// RunCarrierSchema is the cross-SDK serialized workflow lineage schema.
const RunCarrierSchema = "actae.run-carrier/v1"

// Runtime is the low-friction entry point for framework and orchestrator code.
// It never schedules or remotely executes application code.
type Runtime struct {
	Client          *Client
	LifecycleErrors LifecycleErrorMode
}

func NewRuntime(client *Client) (*Runtime, error) {
	if client == nil {
		return nil, errors.New("client is required")
	}
	return &Runtime{Client: client, LifecycleErrors: LifecycleErrorsRaise}, nil
}

func NewRuntimeFromEnv(opts ClientOptions) (*Runtime, error) {
	client, err := NewClientFromEnv(opts)
	if err != nil {
		return nil, err
	}
	return NewRuntime(client)
}

// RunOptions maps an orchestrator/framework execution to one Actae run.
type RunOptions struct {
	ID          string
	Framework   string
	ChannelID   string
	ParentRunID string
	Actor       string
	WorkflowID  string
	NativeRunID string
	Attempt     *int
	ExecutionID string
	Metadata    map[string]any
}

// Scope is the ambient logical execution exposed to tools and child tasks.
type Scope struct {
	Runtime      *Runtime
	Run          RunContext
	WorkflowID   string
	NativeRunID  string
	Attempt      *int
	InvocationID string
	Metadata     map[string]any
}

// RunCarrier contains no credentials and is safe to pass through a task
// queue so activities/workers can retain causal lineage across processes.
type RunCarrier struct {
	Schema     string `json:"schema"`
	ChannelID  string `json:"channel_id"`
	RunID      string `json:"run_id"`
	Framework  string `json:"framework"`
	WorkflowID string `json:"-"`
}

// MarshalJSON keeps the cross-SDK shape stable: an absent workflow ID is
// represented as null instead of disappearing or becoming an empty string.
func (c RunCarrier) MarshalJSON() ([]byte, error) {
	type wireCarrier struct {
		Schema     string  `json:"schema"`
		ChannelID  string  `json:"channel_id"`
		RunID      string  `json:"run_id"`
		Framework  string  `json:"framework"`
		WorkflowID *string `json:"workflow_id"`
	}
	var workflowID *string
	if c.WorkflowID != "" {
		workflowID = &c.WorkflowID
	}
	return json.Marshal(wireCarrier{
		Schema: c.Schema, ChannelID: c.ChannelID, RunID: c.RunID,
		Framework: c.Framework, WorkflowID: workflowID,
	})
}

// UnmarshalJSON rejects extension fields so a carrier can never silently
// transport credentials or application payload under this identity schema.
func (c *RunCarrier) UnmarshalJSON(value []byte) error {
	type wireCarrier struct {
		Schema     string  `json:"schema"`
		ChannelID  string  `json:"channel_id"`
		RunID      string  `json:"run_id"`
		Framework  string  `json:"framework"`
		WorkflowID *string `json:"workflow_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.DisallowUnknownFields()
	var wire wireCarrier
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	*c = RunCarrier{
		Schema: wire.Schema, ChannelID: wire.ChannelID, RunID: wire.RunID,
		Framework: wire.Framework,
	}
	if wire.WorkflowID != nil {
		if *wire.WorkflowID == "" {
			return errors.New("carrier workflow ID must not be empty")
		}
		c.WorkflowID = *wire.WorkflowID
	}
	return nil
}

func (c RunCarrier) Validate() error {
	if c.Schema != RunCarrierSchema {
		return errors.New("unsupported or invalid Actae run carrier")
	}
	if err := validateChannelID(c.ChannelID); err != nil {
		return err
	}
	if c.RunID == "" || c.Framework == "" {
		return errors.New("carrier run ID and framework are required")
	}
	return nil
}

func ParseRunCarrier(value []byte) (RunCarrier, error) {
	var carrier RunCarrier
	if err := json.Unmarshal(value, &carrier); err != nil {
		return RunCarrier{}, fmt.Errorf("decode Actae run carrier: %w", err)
	}
	if err := carrier.Validate(); err != nil {
		return RunCarrier{}, err
	}
	return carrier, nil
}

func (s Scope) ChannelID() string    { return s.Run.ChannelID }
func (s Scope) RunID() string        { return s.Run.RunID }
func (s Scope) ParentRunID() string  { return s.Run.ParentRunID }
func (s Scope) Framework() string    { return s.Run.Framework }
func (s Scope) Tools() *ToolExecutor { return s.Run.Tools() }

func (s Scope) Carrier() RunCarrier {
	return RunCarrier{
		Schema: RunCarrierSchema, ChannelID: s.ChannelID(), RunID: s.RunID(),
		Framework: s.Framework(), WorkflowID: s.WorkflowID,
	}
}

// ChildOptions derives a correctly-linked child run configuration.
func (s Scope) ChildOptions(runID string) RunOptions {
	return RunOptions{ID: runID, Framework: s.Run.Framework, ParentRunID: s.Run.RunID}
}

// Effect claims/replays an external effect using the current run's channel.
func (s Scope) Effect(ctx context.Context, keyName, toolName string, params any, invoke ToolFunc, opts ToolExecutionOptions) (any, error) {
	return s.Tools().Execute(ctx, keyName, toolName, params, invoke, opts)
}

// Do is the concise, typed effect path. Both a newly executed result and a
// replayed JSON result are decoded into T, so callers do not need type
// assertions that behave differently after a retry.
func Do[T any](ctx context.Context, scope Scope, keyName, toolName string, params any, invoke func(context.Context) (T, error)) (T, error) {
	return DoWithOptions(ctx, scope, keyName, toolName, params, invoke, ToolExecutionOptions{})
}

// DoWithOptions is Do with explicit lease, heartbeat and dedup controls.
func DoWithOptions[T any](ctx context.Context, scope Scope, keyName, toolName string, params any, invoke func(context.Context) (T, error), opts ToolExecutionOptions) (T, error) {
	var zero T
	if invoke == nil {
		return zero, errors.New("effect invoke function is required")
	}
	raw, err := scope.Effect(ctx, keyName, toolName, params, func(callCtx context.Context) (any, error) {
		return invoke(callCtx)
	}, opts)
	if err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return zero, fmt.Errorf("encode effect result: %w", err)
	}
	if normalized, decodeErr := decodeValue(encoded); decodeErr == nil {
		if result, ok := normalized.(T); ok {
			return result, nil
		}
	}
	var result T
	if err := json.Unmarshal(encoded, &result); err != nil {
		return zero, fmt.Errorf("decode effect result: %w", err)
	}
	return result, nil
}

type scopeContextKey struct{}

// ScopeFromContext returns the execution installed by Runtime.Run/WithRun.
func ScopeFromContext(ctx context.Context) (Scope, bool) {
	scope, ok := ctx.Value(scopeContextKey{}).(Scope)
	return scope, ok
}

// Current returns this Runtime's ambient scope.
func (r *Runtime) Current(ctx context.Context) (Scope, bool) {
	scope, ok := ScopeFromContext(ctx)
	return scope, ok && scope.Runtime == r
}

// MustCurrent is the concise option for tool code that requires a run.
func (r *Runtime) MustCurrent(ctx context.Context) Scope {
	scope, ok := r.Current(ctx)
	if !ok {
		panic("actae: no active run; use Runtime.Run or WithRun")
	}
	return scope
}

// Close releases the optional WebSocket connection. HTTP requests use the
// caller/client transport and require no separate session shutdown.
func (r *Runtime) Close() error {
	if r == nil || r.Client == nil {
		return nil
	}
	return r.Client.Disconnect()
}

// ChannelForRun maps arbitrary native IDs to deterministic Actae channel IDs.
func ChannelForRun(runID, namespace string) (string, error) {
	if runID == "" {
		return "", errors.New("run ID is required")
	}
	if namespace == "" {
		namespace = "run"
	}
	candidate := namespace + ":" + runID
	if len(candidate) <= 256 && isSafeChannel(candidate) {
		return candidate, nil
	}
	safeNamespace := sanitizeChannelPart(namespace, 80)
	if safeNamespace == "" {
		safeNamespace = "run"
	}
	slug := sanitizeChannelPart(runID, 80)
	if slug == "" {
		slug = "run"
	}
	digest := sha256.Sum256([]byte(runID))
	result := safeNamespace + ":" + slug + ":" + hex.EncodeToString(digest[:8])
	if err := validateChannelID(result); err != nil {
		return "", err
	}
	return result, nil
}

func isSafeChannel(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

func sanitizeChannelPart(value string, limit int) string {
	var b strings.Builder
	invalid := false
	for _, r := range value {
		valid := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == ':' || r == '-'
		if valid {
			if b.Len() < limit {
				b.WriteRune(r)
			}
			invalid = false
		} else if !invalid && b.Len() > 0 && b.Len() < limit {
			b.WriteByte('-')
			invalid = true
		}
	}
	return strings.Trim(b.String(), "-._:")
}

// Run establishes an ambient scope and records its lifecycle.
func (r *Runtime) Run(ctx context.Context, opts RunOptions, fn func(context.Context, Scope) error) error {
	_, err := WithRun(ctx, r, opts, func(runCtx context.Context, scope Scope) (struct{}, error) {
		return struct{}{}, fn(runCtx, scope)
	})
	return err
}

// WithRun is the value-returning variant of Runtime.Run.
func WithRun[T any](ctx context.Context, runtime *Runtime, opts RunOptions, fn func(context.Context, Scope) (T, error)) (T, error) {
	var zero T
	if runtime == nil || runtime.Client == nil {
		return zero, errors.New("runtime and client are required")
	}
	if fn == nil {
		return zero, errors.New("run function is required")
	}
	if opts.ID == "" {
		return zero, errors.New("run ID is required")
	}
	framework := opts.Framework
	if framework == "" {
		framework = "custom"
	}
	if opts.Attempt != nil && *opts.Attempt < 0 {
		return zero, errors.New("attempt must be non-negative")
	}
	if _, err := json.Marshal(opts.Metadata); err != nil {
		return zero, fmt.Errorf("run metadata must be JSON-serializable: %w", err)
	}
	parent := opts.ParentRunID
	if parent == "" {
		if ambient, ok := runtime.Current(ctx); ok {
			parent = ambient.Run.RunID
		}
	}
	channelID := opts.ChannelID
	if channelID == "" {
		var err error
		channelID, err = ChannelForRun(opts.ID, framework)
		if err != nil {
			return zero, err
		}
	} else if err := validateChannelID(channelID); err != nil {
		return zero, err
	}
	run, err := NewRunContext(runtime.Client, channelID, framework, opts.ID)
	if err != nil {
		return zero, err
	}
	run.ParentRunID = parent
	run.Actor = opts.Actor
	metadata := cloneMap(opts.Metadata)
	invocationID := opts.ExecutionID
	if invocationID == "" {
		if opts.Attempt != nil {
			invocationID = "attempt:" + fmt.Sprint(*opts.Attempt)
		} else {
			invocationID = uuid.NewString()
		}
	}
	scope := Scope{Runtime: runtime, Run: run, WorkflowID: opts.WorkflowID, NativeRunID: opts.NativeRunID, Attempt: opts.Attempt, InvocationID: invocationID, Metadata: metadata}
	runCtx := context.WithValue(ctx, scopeContextKey{}, scope)
	if err := runtime.lifecycle(runCtx, scope, "run.started", lifecyclePayload(scope)); err != nil {
		return zero, err
	}

	result, invokeErr := fn(runCtx, scope)
	if invokeErr == nil && runCtx.Err() != nil {
		invokeErr = runCtx.Err()
	}
	if invokeErr != nil {
		payload := lifecyclePayload(scope)
		kind := "run.failed"
		field := "error"
		if errors.Is(invokeErr, context.Canceled) || errors.Is(invokeErr, context.DeadlineExceeded) {
			kind = "run.cancelled"
			field = "cancellation"
		}
		payload[field] = map[string]any{"type": fmt.Sprintf("%T", invokeErr), "message": truncate(invokeErr.Error(), 4000)}
		recordCtx := runCtx
		var cancel context.CancelFunc
		if runCtx.Err() != nil {
			recordCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
		}
		if recordErr := runtime.lifecycle(recordCtx, scope, kind, payload); recordErr != nil {
			log.Printf("actae: failed to record run failure; preserving application error: %v", recordErr)
		}
		return zero, invokeErr
	}
	payload := lifecyclePayload(scope)
	if err := runtime.lifecycle(runCtx, scope, "run.completed", payload); err != nil {
		return zero, err
	}
	return result, nil
}

// Workflow is Run with orchestrator ownership metadata filled in.
func (r *Runtime) Workflow(ctx context.Context, workflowID, orchestrator string, attempt *int, fn func(context.Context, Scope) error) error {
	if orchestrator == "" {
		orchestrator = "orchestrator"
	}
	if isKnownDeterministicOrchestrator(orchestrator) {
		return fmt.Errorf("%s workflow code is replayed deterministically; use Runtime.Orchestrator(%q).Carrier in the workflow and Orchestrator.Activity at the worker boundary", orchestrator, orchestrator)
	}
	return r.Run(ctx, RunOptions{
		ID: workflowID, Framework: orchestrator, WorkflowID: workflowID,
		NativeRunID: workflowID, Attempt: attempt,
	}, fn)
}

// Orchestrator binds workflow/activity helpers to a scheduler name without
// making Actae a second scheduler.
type Orchestrator struct {
	Runtime       *Runtime
	Name          string
	Deterministic bool
}

func (r *Runtime) Orchestrator(name string) (*Orchestrator, error) {
	return r.NewOrchestrator(name, isKnownDeterministicOrchestrator(name))
}

func (r *Runtime) NewOrchestrator(name string, deterministic bool) (*Orchestrator, error) {
	if r == nil || r.Client == nil {
		return nil, errors.New("runtime and client are required")
	}
	if name == "" {
		return nil, errors.New("orchestrator name is required")
	}
	return &Orchestrator{Runtime: r, Name: name, Deterministic: deterministic}, nil
}

func isKnownDeterministicOrchestrator(name string) bool {
	switch strings.ToLower(name) {
	case "temporal", "durable-functions", "azure-durable-functions":
		return true
	default:
		return false
	}
}

// Carrier is pure and safe to call from deterministic workflow replay code.
func (o *Orchestrator) Carrier(workflowID string) (RunCarrier, error) {
	channelID, err := ChannelForRun(workflowID, o.Name)
	if err != nil {
		return RunCarrier{}, err
	}
	return RunCarrier{
		Schema: RunCarrierSchema, ChannelID: channelID, RunID: workflowID,
		Framework: o.Name, WorkflowID: workflowID,
	}, nil
}

func (o *Orchestrator) Workflow(ctx context.Context, opts RunOptions, fn func(context.Context, Scope) error) error {
	if o.Deterministic {
		return fmt.Errorf("%s workflow code must not perform Actae network I/O during deterministic replay; use Orchestrator.Carrier in the workflow and Orchestrator.Activity inside the activity/worker boundary", o.Name)
	}
	if opts.ID == "" {
		return errors.New("workflow run ID is required")
	}
	opts.Framework = o.Name
	opts.WorkflowID = opts.ID
	if opts.NativeRunID == "" {
		opts.NativeRunID = opts.ID
	}
	return o.Runtime.Run(ctx, opts, fn)
}

// Activity restores parent lineage from a serialized carrier. The activity
// attempt is metadata only and never participates in effect identity.
func (o *Orchestrator) Activity(ctx context.Context, carrier RunCarrier, opts RunOptions, fn func(context.Context, Scope) error) error {
	if err := carrier.Validate(); err != nil {
		return err
	}
	if opts.ID == "" {
		return errors.New("activity run ID is required")
	}
	opts.Framework = o.Name + ".activity"
	opts.ParentRunID = carrier.RunID
	opts.WorkflowID = carrier.WorkflowID
	if opts.WorkflowID == "" {
		opts.WorkflowID = carrier.RunID
	}
	if opts.NativeRunID == "" {
		opts.NativeRunID = opts.ID
	}
	return o.Runtime.Run(ctx, opts, fn)
}

func (o *Orchestrator) EffectKey(logicalTaskID, effect, businessID string) (string, error) {
	if logicalTaskID == "" || effect == "" || businessID == "" {
		return "", errors.New("logical task ID, effect and business ID are required")
	}
	return DeterministicOperationKey("actae.orchestrator.effect", effect, logicalTaskID, businessID), nil
}

// Effect executes against the ambient scope. The attempt number is not part
// of keyName, so orchestrator retries replay the same logical effect.
func (r *Runtime) Effect(ctx context.Context, keyName, toolName string, params any, invoke ToolFunc, opts ToolExecutionOptions) (any, error) {
	scope, ok := r.Current(ctx)
	if !ok {
		return nil, errors.New("no active Actae run; use Runtime.Run or WithRun")
	}
	return scope.Effect(ctx, keyName, toolName, params, invoke, opts)
}

// Observe wraps an existing context-aware entry point. resolve runs for each
// invocation so request/workflow IDs can come from native arguments/context.
func (r *Runtime) Observe(resolve func(context.Context) (RunOptions, error), fn func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if resolve == nil || fn == nil {
			return errors.New("Actae observe resolver and function are required")
		}
		opts, err := resolve(ctx)
		if err != nil {
			return err
		}
		return r.Run(ctx, opts, func(runCtx context.Context, _ Scope) error {
			return fn(runCtx)
		})
	}
}

func (r *Runtime) lifecycle(ctx context.Context, scope Scope, kind string, payload map[string]any) error {
	mode := r.LifecycleErrors
	if mode == "" {
		mode = LifecycleErrorsRaise
	}
	if mode != LifecycleErrorsRaise && mode != LifecycleErrorsWarn {
		return errors.New("LifecycleErrors must be raise or warn")
	}
	operationID := DeterministicOperationKey("actae.runtime", kind, scope.Run.ChannelID, scope.Run.RunID, scope.InvocationID)
	_, err := scope.Run.RecordWithOptions(ctx, kind, payload, FrameworkRecordOptions{OperationID: &operationID})
	if err != nil && mode == LifecycleErrorsWarn {
		log.Printf("actae: lifecycle write failed (%s): %v", kind, err)
		return nil
	}
	return err
}

func lifecyclePayload(scope Scope) map[string]any {
	return map[string]any{
		"workflow_id":   scope.WorkflowID,
		"native_run_id": scope.NativeRunID,
		"attempt":       scope.Attempt,
		"invocation_id": scope.InvocationID,
		"metadata":      cloneMap(scope.Metadata),
	}
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
