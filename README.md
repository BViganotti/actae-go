# Actae Go SDK

> Distributed agent coordination and Group Fork are documented in [`../../docs/EXECUTION_GROUPS.md`](../../docs/EXECUTION_GROUPS.md). `Client.ExecutionGroup` exposes durable messaging, fenced member leases, `WaitFor`/`WaitAny`/`WaitAll`, acknowledgements, promotion, and receipts.

Go client for [Actae](https://actae.dev) — durable, forkable execution for
AI-agent workflows. It mirrors the [Python SDK](../python/README.md) API surface
and adds a first-class integration with the
[GitHub Copilot SDK for Go](https://github.com/github/copilot-sdk/go)
(`actae/copilot`).

```bash
go get github.com/BViganotti/actae-go
```

Go 1.25.12+ · dependency: `github.com/gorilla/websocket v1.5.3`.

> **Gotchas at a glance** (full details below): the server never echoes your
> own WebSocket publishes back to you (use `EchoSelf`), payload numbers are
> `int64` or `float64` depending on how they were stored (use the `As*`
> accessors), and optional options fields are pointers (use `actae.Ptr`).

## Start with self-hosted Actae (free)

Self-hosting is the default way to run Actae: one free, self-contained binary
with the API, dashboard, and PostgreSQL embedded. No account, card, or control
plane.

```bash
# Download for your platform: https://actae.dev/download
tar -xzf actae-<version>-<platform>.tar.gz && chmod +x ./actae
./actae
# The banner ends with your endpoint and a one-time API key:
#   Dashboard:   http://127.0.0.1:8002
#   API key:     sk-...   (shown once — store it now)
```

**Self-hosted still needs an API key** — a *local* key the binary generates and
prints once on first boot, not an account or portal credential. There is no
signup. (Anonymous access is off by default.) Mint more with
`actae keys create <name>`.

```bash
export ACTAE_URL=http://127.0.0.1:8002
export ACTAE_API_KEY=sk-...        # from the banner
```

```go
client, err := actae.NewClientFromEnv(actae.ClientOptions{}) // reads ACTAE_URL + ACTAE_API_KEY
```

Prefer not to operate it? **Actae Cloud** is the managed runtime — create an
API key in the portal and use your instance URL. The rest of this README is
identical for either deployment.

## 60-second start

```go
import (
    "context"
    actae "github.com/BViganotti/actae-go"
)

func main() {
    ctx := context.Background()
    client, err := actae.NewClientFromEnv(actae.ClientOptions{}) // ACTAE_URL + ACTAE_API_KEY
    if err != nil { panic(err) }

    // Record an event, then replay it back
    ev, _ := client.Record(ctx, "my-channel", "agent.step",
        map[string]any{"input": "hello"}, actae.RecordOptions{Actor: "agent"})
    events, _ := client.Replay(ctx, "my-channel", actae.ReplayOptions{Limit: 100})

    _ = ev; _ = events
}
```

That's the whole loop: **channels** are named event streams, **events** are
immutable JSON blobs with a monotonic cursor, and **replay** reads them back.
Everything else in this SDK is a convenience or a specialization on top of
that. (For env-based config: `ACTAE_URL`, `ACTAE_API_KEY`, `ACTAE_WS_URL`.)

### Orchestration-safe Runtime

For an orchestrator, the shortest orchestration-safe entry point is `Runtime`:

```go
runtime, err := actae.NewRuntimeFromEnv(actae.ClientOptions{})
err = runtime.Run(ctx, actae.RunOptions{ID: job.ID, Framework: "worker"},
    func(ctx context.Context, run actae.Scope) error {
        _, err := actae.Do(ctx, run, "job:"+job.ID+":send", "send",
            payload, send)
        return err
    })
```

`Scope` travels through `context.Context`; `RunCarrier` safely crosses task
queues, and deterministic orchestrators keep all Actae network I/O inside
activities. See [SDK Integration](../../docs/SDK_INTEGRATION.md).

## Which feature do I need?

| I want to… | Use | See |
|------------|-----|-----|
| Record + replay agent steps | `Record` / `Replay` / `Query` | [Events](#events) |
| Real-time subscribe/publish | `Connect` + `Subscribe` + `Publish` | [WebSocket](#websocket) |
| Persist agent state with versions | `StateManager` | [State](#state) |
| Record a full agent run with step numbers, forks, crash recovery | `AgentSession` | [AgentSession](#agentsession) |
| Run experiments off a past state | `Fork` + `AgentSession.Fork` | [Forks](#forks) |
| Durable work queues (at-least-once) | consumer groups | [Consumer groups](#consumer-groups) |
| Schedule delayed events | `ScheduleWakeup` | [Wake-ups](#wake-ups) |
| Idempotent tool calls across retries | tool executions | [Tool executions](#tool-executions) |
| Record every Copilot session, fork at any event | `actae/copilot` | [Copilot](#copilot) |

## Events

```go
ev, _ := client.Record(ctx, "my-channel", "agent.step",
    map[string]any{"input": "hello"}, actae.RecordOptions{Actor: "agent"})

// ev.Cursor is the per-channel gapless cursor — pass it back to resume.
events, _ := client.Replay(ctx, "my-channel", actae.ReplayOptions{
    Cursor: actae.Ptr(ev.Cursor), // resume after ev
    Limit:  100,
})
// Search across channels with filters:
hits, _ := client.Query(ctx, actae.QueryOptions{
    ChannelIDs: []string{"my-channel"},
    EventType:  actae.Ptr("agent.step"),
})
```

- **Payload numbers**: JSON is decoded with integer fidelity — `{"n": 1}`
  comes back as `int64(1)`, `{"n": 1.0}` as `float64(1.0)`. Use
  `actae.AsInt64(value, def)`, `AsString`, `AsMap`, `AsList`, or
  `json.Marshal(ev.Payload)` into your own struct.
- **Optional params are pointers**: `Cursor: actae.Ptr(int64(42))`. The
  `actae.Ptr` helper exists so you never write the `v := …; &v` dance.
- **Retries are safe**: pass `RecordOptions.OperationID` (a stable UUID) and
  the server returns the original event instead of duplicating on retry.
  The WS `Publish` accepts the same via `PublishOptions.OperationID`.
- **`ev.Cursor`**: the gapless per-channel index (each channel numbers its
  events 1, 2, 3, … independently) — use it for replaying one channel,
  subscribe resume, and group acks. `ev.ChannelCursor` is a deprecated alias
  carrying the same value (kept so older clients keep working).

## WebSocket

```go
client.OnMessage(func(topic string, event actae.Event) { /* live events */ })
_ = client.Connect(ctx)
_ = client.SubscribeAndWait(ctx, "my-channel", nil) // cursor to resume from
persisted, _ := client.Publish(ctx, "my-channel", map[string]any{"x": 1})
_ = client.Disconnect()
```

- **Per-event async-style iteration**: `events, _ := client.Stream(ctx, "my-channel", nil)`
  returns a channel of live `Event`s (Python `stream()` parity) — subscribe
  and collect in one call; the channel closes on disconnect or `ctx` cancel.
- **The echo gotcha**: the server never broadcasts your own publish back to
  the connection that sent it (verified identical in the Python SDK). A
  client that subscribes AND publishes will not see its own events. Either
  use two connections, or set `ClientOptions{EchoSelf: true}` to deliver
  your publishes to your own `OnMessage` callbacks.
- `OnMessage` callbacks **accumulate** — register as many as you need; every
  callback gets every broadcast. `OnError`/`OnSubscribed`/`OnDisconnected`/
  `OnReconnect` are single-slot (last registration wins) — use `OnError`
  for observability and `OnReconnect` to know when resubscription finished.
- Reconnect is automatic (exponential backoff, resubscribes all topics with
  their last cursors, fires `OnReconnect`). `Disconnect` disables it.
- Keepalive pings every `KeepAliveInterval` (default 30s; negative disables)
  — the server drops connections idle > 300s.
- `Connect` blocks until the server authenticates your API key (→ `AuthError`
  on rejection). `SubscribeAndWait` blocks until the server confirms the
  subscription (10s timeout → `ConnectionError`).

## State

Two ways to persist state — here's the decision:

| API | What it is | When to use |
|-----|------------|-------------|
| `state.Store[T]` (`state/` subpackage) | Typed transactional store: atomic event + snapshot in one server-side transaction, guarded read-modify-write CAS loop with automatic conflict retries | Apps that mutate state and want the event stream and the state object to never disagree |
| `StateManager` | Facade over `SaveState`, auto-aligns to the latest cursor | Most apps — "save my agent's memory", "load it back" |
| `SaveState`/`LatestState`/`GetState` | Raw cursor-aligned versioned snapshots with optimistic-concurrency guards | Custom alignment or guard logic (`ExpectedVersion`/`ExpectedCursor`) |

```go
sm := actae.NewStateManager(client, "my-channel")
version, _ := sm.Save(ctx, map[string]any{"mem": "one"}) // aligned to latest cursor
state, _ := sm.Resume(ctx, map[string]any{"mem": "default"}) // or sm.Load()
_ = version; _ = state
```

Each `Save` creates a new immutable version — `ListVersions`/`GetVersion`/
`DeleteVersion` browse and prune history, and `Fork(ctx, newChannel,
StateManagerForkOptions{Reason})` forks the latest state into a new channel
(returning a `StateManager` bound to it; `Load()` on it returns the
inherited state — a `SnapshotBoundaryError` at the latest cursor retries once
at `at_cursor=0`, matching `StateManager.fork` in the Python SDK). The raw
API is typed: `LatestState`/`GetState` return `*StateSnapshot{Cursor,
Version, Timestamp, State}` (nil when absent) and `ListStates` returns
`[]StateVersionInfo`.

### Transactional store (`state.Store[T]`)

```go
import "github.com/BViganotti/actae-go/state"

s, _ := state.New(client, "counter-chan", func(cur *counter) (state.Command, error) {
    cur.Count++
    return state.Command{
        EventType:    "counter.incremented",
        OperationKey: "increment", // stable per logical operation
        Payload:      map[string]any{"to": cur.Count},
    }, nil
})
res, err := s.Commit(ctx) // Result[T]{Event, StateVersion, State}
```

`Commit` runs the classic optimistic-concurrency loop: load latest snapshot
→ `Mutate` → `Transition` (which publishes the event **and** the resulting
state snapshot in one server-side transaction) guarded by
`expected_version`/`expected_cursor`. `VersionConflictError` reloads and
re-applies `Mutate` (max 5 attempts); transport failures retry the exact
same transition with a **deterministic operation id** —
`actae.DeterministicOperationKey(scope, action, identity...)` derives a
stable UUIDv5 from the event type, channel and `OperationKey`, so retries
and restarts resolve server-side idempotently instead of duplicating.

Contract for `Mutate`: it must be **pure** w.r.t. its input (the conflicted
path re-runs it against a freshly loaded snapshot) and **idempotent** (same
`OperationKey` for the same logical operation even when the state already
carries its effect). Because the operation id is a digest of the exact
transition content, two `Commit`s with the same `OperationKey` and
byte-identical content are the **same logical operation** — the second
replays the original event+snapshot and does not advance the ledger; a
mutation that changes the content derives a fresh id and records a new
transition. The server enforces the same rule on the raw API: reusing an
`operation_id` with different content fails loudly with
`*actae.IdempotencyConflictError` (409 `idempotency_conflict`) instead of
replaying an old result. `Load`/`LoadVersion` read snapshots back into `T`
(missing versions → 404 `*actae.APIError`).

## AgentSession

The flagship: a named channel that records a full agent run with
user-facing **step numbers** (not raw cursors), optional state snapshots
every N steps, forks at any step, and crash recovery.

```go
sess, _ := actae.NewAgentSession(client, "exp-v1", actae.AgentSessionOptions{
    DisplayName:      "my experiment",
    SnapshotInterval: 1, // save state every step
    StateFn:          func() map[string]any { return map[string]any{"mem": agent.memory} },
})
_ = sess.Start(ctx)
_, _ = sess.Step(ctx, "inference", actae.StepOptions{Input: "q", Output: "a"})
_ = sess.Complete(ctx)          // or sess.Crash(ctx, "why")
```

- **Lifecycle**: `created → started → stepping → completed | crashed`.
  `Step` records one event per call with `step_number` in metadata and
  tracks `Cursors()` (index == step − 1). Steps retry transient failures
  automatically — and each retry is idempotent (per-step `operation_id`),
  so a lost response can never double-record. The per-step id is
  **deterministic** — `DeterministicOperationKey("agent-session", stepType,
  channelID, stepNumber, CanonicalStepContent(payload, metadata))`, a
  UUIDv5 byte-identical across the Go/Python/TS SDKs — so a crash-recovery
  re-drive of the same step (same step_number, same content) replays
  server-side instead of duplicating; changed content records anew.
  NaN/Infinity fail loudly (json.Marshal rejects them).
- **Fork at a step**: `fork, _ := sess.Fork(ctx, 2, "exp-v2", actae.ForkSessionOptions{})`
  returns an *unstarted* session — call `Start()` on it to begin recording
  on the fork channel.
- **Resume**: `actae.Resume(ctx, client, channelID, opts)` (or the
  equivalent `sess.Resume(ctx, opts)` method) handles two cases:
  - `ForkAtStep: nil` — crash recovery in place (fails with
    `SessionCompletedError` if the session already completed).
  - `ForkAtStep: &2` + `Name` — fork-from-existing: forks the source at
    step 2 into a fresh channel and resumes there.
- Fork boundaries fall back gracefully: if no state snapshot exists exactly
  at the fork step, the fork uses the latest state (`at_cursor=0`) — so
  event-only sessions still fork and resume. Step→cursor resolution outside
  a live session pages through the replay fallback (handles channels with
  >1000 events; system `fork.started` markers are excluded).
- **Intervention + tool policies**: `ForkSessionOptions{Intervention,
  ToolPolicies}` (and `ResumeOptions`) store an opaque intervention descriptor
  (the application applies it via `session.Intervention()`) and a child
  side-effect policy (`replay`/`block`/`live`/`auto`, default `auto`) so a
  fork cannot silently re-fire an inherited effect. A blocked call returns a
  `ForkToolBlockedError`; `session.ToolPolicies()` /
  `forkReceipt.ToolPolicies` echo the policy.
- **Hard-kill crash recovery**: `Resume` reconstructs true step progress from
  the server step index (`LatestStepNumber`, O(1)) and the event log, so a
  process killed with `SIGKILL`/`os.Exit` resumes at the real step instead of
  re-running the prefix.

## Forks

Forking copies a channel's state at a cursor into a new channel and records
the full immutable provenance (`parent_channel_id`, `forked_at_cursor` =
the **resolved** boundary, `requested_at_cursor`, `resolved_state_cursor`,
`source_state_version`, `source_state_sha256`, `restorable`, `manifest`,
`reproducibility`).

```go
receipt, _ := client.Fork(ctx, "my-channel", "exp/v2", 0, actae.ForkOptions{
    DisplayName: actae.Ptr("v2"), Reason: actae.Ptr("try harder"),
})
// receipt.ResolvedCursor / RequestedCursor / SourceStateSHA256 / Restorable
tree, _ := client.GetForkTree(ctx, "my-channel") // recursive ForkInfo
receipt2, _ := client.GetForkReceipt(ctx, "exp/v2")   // immutable receipt
```

- `at_cursor=0` forks from the latest saved state; `at_cursor>0` forks from
  the snapshot nearest at-or-before that cursor (`SnapshotBoundaryError` when
  none exists).
- **Standard idempotency**: identical replay (same `OperationID`) returns the
  original child; request drift → `IdempotencyConflictError`; child-id reuse
  under a different definition → `ChannelConflictError`.
- **Strict boundary policy**: `AgentSession.Fork` / `Resume(ForkAtStep)`
  default to `BoundaryMode="exact"` and return `NoRestorableCheckpointError`
  when no snapshot exists at the step — never silently falling forward.
  `"approximate"` / `"lineage_only"` are explicit opt-ins.
- **Optimistic guards**: `ForkOptions.ExpectedVersion` / `ExpectedCursor`.
- **Manifest**: `ForkOptions.Manifest` (auto-merged with an environment
  fingerprint; `context_exact` grading requires model+environment+deps+seed).

**Counterfactual debugging + audit** (see `docs/COUNTERFACTUAL.md`):

```go
// Prove which fork fixed a failure by diffing their final states.
diff, _ := client.DiffStates(ctx, "fix-a", "fix-b")
for _, e := range diff.Entries {
    log.Printf("%s %v → %v", e.Kind, e.Path, e.Right)
}
// diff.Common (shared ancestor state) and the per-side divergence
// diff.LeftDivergedAtCursor / diff.RightDivergedAtCursor show where the
// forks split; diff.Truncated/EntryCountTotal report honest truncation.

// Lineage as audit: chain back to the root run + tool-execution ledger.
trail, _ := client.DecisionTrail(ctx, "fix-a")
for _, hop := range trail.Ancestry { log.Printf("%s forked@%d", hop.ChannelID, hop.ForkedAtCursor) }
```

- `DiffStates(ctx, left, right)` → `*StateDiff` with `Entries` (`Kind` is
  `added`/`removed`/`changed`), `Common`, `LeftDivergedAtCursor`,
  `RightDivergedAtCursor`, `Truncated`, `EntryCountTotal`.
- `DecisionTrail(ctx, channel)` → `*DecisionTrail` with `Ancestry`,
  `Boundary` (the immutable fork-time boundary), `Executions`, `Truncated`.
- **Experiments**: `CreateExperiment`, `ListExperiments`,
  `AddExperimentMember`, `RankExperiment`, `SetOutcome`, `PromoteChannel`,
  `DeleteChannel`, `CompareChannels`, `ResolveStep`, `LatestStepNumber`.

## Consumer groups

Durable at-least-once work queues over a channel: multiple consumers claim
batches, ack progress, and renew leases.

```go
_, _ = client.CreateGroup(ctx, "workers", "my-channel", nil)
_, _ = client.JoinGroup(ctx, "workers", "worker-1", 60) // lease seconds
work, _ := client.ClaimWork(ctx, "workers", "worker-1", 100)
var ackCursor int64
for _, ev := range work.Events {
    /* process */
    if ev.ChannelCursor != nil {
        ackCursor = *ev.ChannelCursor // watermark: highest processed channel cursor
    }
}
_ = client.AckWork(ctx, "workers", "worker-1", ackCursor) // advance offset
_ = client.Heartbeat(ctx, "workers", "worker-1", 60)      // extend lease
```

- The flow is always **join → claim → ack** (heartbeat to extend long runs).
- **Ack is a watermark on the per-channel cursor** (`Event.Cursor`) — always
  ack the highest contiguously processed cursor; acking higher skips
  intervening events permanently.
- A stale/expired lease raises `ConsumerError` (HTTP 409) — re-join and
  reclaim. Offsets are per-consumer (`GroupOffsets`).

## Wake-ups

```go
w, _ := client.ScheduleWakeup(ctx, "my-channel", "2026-08-06T09:00:00Z", map[string]any{"todo": 1})
// At run_at the server fires a scheduler.wakeup event on the channel.
cancelled, err := client.CancelWakeup(ctx, w.ID)
if errors.Is(err, actae.ErrWakeupAlreadyFired) { /* already fired — no-op */ }
```

`CancelWakeup` returns `(true, nil)` on success and
`(false, actae.ErrWakeupAlreadyFired)` when the wake-up already fired,
failed, or was cancelled — treat that as benign, not an error.

### Durable human approval

```go
requestID, _ := client.RequestApproval(ctx, "ch", actae.RequestApprovalOptions{
    Summary: "Send the contract", TimeoutSeconds: actae.Ptr(86400.0),
})
client.DecideApproval(ctx, "ch", requestID, actae.DecideApprovalOptions{Decision: "approved", Actor: "alice"})
event, _ := client.WaitForApproval(ctx, "ch", requestID, time.Hour, time.Second, 0)
```
Records `approval.requested` / `approval.decided` / `approval.expired`; the
agent process need not stay alive and Actae never resumes anything itself. See
`docs/HUMAN_APPROVAL.md`.

## OpenTelemetry bridge

Mirror a channel's durable events into OTel spans (GenAI semantic conventions
plus `actae.*` identity); attach `actae.TraceContext(traceID, spanID)` as event
metadata to correlate with an external trace. `tracer` is any `TracerStarter`
(a real `trace.Tracer` satisfies it).

```go
spans, err := client.ExportChannelToOTel(ctx, "run-123", tracer, actae.ReplayOptions{})
bridge := actae.NewOTelBridge(tracer)
span := bridge.ExportEvent(ctx, event)
```
See `docs/OBSERVABILITY_INTEROP.md`.

## Tool executions

Idempotent tool-call ledger: the idempotency unit is
`(channel_id, key_name)`. A completed execution with a matching request
hash replays its persisted result; a mismatched one raises
`IdempotencyKeyMismatchError`.

```go
claim, _ := client.ClaimExecution(ctx, "my-channel", "refactor", "run_tool",
    map[string]any{"file": "x.go"}, actae.ClaimExecutionOptions{DedupFields: []string{"file"}})
if claim.Status == "replayed" { /* use claim.Result, skip the work */ }
done, _ := client.CompleteExecution(ctx, claim.Execution.ID, claim.ClaimToken, map[string]any{"ok": true})
```

Lifecycle: `claim → complete | fail | cancel`, with `HeartbeatExecution`
extending the lease. A stale claim token raises `ExecutionNotOwnedError`.
Actae never executes your tools — it coordinates the claim so exactly one
caller runs them (`docs/TOOL_EXECUTIONS.md`).

## Copilot

The root package also exposes the cross-SDK framework contract:
`RunContext`, `NewCheckpointEnvelope`, `EmbedCheckpointMetadata`,
`NewToolExecutor`, and `CopilotCapabilities`. This gives Copilot integrations
the same Actae surface and checkpoint schema as Python and TypeScript. See
[`docs/FRAMEWORK_ADAPTER_CONTRACT.md`](../../docs/FRAMEWORK_ADAPTER_CONTRACT.md).

`actae/copilot` records every GitHub Copilot SDK session — user messages,
assistant turns, tool calls, errors, sub-agents — as Actae events, and
supports forking a session at any event. Full reference:
[docs/GOLANG_SDK.md](../../docs/GOLANG_SDK.md).

```go
mgr := copilot.NewManager(db, copilot.ManagerOptions{})
defer mgr.StopAll()
handle, _ := mgr.StartSession(ctx, cli, &copilotsdk.SessionConfig{})
_, _ = handle.Session.SendPromptAndWait(ctx, "Refactor the parser")
newChannel, _ := mgr.Fork(ctx, handle.Session.SessionID, eventID,
    copilot.ForkOptions{NewChannelID: "experiment/refactor-v2"})
```

## Auth & health

`Signup`/`Login`/`Logout`/`GetMe` manage dashboard users (JWT); `HealthCheck`/
`ReadinessCheck`/`GetMetricsText`/`GetMetricsJSON` are ops endpoints. For
server-side API-key auth you only need `NewClient` with an `APIKey`.

---

# Reference

## Package layout

| File | Contents |
|------|----------|
| `actae.go` | Package doc; `SessionStarted` / `SessionCompleted` event types; `Ptr` helper |
| `client.go` | `ClientOptions`, `NewClient`, `NewClientFromEnv`, full HTTP API (see table below) |
| `client_ws.go` | WebSocket: `Connect`/`Disconnect`, `Subscribe`/`SubscribeAndWait`/`Unsubscribe`/`Publish`/`Stream`, callbacks, auto-reconnect, keepalive |
| `errors.go` | Typed errors (all implement `ActaeError`; use `errors.As`) + `ErrWakeupAlreadyFired` sentinel |
| `sonic.go` | sonic-rs `{"$sonic_rs::private::JsonNumber": "…"}` unwrapping + `As*` payload accessors |
| `types.go` | `Event`, `ChannelMetadata`, `ForkInfo`, `TransitionResult`, `GroupInfo`, `GroupOffset`, `ClaimedWork`, `Wakeup`, `ExecutionInfo`, `ExecutionClaim`, health/metrics/auth types |
| `session.go` | `AgentSession` (step/fork/resume/crash recovery, retry, snapshots) + `Resume` + exported `CanonicalStepContent` (Go-exact canonical JSON — payload-first, sorted keys — byte-mirrored by the Python/TS SDKs) |
| `state_manager.go` | `StateManager` (save/load/resume/fork/list_versions/get_version/delete_version) |
| `deterministic.go` | `DeterministicOperationKey` (stable UUIDv5 for idempotent retries) |
| `state/` | Typed transactional `state.Store[T]` (atomic event+snapshot commits, CAS loop) |
| `copilot/` | Copilot SDK integration: `Recorder`, `RecordingHooks`, `Manager` (full reference in `docs/GOLANG_SDK.md`) |
| `smoke/smoke.go` | Live smoke test against `cargo run -- --dev` |
| `examples/` | Runnable examples: quickstart, ws, session, copilot, fidelity (LLM-judged fork-vs-re-run fidelity suite — `go run ./examples/fidelity --dry-run`) |

## Client construction

`NewClient(ClientOptions{...})` — `APIKey` is required; `Endpoint` or
`WSEndpoint` required (WS is auto-derived from Endpoint). Options worth
knowing:

| Option | Default | Notes |
|--------|---------|-------|
| `Timeout` | 30s | HTTP + WS operations. Setting it does NOT change other defaults. |
| `AutoReconnect *bool` | `nil` → true | Pointer so setting `Timeout` can't flip it. |
| `MaxReconnectFailures` | 10 | Consecutive failed reconnects before giving up. |
| `KeepAliveInterval` | 30s | Negative disables client pings. |
| `EchoSelf` | false | Deliver your own publishes to local `OnMessage` callbacks. |
| `TLSConfig` / `CACert` / `ClientCert`+`ClientKey` | — | Custom CA / mTLS. Installed on both the HTTP transport and the WS dialer. |
| `HTTPClient` | shared | Overrides the underlying client; `Timeout` ignored for HTTP and TLS options NOT applied — configure its own `Transport` for TLS. |

`NewClientFromEnv(opts)` builds a client from `ACTAE_URL`/`ACTAE_WS_URL`/
`ACTAE_API_KEY` — a set env var overrides the corresponding `opts` field.

## HTTP API

All methods take `ctx` first and return `(T, error)`. Options structs are
passed as the last argument where the Python SDK uses keyword args.

| Area | Methods |
|------|---------|
| Events | `Record`, `Replay`, `Query`, `GetCursor`/`LatestCursor`, `Transition` |
| State | `SaveState`, `LatestState`, `ListStates`, `GetState`, `DeleteState` |
| Channels | `ListChannels`, `Fork`, `GetChannelMetadata` (nil on 404), `ListForks`, `GetForkTree`, `UpdateMetadata`, `DiffStates`, `DecisionTrail` |
| Health | `HealthCheck`, `ReadinessCheck`, `GetMetricsText` (Prometheus), `GetMetricsJSON` |
| Auth | `Signup`, `Login`, `Logout`, `GetMe` |
| Consumer groups | `CreateGroup`, `ListGroups`, `DeleteGroup`, `JoinGroup`, `ClaimWork`, `AckWork`, `Heartbeat`, `GroupOffsets` |
| Wakeups | `ScheduleWakeup`, `ListWakeups`, `GetWakeup`, `CancelWakeup` |
| Tool executions | `ClaimExecution`, `CompleteExecution`, `FailExecution`, `HeartbeatExecution`, `CancelExecution`, `GetExecution`, `ListExecutions`, `DeleteExecution` |

### Options structs (one per Python `*`-kwargs group)

`RecordOptions{ Actor, AgentID, UserID, Metadata, OperationID }` ·
`ReplayOptions{ Cursor, Limit, EventType }` ·
`QueryOptions{ ChannelIDs, EventType, Actor, CursorStart, CursorEnd, FromTime, ToTime, Limit, Offset }` ·
`TransitionOptions{ Actor, AgentID, UserID, Metadata, ExpectedVersion, ExpectedCursor }` ·
`SaveStateOptions{ ExpectedVersion, ExpectedCursor }` ·
`ForkOptions{ DisplayName, Reason, ExperimentMetadata, OperationID }` ·
`UpdateMetadataOptions{ DisplayName, Reason, ExperimentMetadata }` ·
`ListStatesOptions{ Limit, Offset }` ·
`ListGroupsOptions{ ChannelID }` ·
`ListWakeupsOptions{ ChannelID, Status }` ·
`SignupOptions{ Email, Password, Name }` ·
`ClaimExecutionOptions{ DedupFields, LeaseSeconds, EmitReplayEvent, Actor }` ·
`FailExecutionOptions{ ClaimToken, ErrorType, Stack }` ·
`PublishOptions{ OperationID }`

Optional scalar fields are pointers; use `actae.Ptr(v)`.

### HTTP endpoints (reference)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/events/record` | Record event |
| GET | `/api/v1/events/replay/{channel_id}` | Replay events (exclusive `cursor`, limit ≤ 1000) |
| POST | `/api/v1/events/query` | Cross-channel query (limit ≤ 1000) |
| GET | `/api/v1/events/cursor/{channel_id}` | Latest per-channel cursor |
| POST | `/api/v1/events/transition` | Atomic event + state snapshot |
| POST | `/api/v1/state/{channel_id}` | Save versioned state |
| GET | `/api/v1/state/{channel_id}` | Latest state |
| GET | `/api/v1/state/{channel_id}/versions` | State version history |
| GET | `/api/v1/state/{channel_id}/version/{version}` | Get state version |
| DELETE | `/api/v1/state/{channel_id}/version/{version}` | Delete state version |
| GET | `/api/v1/channels` | List channels |
| POST | `/api/v1/channels/fork` | Fork a channel (idempotent via `operation_id`) |
| GET | `/api/v1/channels/diff?left=&right=` | Structural state diff between two channels |
| GET | `/api/v1/channels/{id}/metadata` | Channel metadata (nil on 404) |
| PUT | `/api/v1/channels/{id}/metadata` | Partial metadata update |
| GET | `/api/v1/channels/{id}/forks` | Direct fork children |
| GET | `/api/v1/channels/{id}/fork-tree` | Recursive execution tree |
| GET | `/api/v1/channels/{id}/trail` | Decision trail (lineage + boundary + tool executions) |
| POST | `/api/v1/groups` | Create consumer group |
| GET | `/api/v1/groups` | List groups |
| DELETE | `/api/v1/groups/{group_id}` | Delete group |
| POST | `/api/v1/groups/{group_id}/join` | Join group (lease + offset) |
| POST | `/api/v1/groups/{group_id}/work` | Claim work batch |
| POST | `/api/v1/groups/{group_id}/ack` | Ack watermark (channel cursor) |
| POST | `/api/v1/groups/{group_id}/heartbeat` | Extend lease |
| GET | `/api/v1/groups/{group_id}/offsets` | Per-consumer offsets |
| POST | `/api/v1/scheduler/wakeups` | Schedule wake-up |
| GET | `/api/v1/scheduler/wakeups` | List wake-ups |
| GET | `/api/v1/scheduler/wakeups/{id}` | Get wake-up |
| DELETE | `/api/v1/scheduler/wakeups/{id}` | Cancel wake-up |
| POST | `/api/v1/executions/claim` | Claim idempotent tool execution |
| POST | `/api/v1/executions/{id}/complete` | Complete execution |
| POST | `/api/v1/executions/{id}/fail` | Fail execution |
| POST | `/api/v1/executions/{id}/heartbeat` | Extend execution lease |
| POST | `/api/v1/executions/{id}/cancel` | Cancel execution |
| GET | `/api/v1/executions/{id}` | Get execution |
| GET | `/api/v1/executions` | List executions (limit ≤ 100) |
| DELETE | `/api/v1/executions/{id}` | Delete execution |
| GET | `/healthz` `/readyz` `/metrics` `/metrics.json` | Health, readiness, metrics |
| POST | `/api/v1/auth/signup` `/login` `/logout` | User auth (JWT) |
| GET | `/api/v1/auth/me` | Current user |

## Error handling

All SDK errors implement `ActaeError` and are created via `New*` constructors.
Use `errors.As` to inspect:

```go
_, err := client.Record(ctx, "c", "t", p, actae.RecordOptions{Actor: "a"})
var apiErr *actae.APIError
if errors.As(err, &apiErr) && apiErr.StatusCode == 429 { /* back off */ }
```

| Error | Raised for |
|-------|-----------|
| `AuthError` | WS authentication rejected/timed out, HTTP 401 (wrong/expired credentials) |
| `ConnectionError` | WS handshake/drop, subscription ack timeout, not connected, HTTP transport failures (server down) |
| `APIError{StatusCode}` | Any HTTP non-2xx not otherwise classified |
| `RateLimitError{RetryAfterSeconds}` | HTTP 429 |
| `SnapshotBoundaryError` | HTTP 409 `snapshot_boundary_required` |
| `VersionConflictError` | HTTP 409 `version_conflict` |
| `ConsumerError` | HTTP 409 `consumer_not_found` |
| `IdempotencyKeyMismatchError` | HTTP 409 `idempotency_key_mismatch` |
| `ExecutionNotOwnedError` | HTTP 409 `execution_not_owned` |
| `ExecutionNotFoundError` | HTTP 404 `execution_not_found` |
| `ForkToolBlockedError` | HTTP 409 `fork_tool_blocked` — a forked channel's tool policy refused a call |
| `SessionError` / `SessionCompletedError` | `AgentSession` lifecycle violations |
| `ErrWakeupAlreadyFired` (sentinel) | `CancelWakeup` on an already-fired/cancelled wake-up (use `errors.Is`) |

HTTP transport errors (connection refused, TLS failure) are wrapped in
`ConnectionError` so the AgentSession retry logic and your error handling
cover server-down uniformly. Context cancellation/deadline pass through
unwrapped so `errors.Is(err, context.DeadlineExceeded)` works.

## WebSocket semantics (reference)

- **Wire format**: the server serializes externally-tagged enums
  (`{"Broadcast": {…}}`); the SDK normalizes them to flat
  `{"type": "broadcast", …}` (`normalizeMsg`, mirrors the Python SDK).
- **Auth**: `{"type": "auth", "api_key": …}`; `Connect` blocks until the
  `authenticated` connection event or `Timeout` (→ `AuthError`).
- **Publish**: sends `{"type": "broadcast", …}` and blocks for the Ack,
  returning the persisted `Event` (no HTTP round-trip). Timeout → `APIError(500)`.
  Retries are safe: pass `PublishOptions{OperationID: actae.Ptr("stable-uuid")}`
  and a retried publish with the same `(topic, operation_id)` replays the
  original event instead of duplicating.
- **Subscribe**: `wait=true` (or `SubscribeAndWait`) blocks until the
  `subscribed` confirmation (10s, `subscribeAckTimeout` package var) or
  `ConnectionError`. Use `Stream(ctx, topic, cursor)` for a channel-based
  event iterator (Python `stream()` parity) — it ends on disconnect.
- **Keepalive**: JSON `{"type": "connection", "event": "ping"}` every
  `KeepAliveInterval` (0 → 30s default; negative disables). Server replies
  `pong` which dispatch ignores.
- **Auto-reconnect**: exponential backoff 0.5s → 30s, `MaxReconnectFailures`
  (default 10) attempts, resubscribes all topics with their last cursors,
  fires `OnReconnect`. `Disconnect` permanently disables it (Python parity).
- **Callbacks**: `OnMessage` accumulates (all registered callbacks receive
  every broadcast, including `EchoSelf` echoes); `OnError`, `OnSubscribed`,
  `OnDisconnected`, `OnReconnect` are single-slot (later registration
  replaces). Panics inside callbacks are recovered and logged.
- **Numbers**: server JSON is decoded with `json.Decoder.UseNumber()` so
  integers arrive as `int64` (not `float64`) and sonic-rs wrappers are
  unwrapped — consistent with the HTTP path.

## Gotchas FAQ

1. **My publish never arrives at my own OnMessage.** The server does not
   echo broadcasts to the publishing connection. Use two clients, or
   `EchoSelf: true` (client-side local delivery).
2. **`{1: 1}` became `1.0`/`int64` mismatches.** JSON numbers decode as
   `int64` when integral and `float64` otherwise. Use `actae.AsInt64` etc.,
   or `json.Marshal` the payload into your own struct.
3. **"Not connected" on Subscribe/Publish.** Call `Connect(ctx)` first and
   check the error — `Subscribe` before connect returns `ConnectionError`.
4. **`AuthError` on Connect.** Wrong API key, or `ACTAE_API_KEY` missing
   (NewClient validates this up front; NewClientFromEnv too).
5. **Auto-reconnect stopped working.** Only if you explicitly passed
   `AutoReconnect: actae.Ptr(false)` — it defaults to on regardless of
   `Timeout`.
6. **`CancelWakeup` returns `(false, …)`.** That's
   `ErrWakeupAlreadyFired` — the wake-up already fired/failed/cancelled;
   nothing to do.
7. **Server-down errors during Step look different.** They're
   `ConnectionError` now (wrapped) and retried by `AgentSession`; context
   deadline errors pass through as `context.DeadlineExceeded`.
8. **`GetChannelMetadata` returns `nil, nil` on 404** and `GetWakeup`
   likewise — check for nil, not error.
9. **Retrying `Record` can duplicate.** Use `RecordOptions.OperationID`
   (stable UUID per logical attempt) — the server replays the original
   event. `AgentSession.Step` does this automatically.
10. **"channel" vs "topic".** Same concept; HTTP calls it channel, the WS
    layer calls it topic.

## Testing

```bash
go build ./... && go vet ./... && gofmt -l .
go test ./...                    # unit tests (no server required)
go test -race ./...              # race detector
go run ./smoke/smoke.go          # live smoke vs dev Actae (cargo run -- --dev)
go run ./examples/quickstart     # live quickstart, prints a walkthrough
```

Unit test coverage mirrors the Python SDK suites: `sonic_test.go`,
`errors_test.go`, `client_test.go` (httptest fake covering every HTTP
method + error mapping + API key header), `client_ws_test.go` (real
gorilla/websocket test server speaking the Actae protocol: auth, subscribe
wait, publish ack, broadcast delivery in all three shapes, error frames,
keepalive, reconnect+resubscribe, panic safety, multi-callback, echo-self),
`session_test.go` (stateful in-memory fake Actae: lifecycle, snapshots,
fork, boundary fallback, resume crash-recovery/fork-from-existing, replay
fallback with monotonicity validation, retries, StateManager), and
`copilot/recorder_test.go`.
Execution-group members also provide `Subscribe`, `Unsubscribe`, and `Stream`
for cursor-replay plus live WebSocket delivery of group messages.
