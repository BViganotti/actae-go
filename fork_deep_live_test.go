package actae

// Deep fork-resume comparison & edge-case suite (live). Port of the Python
// test_fork_resume_deep.py deterministic (no-LLM) suite:
//
//   - A FULL run (steps 1-9, fresh state) vs a FORK run (fork at step 5, run
//     steps 6-9): are the results identical for the shared steps?
//   - A FULL run vs a FORK with a MODIFIED prompt at step 6: does the result
//     differ ONLY in the refined step, while steps 1-5 stay byte-identical?
//   - Every edge case: fork at step 1 / last / beyond range, fork-of-fork,
//     fork with no saved state, concurrent forks, idempotent forks,
//     non-monotonic-cursor detection, crash recovery.
//
// Uses a deterministic pipeline (no LLM) so assertions are exact.
//
// Skipped unless ACTAE_URL and ACTAE_API_KEY are set.
//
//	ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
//	    go test ./ -run TestForkDeepLive -v

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	forkDeepForkStep  = 5
	forkDeepPipelineN = 9
)

// forkDeepAllStateKeys is the ordered list of the pipeline's output keys.
var forkDeepAllStateKeys = []string{
	"s1_out", "s2_out", "s3_out", "s4_out", "s5_out",
	"s6_out", "s7_out", "s8_out", "s9_out",
}

// forkDeepRegistry counts how many times each step function was invoked.
type forkDeepRegistry struct {
	calls map[string]int
}

func newForkDeepRegistry() *forkDeepRegistry {
	return &forkDeepRegistry{calls: map[string]int{}}
}

func (r *forkDeepRegistry) mark(step string) { r.calls[step]++ }
func (r *forkDeepRegistry) count(step string) int {
	return r.calls[step]
}

// Deterministic 9-step pipeline. Each step produces a value that depends on
// all previous steps' values (so any deviation is caught). Step 6 is the
// "refined" step — its output depends on a style parameter.
func forkDeepPrev(state map[string]any, n int) string {
	var parts []string
	for i := 1; i < n; i++ {
		if v, ok := state[fmt.Sprintf("s%d_out", i)].(string); ok {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "|")
}

func forkDeepStep1(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s1")
	return map[string]any{"s1_out": "r:" + state["topic"].(string)}
}

func forkDeepStep2(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s2")
	return map[string]any{"s2_out": "i(" + forkDeepPrev(state, 2) + ")"}
}

func forkDeepStep3(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s3")
	return map[string]any{"s3_out": "sh(" + forkDeepPrev(state, 3) + ")"}
}

func forkDeepStep4(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s4")
	return map[string]any{"s4_out": "rk(" + forkDeepPrev(state, 4) + ")"}
}

func forkDeepStep5(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s5")
	return map[string]any{"s5_out": "m(" + forkDeepPrev(state, 5) + ")"}
}

func forkDeepStep6(reg *forkDeepRegistry, state map[string]any, style string) map[string]any {
	reg.mark("s6")
	return map[string]any{"s6_out": fmt.Sprintf("[%s] rec(%s)", style, forkDeepPrev(state, 6))}
}

func forkDeepStep7(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s7")
	return map[string]any{"s7_out": "sum(" + forkDeepPrev(state, 7) + ")"}
}

func forkDeepStep8(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s8")
	return map[string]any{"s8_out": "nxt(" + forkDeepPrev(state, 8) + ")"}
}

func forkDeepStep9(reg *forkDeepRegistry, state map[string]any) map[string]any {
	reg.mark("s9")
	return map[string]any{"s9_out": "fin(" + forkDeepPrev(state, 9) + ")"}
}

type forkDeepRunOptions struct {
	fromStep  int
	stopAfter int
	style     string
}

// forkDeepRun records steps [fromStep..stopAfter] on sess, evolving the live
// state map exactly like the Python `_run` helper: each step's delta is
// merged into `live` before the step is recorded, step 6's recorded input is
// the full inherited context (all live s*_out keys), and every other step's
// input is the single-step delta dict.
func forkDeepRun(t *testing.T, ctx context.Context, sess *AgentSession, reg *forkDeepRegistry, live map[string]any, opts forkDeepRunOptions) {
	t.Helper()
	from := opts.fromStep
	if from == 0 {
		from = 1
	}
	stop := opts.stopAfter
	if stop == 0 {
		stop = forkDeepPipelineN
	}
	style := opts.style
	if style == "" {
		style = "neutral"
	}
	for step := from; step <= stop; step++ {
		var delta map[string]any
		switch step {
		case 1:
			delta = forkDeepStep1(reg, live)
		case 2:
			delta = forkDeepStep2(reg, live)
		case 3:
			delta = forkDeepStep3(reg, live)
		case 4:
			delta = forkDeepStep4(reg, live)
		case 5:
			delta = forkDeepStep5(reg, live)
		case 6:
			delta = forkDeepStep6(reg, live, style)
		case 7:
			delta = forkDeepStep7(reg, live)
		case 8:
			delta = forkDeepStep8(reg, live)
		case 9:
			delta = forkDeepStep9(reg, live)
		}
		for k, v := range delta {
			live[k] = v
		}
		stepInput := delta
		if step == 6 {
			stepInput = map[string]any{}
			for _, k := range forkDeepAllStateKeys {
				if v, ok := live[k]; ok {
					stepInput[k] = v
				}
			}
		}
		if _, err := sess.Step(ctx, fmt.Sprintf("step.%d", step), StepOptions{
			Input:   stepInput,
			Output:  delta,
			Context: delta,
		}); err != nil {
			t.Fatalf("step.%d: %v", step, err)
		}
	}
}

// forkDeepCopyLive returns a shallow copy of live — the StateFn equivalent of
// Python's `dict(live)`.
func forkDeepCopyLive(live map[string]any) map[string]any {
	out := make(map[string]any, len(live))
	for k, v := range live {
		out[k] = v
	}
	return out
}

// forkDeepMerge folds src's keys into dst (no-op for a nil src).
func forkDeepMerge(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

// forkDeepStateOf returns the channel's latest saved state dict (empty when
// none saved), mirroring Python's `_state_of`.
func forkDeepStateOf(t *testing.T, ctx context.Context, c *Client, channel string) map[string]any {
	t.Helper()
	snap, err := c.LatestState(ctx, channel)
	if err != nil {
		t.Fatalf("latest_state(%s): %v", channel, err)
	}
	if snap == nil {
		return map[string]any{}
	}
	return snap.State
}

// forkDeepTypes returns the channel's event types in replay order.
func forkDeepTypes(t *testing.T, ctx context.Context, c *Client, channel string) []string {
	t.Helper()
	events, err := c.Replay(ctx, channel, ReplayOptions{Limit: 500})
	if err != nil {
		t.Fatalf("replay(%s): %v", channel, err)
	}
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.EventType)
	}
	return out
}

func forkDeepContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func forkDeepAssertStateEqual(t *testing.T, got, want map[string]any, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: key count mismatch: got %d, want %d\n  got:  %v\n  want: %v", what, len(got), len(want), got, want)
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Errorf("%s: missing key %q", what, k)
			continue
		}
		if gv != wv {
			t.Errorf("%s: key %q: got %v (%T), want %v (%T)", what, k, gv, gv, wv, wv)
		}
	}
}

// forkDeepFirstEvent returns the first event of the given type in the replay.
func forkDeepFirstEvent(t *testing.T, events []Event, typ string) Event {
	t.Helper()
	for _, e := range events {
		if e.EventType == typ {
			return e
		}
	}
	t.Fatalf("no %q event in replay", typ)
	return Event{}
}

// forkDeepEventInput reads a step event's recorded `input` dict (the step
// payload is `{"step_number": N, "input": {...}, "output": {...},
// "context": {...}}`; JSON numbers arrive as int64/float64, all pipeline
// values are strings so plain == comparisons work).
func forkDeepEventInput(t *testing.T, e Event) map[string]any {
	t.Helper()
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload is %T, want map[string]any", e.Payload)
	}
	inp, _ := payload["input"].(map[string]any)
	return inp
}

func forkDeepChan(prefix string) string {
	return fmt.Sprintf("go-fork-deep-%s-%d", prefix, time.Now().UnixNano())
}

func forkDeepSkipUnlessLive(t *testing.T) {
	t.Helper()
	if os.Getenv("ACTAE_URL") == "" || os.Getenv("ACTAE_API_KEY") == "" {
		t.Skip("ACTAE_URL/ACTAE_API_KEY not set")
	}
}

func forkDeepLiveClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	return c
}

// forkDeepNewSession builds a fresh session whose StateFn returns a copy of
// the (evolving) live map — Python's `state_fn=lambda: dict(live)`.
func forkDeepNewSession(t *testing.T, c *Client, name string, live map[string]any) *AgentSession {
	t.Helper()
	s, err := NewAgentSession(c, name, AgentSessionOptions{
		StateFn: func() map[string]any { return forkDeepCopyLive(live) },
	})
	if err != nil {
		t.Fatalf("NewAgentSession(%s): %v", name, err)
	}
	return s
}

// forkDeepRunFull runs the 9-step pipeline on a fresh session (with state
// snapshots every step) and returns the server-side final state.
func forkDeepRunFull(t *testing.T, ctx context.Context, c *Client, base string) map[string]any {
	t.Helper()
	reg := newForkDeepRegistry()
	live := map[string]any{"topic": "T"}
	s := forkDeepNewSession(t, c, base, live)
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	forkDeepRun(t, ctx, s, reg, live, forkDeepRunOptions{})
	if err := s.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return forkDeepStateOf(t, ctx, c, base)
}

// ---------------------------------------------------------------------------
// 1. FULL run vs FORK run (same prompt): identical results for shared steps.
// ---------------------------------------------------------------------------
func forkDeepFullVsFork(t *testing.T, ctx context.Context, c *Client) {
	base := forkDeepChan("cmp")
	fork := base + "-fork"

	// FULL: run all 9 steps from scratch.
	fullState := forkDeepRunFull(t, ctx, c, base)

	// FORK at step 5, run steps 6-9 with the SAME step 6 prompt.
	forkReg := newForkDeepRegistry()
	forkLive := map[string]any{"topic": "T"}
	step := forkDeepForkStep
	forkSess, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step,
		Name:       fork,
		StateFn:    func() map[string]any { return forkDeepCopyLive(forkLive) },
	})
	if err != nil {
		t.Fatalf("Resume(base, fork at %d): %v", forkDeepForkStep, err)
	}
	forkDeepMerge(forkLive, forkSess.InheritedState())
	if err := forkSess.Start(ctx); err != nil {
		t.Fatalf("fork.Start: %v", err)
	}
	forkDeepRun(t, ctx, forkSess, forkReg, forkLive, forkDeepRunOptions{fromStep: forkDeepForkStep + 1})
	if err := forkSess.Complete(ctx); err != nil {
		t.Fatalf("fork.Complete: %v", err)
	}
	forkState := forkDeepStateOf(t, ctx, c, fork)

	// (a) Steps 1-5 identical (inherited, not regenerated) — server-side.
	for i := 1; i <= forkDeepForkStep; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if forkState[k] != fullState[k] {
			t.Errorf("%s: fork %v != full %v", k, forkState[k], fullState[k])
		}
	}
	// (b) Steps 6-9 identical too (same prompt, same inputs → same outputs).
	for i := forkDeepForkStep + 1; i <= forkDeepPipelineN; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if forkState[k] != fullState[k] {
			t.Errorf("%s: fork %v != full %v", k, forkState[k], fullState[k])
		}
	}
	// (c) Full determinism: the whole state matches (the fork's own evolving
	// live state is persisted through its StateFn — Python parity).
	forkDeepAssertStateEqual(t, forkState, fullState, "fork state vs full state")
	// (d) Event logs: fork has fork.started + session.started + steps 6-9.
	forkTypes := forkDeepTypes(t, ctx, c, fork)
	if len(forkTypes) == 0 || forkTypes[0] != "fork.started" {
		t.Errorf("fork event log starts with %v, want fork.started", forkTypes)
	}
	if !forkDeepContains(forkTypes, "step.6") || !forkDeepContains(forkTypes, "step.9") {
		t.Errorf("fork event log missing step.6/step.9: %v", forkTypes)
	}
	for i := 1; i <= forkDeepForkStep; i++ {
		if forkDeepContains(forkTypes, fmt.Sprintf("step.%d", i)) {
			t.Errorf("fork re-recorded step.%d: %v", i, forkTypes)
		}
	}
	// (e) Execution counters: steps 1-5 not invoked on fork.
	for i := 1; i <= forkDeepForkStep; i++ {
		if n := forkReg.count(fmt.Sprintf("s%d", i)); n != 0 {
			t.Errorf("s%d re-invoked %d times on fork", i, n)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. FULL vs FORK with MODIFIED prompt at step 6: differs ONLY in step 6.
// ---------------------------------------------------------------------------
func forkDeepFullVsForkModified(t *testing.T, ctx context.Context, c *Client) {
	base := forkDeepChan("mod")
	fork := base + "-forkmod"

	fullState := forkDeepRunFull(t, ctx, c, base)

	forkReg := newForkDeepRegistry()
	forkLive := map[string]any{"topic": "T"}
	step := forkDeepForkStep
	forkSess, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step,
		Name:       fork,
		StateFn:    func() map[string]any { return forkDeepCopyLive(forkLive) },
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	forkDeepMerge(forkLive, forkSess.InheritedState())
	if err := forkSess.Start(ctx); err != nil {
		t.Fatalf("fork.Start: %v", err)
	}
	forkDeepRun(t, ctx, forkSess, forkReg, forkLive, forkDeepRunOptions{fromStep: forkDeepForkStep + 1, style: "aggressive"})
	if err := forkSess.Complete(ctx); err != nil {
		t.Fatalf("fork.Complete: %v", err)
	}
	forkState := forkDeepStateOf(t, ctx, c, fork)

	// Steps 1-5 identical (inherited), both on the server and in the live map.
	for i := 1; i <= forkDeepForkStep; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if forkState[k] != fullState[k] {
			t.Errorf("%s should be inherited: server %v != %v", k, forkState[k], fullState[k])
		}
		if forkLive[k] != fullState[k] {
			t.Errorf("%s should be inherited: live %v != %v", k, forkLive[k], fullState[k])
		}
	}
	// Step 6 DIFFERS (the modified prompt).
	if forkLive["s6_out"] == fullState["s6_out"] {
		t.Errorf("step 6 must differ: %v", forkLive["s6_out"])
	}
	// Steps 7-9 differ transitively (they depend on s6).
	if forkLive["s7_out"] == fullState["s7_out"] {
		t.Errorf("step 7 must differ transitively")
	}
	if forkLive["s9_out"] == fullState["s9_out"] {
		t.Errorf("step 9 must differ transitively")
	}
	// But step 6's INPUT (the inherited context) is identical to the full run.
	baseEvents, err := c.Replay(ctx, base, ReplayOptions{Limit: 500})
	if err != nil {
		t.Fatalf("replay(base): %v", err)
	}
	forkEvents, err := c.Replay(ctx, fork, ReplayOptions{Limit: 500})
	if err != nil {
		t.Fatalf("replay(fork): %v", err)
	}
	fullS6 := forkDeepFirstEvent(t, baseEvents, "step.6")
	forkS6 := forkDeepFirstEvent(t, forkEvents, "step.6")
	fullInp := forkDeepEventInput(t, fullS6)
	forkInp := forkDeepEventInput(t, forkS6)
	for i := 1; i <= forkDeepForkStep; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if forkInp[k] != fullInp[k] {
			t.Errorf("step6 input %s differs: fork %v != full %v", k, forkInp[k], fullInp[k])
		}
	}
}

// ---------------------------------------------------------------------------
// 3. Edge cases.
// ---------------------------------------------------------------------------
func forkDeepForkAtEveryStep(t *testing.T, ctx context.Context, c *Client) {
	// Fork at step 1..9; each fork must inherit the right prefix of state.
	base := forkDeepChan("edge-a")
	full := forkDeepRunFull(t, ctx, c, base)

	for at := 1; at <= forkDeepPipelineN; at++ {
		step := at
		fs, err := Resume(ctx, c, base, ResumeOptions{
			ForkAtStep: &step,
			Name:       fmt.Sprintf("%s-f%d", base, at),
			StateFn:    func() map[string]any { return map[string]any{} },
		})
		if err != nil {
			t.Fatalf("fork@%d: %v", at, err)
		}
		fstate := fs.InheritedState()
		// Fork at step N inherits steps 1..N.
		for i := 1; i <= at; i++ {
			k := fmt.Sprintf("s%d_out", i)
			if fstate[k] != full[k] {
				t.Errorf("fork@%d missing inherited %s: %v", at, k, fstate[k])
			}
		}
		// It does NOT inherit steps after N.
		for i := at + 1; i <= forkDeepPipelineN; i++ {
			k := fmt.Sprintf("s%d_out", i)
			if v := fstate[k]; v != nil {
				t.Errorf("fork@%d should not have %s: %v", at, k, v)
			}
		}
		if got := fs.StepCount(); got != at {
			t.Errorf("fork@%d step_count %d, want %d", at, got, at)
		}
	}
}

func forkDeepForkOfFork(t *testing.T, ctx context.Context, c *Client) {
	// Fork of a fork: the second fork inherits the deepest state.
	base := forkDeepChan("edge-b")
	f1 := base + "-f1"
	f2 := base + "-f2"
	full := forkDeepRunFull(t, ctx, c, base)

	// f1: fork at 3, run steps 4-9.
	var l1 map[string]any
	step3 := 3
	s1, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step3,
		Name:       f1,
		StateFn:    func() map[string]any { return forkDeepCopyLive(l1) },
	})
	if err != nil {
		t.Fatalf("Resume f1: %v", err)
	}
	l1 = forkDeepCopyLive(s1.InheritedState())
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("f1.Start: %v", err)
	}
	forkDeepRun(t, ctx, s1, newForkDeepRegistry(), l1, forkDeepRunOptions{fromStep: 4})
	if err := s1.Complete(ctx); err != nil {
		t.Fatalf("f1.Complete: %v", err)
	}

	// f2: fork f1 at 6 — the deepest state f1 has saved (its own evolving
	// live state, steps 1..9) is inherited at step 6.
	var l2 map[string]any
	step6 := 6
	s2, err := Resume(ctx, c, f1, ResumeOptions{
		ForkAtStep: &step6,
		Name:       f2,
		StateFn:    func() map[string]any { return forkDeepCopyLive(l2) },
	})
	if err != nil {
		t.Fatalf("Resume f2: %v", err)
	}
	l2 = forkDeepCopyLive(s2.InheritedState())
	if l2["s1_out"] != full["s1_out"] {
		t.Errorf("f2 s1 not inherited: %v != %v", l2["s1_out"], full["s1_out"])
	}
	if l2["s6_out"] != l1["s6_out"] {
		t.Errorf("f2 s6 not inherited from f1: %v != %v", l2["s6_out"], l1["s6_out"])
	}
	if got := s2.StepCount(); got != 6 {
		t.Errorf("f2 step_count %d, want 6", got)
	}

	if err := s2.Start(ctx); err != nil {
		t.Fatalf("f2.Start: %v", err)
	}
	forkDeepRun(t, ctx, s2, newForkDeepRegistry(), l2, forkDeepRunOptions{fromStep: 7})
	if err := s2.Complete(ctx); err != nil {
		t.Fatalf("f2.Complete: %v", err)
	}
	types := forkDeepTypes(t, ctx, c, f2)
	if len(types) == 0 || types[0] != "fork.started" {
		t.Errorf("f2 event log starts with %v, want fork.started", types)
	}
	for i := 1; i <= 6; i++ {
		if forkDeepContains(types, fmt.Sprintf("step.%d", i)) {
			t.Errorf("f2 re-recorded step.%d: %v", i, types)
		}
	}
}

func forkDeepForkWithoutStateSnapshot(t *testing.T, ctx context.Context, c *Client) {
	// Forking an event-only channel (no state_fn) is strict by default: it
	// raises NoRestorableCheckpointError; with boundary_mode='approximate' it
	// falls back to the latest state.
	base := forkDeepChan("edge-c")
	reg := newForkDeepRegistry()
	live := map[string]any{"topic": "T"}
	// No state_fn → no snapshots saved.
	s, err := NewAgentSession(c, base, AgentSessionOptions{})
	if err != nil {
		t.Fatalf("NewAgentSession: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	forkDeepRun(t, ctx, s, reg, live, forkDeepRunOptions{})
	if err := s.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Strict default: no snapshot at the boundary → clear error.
	step3 := 3
	if _, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step3,
		Name:       base + "-f",
		StateFn:    func() map[string]any { return map[string]any{} },
	}); err == nil {
		t.Fatal("expected NoRestorableCheckpointError in strict mode")
	} else if _, ok := err.(*NoRestorableCheckpointError); !ok {
		t.Fatalf("expected NoRestorableCheckpointError, got %T: %v", err, err)
	}

	// Approximate mode: falls back to the latest state instead of erroring.
	fs, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep:   &step3,
		Name:         base + "-f-approx",
		StateFn:      func() map[string]any { return map[string]any{} },
		BoundaryMode: "approximate",
	})
	if err != nil {
		t.Fatalf("approximate fork: %v", err)
	}
	if got := fs.StepCount(); got != 3 {
		t.Errorf("approximate fork step_count %d, want 3", got)
	}
}

func forkDeepConcurrentForks(t *testing.T, ctx context.Context, c *Client) {
	// Two forks created concurrently from the same base are independent.
	base := forkDeepChan("edge-d")
	forkDeepRunFull(t, ctx, c, base)

	faName := base + "-fa"
	fbName := base + "-fb"
	var a, b map[string]any
	step5 := 5
	f1, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step5,
		Name:       faName,
		StateFn:    func() map[string]any { return forkDeepCopyLive(a) },
	})
	if err != nil {
		t.Fatalf("Resume fa: %v", err)
	}
	a = forkDeepCopyLive(f1.InheritedState())
	f2, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step5,
		Name:       fbName,
		StateFn:    func() map[string]any { return forkDeepCopyLive(b) },
	})
	if err != nil {
		t.Fatalf("Resume fb: %v", err)
	}
	b = forkDeepCopyLive(f2.InheritedState())

	// Both inherited the same step-5 state independently.
	for i := 1; i <= forkDeepForkStep; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if a[k] != b[k] {
			t.Errorf("%s: %v != %v", k, a[k], b[k])
		}
	}

	// Different step-6 styles → independent evolution, server-side.
	if err := f1.Start(ctx); err != nil {
		t.Fatalf("fa.Start: %v", err)
	}
	forkDeepRun(t, ctx, f1, newForkDeepRegistry(), a, forkDeepRunOptions{fromStep: 6, style: "aggressive"})
	if err := f1.Complete(ctx); err != nil {
		t.Fatalf("fa.Complete: %v", err)
	}
	if err := f2.Start(ctx); err != nil {
		t.Fatalf("fb.Start: %v", err)
	}
	forkDeepRun(t, ctx, f2, newForkDeepRegistry(), b, forkDeepRunOptions{fromStep: 6})
	if err := f2.Complete(ctx); err != nil {
		t.Fatalf("fb.Complete: %v", err)
	}
	// Server-side: the forks' saved states diverge from step 6 but share the
	// inherited prefix.
	fa := forkDeepStateOf(t, ctx, c, faName)
	fb := forkDeepStateOf(t, ctx, c, fbName)
	if fa["s6_out"] == fb["s6_out"] {
		t.Errorf("concurrent forks must evolve independently: both %v", fa["s6_out"])
	}
	if fa["s1_out"] != fb["s1_out"] {
		t.Errorf("inherited prefix must match: %v != %v", fa["s1_out"], fb["s1_out"])
	}
}

func forkDeepIdempotentFork(t *testing.T, ctx context.Context, c *Client) {
	// Re-forking the same name is a no-op (returns the original fork).
	base := forkDeepChan("edge-e")
	forkDeepRunFull(t, ctx, c, base)
	name := base + "-idem"
	step5 := 5
	f1, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step5,
		Name:       name,
		StateFn:    func() map[string]any { return map[string]any{} },
	})
	if err != nil {
		t.Fatalf("Resume f1: %v", err)
	}
	f2, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step5,
		Name:       name,
		StateFn:    func() map[string]any { return map[string]any{} },
	})
	if err != nil {
		t.Fatalf("Resume f2: %v", err)
	}
	// Both resolve to the same channel; the server is idempotent.
	if f1.Name() != name || f2.Name() != name {
		t.Errorf("f1.Name()=%q f2.Name()=%q want %q", f1.Name(), f2.Name(), name)
	}
	if f1.ChannelID() != f2.ChannelID() || f2.ChannelID() != name {
		t.Errorf("idempotent fork resolved to different channels: %q vs %q (want %q)", f1.ChannelID(), f2.ChannelID(), name)
	}
}

func forkDeepResumeCompletedRaises(t *testing.T, ctx context.Context, c *Client) {
	base := forkDeepChan("edge-f")
	forkDeepRunFull(t, ctx, c, base)
	if _, err := Resume(ctx, c, base, ResumeOptions{}); err == nil {
		t.Fatal("expected SessionCompletedError")
	} else if _, ok := err.(*SessionCompletedError); !ok {
		t.Fatalf("expected SessionCompletedError, got %T: %v", err, err)
	}
}

func forkDeepResumeUnknownChannelRaises(t *testing.T, ctx context.Context, c *Client) {
	unknown := fmt.Sprintf("go-fork-deep-missing-%d", time.Now().UnixNano())
	if _, err := Resume(ctx, c, unknown, ResumeOptions{}); err == nil {
		t.Fatal("expected SessionError for unknown channel")
	} else if _, ok := err.(*SessionError); !ok {
		t.Fatalf("expected SessionError, got %T: %v", err, err)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Errorf("SessionError message should mention 'not found': %v", err)
	}
}

func forkDeepNestedStateConsistency(t *testing.T, ctx context.Context, c *Client) {
	// Deep state comparison: fork's inherited state == parent's snapshot at
	// the fork cursor (byte-for-byte).
	base := forkDeepChan("edge-h")
	full := forkDeepRunFull(t, ctx, c, base)

	step7 := 7
	fs, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step7,
		Name:       base + "-deep",
		StateFn:    func() map[string]any { return map[string]any{} },
	})
	if err != nil {
		t.Fatalf("Resume at 7: %v", err)
	}
	inherited := fs.InheritedState()
	// Fork at step 7 inherits steps 1..7 exactly; steps 8-9 do not exist yet.
	for i := 1; i <= 7; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if inherited[k] != full[k] {
			t.Errorf("%s: %v != %v", k, inherited[k], full[k])
		}
	}
	for i := 8; i <= forkDeepPipelineN; i++ {
		k := fmt.Sprintf("s%d_out", i)
		if v := inherited[k]; v != nil {
			t.Errorf("%s should not exist at step-7 fork: %v", k, v)
		}
	}
	if inherited["s7_out"] != full["s7_out"] {
		t.Errorf("s7_out: %v != %v", inherited["s7_out"], full["s7_out"])
	}
}

func forkDeepForkRawChannelOutsideSession(t *testing.T, ctx context.Context, c *Client) {
	// Forking a channel created outside AgentSession (raw record calls, no
	// metadata, single state snapshot at a later cursor) — exercises the
	// replay-fallback path and the strict/approximate boundary policy.
	base := forkDeepChan("edge-i")
	live := map[string]any{"topic": "T"}
	var lastCursor int64
	for i := 0; i < 5; i++ {
		ev, err := c.Record(ctx, base, fmt.Sprintf("ev.%d", i), map[string]any{"i": i}, RecordOptions{Actor: "raw"})
		if err != nil {
			t.Fatalf("record ev.%d: %v", i, err)
		}
		lastCursor = ev.Cursor
	}
	if _, err := c.SaveState(ctx, base, lastCursor, live, SaveStateOptions{}); err != nil {
		t.Fatalf("save_state: %v", err)
	}

	// The only snapshot is at cursor 5; forking at step 3 (cursor 3) has no
	// boundary snapshot → strict default raises.
	step3 := 3
	if _, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step3,
		Name:       base + "-fork",
		StateFn:    func() map[string]any { return map[string]any{} },
	}); err == nil {
		t.Fatal("expected NoRestorableCheckpointError in strict mode")
	} else if _, ok := err.(*NoRestorableCheckpointError); !ok {
		t.Fatalf("expected NoRestorableCheckpointError, got %T: %v", err, err)
	}

	// Approximate mode: falls back to the latest state (cursor 5's snapshot).
	// The fallback resolves BEYOND the requested boundary, so the SDK refuses
	// to prime with the contaminated state — inherited_state is nil.
	fs, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep:   &step3,
		Name:         base + "-fork-approx",
		StateFn:      func() map[string]any { return map[string]any{} },
		BoundaryMode: "approximate",
	})
	if err != nil {
		t.Fatalf("approximate raw-channel fork: %v", err)
	}
	if got := fs.StepCount(); got != 3 {
		t.Errorf("raw-channel fork step_count %d, want 3", got)
	}
	if fs.InheritedState() != nil {
		t.Error("raw-channel approximate fork inherited contaminated state")
	}
}

func forkDeepForkStepBeyondPipelineRaises(t *testing.T, ctx context.Context, c *Client) {
	// Forking at a step beyond the recorded steps must raise a clear error.
	base := forkDeepChan("edge-j")
	forkDeepRunFull(t, ctx, c, base) // 9 steps
	step10 := 10
	if _, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step10,
		Name:       base + "-f10",
		StateFn:    func() map[string]any { return map[string]any{} },
	}); err == nil {
		t.Fatal("expected SessionError for step beyond the pipeline")
	} else if _, ok := err.(*SessionError); !ok {
		t.Fatalf("expected SessionError, got %T: %v", err, err)
	}
}

func forkDeepSnapshotIntervalGtOneFallsBack(t *testing.T, ctx context.Context, c *Client) {
	// With snapshot_interval > 1, a fork at a step with no snapshot is strict
	// by default (raises); boundary_mode='approximate' falls back to the
	// latest state (drift surfaced, contaminated state refused).
	base := forkDeepChan("edge-k")
	reg := newForkDeepRegistry()
	live := map[string]any{"topic": "T"}
	// Save snapshots every 3 steps: steps 3, 6, 9 have snapshots.
	s, err := NewAgentSession(c, base, AgentSessionOptions{
		SnapshotInterval: 3,
		StateFn:          func() map[string]any { return forkDeepCopyLive(live) },
	})
	if err != nil {
		t.Fatalf("NewAgentSession: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	forkDeepRun(t, ctx, s, reg, live, forkDeepRunOptions{})
	if err := s.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Fork at step 2 (no snapshot at/before cursor 2) → strict raises.
	step2 := 2
	if _, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step2,
		Name:       base + "-f2",
		StateFn:    func() map[string]any { return map[string]any{} },
	}); err == nil {
		t.Fatal("expected NoRestorableCheckpointError for step-2 fork")
	} else if _, ok := err.(*NoRestorableCheckpointError); !ok {
		t.Fatalf("expected NoRestorableCheckpointError, got %T: %v", err, err)
	}

	// Approximate mode: falls back to the latest state. The fallback resolves
	// BEYOND the requested boundary → the SDK refuses the contaminated state.
	fs, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep:   &step2,
		Name:         base + "-f2-approx",
		StateFn:      func() map[string]any { return map[string]any{} },
		BoundaryMode: "approximate",
	})
	if err != nil {
		t.Fatalf("approximate step-2 fork: %v", err)
	}
	if got := fs.StepCount(); got != 2 {
		t.Errorf("interval>1 approximate fork step_count %d, want 2", got)
	}
	if fs.InheritedState() != nil {
		t.Error("interval>1 approximate fork inherited contaminated state")
	}
}

func forkDeepForkLineageOnly(t *testing.T, ctx context.Context, c *Client) {
	// boundary_mode='lineage_only' creates the fork with NO state copy —
	// the escape hatch for event-only channels. Must succeed, report
	// restorable=False, expose no inherited state, and record the parent
	// lineage in the child's channel metadata.
	base := forkDeepChan("edge-m")
	reg := newForkDeepRegistry()
	live := map[string]any{"topic": "T"}
	// Event-only channel: no state_fn → zero snapshots saved.
	s, err := NewAgentSession(c, base, AgentSessionOptions{})
	if err != nil {
		t.Fatalf("NewAgentSession: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	forkDeepRun(t, ctx, s, reg, live, forkDeepRunOptions{})
	if err := s.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	name := base + "-lo"
	step5 := 5
	fs, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep:   &step5,
		Name:         name,
		StateFn:      func() map[string]any { return map[string]any{} },
		BoundaryMode: "lineage_only",
	})
	if err != nil {
		t.Fatalf("lineage_only fork: %v", err)
	}
	if got := fs.StepCount(); got != 5 {
		t.Errorf("lineage_only fork step_count %d, want 5", got)
	}
	if r := fs.BoundaryRestorable(); r == nil || *r {
		t.Errorf("lineage_only fork must report restorable=false, got %v", r)
	}
	if got := fs.ResolvedBoundaryCursor(); got != 0 {
		t.Errorf("lineage_only fork must not copy state (cursor 0), got %d", got)
	}
	if fs.InheritedState() != nil {
		t.Error("lineage_only fork must expose no inherited state")
	}

	// The fork channel exists, is replayable, and its metadata records the
	// lineage to the parent.
	types := forkDeepTypes(t, ctx, c, fs.ChannelID())
	if len(types) == 0 {
		t.Error("lineage_only fork channel must exist and be replayable")
	}
	meta, err := c.GetChannelMetadata(ctx, fs.ChannelID())
	if err != nil {
		t.Fatalf("get_channel_metadata: %v", err)
	}
	if meta == nil {
		t.Fatal("lineage_only fork must have channel metadata")
	}
	if meta.ParentChannelID == nil || *meta.ParentChannelID != base {
		t.Errorf("lineage_only fork parent = %v, want %q", meta.ParentChannelID, base)
	}
	// Stepping the fork works and continues at step 6 (no inherited prefix).
	if err := fs.Start(ctx); err != nil {
		t.Fatalf("fork.Start: %v", err)
	}
	if _, err := fs.Step(ctx, "step.6", StepOptions{
		Input:   map[string]any{},
		Output:  map[string]any{"s6_out": "refined"},
		Context: map[string]any{},
	}); err != nil {
		t.Fatalf("step.6 on lineage_only fork: %v", err)
	}
	post := forkDeepTypes(t, ctx, c, fs.ChannelID())
	if !forkDeepContains(post, "step.6") {
		t.Errorf("lineage_only fork must record steps after the boundary: %v", post)
	}
}

func forkDeepForkOfForkAtInheritedStep(t *testing.T, ctx context.Context, c *Client) {
	// Forking a fork at an INHERITED step must resolve against the ancestor
	// that owns that step, not silently map to the fork's own first event.
	base := forkDeepChan("edge-l")
	f1 := base + "-f1"
	f2 := base + "-f2"
	full := forkDeepRunFull(t, ctx, c, base)

	// f1: fork at step 3 (inherits steps 1-3), then run steps 4-9.
	var l1 map[string]any
	step3 := 3
	s1, err := Resume(ctx, c, base, ResumeOptions{
		ForkAtStep: &step3,
		Name:       f1,
		StateFn:    func() map[string]any { return forkDeepCopyLive(l1) },
	})
	if err != nil {
		t.Fatalf("Resume f1: %v", err)
	}
	l1 = forkDeepCopyLive(s1.InheritedState())
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("f1.Start: %v", err)
	}
	forkDeepRun(t, ctx, s1, newForkDeepRegistry(), l1, forkDeepRunOptions{fromStep: 4})
	if err := s1.Complete(ctx); err != nil {
		t.Fatalf("f1.Complete: %v", err)
	}

	// f2: fork f1 at step 2 — an INHERITED step. The correct state is on the
	// root (base), and the fork boundary must be the root's step-2 cursor.
	var l2 map[string]any
	step2 := 2
	s2, err := Resume(ctx, c, f1, ResumeOptions{
		ForkAtStep: &step2,
		Name:       f2,
		StateFn:    func() map[string]any { return forkDeepCopyLive(l2) },
	})
	if err != nil {
		t.Fatalf("Resume f2: %v", err)
	}
	l2 = forkDeepCopyLive(s2.InheritedState())
	// f2 must inherit steps 1-2 from the ROOT, not f1's own first events.
	if l2["s1_out"] != full["s1_out"] {
		t.Errorf("f2 s1: %v != %v", l2["s1_out"], full["s1_out"])
	}
	if l2["s2_out"] != full["s2_out"] {
		t.Errorf("f2 s2: %v != %v", l2["s2_out"], full["s2_out"])
	}
	// It must NOT accidentally carry f1's later steps (4+) or even step 3.
	if v := l2["s3_out"]; v != nil {
		t.Errorf("f2 should not inherit s3: %v", v)
	}
	if v := l2["s4_out"]; v != nil {
		t.Errorf("f2 should not inherit s4: %v", v)
	}
	if got := s2.StepCount(); got != 2 {
		t.Errorf("f2 step_count %d, want 2", got)
	}
}

// TestForkDeepLive is the Go port of the Python test_fork_resume_deep.py
// deterministic live suite (the framework-agnostic/Claude/Codex tests in that
// file are separate concerns and are NOT ported).
func TestForkDeepLive(t *testing.T) {
	forkDeepSkipUnlessLive(t)
	c := forkDeepLiveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	t.Run("full_vs_fork", func(t *testing.T) { forkDeepFullVsFork(t, ctx, c) })
	t.Run("full_vs_fork_modified", func(t *testing.T) { forkDeepFullVsForkModified(t, ctx, c) })
	t.Run("fork_at_every_step", func(t *testing.T) { forkDeepForkAtEveryStep(t, ctx, c) })
	t.Run("fork_of_fork", func(t *testing.T) { forkDeepForkOfFork(t, ctx, c) })
	t.Run("fork_without_state_snapshot", func(t *testing.T) { forkDeepForkWithoutStateSnapshot(t, ctx, c) })
	t.Run("concurrent_forks", func(t *testing.T) { forkDeepConcurrentForks(t, ctx, c) })
	t.Run("idempotent_fork", func(t *testing.T) { forkDeepIdempotentFork(t, ctx, c) })
	t.Run("resume_completed_raises", func(t *testing.T) { forkDeepResumeCompletedRaises(t, ctx, c) })
	t.Run("resume_unknown_channel_raises", func(t *testing.T) { forkDeepResumeUnknownChannelRaises(t, ctx, c) })
	t.Run("nested_state_consistency", func(t *testing.T) { forkDeepNestedStateConsistency(t, ctx, c) })
	t.Run("fork_raw_channel_outside_session", func(t *testing.T) { forkDeepForkRawChannelOutsideSession(t, ctx, c) })
	t.Run("fork_step_beyond_pipeline_raises", func(t *testing.T) { forkDeepForkStepBeyondPipelineRaises(t, ctx, c) })
	t.Run("snapshot_interval_gt_one_fork_falls_back", func(t *testing.T) { forkDeepSnapshotIntervalGtOneFallsBack(t, ctx, c) })
	t.Run("fork_lineage_only", func(t *testing.T) { forkDeepForkLineageOnly(t, ctx, c) })
	t.Run("fork_of_fork_at_inherited_step", func(t *testing.T) { forkDeepForkOfForkAtInheritedStep(t, ctx, c) })
}
