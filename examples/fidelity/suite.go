// Suite driver — port of the evaluate/run machinery from fork_fidelity_lib.py.
//
// For each scenario: run a baseline, a full no-fork re-run, and a fork at
// step N (Go AgentSession/Resume), verify fork provenance, byte-compare the
// inherited prefix, then judge the tail outputs (fork_fidelity vs the
// rerun_fidelity control). `repeats` independent runs per scenario are
// averaged; pass/fail is decided on the MEAN.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	actae "github.com/BViganotti/actae-go"
)

// ---------------------------------------------------------------------------
// Pipeline execution (baseline / re-run / fork)
// ---------------------------------------------------------------------------

const defaultDrySystem = "dry-run deterministic step"

// stepRegistry mirrors fork_token_report.StepRegistry — additive per-step
// sums of calls and tokens.
type stepRegistry struct {
	order      []string
	calls      map[string]int
	prompt     map[string]int
	completion map[string]int
}

func newStepRegistry() *stepRegistry {
	return &stepRegistry{
		calls:      map[string]int{},
		prompt:     map[string]int{},
		completion: map[string]int{},
	}
}

func (r *stepRegistry) record(step string, promptTokens, completionTokens int) {
	if _, ok := r.prompt[step]; !ok {
		r.order = append(r.order, step)
	}
	r.calls[step]++
	r.prompt[step] += promptTokens
	r.completion[step] += completionTokens
}

func (r *stepRegistry) stepTokens(step string) int {
	return r.prompt[step] + r.completion[step]
}

func (r *stepRegistry) totalCalls() int {
	total := 0
	for _, n := range r.calls {
		total += n
	}
	return total
}

func (r *stepRegistry) totalTokens() int {
	total := 0
	for _, step := range r.order {
		total += r.stepTokens(step)
	}
	return total
}

// llmText is one LLM call. Real mode calls the model; dry-run returns a
// deterministic token budget. Returns (content, prompt_tokens,
// completion_tokens).
func llmText(ctx context.Context, chat *chatClient, model, stepKey, system, user string, temperature float64, maxTokens int, dryRun bool) (string, int, int, error) {
	if dryRun {
		content := fmt.Sprintf("[dry-run:%s] %s", stepKey, clip(system, 80))
		return content, 60 + len(user)/4, 20 + len(content)/4, nil
	}
	content, usage, err := chat.complete(ctx, model, []chatMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}, temperature, maxTokens)
	if err != nil {
		return "", 0, 0, err
	}
	return strings.TrimSpace(content), usage.PromptTokens, usage.CompletionTokens, nil
}

func copyLive(live map[string]any) map[string]any {
	out := make(map[string]any, len(live))
	for k, v := range live {
		out[k] = v
	}
	return out
}

func mergeLive(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

type pipelineResult struct {
	state    map[string]any
	registry *stepRegistry
	trace    []map[string]any
}

// runSteps runs scenario steps fromIdx..end on an already-started session.
func runSteps(ctx context.Context, sess *actae.AgentSession, sc *Scenario, registry *stepRegistry, live map[string]any, chat *chatClient, model string, dryRun bool, fromIdx int, trace *[]map[string]any) error {
	for i := fromIdx; i < len(sc.Steps); i++ {
		step := sc.Steps[i]
		user := step.User(live)
		content, pt, ct, err := llmText(ctx, chat, model, step.Key, step.System, user, step.Temperature, step.MaxTokens, dryRun)
		if err != nil {
			return err
		}
		registry.record(step.Key, pt, ct)
		live[step.Key] = content
		*trace = append(*trace, map[string]any{
			"step_number": i + 1,
			"key":         step.Key,
			"system":      step.System,
			"user":        user,
			"temperature": step.Temperature,
			"max_tokens":  step.MaxTokens,
			"output":      content,
		})
		delta := map[string]any{step.Key: content}
		if _, err := sess.Step(ctx, fmt.Sprintf("step.%d.%s", i+1, step.Key), actae.StepOptions{
			Input:   user,
			Output:  map[string]any{"tokens": registry.stepTokens(step.Key)},
			Context: delta,
		}); err != nil {
			return err
		}
	}
	return nil
}

// runPipeline runs the full pipeline (baseline or re-run) on a fresh channel.
func runPipeline(ctx context.Context, c *actae.Client, sc *Scenario, channel string, chat *chatClient, model string, dryRun bool) (*pipelineResult, error) {
	registry := newStepRegistry()
	trace := []map[string]any{}
	live := initialLive(sc)
	params := map[string]any{"run": "fidelity-pipeline"}
	for k, v := range sc.Params {
		params[k] = v
	}
	sess, err := actae.NewAgentSession(c, channel, actae.AgentSessionOptions{
		DisplayName:      "Fidelity " + sc.Name,
		StateFn:          func() map[string]any { return copyLive(live) },
		SnapshotInterval: 1,
		Params:           params,
	})
	if err != nil {
		return nil, err
	}
	if err := sess.Start(ctx); err != nil {
		return nil, err
	}
	if err := runSteps(ctx, sess, sc, registry, live, chat, model, dryRun, 0, &trace); err != nil {
		return nil, err
	}
	if err := sess.Complete(ctx); err != nil {
		return nil, err
	}
	return &pipelineResult{state: copyLive(live), registry: registry, trace: trace}, nil
}

// runFork forks baseChannel at sc.ForkAtStep and runs only the tail. It
// verifies fork provenance loudly (restorable boundary, matching
// requested/resolved cursors, non-empty source-state SHA-256), mirroring the
// Python failure contract.
func runFork(ctx context.Context, c *actae.Client, sc *Scenario, baseChannel, forkChannel string, chat *chatClient, model string, dryRun bool) (*pipelineResult, map[string]any, error) {
	registry := newStepRegistry()
	trace := []map[string]any{}
	live := initialLive(sc)
	params := map[string]any{"run": "fidelity-fork"}
	for k, v := range sc.Params {
		params[k] = v
	}
	forkAtStep := sc.ForkAtStep
	sess, err := actae.Resume(ctx, c, baseChannel, actae.ResumeOptions{
		ForkAtStep: &forkAtStep,
		Name:       forkChannel,
		StateFn:    func() map[string]any { return copyLive(live) },
		Params:     params,
	})
	if err != nil {
		return nil, nil, err
	}

	inherited := sess.InheritedState()
	if inherited == nil {
		inherited = map[string]any{}
	}
	provenance := map[string]any{
		"boundary_restorable":       false,
		"requested_boundary_cursor": sess.RequestedBoundaryCursor(),
		"resolved_boundary_cursor":  sess.ResolvedBoundaryCursor(),
		"source_state_version":      sess.SourceStateVersion(),
		"source_state_sha256":       sess.SourceStateSHA256(),
		"reproducibility":           sess.Reproducibility(),
		"inherited_state":           inherited,
	}
	if br := sess.BoundaryRestorable(); br != nil {
		provenance["boundary_restorable"] = *br
	}
	if br := sess.BoundaryRestorable(); br == nil || !*br {
		return nil, nil, fmt.Errorf("%s: fork boundary is not restorable", sc.Name)
	}
	if sess.RequestedBoundaryCursor() != sess.ResolvedBoundaryCursor() {
		return nil, nil, fmt.Errorf("%s: requested cursor %d resolved to %d",
			sc.Name, sess.RequestedBoundaryCursor(), sess.ResolvedBoundaryCursor())
	}
	if sess.SourceStateSHA256() == "" {
		return nil, nil, fmt.Errorf("%s: fork receipt has no source-state fingerprint", sc.Name)
	}

	mergeLive(live, inherited)
	if sc.InputText != "" {
		if _, ok := live["_input"]; !ok {
			live["_input"] = sc.InputText
		}
	}

	if err := sess.Start(ctx); err != nil {
		return nil, nil, err
	}
	if err := runSteps(ctx, sess, sc, registry, live, chat, model, dryRun, sc.ForkAtStep, &trace); err != nil {
		return nil, nil, err
	}
	if err := sess.Complete(ctx); err != nil {
		return nil, nil, err
	}
	return &pipelineResult{state: copyLive(live), registry: registry, trace: trace}, provenance, nil
}

// ---------------------------------------------------------------------------
// Comparison helpers (pure)
// ---------------------------------------------------------------------------

// tailText joins the tail steps' outputs (steps fork_at_step+1..M) into one
// text — Python's tail_text.
func tailText(state map[string]any, sc *Scenario) string {
	parts := []string{}
	for _, key := range tailKeys(sc) {
		if v, ok := state[key].(string); ok && v != "" {
			parts = append(parts, fmt.Sprintf("[%s]\n%s", key, v))
		}
	}
	return strings.Join(parts, "\n\n")
}

// prefixIdentical byte-compares the inherited steps between baseline and
// fork — Python's prefix_identical.
func prefixIdentical(baseState, forkState map[string]any, sc *Scenario) (bool, []string) {
	mismatches := []string{}
	for _, key := range inheritedKeys(sc) {
		base, bOK := baseState[key].(string)
		fork, fOK := forkState[key].(string)
		if !bOK || !fOK || base != fork {
			mismatches = append(mismatches, key)
		}
	}
	return len(mismatches) == 0, mismatches
}

// ---------------------------------------------------------------------------
// Scenario evaluation + aggregation
// ---------------------------------------------------------------------------

type scenarioResult struct {
	sc               *Scenario
	prefixOK         bool
	prefixMismatches []string
	fork             fidelityComparison
	rerun            fidelityComparison
	repeats          []map[string]any
	passed           bool
	failReason       *string
	calls            int
	tokens           int
	channels         []map[string]string
}

type repeatRow struct {
	fork             fidelityComparison
	rerun            fidelityComparison
	prefixOK         bool
	prefixMismatches []string
	retried          bool
	channels         map[string]string
	observations     map[string]any
	forkProvenance   map[string]any
	calls            int
	tokens           int
}

// aggregateComparison combines judge comparisons from repeated runs into one
// (mean) comparison — Python's aggregate_comparison.
func aggregateComparison(cmps []fidelityComparison) fidelityComparison {
	var sum float64
	rationales := []string{}
	samples := []judgeSample{}
	for _, c := range cmps {
		sum += c.Score
		if c.Rationale != "" {
			rationales = append(rationales, c.Rationale)
		}
		samples = append(samples, c.Samples...)
	}
	score := sum / float64(len(cmps))
	return fidelityComparison{
		Score:     score,
		Verdict:   verdictFor(score),
		Rationale: strings.Join(rationales, " | "),
		Samples:   samples,
	}
}

// isTransient reports whether an error is worth retrying the whole scenario
// repeat — Python's _is_transient (connection drops, rate limits, 5xx, and
// SessionError after a server restart that 404s an earlier channel).
func isTransient(err error) bool {
	var connErr *actae.ConnectionError
	if errors.As(err, &connErr) {
		return true
	}
	var rateErr *actae.RateLimitError
	if errors.As(err, &rateErr) {
		return true
	}
	var apiErr *actae.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode >= 500 || apiErr.StatusCode == 429
	}
	var sessErr *actae.SessionError
	if errors.As(err, &sessErr) {
		return true
	}
	return false
}

// runOneRepeat runs baseline + re-run + fork once, plus both judge
// comparisons — Python's _run_one_repeat.
func runOneRepeat(ctx context.Context, c *actae.Client, sc *Scenario, chat *chatClient, pipelineModel string, dryRun bool, judge fidelityJudge) (repeatRow, error) {
	suffix := uuid.NewString()[:8]
	baseCh := fmt.Sprintf("fid-%s-%s", sc.Name, suffix)
	rerunCh := baseCh + "-rerun"
	forkCh := baseCh + "-fork"

	baseRes, err := runPipeline(ctx, c, sc, baseCh, chat, pipelineModel, dryRun)
	if err != nil {
		return repeatRow{}, err
	}
	rerunRes, err := runPipeline(ctx, c, sc, rerunCh, chat, pipelineModel, dryRun)
	if err != nil {
		return repeatRow{}, err
	}
	forkRes, provenance, err := runFork(ctx, c, sc, baseCh, forkCh, chat, pipelineModel, dryRun)
	if err != nil {
		return repeatRow{}, err
	}

	prefixOK, mismatches := prefixIdentical(baseRes.state, forkRes.state, sc)
	forkCmp, err := judge.score(ctx, tailText(baseRes.state, sc), tailText(forkRes.state, sc))
	if err != nil {
		return repeatRow{}, err
	}
	rerunCmp, err := judge.score(ctx, tailText(baseRes.state, sc), tailText(rerunRes.state, sc))
	if err != nil {
		return repeatRow{}, err
	}

	channels := map[string]string{"baseline": baseCh, "rerun": rerunCh, "fork": forkCh}
	return repeatRow{
		fork:             forkCmp,
		rerun:            rerunCmp,
		prefixOK:         prefixOK,
		prefixMismatches: mismatches,
		retried:          false,
		channels:         channels,
		observations: map[string]any{
			"baseline": map[string]any{"state": baseRes.state, "trace": baseRes.trace, "tail": tailText(baseRes.state, sc)},
			"rerun":    map[string]any{"state": rerunRes.state, "trace": rerunRes.trace, "tail": tailText(rerunRes.state, sc)},
			"fork":     map[string]any{"state": forkRes.state, "trace": forkRes.trace, "tail": tailText(forkRes.state, sc)},
		},
		forkProvenance: provenance,
		calls:          baseRes.registry.totalCalls() + rerunRes.registry.totalCalls() + forkRes.registry.totalCalls(),
		tokens:         baseRes.registry.totalTokens() + rerunRes.registry.totalTokens() + forkRes.registry.totalTokens(),
	}, nil
}

// scenarioPassed evaluates the per-scenario claims — Python's
// scenario_passed.
func scenarioPassed(prefixOK bool, mismatches []string, fork, rerun fidelityComparison, threshold, tolerance float64) (bool, *string) {
	if !prefixOK {
		msg := "inherited prefix mismatch: " + strings.Join(mismatches, ", ")
		return false, &msg
	}
	if fork.Score < threshold {
		msg := fmt.Sprintf("fork fidelity %.1f < threshold %.1f", fork.Score, threshold)
		return false, &msg
	}
	if fork.Score < rerun.Score-tolerance {
		msg := fmt.Sprintf("fork fidelity %.1f is > %.1f below the no-fork re-run control (%.1f)", fork.Score, tolerance, rerun.Score)
		return false, &msg
	}
	return true, nil
}

// repeatWithRetry runs one scenario repeat, retrying on transient failures —
// Python's _repeat_with_retry (retries=2 → up to 3 attempts).
func repeatWithRetry(ctx context.Context, c *actae.Client, sc *Scenario, chat *chatClient, pipelineModel string, dryRun bool, judge fidelityJudge) (repeatRow, bool, error) {
	const retries = 2
	for attempt := 0; attempt <= retries; attempt++ {
		row, err := runOneRepeat(ctx, c, sc, chat, pipelineModel, dryRun, judge)
		if err == nil {
			return row, attempt > 0, nil
		}
		if !isTransient(err) || attempt == retries {
			return repeatRow{}, false, err
		}
		secs := 10 * (1 << attempt)
		if secs > 60 {
			secs = 60
		}
		time.Sleep(time.Duration(secs) * time.Second)
	}
	return repeatRow{}, false, fmt.Errorf("unreachable: repeatWithRetry exhausted")
}

// evaluateSuite runs every scenario (baseline + re-run + fork) and judges the
// tails — Python's evaluate_suite. Pass/fail is decided on the MEAN scores
// across repeats; per-repeat rows are kept in the report.
func evaluateSuite(ctx context.Context, c *actae.Client, scenarios []*Scenario, chat *chatClient, pipelineModel, judgeModel string, dryRun bool, judgeReps, repeats int, threshold, tolerance float64) (*suiteReport, error) {
	var judge fidelityJudge
	if dryRun {
		judge = &deterministicJudge{}
	} else {
		judge = newLLMJudge(chat, judgeModel, judgeReps)
	}

	results := []*scenarioResult{}
	for _, sc := range scenarios {
		if err := validateScenario(sc); err != nil {
			return nil, err
		}
		forkCmps := []fidelityComparison{}
		rerunCmps := []fidelityComparison{}
		repeatRows := []map[string]any{}
		prefixOKAll := true
		mismatchesAll := []string{}
		channelsAll := []map[string]string{}
		totalCalls, totalTokens := 0, 0

		for i := 0; i < repeats; i++ {
			row, retried, err := repeatWithRetry(ctx, c, sc, chat, pipelineModel, dryRun, judge)
			if err != nil {
				return nil, fmt.Errorf("scenario %s repeat %d failed: %w", sc.Name, i+1, err)
			}
			prefixOKAll = prefixOKAll && row.prefixOK
			mismatchesAll = append(mismatchesAll, row.prefixMismatches...)
			channelsAll = append(channelsAll, row.channels)
			forkCmps = append(forkCmps, row.fork)
			rerunCmps = append(rerunCmps, row.rerun)
			repeatRows = append(repeatRows, map[string]any{
				"fork_fidelity":    round2(row.fork.Score),
				"rerun_fidelity":   round2(row.rerun.Score),
				"prefix_identical": row.prefixOK,
				"retried":          retried,
				"channels":         row.channels,
				"observations":     row.observations,
				"fork_provenance":  row.forkProvenance,
			})
			totalCalls += row.calls
			totalTokens += row.tokens
		}

		forkAgg := aggregateComparison(forkCmps)
		rerunAgg := aggregateComparison(rerunCmps)
		passed, failReason := scenarioPassed(prefixOKAll, mismatchesAll, forkAgg, rerunAgg, threshold, tolerance)
		results = append(results, &scenarioResult{
			sc:               sc,
			prefixOK:         prefixOKAll,
			prefixMismatches: mismatchesAll,
			fork:             forkAgg,
			rerun:            rerunAgg,
			repeats:          repeatRows,
			passed:           passed,
			failReason:       failReason,
			calls:            totalCalls,
			tokens:           totalTokens,
			channels:         channelsAll,
		})
	}

	return aggregateResults(results, threshold, tolerance, dryRun, pipelineModel, judgeModel, judgeReps), nil
}

// aggregateResults computes the suite-level report — Python's aggregate().
func aggregateResults(results []*scenarioResult, threshold, tolerance float64, dryRun bool, pipelineModel, judgeModel string, judgeReps int) *suiteReport {
	passed := []*scenarioResult{}
	forkScores := []float64{}
	rerunScores := []float64{}
	for _, r := range results {
		if r.passed {
			passed = append(passed, r)
		}
		forkScores = append(forkScores, r.fork.Score)
		rerunScores = append(rerunScores, r.rerun.Score)
	}
	if len(forkScores) == 0 {
		forkScores = []float64{0.0}
		rerunScores = []float64{0.0}
	}
	worst := forkScores[0]
	var mf, mr float64
	for _, s := range forkScores {
		if s < worst {
			worst = s
		}
		mf += s
	}
	for _, s := range rerunScores {
		mr += s
	}
	mf /= float64(len(forkScores))
	mr /= float64(len(rerunScores))
	totalCalls, totalTokens := 0, 0
	for _, r := range results {
		totalCalls += r.calls
		totalTokens += r.tokens
	}
	return &suiteReport{
		generatedAt:       time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		pipelineModel:     pipelineModel,
		judgeModel:        judgeModel,
		judgeTemperature:  0.0,
		judgeReps:         judgeReps,
		threshold:         threshold,
		tolerance:         tolerance,
		dryRun:            dryRun,
		scenarios:         results,
		passed:            len(passed) == len(results),
		passedCount:       len(passed),
		scenarioCount:     len(results),
		meanForkFidelity:  mf,
		meanRerunFidelity: mr,
		worstForkFidelity: worst,
		totalLLMCalls:     totalCalls,
		totalTokens:       totalTokens,
	}
}

// ---------------------------------------------------------------------------
// Report model (JSON schema matches Python's SuiteReport.to_dict())
// ---------------------------------------------------------------------------

type suiteReport struct {
	generatedAt       string
	pipelineModel     string
	judgeModel        string
	judgeTemperature  float64
	judgeReps         int
	threshold         float64
	tolerance         float64
	dryRun            bool
	scenarios         []*scenarioResult
	passed            bool
	passedCount       int
	scenarioCount     int
	meanForkFidelity  float64
	meanRerunFidelity float64
	worstForkFidelity float64
	totalLLMCalls     int
	totalTokens       int
}

type judgeSampleJSON struct {
	Score     int    `json:"score"`
	Verdict   string `json:"verdict"`
	Rationale string `json:"rationale"`
}

type fidelityComparisonJSON struct {
	Score     float64           `json:"score"`
	Verdict   string            `json:"verdict"`
	Rationale string            `json:"rationale"`
	Samples   []judgeSampleJSON `json:"samples"`
}

type scenarioResultJSON struct {
	Name             string                 `json:"name"`
	Description      string                 `json:"description"`
	Topic            string                 `json:"topic"`
	StepCount        int                    `json:"step_count"`
	ForkAtStep       int                    `json:"fork_at_step"`
	InheritedSteps   []string               `json:"inherited_steps"`
	TailSteps        []string               `json:"tail_steps"`
	PrefixIdentical  bool                   `json:"prefix_identical"`
	PrefixMismatches []string               `json:"prefix_mismatches"`
	ForkFidelity     fidelityComparisonJSON `json:"fork_fidelity"`
	RerunFidelity    fidelityComparisonJSON `json:"rerun_fidelity"`
	Repeats          []map[string]any       `json:"repeats"`
	Pass             bool                   `json:"pass"`
	FailReason       *string                `json:"fail_reason"`
	LLMCalls         int                    `json:"llm_calls"`
	Tokens           int                    `json:"tokens"`
	Channels         []map[string]string    `json:"channels"`
}

type overallJSON struct {
	Pass              bool    `json:"pass"`
	Passed            int     `json:"passed"`
	Scenarios         int     `json:"scenarios"`
	MeanForkFidelity  float64 `json:"mean_fork_fidelity"`
	MeanRerunFidelity float64 `json:"mean_rerun_fidelity"`
	WorstForkFidelity float64 `json:"worst_fork_fidelity"`
	TotalLLMCalls     int     `json:"total_llm_calls"`
	TotalTokens       int     `json:"total_tokens"`
}

type suiteReportJSON struct {
	GeneratedAt      string               `json:"generated_at"`
	PipelineModel    string               `json:"pipeline_model"`
	JudgeModel       string               `json:"judge_model"`
	JudgeTemperature float64              `json:"judge_temperature"`
	JudgeReps        int                  `json:"judge_reps"`
	Threshold        float64              `json:"threshold"`
	Tolerance        float64              `json:"tolerance"`
	DryRun           bool                 `json:"dry_run"`
	Scenarios        []scenarioResultJSON `json:"scenarios"`
	Overall          overallJSON          `json:"overall"`
}

func (r *scenarioResult) toDict() scenarioResultJSON {
	repeats := r.repeats
	if repeats == nil {
		repeats = []map[string]any{}
	}
	channels := r.channels
	if channels == nil {
		channels = []map[string]string{}
	}
	return scenarioResultJSON{
		Name:             r.sc.Name,
		Description:      r.sc.Description,
		Topic:            r.sc.Topic,
		StepCount:        len(r.sc.Steps),
		ForkAtStep:       r.sc.ForkAtStep,
		InheritedSteps:   inheritedKeys(r.sc),
		TailSteps:        tailKeys(r.sc),
		PrefixIdentical:  r.prefixOK,
		PrefixMismatches: r.prefixMismatches,
		ForkFidelity:     r.fork.toDict(),
		RerunFidelity:    r.rerun.toDict(),
		Repeats:          repeats,
		Pass:             r.passed,
		FailReason:       r.failReason,
		LLMCalls:         r.calls,
		Tokens:           r.tokens,
		Channels:         channels,
	}
}

func (r *suiteReport) toDict() suiteReportJSON {
	scenarios := make([]scenarioResultJSON, 0, len(r.scenarios))
	for _, s := range r.scenarios {
		scenarios = append(scenarios, s.toDict())
	}
	return suiteReportJSON{
		GeneratedAt:      r.generatedAt,
		PipelineModel:    r.pipelineModel,
		JudgeModel:       r.judgeModel,
		JudgeTemperature: r.judgeTemperature,
		JudgeReps:        r.judgeReps,
		Threshold:        r.threshold,
		Tolerance:        r.tolerance,
		DryRun:           r.dryRun,
		Scenarios:        scenarios,
		Overall: overallJSON{
			Pass:              r.passed,
			Passed:            r.passedCount,
			Scenarios:         r.scenarioCount,
			MeanForkFidelity:  round2(r.meanForkFidelity),
			MeanRerunFidelity: round2(r.meanRerunFidelity),
			WorstForkFidelity: round2(r.worstForkFidelity),
			TotalLLMCalls:     r.totalLLMCalls,
			TotalTokens:       r.totalTokens,
		},
	}
}
