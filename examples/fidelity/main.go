// Command fidelity is the Go port of examples/fork_fidelity_suite.py: an
// LLM-judged fork-vs-no-fork re-run fidelity suite for the Actae fork
// runtime.
//
// It runs five unrelated pipelines against a real Actae server; for each, a
// baseline, a full no-fork re-run, and a fork at step N. A DeepSeek judge
// (temperature 0) scores semantic equivalence of the tail outputs, and the
// suite asserts:
//
//  1. fork fidelity >= threshold             (fork output equivalent to baseline)
//  2. fork fidelity >= rerun fidelity - tolerance   (forking not worse than re-running)
//
// The inherited prefix (steps 1..N) is verified byte-identical.
//
// Usage (run from sdks/go/examples/fidelity):
//
//	go run . --dry-run                                  # plumbing, no API key
//	go run .                                            # real evidence run
//	go run . --scenarios s3-localization,s5-product-brief --repeats 2
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	actae "github.com/BViganotti/actae-go"
)

// repoRoot walks up from this source file to the repository root (the first
// ancestor containing both an `examples` and a `sdks` directory). This is
// how the default report dir is resolved relative to the repo root.
func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	srcDir := filepath.Dir(file)
	if !filepath.IsAbs(srcDir) {
		if wd, err := os.Getwd(); err == nil {
			srcDir = filepath.Join(wd, srcDir)
		}
	}
	d := srcDir
	for i := 0; i < 8; i++ {
		if isDir(filepath.Join(d, "examples")) && isDir(filepath.Join(d, "sdks")) {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	return ""
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// deepseekEnvCandidates returns the candidate paths for actae/.env (the
// repo-root-derived one first, then the canonical absolute path).
func deepseekEnvCandidates() []string {
	candidates := []string{}
	if root := repoRoot(); root != "" {
		candidates = append(candidates, filepath.Join(root, "actae", ".env"))
	}
	candidates = append(candidates, "/Users/bviga/Developement/actae/actae/.env")
	return candidates
}

// resolveDeepseekKey mirrors _load_deepseek_key: env first, then actae/.env.
func resolveDeepseekKey() (string, error) {
	if k := os.Getenv("DEEPSEEK_API_KEY"); k != "" {
		return k, nil
	}
	for _, p := range deepseekEnvCandidates() {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "DEEPSEEK_API_KEY=") {
				v := strings.TrimSpace(strings.SplitN(line, "=", 2)[1])
				v = strings.Trim(v, `"'`)
				if v != "" {
					return v, nil
				}
			}
		}
	}
	return "", fmt.Errorf("DEEPSEEK_API_KEY not found. Set it in actae/.env or the environment (or run with --dry-run, which needs no API key).")
}

// pyFloat renders a float the way Python's str() does (always shows a
// fractional part), for the summary table.
func pyFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// summary replicates the Python _summary table.
func summary(report *suiteReport) string {
	var b strings.Builder
	b.WriteString("Fidelity suite — LLM-judged fork vs no-fork re-run\n")
	repeats := 0
	if len(report.scenarios) > 0 {
		repeats = len(report.scenarios[0].repeats)
	}
	fmt.Fprintf(&b, "generated_at: %s   pipeline_model: %s   judge_model: %s (temp %s, reps %d)\n",
		report.generatedAt, report.pipelineModel, report.judgeModel, pyFloat(report.judgeTemperature), report.judgeReps)
	fmt.Fprintf(&b, "threshold: %s   tolerance: %s   dry_run: %s   repeats: %d\n\n",
		pyFloat(report.threshold), pyFloat(report.tolerance), pyBool(report.dryRun), repeats)

	for _, r := range report.scenarios {
		status := "PASS"
		if !r.passed {
			status = "FAIL"
		}
		per := make([]string, 0, len(r.repeats))
		for _, row := range r.repeats {
			f, _ := row["fork_fidelity"].(float64)
			rr, _ := row["rerun_fidelity"].(float64)
			per = append(per, fmt.Sprintf("fork=%.2f/rerun=%.2f", f, rr))
		}
		fmt.Fprintf(&b, "  [%s] %-22s mean fork=%4.1f mean rerun=%4.1f prefix_identical=%t\n",
			status, r.sc.Name, r.fork.Score, r.rerun.Score, r.prefixOK)
		fmt.Fprintf(&b, "          per-repeat: %s\n", strings.Join(per, ", "))
		if r.failReason != nil {
			fmt.Fprintf(&b, "          reason: %s\n", *r.failReason)
		}
		fmt.Fprintf(&b, "          judge: %s — %s\n", r.fork.Verdict, clip(r.fork.Rationale, 140))
	}
	b.WriteString("\n")
	d := report.toDict().Overall
	fmt.Fprintf(&b, "  OVERALL: %s  %d/%d scenarios  mean fork=%s  mean rerun=%s  worst fork=%s  calls=%d tokens=%d\n",
		map[bool]string{true: "PASS", false: "FAIL"}[report.passed],
		d.Passed, d.Scenarios,
		pyFloat(d.MeanForkFidelity), pyFloat(d.MeanRerunFidelity), pyFloat(d.WorstForkFidelity),
		d.TotalLLMCalls, d.TotalTokens)
	return b.String()
}

// writeReport writes fork-fidelity-go-<run-id>.json and latest-go.json,
// replicating the Python _write_report (indent 2, ensure_ascii=False).
func writeReport(report *suiteReport, reportDir string, runID string) (string, error) {
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report.toDict()); err != nil {
		return "", err
	}
	path := filepath.Join(reportDir, fmt.Sprintf("fork-fidelity-go-%s.json", runID))
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(reportDir, "latest-go.json"), buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func main() {
	defaultModel := os.Getenv("DEEPSEEK_MODEL")
	if defaultModel == "" {
		defaultModel = "deepseek-chat"
	}
	defaultReportDir := filepath.Join(repoRoot(), "examples", "fidelity-reports")

	dryRun := flag.Bool("dry-run", false, "deterministic fake pipeline + judge; no API key (plumbing only)")
	scenariosFlag := flag.String("scenarios", "", "comma-separated scenario names (default: all)")
	model := flag.String("model", defaultModel, "pipeline model (default: deepseek-chat)")
	judgeModel := flag.String("judge-model", defaultModel, "judge model (default: deepseek-chat)")
	judgeReps := flag.Int("judge-reps", 2, "judge samples averaged per comparison (default 2)")
	repeats := flag.Int("repeats", 1, "independent scenario runs averaged per scenario (default 1; 3+ for marketing evidence)")
	threshold := flag.Float64("threshold", 7.0, "min fork fidelity to pass (default 7.0)")
	tolerance := flag.Float64("tolerance", 1.5, "max gap fork below re-run to pass (default 1.5)")
	reportDir := flag.String("report-dir", defaultReportDir, "directory for JSON evidence reports")
	runID := flag.String("run-id", "", "stable run id (default: timestamp)")
	flag.Parse()

	if *judgeReps < 1 || *repeats < 1 {
		fmt.Fprintln(os.Stderr, "--judge-reps and --repeats must be >= 1")
		os.Exit(1)
	}

	scenarios := allScenarios
	if *scenariosFlag != "" {
		names := []string{}
		for _, n := range strings.Split(*scenariosFlag, ",") {
			if t := strings.TrimSpace(n); t != "" {
				names = append(names, t)
			}
		}
		selected, err := scenariosByNames(names)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		scenarios = selected
	}

	var chat *chatClient
	if !*dryRun {
		key, err := resolveDeepseekKey()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		chat = newChatClient(key)
	}

	apiKey := os.Getenv("ACTAE_API_KEY")
	if apiKey == "" {
		apiKey = "sk-dev-0000000000000000000000"
	}
	url := os.Getenv("ACTAE_URL")
	if url == "" {
		url = "http://localhost:8002"
	}
	client, err := actae.NewClient(actae.ClientOptions{APIKey: apiKey, Endpoint: url})
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewClient: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	report, err := evaluateSuite(ctx, client, scenarios, chat, *model, *judgeModel, *dryRun, *judgeReps, *repeats, *threshold, *tolerance)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fidelity suite failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(summary(report))

	id := *runID
	if id == "" {
		id = strings.ReplaceAll(report.generatedAt, ":", "-")
		id = strings.ReplaceAll(id, "T", "-")
		if len(id) > 19 {
			id = id[:19]
		}
	}
	path, err := writeReport(report, *reportDir, id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\n  report: %s\n", path)

	if !report.passed {
		os.Exit(1)
	}
}
