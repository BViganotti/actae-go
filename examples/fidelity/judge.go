// Fidelity judge — the ONLY fidelity metric, mirroring fork_fidelity_lib.py.
//
// Two judges:
//   - LLMJudge: a real OpenAI-compatible chat completion against DeepSeek
//     (temperature 0), `reps` samples averaged. All sample scores, verdicts
//     and rationales are kept for the auditable report.
//   - DeterministicJudge: dry-run plumbing only — difflib char-similarity
//     (SequenceMatcher ratio) mapped onto 1-10. Never used for evidence.
//
// The DeepSeek client is stdlib-only (net/http); no new go.mod deps.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// judgeSystem is the byte-for-byte copy of JUDGE_SYSTEM from
// fork_fidelity_lib.py.
const judgeSystem = `You are an impartial fidelity judge for agent-pipeline experiments.

You compare TWO OUTPUTS of the same multi-step pipeline and score how
semantically equivalent they are. A CANDIDATE output is judged against a
REFERENCE output.

Rules:
- Judge FACTS, CONCLUSIONS and COVERAGE — never wording, style, length or
  formatting. Different phrasing, ordering or emphasis is NORMAL (the model
  samples) and must not lower the score.
- When a pipeline step is asked to PROPOSE or INVENT specific values (prices,
  targets, estimates, timelines), different proposed values for the SAME item
  are NOT factual contradictions — both are valid samples. What matters is
  whether the candidate covered the same set of items and its reasoning
  matches. Do NOT penalize differing invented numbers; DO penalize a missing
  item, a genuinely wrong one, or reversed reasoning.
- Equivalent = same facts and conclusions, same coverage of key points.
- Missing, added, or contradicted KEY facts lower the score.
- Return ONLY a JSON object with exactly these fields:
  {"score": <integer 1-10>, "verdict": "equivalent"|"minor_differences"|"substantive_differences", "rationale": "<1-2 sentences>"}
- score 9-10: equivalent; 7-8: minor differences, no substantive change;
  4-6: some key points missing or wrong; 1-3: substantially different.`

// buildJudgeUser replicates _build_judge_user(reference, candidate).
func buildJudgeUser(reference, candidate string) string {
	return "REFERENCE output:\n" +
		"--------------------\n" +
		reference + "\n\n" +
		"CANDIDATE output:\n" +
		"--------------------\n" +
		candidate + "\n"
}

// ---------------------------------------------------------------------------
// Judge reply parsing — port of parse_judge_json
// ---------------------------------------------------------------------------

var (
	judgeScoreRe   = regexp.MustCompile(`"score"\s*:\s*(\d{1,2})`)
	judgeVerdictRe = regexp.MustCompile(`"verdict"\s*:\s*"([^"]+)"`)
	judgeFenceRe   = regexp.MustCompile("^```(?:json)?\\s*")
)

// parseJudgeJSON parses the judge's JSON reply, tolerating code fences and
// loose output — mirroring Python's parse_judge_json.
func parseJudgeJSON(text string) (score int, verdict string, rationale string, err error) {
	raw := strings.TrimSpace(text)
	if strings.HasPrefix(raw, "```") {
		raw = judgeFenceRe.ReplaceAllString(raw, "")
		raw = strings.TrimRight(raw, "`")
		raw = strings.TrimSpace(raw)
	}
	var obj struct {
		Score     int    `json:"score"`
		Verdict   string `json:"verdict"`
		Rationale string `json:"rationale"`
	}
	if jerr := json.Unmarshal([]byte(raw), &obj); jerr == nil {
		if obj.Score < 1 || obj.Score > 10 {
			return 0, "", "", fmt.Errorf("judge score out of range: %d (%q)", obj.Score, clip(text, 200))
		}
		return obj.Score, obj.Verdict, obj.Rationale, nil
	}
	mScore := judgeScoreRe.FindStringSubmatch(raw)
	if mScore == nil {
		return 0, "", "", fmt.Errorf("judge reply unparseable: %q", clip(text, 200))
	}
	s, err := strconv.Atoi(mScore[1])
	if err != nil {
		return 0, "", "", fmt.Errorf("judge reply unparseable: %q", clip(text, 200))
	}
	if s < 1 || s > 10 {
		return 0, "", "", fmt.Errorf("judge score out of range: %d (%q)", s, clip(text, 200))
	}
	v := "unknown"
	if mV := judgeVerdictRe.FindStringSubmatch(raw); mV != nil {
		v = mV[1]
	}
	return s, v, clip(raw, 300), nil
}

// ---------------------------------------------------------------------------
// DeepSeek client (stdlib-only, OpenAI-compatible chat completions)
// ---------------------------------------------------------------------------

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage chatUsage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// chatClient is a minimal OpenAI-compatible chat client for DeepSeek. It
// retries HTTP 429, 5xx and transport errors with exponential backoff
// (mirroring the Python SDK's retry logic), capped at 30s.
type chatClient struct {
	key     string
	baseURL string
	http    *http.Client
	retries int
}

func newChatClient(key string) *chatClient {
	return &chatClient{
		key:     key,
		baseURL: "https://api.deepseek.com",
		http:    &http.Client{Timeout: 180 * time.Second},
		retries: 3,
	}
}

// complete posts one chat completion and returns the assistant content plus
// token usage. Retries transient failures (429/5xx/transport) with backoff
// min(5*2^attempt, 30)s.
func (c *chatClient) complete(ctx context.Context, model string, messages []chatMessage, temperature float64, maxTokens int) (string, chatUsage, error) {
	body, err := json.Marshal(chatCompletionRequest{
		Model:       model,
		Messages:    messages,
		Temperature: temperature,
		MaxTokens:   maxTokens,
	})
	if err != nil {
		return "", chatUsage{}, err
	}
	var lastErr error
	for attempt := 0; attempt < c.retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return "", chatUsage{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.key)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("deepseek transport error: %w", err)
			sleepBackoff(attempt)
			continue
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("deepseek read error: %w", readErr)
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("deepseek HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode != 200 {
			return "", chatUsage{}, fmt.Errorf("deepseek HTTP %d: %s", resp.StatusCode, clip(string(raw), 200))
		}
		var out chatCompletionResponse
		if err := json.Unmarshal(raw, &out); err != nil {
			return "", chatUsage{}, fmt.Errorf("deepseek response parse: %w", err)
		}
		if out.Error != nil {
			return "", chatUsage{}, fmt.Errorf("deepseek error: %s", out.Error.Message)
		}
		if len(out.Choices) == 0 {
			return "", chatUsage{}, fmt.Errorf("deepseek: no choices in response")
		}
		return out.Choices[0].Message.Content, out.Usage, nil
	}
	return "", chatUsage{}, fmt.Errorf("deepseek failed after %d attempts: %v", c.retries, lastErr)
}

func sleepBackoff(attempt int) {
	secs := 5 * (1 << attempt) // 5 * 2^attempt
	if secs > 30 {
		secs = 30
	}
	time.Sleep(time.Duration(secs) * time.Second)
}

// ---------------------------------------------------------------------------
// Judge domain model
// ---------------------------------------------------------------------------

type judgeSample struct {
	Score     int
	Verdict   string
	Rationale string
}

type fidelityComparison struct {
	Score     float64
	Verdict   string
	Rationale string
	Samples   []judgeSample
}

func (c fidelityComparison) toDict() fidelityComparisonJSON {
	samples := make([]judgeSampleJSON, 0, len(c.Samples))
	for _, s := range c.Samples {
		samples = append(samples, judgeSampleJSON{Score: s.Score, Verdict: s.Verdict, Rationale: s.Rationale})
	}
	return fidelityComparisonJSON{
		Score:     round2(c.Score),
		Verdict:   c.Verdict,
		Rationale: c.Rationale,
		Samples:   samples,
	}
}

// verdictFor maps a score to the verdict label — port of _verdict_for.
func verdictFor(score float64) string {
	if score >= 9.0 {
		return "equivalent"
	}
	if score >= 7.0 {
		return "minor_differences"
	}
	if score >= 4.0 {
		return "some_substantive_differences"
	}
	return "substantively_different"
}

// fidelityJudge is the Judge protocol: score(reference, candidate).
type fidelityJudge interface {
	score(ctx context.Context, reference, candidate string) (fidelityComparison, error)
}

// llmJudge is the real judge: an LLM call per sample (DeepSeek, temp 0).
// `reps` samples are averaged; all sample scores, verdicts and rationales are
// kept. Retries on rate limits and on unparseable replies (bounded).
type llmJudge struct {
	chat      *chatClient
	model     string
	reps      int
	maxTokens int
	retries   int
}

func newLLMJudge(chat *chatClient, model string, reps int) *llmJudge {
	return &llmJudge{chat: chat, model: model, reps: reps, maxTokens: 256, retries: 3}
}

func (j *llmJudge) score(ctx context.Context, reference, candidate string) (fidelityComparison, error) {
	samples := make([]judgeSample, 0, j.reps)
	for i := 0; i < j.reps; i++ {
		s, err := j.sample(ctx, reference, candidate)
		if err != nil {
			return fidelityComparison{}, err
		}
		samples = append(samples, s)
	}
	total := 0
	rationales := make([]string, 0, len(samples))
	for _, s := range samples {
		total += s.Score
		rationales = append(rationales, s.Rationale)
	}
	score := float64(total) / float64(len(samples))
	return fidelityComparison{
		Score:     score,
		Verdict:   verdictFor(score),
		Rationale: strings.Join(rationales, " | "),
		Samples:   samples,
	}, nil
}

func (j *llmJudge) sample(ctx context.Context, reference, candidate string) (judgeSample, error) {
	var lastErr error
	for attempt := 0; attempt < j.retries; attempt++ {
		content, _, err := j.chat.complete(ctx, j.model, []chatMessage{
			{Role: "system", Content: judgeSystem},
			{Role: "user", Content: buildJudgeUser(reference, candidate)},
		}, 0.0, j.maxTokens)
		if err != nil {
			// Rate limit / 5xx / transport — backoff and retry.
			lastErr = err
			sleepBackoff(attempt)
			continue
		}
		s, v, r, perr := parseJudgeJSON(content)
		if perr != nil {
			// Unparseable reply — retry once, then give up.
			lastErr = perr
			if attempt == j.retries-1 {
				return judgeSample{}, fmt.Errorf("judge failed after %d attempts: %w", j.retries, perr)
			}
			time.Sleep(time.Second)
			continue
		}
		return judgeSample{Score: s, Verdict: v, Rationale: r}, nil
	}
	return judgeSample{}, fmt.Errorf("judge failed after %d attempts: %w", j.retries, lastErr)
}

// deterministicJudge is the dry-run judge: char-similarity mapped onto 1-10.
// Plumbing only — never used for evidence.
type deterministicJudge struct{}

func (d *deterministicJudge) score(_ context.Context, reference, candidate string) (fidelityComparison, error) {
	ratio := sequenceMatcherRatio(reference, candidate)
	score := float64(int(math.Round(1 + ratio*9))) // 1..10
	verdict := verdictFor(score)
	sample := judgeSample{
		Score:     int(score),
		Verdict:   verdict,
		Rationale: fmt.Sprintf("dry-run deterministic (char-ratio %.2f)", ratio),
	}
	return fidelityComparison{
		Score:     score,
		Verdict:   verdict,
		Rationale: sample.Rationale,
		Samples:   []judgeSample{sample},
	}, nil
}

// ---------------------------------------------------------------------------
// difflib.SequenceMatcher char-ratio (deterministic, autojunk-faithful)
// ---------------------------------------------------------------------------

// sequenceMatcherRatio computes the same ratio as Python's
// difflib.SequenceMatcher(None, a, b).ratio() over the runes of two strings:
// 2*M / (len(a)+len(b)) where M is the total length of matching blocks.
func sequenceMatcherRatio(a, b string) float64 {
	ra, rb := []rune(a), []rune(b)
	if len(ra)+len(rb) == 0 {
		return 1.0
	}
	m := newMatcher(ra, rb)
	matches := m.totalMatching()
	return 2.0 * float64(matches) / float64(len(ra)+len(rb))
}

// matcher mirrors difflib.SequenceMatcher's internals (b2j map, autojunk
// popular-element purge for sequences >= 200 elements, recursive longest-
// match with junk extension).
type matcher struct {
	a, b     []rune
	b2j      map[rune][]int
	bjunk    map[rune]bool
	bpopular map[rune]bool
	autojunk bool
}

func newMatcher(a, b []rune) *matcher {
	m := &matcher{a: a, b: b, autojunk: true}
	m.chainB()
	return m
}

func (m *matcher) chainB() {
	b2j := map[rune][]int{}
	for i, elt := range m.b {
		b2j[elt] = append(b2j[elt], i)
	}
	m.bjunk = map[rune]bool{}
	m.bpopular = map[rune]bool{}
	if m.autojunk && len(m.b) >= 200 {
		ntest := len(m.b)/100 + 1
		for elt, idxs := range b2j {
			if len(idxs) > ntest {
				m.bpopular[elt] = true
			}
		}
		for elt := range m.bpopular {
			delete(b2j, elt)
		}
	}
	m.b2j = b2j
}

func (m *matcher) findLongestMatch(alo, ahi, blo, bhi int) (besti, bestj, bestsize int) {
	besti, bestj, bestsize = alo, blo, 0
	j2len := map[int]int{}
	for i := alo; i < ahi; i++ {
		newj2len := map[int]int{}
		for _, j := range m.b2j[m.a[i]] {
			if j < blo {
				continue
			}
			if j >= bhi {
				break
			}
			k := j2len[j-1] + 1
			newj2len[j] = k
			if k > bestsize {
				besti, bestj, bestsize = i-k+1, j-k+1, k
			}
		}
		j2len = newj2len
	}
	// Extend the best by non-junk elements on each end.
	for besti > alo && bestj > blo && !m.bjunk[m.b[bestj-1]] && m.a[besti-1] == m.b[bestj-1] {
		besti--
		bestj--
		bestsize++
	}
	for besti+bestsize < ahi && bestj+bestsize < bhi && !m.bjunk[m.b[bestj+bestsize]] && m.a[besti+bestsize] == m.b[bestj+bestsize] {
		bestsize++
	}
	// Now suck up the matching junk on each side too.
	for besti > alo && bestj > blo && m.bjunk[m.b[bestj-1]] && m.a[besti-1] == m.b[bestj-1] {
		besti--
		bestj--
		bestsize++
	}
	for besti+bestsize < ahi && bestj+bestsize < bhi && m.bjunk[m.b[bestj+bestsize]] && m.a[besti+bestsize] == m.b[bestj+bestsize] {
		bestsize++
	}
	return besti, bestj, bestsize
}

func (m *matcher) totalMatching() int {
	type quad [4]int
	queue := []quad{{0, len(m.a), 0, len(m.b)}}
	type triple [3]int
	blocks := []triple{}
	for len(queue) > 0 {
		q := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		alo, ahi, blo, bhi := q[0], q[1], q[2], q[3]
		i, j, k := m.findLongestMatch(alo, ahi, blo, bhi)
		if k > 0 {
			blocks = append(blocks, triple{i, j, k})
			if alo < i && blo < j {
				queue = append(queue, quad{alo, i, blo, j})
			}
			if i+k < ahi && j+k < bhi {
				queue = append(queue, quad{i + k, ahi, j + k, bhi})
			}
		}
	}
	sort.Slice(blocks, func(x, y int) bool {
		if blocks[x][0] != blocks[y][0] {
			return blocks[x][0] < blocks[y][0]
		}
		if blocks[x][1] != blocks[y][1] {
			return blocks[x][1] < blocks[y][1]
		}
		return blocks[x][2] < blocks[y][2]
	})
	total := 0
	i1, j1, k1 := 0, 0, 0
	for _, blk := range blocks {
		i2, j2, k2 := blk[0], blk[1], blk[2]
		if i1+k1 == i2 && j1+k1 == j2 {
			k1 += k2
		} else {
			if k1 > 0 {
				total += k1
			}
			i1, j1, k1 = i2, j2, k2
		}
	}
	if k1 > 0 {
		total += k1
	}
	return total
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

// round2 mirrors Python's round(x, 2) used across the report.
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// clip truncates a string to n runes (Python's s[:n]).
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
