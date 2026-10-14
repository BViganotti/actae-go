// Fidelity scenarios — five unrelated pipelines.
//
// This file is a byte-for-byte port of examples/fork_fidelity_scenarios.py:
// the same scenario names, descriptions, topics, step keys, system prompts,
// user-prompt wiring, temperatures, max_tokens and fork points, so the SAME
// scenarios run through every SDK.
//
// Scenarios:
//
//	s1-market-entry       market analysis brief         (5 steps, fork@2, topic)
//	s2-code-review        Python code review report     (5 steps, fork@3, input_text)
//	s3-localization       FR translation + adaptation  (3 steps, fork@1, input_text)
//	s4-incident-analysis  outage postmortem            (4 steps, fork@1, input_text)
//	s5-product-brief      feature spec + GTM           (4 steps, fork@3, topic)
package main

import (
	"fmt"
	"strings"
)

// Step is one pipeline step: an LLM call producing text stored under Key.
type Step struct {
	Key         string
	System      string
	User        func(state map[string]any) string
	Temperature float64
	MaxTokens   int
}

// Scenario is one self-contained, unrelated pipeline to test.
//
// ForkAtStep is the number of inherited steps (1-based): the fork keeps steps
// 1..ForkAtStep and recomputes ForkAtStep+1..len(Steps). InputText (optional)
// is embedded into the live state as `_input` for steps that consume a fixed
// input (code snippets, source documents, ...).
type Scenario struct {
	Name        string
	Description string
	Topic       string
	Steps       []Step
	ForkAtStep  int
	InputText   string
	Params      map[string]any
}

// s joins the given live-state keys into a context block for a step prompt —
// Python's `_s(state, *keys)`.
func s(state map[string]any, keys ...string) string {
	parts := []string{}
	for _, k := range keys {
		if v, ok := state[k].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "\n\n")
}

func topic(state map[string]any) string {
	return state["topic"].(string)
}

func inheritedKeys(sc *Scenario) []string {
	keys := make([]string, 0, sc.ForkAtStep)
	for _, st := range sc.Steps[:sc.ForkAtStep] {
		keys = append(keys, st.Key)
	}
	return keys
}

func tailKeys(sc *Scenario) []string {
	keys := make([]string, 0, len(sc.Steps)-sc.ForkAtStep)
	for _, st := range sc.Steps[sc.ForkAtStep:] {
		keys = append(keys, st.Key)
	}
	return keys
}

func initialLive(sc *Scenario) map[string]any {
	live := map[string]any{"topic": sc.Topic}
	if sc.InputText != "" {
		live["_input"] = sc.InputText
	}
	return live
}

func validateScenario(sc *Scenario) error {
	if sc.Name == "" {
		return fmt.Errorf("scenario name must be a lowercase slug")
	}
	if len(sc.Steps) == 0 {
		return fmt.Errorf("scenario %s: no steps", sc.Name)
	}
	seen := map[string]bool{}
	for _, st := range sc.Steps {
		if seen[st.Key] {
			return fmt.Errorf("scenario %s: duplicate step key %q", sc.Name, st.Key)
		}
		seen[st.Key] = true
	}
	if sc.ForkAtStep < 1 || sc.ForkAtStep >= len(sc.Steps) {
		return fmt.Errorf("scenario %s: fork_at_step=%d must be in [1, %d] (needs >=1 inherited step and >=1 tail step)",
			sc.Name, sc.ForkAtStep, len(sc.Steps)-1)
	}
	return nil
}

// ---------------------------------------------------------------------------
// s1 — market analysis brief
// ---------------------------------------------------------------------------

var s1Market = &Scenario{
	Name:        "s1-market-entry",
	Description: "Market-entry analysis for a plant-based milk startup in Germany",
	Topic:       "Launching a plant-based milk startup in Germany in 2026",
	ForkAtStep:  2,
	Params:      map[string]any{"domain": "market-analysis"},
	Steps: []Step{
		{
			Key:         "research",
			System:      "You are a market research analyst. Produce exactly 3 concrete, quantified findings about this market (size, growth, consumers).",
			User:        topic,
			Temperature: 0.5,
			MaxTokens:   350,
		},
		{
			Key:         "competitors",
			System:      "You are a competitive strategist. Name the 3 most relevant competitors and one positioning fact about each.",
			User:        func(st map[string]any) string { return s(st, "research") },
			Temperature: 0.5,
			MaxTokens:   300,
		},
		{
			Key:         "pricing",
			System:      "You are a pricing strategist. Recommend a concrete pricing model with specific price points and margins.",
			User:        func(st map[string]any) string { return s(st, "research", "competitors") },
			Temperature: 0.6,
			MaxTokens:   350,
		},
		{
			Key:         "risks",
			System:      "You are a risk analyst. List the top 4 risks with a one-line mitigation each.",
			User:        func(st map[string]any) string { return s(st, "research", "competitors", "pricing") },
			Temperature: 0.6,
			MaxTokens:   350,
		},
		{
			Key:         "recommendation",
			System:      "You are the lead consultant. Write the final go/no-go recommendation with the 3 strongest reasons.",
			User:        func(st map[string]any) string { return s(st, "research", "competitors", "pricing", "risks") },
			Temperature: 0.4,
			MaxTokens:   350,
		},
	},
}

// ---------------------------------------------------------------------------
// s2 — code review report
// ---------------------------------------------------------------------------

var s2CodeSnippet = `def process_payment(order, user):
    total = sum(item["price"] * item["qty"] for item in order["items"])
    if total > user["balance"]:
        return "insufficient funds"
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO payments (user_id, total, ref) VALUES ('%s', '%s', '%s')" %
        (user["id"], total, order["ref"])
    )
    cursor.execute(
        "UPDATE users SET balance = balance - %s WHERE id = '%s'" %
        (total, user["id"])
    )
    conn.commit()
    return "ok"
`

var s2CodeReview = &Scenario{
	Name:        "s2-code-review",
	Description: "Security/correctness review of a payment-handling Python snippet",
	Topic:       "Code review of a payment handler",
	InputText:   s2CodeSnippet,
	ForkAtStep:  3,
	Params:      map[string]any{"domain": "code-review"},
	Steps: []Step{
		{
			Key:         "understand",
			System:      "You are a senior engineer. Summarize in 3-4 sentences what this code does.",
			User:        func(st map[string]any) string { return s(st, "_input") },
			Temperature: 0.3,
			MaxTokens:   250,
		},
		{
			Key:         "bugs",
			System:      "You are a code reviewer. List concrete bugs with severity and the line/section where each occurs.",
			User:        func(st map[string]any) string { return s(st, "_input", "understand") },
			Temperature: 0.4,
			MaxTokens:   400,
		},
		{
			Key:         "security",
			System:      "You are a security reviewer. List security issues: injection, authorization, data exposure. Be specific.",
			User:        func(st map[string]any) string { return s(st, "_input") },
			Temperature: 0.5,
			MaxTokens:   350,
		},
		{
			Key:         "improvements",
			System:      "You are a performance/quality expert. Suggest 3 concrete improvements.",
			User:        func(st map[string]any) string { return s(st, "understand", "bugs") },
			Temperature: 0.5,
			MaxTokens:   300,
		},
		{
			Key:         "verdict",
			System:      "You are the review lead. Give the final verdict: is this mergeable, and what are the top blocking issues.",
			User:        func(st map[string]any) string { return s(st, "bugs", "security", "improvements") },
			Temperature: 0.4,
			MaxTokens:   350,
		},
	},
}

// ---------------------------------------------------------------------------
// s3 — translation + adaptation
// ---------------------------------------------------------------------------

var s3SourceText = "Autoregressive large language models generate text token by token: at each step the model predicts a probability distribution over the vocabulary and samples the next token. Decoding strategies such as temperature scaling reshape that distribution to trade off diversity against determinism. KV caching avoids recomputing attention over earlier tokens, which is why longer contexts increase latency roughly linearly rather than quadratically."

var s3Localization = &Scenario{
	Name:        "s3-localization",
	Description: "Translate an English technical paragraph to French and adapt it for a general audience",
	Topic:       "Localization of an LLM technical explainer",
	InputText:   s3SourceText,
	ForkAtStep:  1,
	Params:      map[string]any{"domain": "localization"},
	Steps: []Step{
		{
			Key:         "translate",
			System:      "You are a professional technical translator. Translate the text to French, keeping the technical terms accurate.",
			User:        func(st map[string]any) string { return s(st, "_input") },
			Temperature: 0.3,
			MaxTokens:   350,
		},
		{
			Key:         "adapt",
			System:      "You are an editor. Rewrite the French translation so a general (non-technical) audience can understand it, without changing the facts.",
			User:        func(st map[string]any) string { return s(st, "translate") },
			Temperature: 0.5,
			MaxTokens:   350,
		},
		{
			Key:         "glossary",
			System:      "You are a terminology manager. List the 5 most important technical terms: English term, French translation, one-line explanation.",
			User:        func(st map[string]any) string { return s(st, "_input", "translate") },
			Temperature: 0.4,
			MaxTokens:   300,
		},
	},
}

// ---------------------------------------------------------------------------
// s4 — incident analysis (postmortem)
// ---------------------------------------------------------------------------

var s4Incident = "At 09:14 UTC the payments API started returning 5xx for ~12% of traffic. P95 latency rose from 120ms to 8s. At 09:22 a second deployment rolled out new auth middleware. At 09:31 the error rate hit 38% and the API was put in read-only mode. At 09:47 the auth middleware was rolled back; latency did NOT improve. At 10:02 the nightly batch job was restarted (it had crashed at 09:08 and left 40,000 jobs queued, each holding a DB connection); latency recovered within minutes and the queue drained by 10:40. The DB connection pool maxed out at 09:26. Post-incident investigation confirmed the batch job's crash left queued jobs holding pooled connections until the pool exhausted; the auth middleware rollout was coincidental and verified blameless."

var s4IncidentAnalysis = &Scenario{
	Name:        "s4-incident-analysis",
	Description: "Postmortem of a fictional payments-API outage",
	Topic:       "Outage postmortem analysis",
	InputText:   s4Incident,
	ForkAtStep:  1,
	Params:      map[string]any{"domain": "incident-analysis"},
	Steps: []Step{
		{
			Key:         "timeline",
			System:      "You are an SRE. Reconstruct the incident timeline from the report, in order with timestamps and one-line evidence.",
			User:        func(st map[string]any) string { return s(st, "_input") },
			Temperature: 0.3,
			MaxTokens:   300,
		},
		{
			Key:         "rootcause",
			System:      "You are the incident commander. Identify the most likely root cause and why it was not caught by monitoring.",
			User:        func(st map[string]any) string { return s(st, "_input", "timeline") },
			Temperature: 0.5,
			MaxTokens:   350,
		},
		{
			Key:         "blastradius",
			System:      "You are a systems analyst. Determine the blast radius (users, services, data integrity) and the customer impact.",
			User:        func(st map[string]any) string { return s(st, "timeline", "rootcause") },
			Temperature: 0.5,
			MaxTokens:   300,
		},
		{
			Key:         "actions",
			System:      "You are a reliability engineer. List concrete action items with owner and priority to prevent recurrence.",
			User:        func(st map[string]any) string { return s(st, "timeline", "rootcause", "blastradius") },
			Temperature: 0.4,
			MaxTokens:   350,
		},
	},
}

// ---------------------------------------------------------------------------
// s5 — product brief (offline mode)
// ---------------------------------------------------------------------------

var s5Product = &Scenario{
	Name:        "s5-product-brief",
	Description: "Product brief for adding offline mode to a note-taking app",
	Topic:       "Adding an offline mode to a cross-platform note-taking app",
	ForkAtStep:  3,
	Params:      map[string]any{"domain": "product-brief"},
	Steps: []Step{
		{
			Key:         "userresearch",
			System:      "You are a product researcher. Synthesize 3 concrete user needs for offline mode, each with a supporting scenario.",
			User:        topic,
			Temperature: 0.5,
			MaxTokens:   350,
		},
		{
			Key:         "spec",
			System:      "You are a product manager. Write a concise functional spec: scope, MVP, non-goals, and 2 edge cases.",
			User:        func(st map[string]any) string { return s(st, "userresearch") },
			Temperature: 0.4,
			MaxTokens:   400,
		},
		{
			Key:         "gtm",
			System:      "You are a growth lead. Outline the launch plan: target segments, messaging angle, and rollout.",
			User:        func(st map[string]any) string { return s(st, "userresearch", "spec") },
			Temperature: 0.5,
			MaxTokens:   300,
		},
		{
			Key:         "metrics",
			System:      "You are an analytics lead. Define 5 success metrics with baseline and target values.",
			User:        func(st map[string]any) string { return s(st, "userresearch", "spec", "gtm") },
			Temperature: 0.4,
			MaxTokens:   300,
		},
	},
}

// allScenarios is the ordered list of every fidelity scenario.
var allScenarios = []*Scenario{
	s1Market,
	s2CodeReview,
	s3Localization,
	s4IncidentAnalysis,
	s5Product,
}

// scenariosByNames returns the scenarios named in a comma-separated flag value.
func scenariosByNames(names []string) ([]*Scenario, error) {
	byName := map[string]*Scenario{}
	for _, sc := range allScenarios {
		byName[sc.Name] = sc
	}
	unknown := []string{}
	for _, n := range names {
		if _, ok := byName[n]; !ok {
			unknown = append(unknown, n)
		}
	}
	if len(unknown) > 0 {
		available := make([]string, 0, len(allScenarios))
		for _, sc := range allScenarios {
			available = append(available, sc.Name)
		}
		return nil, fmt.Errorf("unknown scenarios: %s; available: %s",
			strings.Join(unknown, ", "), strings.Join(available, ", "))
	}
	out := make([]*Scenario, 0, len(names))
	for _, n := range names {
		out = append(out, byName[n])
	}
	return out, nil
}
