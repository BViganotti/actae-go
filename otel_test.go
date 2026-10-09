package actae

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// recordingSpan embeds the (unexported-marker) trace.Span interface so we only
// need to override the methods the bridge calls.
type recordingSpan struct {
	trace.Span
	attrs map[string]any
}

func (s *recordingSpan) SetAttributes(kv ...attribute.KeyValue) {
	if s.attrs == nil {
		s.attrs = map[string]any{}
	}
	for _, a := range kv {
		s.attrs[string(a.Key)] = a.Value.AsInterface()
	}
}

func (s *recordingSpan) End(...trace.SpanEndOption) {}

type fakeStarter struct {
	names []string
	spans []*recordingSpan
}

func (f *fakeStarter) Start(_ context.Context, name string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	span := &recordingSpan{}
	f.names = append(f.names, name)
	f.spans = append(f.spans, span)
	return context.Background(), span
}

func TestTraceContext(t *testing.T) {
	got := TraceContext("abc", "def")
	if got["trace_id"] != "abc" || got["span_id"] != "def" {
		t.Fatalf("TraceContext = %v", got)
	}
	if _, ok := TraceContext("abc", "")["span_id"]; ok {
		t.Fatalf("empty span id must be omitted")
	}
}

func TestEventToSpanLiftsIdentityAndGenAI(t *testing.T) {
	ev := Event{
		ChannelID: "ch-1",
		EventType: "llm.call",
		Cursor:    7,
		Actor:     "agent",
		Payload: map[string]any{
			"model":        "gpt-5",
			"input_tokens": int64(1200),
			"tool":         "web_search",
			"latency_ms":   int64(42),
		},
		Metadata: map[string]any{"trace_id": "t-1", "span_id": "s-1"},
	}
	span := EventToSpan(ev)
	if span.Name != "llm.call" {
		t.Errorf("name = %q", span.Name)
	}
	if span.Attributes[ActaeCursor] != int64(7) {
		t.Errorf("cursor = %v", span.Attributes[ActaeCursor])
	}
	if span.Attributes[GenAIRequestModel] != "gpt-5" {
		t.Errorf("model = %v", span.Attributes[GenAIRequestModel])
	}
	if span.Attributes[GenAIUsageInputTokens] != int64(1200) {
		t.Errorf("input tokens = %v", span.Attributes[GenAIUsageInputTokens])
	}
	if span.Attributes[GenAIToolName] != "web_search" {
		t.Errorf("tool = %v", span.Attributes[GenAIToolName])
	}
	if span.Attributes[ActaeTraceID] != "t-1" || span.Attributes[ActaeSpanID] != "s-1" {
		t.Errorf("trace context not propagated: %v", span.Attributes)
	}
}

func TestEventToSpanIgnoresNonPrimitivesAndMissingPayload(t *testing.T) {
	span := EventToSpan(Event{
		EventType: "agent.step",
		Payload:   map[string]any{"model": map[string]any{"nested": 1}},
	})
	if _, ok := span.Attributes[GenAIRequestModel]; ok {
		t.Fatalf("non-primitive model must be ignored")
	}
	if EventToSpan(Event{}).Name != "actae.event" {
		t.Fatalf("empty event type must fall back to actae.event")
	}
}

func TestOTelBridgeExportsSpans(t *testing.T) {
	starter := &fakeStarter{}
	bridge := NewOTelBridge(starter)
	spans := bridge.ExportEvents(context.Background(), []Event{
		{EventType: "tool.started", Payload: map[string]any{"tool": "charge"}},
		{EventType: "tool.completed"},
	})
	if len(spans) != 2 || starter.names[0] != "tool.started" || starter.names[1] != "tool.completed" {
		t.Fatalf("names = %v", starter.names)
	}
	first := starter.spans[0]
	if first.attrs[GenAIToolName] != "charge" {
		t.Fatalf("span attributes = %v", first.attrs)
	}
	if first.attrs[ActaeChannelID] != "" {
		t.Fatalf("channel should default to empty, got %v", first.attrs[ActaeChannelID])
	}
}
