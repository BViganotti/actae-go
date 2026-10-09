package actae

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// OpenTelemetry GenAI semantic-convention attribute keys (subset).
const (
	GenAIRequestModel      = "gen_ai.request.model"
	GenAIResponseModel     = "gen_ai.response.model"
	GenAIUsageInputTokens  = "gen_ai.usage.input_tokens"
	GenAIUsageOutputTokens = "gen_ai.usage.output_tokens"
	GenAIToolName          = "gen_ai.tool.name"
	GenAIOperationName     = "gen_ai.operation.name"

	ActaeChannelID = "actae.channel_id"
	ActaeCursor    = "actae.cursor"
	ActaeEventType = "actae.event_type"
	ActaeActor     = "actae.actor"
	ActaeTraceID   = "actae.trace_id"
	ActaeSpanID    = "actae.span_id"
	ActaeLatencyMS = "actae.latency_ms"
)

// TracerStarter is the minimal OpenTelemetry tracer surface this bridge needs.
// A real `trace.Tracer` (from `go.opentelemetry.io/otel/trace`) satisfies it,
// and so does a test double — the embedded private marker on `trace.Tracer`
// otherwise makes external fakes impossible.
type TracerStarter interface {
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span)
}

// OTelSpan is a span-shaped projection of one Actae event.
type OTelSpan struct {
	Name       string
	Attributes map[string]any
}

// TraceContext builds event metadata that correlates an Actae event with an
// external OTel trace. Pass it as RecordOptions.Metadata.
func TraceContext(traceID, spanID string) map[string]any {
	ctx := map[string]any{"trace_id": traceID}
	if spanID != "" {
		ctx["span_id"] = spanID
	}
	return ctx
}

func primitive(value any) (any, bool) {
	switch typed := value.(type) {
	case string, bool, int64, int, float64:
		return typed, true
	default:
		return nil, false
	}
}

func firstString(payload map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if value, ok := payload[key]; ok {
			if prim, ok := primitive(value); ok {
				return prim, true
			}
		}
	}
	return nil, false
}

// EventToSpan projects an Actae event onto a span name + OTel attributes.
// Pure and dependency-free: unit tests exercise it directly.
func EventToSpan(ev Event) OTelSpan {
	attrs := map[string]any{
		ActaeChannelID: ev.ChannelID,
		ActaeCursor:    ev.Cursor,
		ActaeEventType: ev.EventType,
		ActaeActor:     ev.Actor,
	}
	if ev.Metadata != nil {
		if traceID, ok := primitive(ev.Metadata["trace_id"]); ok {
			attrs[ActaeTraceID] = traceID
		}
		if spanID, ok := primitive(ev.Metadata["span_id"]); ok {
			attrs[ActaeSpanID] = spanID
		}
	}
	if payload, ok := ev.Payload.(map[string]any); ok {
		if model, ok := firstString(payload, "model", "request_model"); ok {
			attrs[GenAIRequestModel] = model
		}
		if responseModel, ok := firstString(payload, "response_model"); ok {
			attrs[GenAIResponseModel] = responseModel
		}
		if tool, ok := firstString(payload, "tool", "tool_name"); ok {
			attrs[GenAIToolName] = tool
		}
		if operation, ok := firstString(payload, "operation", "operation_name"); ok {
			attrs[GenAIOperationName] = operation
		}
		if inputTokens, ok := firstString(payload, "input_tokens", "prompt_tokens", "tokens"); ok {
			attrs[GenAIUsageInputTokens] = inputTokens
		}
		if outputTokens, ok := firstString(payload, "output_tokens", "completion_tokens"); ok {
			attrs[GenAIUsageOutputTokens] = outputTokens
		}
		if latency, ok := firstString(payload, "latency_ms", "duration_ms"); ok {
			attrs[ActaeLatencyMS] = latency
		}
	}
	name := ev.EventType
	if name == "" {
		name = "actae.event"
	}
	return OTelSpan{Name: name, Attributes: attrs}
}

func spanAttributes(attrs map[string]any) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for key, value := range attrs {
		switch typed := value.(type) {
		case string:
			out = append(out, attribute.String(key, typed))
		case bool:
			out = append(out, attribute.Bool(key, typed))
		case int64:
			out = append(out, attribute.Int64(key, typed))
		case int:
			out = append(out, attribute.Int64(key, int64(typed)))
		case float64:
			out = append(out, attribute.Float64(key, typed))
		}
	}
	return out
}

// OTelBridge mirrors Actae events into an OpenTelemetry tracer. Read-only: it
// never mutates events and never executes anything.
type OTelBridge struct {
	tracer TracerStarter
}

// NewOTelBridge wraps a tracer (e.g. tracerProvider.Tracer("actae")).
func NewOTelBridge(tracer TracerStarter) *OTelBridge {
	return &OTelBridge{tracer: tracer}
}

// ExportEvent emits one span for an event and returns its projection.
//
// Attributes are supplied both as a start option (so real samplers observe
// them) and via SetAttributes (so a minimal test double that ignores options
// still records them).
func (b *OTelBridge) ExportEvent(ctx context.Context, ev Event) OTelSpan {
	projection := EventToSpan(ev)
	attrs := spanAttributes(projection.Attributes)
	_, span := b.tracer.Start(ctx, projection.Name, trace.WithAttributes(attrs...))
	span.SetAttributes(attrs...)
	span.End()
	return projection
}

// ExportEvents emits one span per event.
func (b *OTelBridge) ExportEvents(ctx context.Context, events []Event) []OTelSpan {
	spans := make([]OTelSpan, 0, len(events))
	for _, ev := range events {
		spans = append(spans, b.ExportEvent(ctx, ev))
	}
	return spans
}

// ExportChannelToOTel replays a channel once and exports every event as a span.
func (c *Client) ExportChannelToOTel(ctx context.Context, channelID string, tracer TracerStarter, opts ReplayOptions) ([]OTelSpan, error) {
	events, err := c.Replay(ctx, channelID, opts)
	if err != nil {
		return nil, err
	}
	return NewOTelBridge(tracer).ExportEvents(ctx, events), nil
}
