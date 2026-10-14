// Package actae is a Go client for Actae, a real-time event store for agent
// workflows. It mirrors the Python SDK (actae-client) API surface 1:1:
//
//   - HTTP: record/replay/query events, versioned state snapshots, atomic
//     transitions, channel forks and execution trees, consumer groups,
//     persisted wake-ups, idempotent tool executions, health/metrics and
//     auth.
//   - WebSocket: subscribe/unsubscribe/publish with dual-format message
//     normalization (externally-tagged and flat server frames), ack-based
//     publish confirmation, cursor tracking and auto-reconnect.
//   - AgentSession: high-level agent instrumentation with step recording,
//     fork at any step, crash recovery and cursor-aligned state snapshots.
//   - StateManager: generic versioned state save/load/resume.
//
// The companion package actae/copilot integrates with the GitHub Copilot SDK
// for Go (github.com/github/copilot-sdk/go) so every Copilot session — user
// messages, assistant turns, tool executions, errors, sub-agents — can be
// recorded, replayed, forked and resumed through Actae.
//
// Example:
//
//	client, _ := actae.NewClient(actae.ClientOptions{
//	    APIKey:   "sk-dev-0000000000000000000000",
//	    Endpoint: "http://localhost:8002",
//	})
//	event, err := client.Record(ctx, "my-channel", "agent.step",
//	    map[string]any{"input": "hello"}, actae.RecordOptions{Actor: "agent"})
package actae

// SESSION_STARTED / SESSION_COMPLETED are the event types recorded by
// AgentSession lifecycle transitions.
const (
	SessionStarted   = "session.started"
	SessionCompleted = "session.completed"
)

// Ptr returns a pointer to v. Go's zero values cannot represent "field
// unset", so optional options-struct fields are pointers; Ptr makes
// setting them one line:
//
//	client.Replay(ctx, "ch", actae.ReplayOptions{Cursor: actae.Ptr(int64(42))})
func Ptr[T any](v T) *T {
	return &v
}
