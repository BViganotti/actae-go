package copilot

// Live integration test against a real Copilot runtime + Actae. Skipped
// unless the environment is provisioned:
//
//	ACTAE_URL=http://localhost:8002 \
//	ACTAE_API_KEY=sk-dev-0000000000000000000000 \
//	COPILOT_CLI=/path/to/copilot (optional; defaults to the SDK's bundled runtime) \
//	go test ./copilot/ -run TestLive -v
//
// The test drives the full lifecycle through Manager.StartSession: a real
// Copilot session sends a prompt, events stream into Actae, the session is
// forked at a real recorded event, and session.ended is recorded.

import (
	"context"
	"os"
	"testing"
	"time"

	actae "github.com/BViganotti/actae-go"
	copilotsdk "github.com/github/copilot-sdk/go"
)

func recentEventTypes(ctx context.Context, db *actae.Client, channel string) []map[string]any {
	evs, err := db.Replay(ctx, channel, actae.ReplayOptions{Limit: 100})
	if err != nil {
		return nil
	}
	out := []map[string]any{}
	for _, e := range evs {
		out = append(out, map[string]any{
			"type":     e.EventType,
			"event_id": eventIDOf(e),
		})
	}
	return out
}

func eventIDOf(e actae.Event) string {
	if id, ok := e.Metadata["event_id"].(string); ok {
		return id
	}
	return ""
}

func TestLiveEndToEnd(t *testing.T) {
	actaeURL := os.Getenv("ACTAE_URL")
	actaeKey := os.Getenv("ACTAE_API_KEY")
	if actaeURL == "" || actaeKey == "" {
		t.Skip("ACTAE_URL/ACTAE_API_KEY not set")
	}

	db, err := actae.NewClient(actae.ClientOptions{APIKey: actaeKey, Endpoint: actaeURL})
	if err != nil {
		t.Fatal(err)
	}

	conn := copilotsdk.StdioConnection{}
	if path := os.Getenv("COPILOT_CLI"); path != "" {
		conn.Path = path
	}
	cli := copilotsdk.NewClient(&copilotsdk.ClientOptions{
		Connection: conn,
		LogLevel:   "error",
	})
	defer cli.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mgr := NewManager(db, ManagerOptions{})
	defer mgr.StopAll()

	handle, err := mgr.StartSession(ctx, cli, &copilotsdk.SessionConfig{
		ClientName: "actae-live-test",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	t.Logf("session %s → channel %s", handle.Session.SessionID, handle.Channel)

	// A fork requires a restorable state boundary at-or-before the fork
	// event's cursor, so seed one at session start.
	if _, err := mgr.Snapshot(ctx, handle.Session.SessionID, map[string]any{
		"session_id": handle.Session.SessionID,
		"step":       0,
	}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// send a prompt and wait for the turn to finish
	if _, err := handle.Session.SendPromptAndWait(ctx, "Reply with exactly: OK"); err != nil {
		t.Fatalf("SendPromptAndWait: %v", err)
	}

	// events must land on the Actae channel
	var events []map[string]any
	waitFor(t, 60*time.Second, func() bool {
		events = recentEventTypes(ctx, db, handle.Channel)
		for _, e := range events {
			if e["type"] == "copilot.user.message" {
				return true
			}
		}
		return false
	})
	t.Logf("channel events: %v", events)
	if len(events) < 2 {
		t.Fatalf("too few events recorded: %v", events)
	}
	// session info metadata must be present (StartSession records it)
	waitFor(t, 30*time.Second, func() bool {
		for _, e := range recentEventTypes(ctx, db, handle.Channel) {
			if e["type"] == "copilot.session.info" {
				return true
			}
		}
		return false
	})

	// fork at the first user.message event — the session-start snapshot is a
	// restorable boundary at-or-before it (early hook/session events land
	// before the snapshot cursor and have no boundary).
	var forked string
	var forkEventID string
	for _, e := range events {
		if e["type"] == "copilot.user.message" {
			id, ok := e["event_id"].(string)
			if !ok || id == "" {
				continue
			}
			forked, err = mgr.Fork(ctx, handle.Session.SessionID, id, ForkOptions{
				NewChannelID: "live-fork-" + handle.Session.SessionID,
			})
			if err != nil {
				t.Fatalf("Fork at event %s: %v", id, err)
			}
			forkEventID = id
			break
		}
	}
	if forked == "" {
		t.Fatal("no event with a copilot event id was recorded; cannot fork")
	}
	t.Logf("forked to %s", forked)

	// continue: start a NEW copilot session that records into the fork
	// channel and confirm its events land there (continuation lineage)
	contHandle, err := mgr.ContinueSession(ctx, cli, handle.Session.SessionID, forkEventID,
		&copilotsdk.SessionConfig{ClientName: "actae-live-test"},
		ContinueSessionOptions{ForkChannelID: "live-cont-" + handle.Session.SessionID})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if contHandle.Channel != "live-cont-"+handle.Session.SessionID {
		t.Fatalf("continuation channel = %q", contHandle.Channel)
	}
	if _, err := contHandle.Session.SendPromptAndWait(ctx, "Reply with exactly: OK"); err != nil {
		t.Fatalf("continuation SendPromptAndWait: %v", err)
	}
	waitFor(t, 60*time.Second, func() bool {
		var forkedEvt, userMsg bool
		for _, e := range recentEventTypes(ctx, db, contHandle.Channel) {
			switch e["type"] {
			case "copilot.session.forked":
				forkedEvt = true
			case "copilot.user.message":
				userMsg = true
			}
		}
		return forkedEvt && userMsg
	})
	t.Logf("continuation recorded on %s", contHandle.Channel)

	// end the session explicitly (idempotent with the session_end hook)
	if err := mgr.EndSession(ctx, handle.Session.SessionID, "test-complete"); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		for _, e := range recentEventTypes(ctx, db, handle.Channel) {
			if e["type"] == "copilot.session.ended" {
				return true
			}
		}
		return false
	})
	t.Log("live end-to-end OK")
}
