// Live smoke test for the Actae Go SDK against a dev-mode server
// (cargo run -- --dev, http://localhost:8002).
//
// Run: go run ./smoke/smoke.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	actae "github.com/BViganotti/actae-go"
	"github.com/BViganotti/actae-go/state"
)

func check(step string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", step, err)
	}
	fmt.Printf("ok: %s\n", step)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := actae.NewClientFromEnv(actae.ClientOptions{})
	check("NewClient", err)

	channel := "go-smoke-" + fmt.Sprintf("%d", time.Now().UnixNano()%1000000)

	// HTTP: record + replay
	ev, err := c.Record(ctx, channel, "smoke.step", map[string]any{"n": 1}, actae.RecordOptions{Actor: "smoke"})
	check("Record", err)
	if ev.Cursor <= 0 || ev.ID == "" {
		log.Fatalf("bad event: %+v", ev)
	}
	ev2, err := c.Record(ctx, channel, "smoke.step", map[string]any{"n": 2}, actae.RecordOptions{Actor: "smoke"})
	check("Record 2", err)
	events, err := c.Replay(ctx, channel, actae.ReplayOptions{Limit: 10})
	check("Replay", err)
	if len(events) != 2 || events[0].Cursor >= events[1].Cursor {
		log.Fatalf("bad replay: %+v", events)
	}

	// Idempotent record: retrying with the same operation_id must replay
	// the original event (no duplicate, same cursor).
	opID := uuid.NewString()
	idem1, err := c.Record(ctx, channel, "smoke.step", map[string]any{"n": 1},
		actae.RecordOptions{Actor: "smoke", OperationID: &opID})
	check("Record (idempotent)", err)
	idem2, err := c.Record(ctx, channel, "smoke.step", map[string]any{"n": 1},
		actae.RecordOptions{Actor: "smoke", OperationID: &opID})
	check("Record (idempotent retry)", err)
	if idem2.ID != idem1.ID || idem2.Cursor != idem1.Cursor {
		log.Fatalf("idempotent retry returned a different event: %+v vs %+v", idem2, idem1)
	}
	events, err = c.Replay(ctx, channel, actae.ReplayOptions{Limit: 10})
	check("Replay (after idempotent)", err)
	if len(events) != 3 {
		log.Fatalf("idempotent retry duplicated: %d events", len(events))
	}

	// State snapshot
	ver, err := c.SaveState(ctx, channel, ev2.Cursor, map[string]any{"mem": "x"}, actae.SaveStateOptions{})
	check("SaveState", err)
	latest, err := c.LatestState(ctx, channel)
	check("LatestState", err)
	if latest.State["mem"] != "x" || latest.Cursor != ev2.Cursor {
		log.Fatalf("bad latest state: %v", latest)
	}
	fmt.Printf("ok: state version %d\n", ver)

	// Fork
	forkName := channel + "-fork"
	forkReceipt, err := c.Fork(ctx, channel, forkName, 0, actae.ForkOptions{})
	check("Fork", err)
	if forkReceipt.ChildChannelID != forkName {
		log.Fatalf("bad fork: %+v", forkReceipt)
	}
	fmt.Printf("ok: fork resolved_cursor=%d restorable=%t\n", forkReceipt.ResolvedCursor, forkReceipt.Restorable)

	// AgentSession
	var s *actae.AgentSession
	s, err = actae.NewAgentSession(c, channel+"-session", actae.AgentSessionOptions{
		DisplayName:      "smoke session",
		SnapshotInterval: 1,
		StateFn: func() map[string]any {
			return map[string]any{"step": s.StepCount()}
		},
	})
	check("NewAgentSession", err)
	if err := s.Start(ctx); err != nil {
		check("Session.Start", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Step(ctx, "smoke.step", actae.StepOptions{Input: i, Output: i * 2}); err != nil {
			check(fmt.Sprintf("Session.Step %d", i), err)
		}
	}
	if err := s.Complete(ctx); err != nil {
		check("Session.Complete", err)
	}
	if s.StepCount() != 3 || s.Status() != actae.SessionStatusCompleted {
		log.Fatalf("bad session state: %+v", s)
	}

	// Resume (crash recovery path: fork from completed session)
	step := 2
	resumed, err := actae.Resume(ctx, c, channel+"-session", actae.ResumeOptions{ForkAtStep: &step, Name: channel + "-session-fork"})
	check("Resume fork", err)
	if resumed.ChannelID() != channel+"-session-fork" {
		log.Fatalf("bad resume channel: %q", resumed.ChannelID())
	}
	// The fork continues at step 3 (not step 1) and inherited the parent's
	// state snapshot (StateFn returns {"step": <count>} saved every step).
	if resumed.StepCount() != 2 {
		log.Fatalf("resumed fork StepCount = %d, want 2 (continue at step 3)", resumed.StepCount())
	}
	if inh := resumed.InheritedState(); inh == nil {
		log.Fatalf("resumed fork InheritedState() = nil, want inherited snapshot")
	}

	// WebSocket: two clients — c2 subscribes, c1 publishes (the server does
	// not echo broadcasts back to the publishing connection)
	subReceived := make(chan actae.Event, 1)
	c2, err := actae.NewClientFromEnv(actae.ClientOptions{})
	check("NewClient (ws)", err)
	defer c2.Disconnect()
	c2.OnMessage(func(topic string, event actae.Event) {
		if topic == channel+"-ws" {
			subReceived <- event
		}
	})
	if err := c.Connect(ctx); err != nil {
		check("WS Connect", err)
	}
	defer c.Disconnect()
	if err := c2.Connect(ctx); err != nil {
		check("WS Connect (subscriber)", err)
	}
	if err := c2.Subscribe(ctx, channel+"-ws", nil, true); err != nil {
		check("WS Subscribe", err)
	}
	pubEv, err := c.Publish(ctx, channel+"-ws", map[string]any{"hello": "world"})
	check("WS Publish", err)
	if pubEv.Cursor <= 0 {
		log.Fatalf("bad publish event: %+v", pubEv)
	}
	select {
	case got := <-subReceived:
		payload := got.Payload.(map[string]any)
		if payload["hello"] != "world" {
			log.Fatalf("bad broadcast payload: %v", got.Payload)
		}
		fmt.Println("ok: WS broadcast received (cross-connection)")
	case <-time.After(5 * time.Second):
		log.Fatal("no broadcast received")
	}
	if err := c.Disconnect(); err != nil {
		check("WS Disconnect", err)
	}
	if err := c2.Disconnect(); err != nil {
		check("WS Disconnect (subscriber)", err)
	}

	// StateManager
	sm := actae.NewStateManager(c, channel+"-sm")
	if _, err := sm.Save(ctx, map[string]any{"v": 1}); err != nil {
		check("StateManager.Save", err)
	}
	st, err := sm.Load(ctx)
	check("StateManager.Load", err)
	if st["v"] != float64(1) && st["v"] != int64(1) {
		log.Fatalf("bad state manager load: %v", st)
	}

	// Transactional state store: two commits race-free, then load back.
	type tally struct {
		Count int64 `json:"count"`
	}
	store, err := state.New(c, channel+"-store", func(cur *tally) (state.Command, error) {
		cur.Count++
		return state.Command{
			EventType:    "smoke.incremented",
			OperationKey: "increment",
			Payload:      map[string]any{"to": cur.Count},
			Actor:        "smoke",
		}, nil
	})
	check("state.New", err)
	var firstVersion int64
	for i := 0; i < 2; i++ {
		res, err := store.Commit(ctx)
		check(fmt.Sprintf("state.Commit #%d", i+1), err)
		if res.State.Count != int64(i+1) {
			log.Fatalf("bad committed state: %+v", res.State)
		}
		if i == 0 {
			firstVersion = res.StateVersion
		}
	}
	loaded, err := store.Load(ctx)
	check("state.Load", err)
	if loaded == nil || loaded.Count != 2 {
		log.Fatalf("bad store load: %+v", loaded)
	}
	// Versions come from the server's global state-version sequence, so
	// they are not gapless per channel — always address snapshots by the
	// version the Commit/ListStates returned.
	v1, err := store.LoadVersion(ctx, firstVersion)
	check("state.LoadVersion", err)
	if v1 == nil || v1.Count != 1 {
		log.Fatalf("bad versioned load: %+v", v1)
	}

	fmt.Printf("\nALL SMOKE TESTS PASSED (channel %s)\n", channel)
}
