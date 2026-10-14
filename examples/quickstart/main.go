// Command quickstart is a live, walkthrough-style introduction to Actae:
// record an event, replay it, save and load state.
//
// Requires a running Actae server. Set ACTAE_URL (default
// http://localhost:8002) and ACTAE_API_KEY (default the dev key).
//
//	actae --dev          # in the actae/ directory
//	go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	actae "github.com/BViganotti/actae-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
		log.Fatalf("NewClient: %v", err)
	}

	channel := fmt.Sprintf("quickstart-%d", time.Now().UnixNano()%1_000_000)
	fmt.Printf("1. Recording events on channel %q…\n", channel)

	// Record: events are immutable JSON blobs with a monotonic cursor.
	ev1, err := client.Record(ctx, channel, "agent.step",
		map[string]any{"input": "hello"}, actae.RecordOptions{Actor: "example"})
	if err != nil {
		log.Fatalf("Record: %v", err)
	}
	ev2, err := client.Record(ctx, channel, "agent.step",
		map[string]any{"input": "world"}, actae.RecordOptions{Actor: "example"})
	if err != nil {
		log.Fatalf("Record: %v", err)
	}
	fmt.Printf("   recorded step 1 cursor=%d, step 2 cursor=%d\n", ev1.Cursor, ev2.Cursor)

	// Replay: read events back. Pass a cursor to resume from a point.
	fmt.Println("2. Replaying all events…")
	events, err := client.Replay(ctx, channel, actae.ReplayOptions{Limit: 100})
	if err != nil {
		log.Fatalf("Replay: %v", err)
	}
	for _, e := range events {
		fmt.Printf("   cursor=%d type=%s payload=%v\n", e.Cursor, e.EventType, e.Payload)
	}

	// Payload numbers are int64 when integral, float64 otherwise:
	// use actae.AsInt64 to normalize.
	fmt.Println("3. Saving and loading state…")
	sm := actae.NewStateManager(client, channel)
	version, err := sm.Save(ctx, map[string]any{"count": 2})
	if err != nil {
		log.Fatalf("StateManager.Save: %v", err)
	}
	state, err := sm.Resume(ctx, map[string]any{"count": 0})
	if err != nil {
		log.Fatalf("StateManager.Resume: %v", err)
	}
	fmt.Printf("   saved state version=%d, resumed count=%d\n", version, actae.AsInt64(state["count"], -1))

	fmt.Println("\nQuickstart complete — the pattern is record → replay → resume state.")
}
