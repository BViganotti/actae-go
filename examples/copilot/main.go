// Command copilot demonstrates the actae/copilot integration: record a
// GitHub Copilot SDK session into Actae and fork it at a specific event.
//
// Requires a running Actae server AND a Copilot runtime. Without the
// runtime, run with COPILOT_SKIP=1 to see the wiring without executing a
// prompt. Full reference: docs/GOLANG_SDK.md.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	actae "github.com/BViganotti/actae-go"
	copilot "github.com/BViganotti/actae-go/copilot"
	copilotsdk "github.com/github/copilot-sdk/go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	apiKey := os.Getenv("ACTAE_API_KEY")
	if apiKey == "" {
		apiKey = "sk-dev-0000000000000000000000"
	}
	url := os.Getenv("ACTAE_URL")
	if url == "" {
		url = "http://localhost:8002"
	}
	db, err := actae.NewClient(actae.ClientOptions{APIKey: apiKey, Endpoint: url})
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}

	fmt.Println("1. Wiring the manager (tracks one recorder per session)…")
	mgr := copilot.NewManager(db, copilot.ManagerOptions{})
	defer mgr.StopAll()

	if os.Getenv("COPILOT_SKIP") == "1" {
		fmt.Println("   COPILOT_SKIP=1 — skipping live Copilot session (see docs/GOLANG_SDK.md for the live flow)")
		return
	}

	cli := copilotsdk.NewClient(&copilotsdk.ClientOptions{})
	defer cli.Stop()

	fmt.Println("2. Starting a recorded session…")
	handle, err := mgr.StartSession(ctx, cli, &copilotsdk.SessionConfig{})
	if err != nil {
		log.Fatalf("StartSession: %v", err)
	}
	fmt.Printf("   channel: %s\n", handle.Channel)

	_, err = handle.Session.SendPromptAndWait(ctx, "What is 2+2? Reply with a single number.")
	if err != nil {
		log.Fatalf("SendPromptAndWait: %v", err)
	}

	fmt.Println("3. Every event of that session is now in Actae — replay it…")
	events, err := db.Replay(ctx, handle.Channel, actae.ReplayOptions{Limit: 20})
	if err != nil {
		log.Fatalf("Replay: %v", err)
	}
	for _, e := range events {
		fmt.Printf("   %s\n", e.EventType)
	}

	fmt.Println("4. Fork the session at its last event (if any)…")
	if len(events) > 0 {
		last := events[len(events)-1]
		newChannel := fmt.Sprintf("copilot-example-fork-%d", time.Now().UnixNano()%1_000_000)
		if _, err := mgr.Fork(ctx, handle.Session.SessionID, last.ID, copilot.ForkOptions{
			NewChannelID: newChannel,
		}); err != nil {
			log.Fatalf("Fork: %v", err)
		}
		fmt.Printf("   forked to %s at event %s\n", newChannel, last.ID)
	}
}
