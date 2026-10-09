// Command session demonstrates AgentSession: step recording, fork at a
// step, crash recovery resume, and fork-from-existing resume.
//
// Requires a running Actae server (actae --dev). See quickstart for env
// config.
//
//	go run ./examples/session
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	base := fmt.Sprintf("session-example-%d", time.Now().UnixNano()%1_000_000)

	fmt.Println("1. Starting a session — steps are recorded as events with step numbers…")
	memory := map[string]any{"stage": "init"}
	sess, err := actae.NewAgentSession(client, base, actae.AgentSessionOptions{
		DisplayName:      "session example",
		SnapshotInterval: 1,
		StateFn:          func() map[string]any { return memory },
	})
	if err != nil {
		log.Fatalf("NewAgentSession: %v", err)
	}
	if err := sess.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	for i, in := range []string{"parse", "plan", "execute"} {
		memory["stage"] = in
		ev, err := sess.Step(ctx, "agent.step", actae.StepOptions{Input: in, Output: fmt.Sprintf("done-%d", i)})
		if err != nil {
			log.Fatalf("Step: %v", err)
		}
		fmt.Printf("   step %d → cursor=%d\n", i+1, ev.Cursor)
	}

	fmt.Println("2. Forking at step 2 into a new experiment fork…")
	fork, err := sess.Fork(ctx, 2, base+"-fork", actae.ForkSessionOptions{Reason: "try plan-v2"})
	if err != nil {
		log.Fatalf("Fork: %v", err)
	}
	if err := fork.Start(ctx); err != nil {
		log.Fatalf("fork.Start: %v", err)
	}
	fmt.Printf("   fork channel: %s (started — record steps on it like any session)\n", fork.ChannelID())

	fmt.Println("3. Crash + resume in place (crash recovery)…")
	if err := sess.Crash(ctx, "simulated crash"); err != nil {
		log.Fatalf("Crash: %v", err)
	}
	recovered, err := actae.Resume(ctx, client, base, actae.ResumeOptions{})
	if err != nil {
		log.Fatalf("Resume: %v", err)
	}
	fmt.Printf("   resumed %s at step %d (status %s)\n", recovered.Name(), recovered.StepCount(), recovered.Status())

	fmt.Println("4. Resume from a completed session via fork-from-existing…")
	if err := sess.Complete(ctx); err != nil {
		log.Fatalf("Complete: %v", err)
	}
	step := 3
	resumed, err := actae.Resume(ctx, client, base, actae.ResumeOptions{
		ForkAtStep: &step,
		Name:       base + "-resume",
	})
	if err != nil {
		log.Fatalf("Resume (fork): %v", err)
	}
	fmt.Printf("   resumed channel %s forked at step %d\n", resumed.ChannelID(), step)
}
