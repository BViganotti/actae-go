// Command ws demonstrates the WebSocket API: subscribe on one client,
// publish on another, and the EchoSelf option for single-client demos.
//
// Requires a running Actae server (actae --dev) — see quickstart for env
// config. This example runs two clients on purpose: the server does NOT
// echo a broadcast back to the connection that published it.
//
//	go run ./examples/ws
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

	opts := actae.ClientOptions{APIKey: apiKey, Endpoint: url}
	publisher, err := actae.NewClient(opts)
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	subscriber, err := actae.NewClient(opts)
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	defer publisher.Disconnect()
	defer subscriber.Disconnect()

	topic := fmt.Sprintf("ws-example-%d", time.Now().UnixNano()%1_000_000)

	fmt.Println("1. Connecting both clients (Connect blocks until authenticated)…")
	if err := publisher.Connect(ctx); err != nil {
		log.Fatalf("Connect: %v", err)
	}
	if err := subscriber.Connect(ctx); err != nil {
		log.Fatalf("Connect: %v", err)
	}

	fmt.Println("2. Subscribing (SubscribeAndWait blocks for server confirmation)…")
	if err := subscriber.SubscribeAndWait(ctx, topic, nil); err != nil {
		log.Fatalf("Subscribe: %v", err)
	}

	received := make(chan actae.Event, 1)
	subscriber.OnMessage(func(t string, event actae.Event) {
		if t == topic {
			received <- event
		}
	})

	fmt.Println("3. Publishing from the other client…")
	persisted, err := publisher.Publish(ctx, topic, map[string]any{"hello": "world"})
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Printf("   publish persisted cursor=%d\n", persisted.Cursor)

	select {
	case ev := <-received:
		fmt.Printf("   subscriber got event: %v\n", ev.Payload)
	case <-time.After(5 * time.Second):
		log.Fatal("no broadcast received — the publisher never sees its own events")
	}

	fmt.Println("4. EchoSelf: a single client CAN see its own publishes…")
	echoClient, err := actae.NewClient(actae.ClientOptions{
		APIKey: apiKey, Endpoint: url, EchoSelf: true,
	})
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	defer echoClient.Disconnect()
	if err := echoClient.Connect(ctx); err != nil {
		log.Fatalf("Connect: %v", err)
	}
	echoed := make(chan actae.Event, 1)
	echoClient.OnMessage(func(t string, event actae.Event) { echoed <- event })
	if _, err := echoClient.Publish(ctx, topic+"-echo", map[string]any{"n": 1}); err != nil {
		log.Fatalf("Publish: %v", err)
	}
	select {
	case <-echoed:
		fmt.Println("   echo-self delivered the publish to its own OnMessage")
	case <-time.After(5 * time.Second):
		log.Fatal("echo-self did not deliver")
	}
}
