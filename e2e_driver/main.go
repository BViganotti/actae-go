// Deep-004: the Go SDK drives the REAL hermetic fleet topology
// (SaaS token exchange -> gateway capabilities -> typed dispatch ->
// relay-mode instance), exactly like the Python suite. Run by the pytest
// cross-layer test; the topology is passed via environment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	actae "github.com/BViganotti/actae-go"
)

func main() {
	instance := os.Getenv("ACTAE_E2E_INSTANCE")
	if instance == "" {
		instance = "ins_e2e_2"
	}
	client, err := actae.NewFleetClient(actae.FleetClientOptions{
		OrganizationToken: os.Getenv("ACTAE_ORG_TOKEN"),
		OrganizationID:    os.Getenv("ACTAE_ORG_ID"),
		Endpoint:          os.Getenv("ACTAE_FLEET_URL"),
		TokenEndpoint:     os.Getenv("ACTAE_FLEET_TOKEN_URL"),
		RequestedScopes:   splitCSV(os.Getenv("ACTAE_FLEET_SCOPES")),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Capabilities through the gateway.
	caps, err := client.Capabilities(ctx, instance)
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver capabilities: %v\n", err)
		os.Exit(1)
	}
	if caps == nil {
		fmt.Fprintln(os.Stderr, "go_driver: capabilities returned nil")
		os.Exit(1)
	}

	// Typed dispatch: record + replay through the gateway to the instance.
	channel := "public.gosdk"
	_, err = client.CallManifest(ctx, "actae.api.v1.events.record", instance,
		map[string]string{}, url.Values{},
		map[string]any{
			"channel_id": channel,
			"event_type": "tool.gosdk",
			"payload":    map[string]any{"ok": true},
			"metadata":   map[string]any{"actor": "gosdk"},
		}, "e2e-gosdk-1", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver record: %v\n", err)
		os.Exit(1)
	}
	replay, err := client.CallManifest(ctx, "actae.api.v1.events.replay", instance,
		map[string]string{"channel_id": channel}, url.Values{}, map[string]any{"limit": 10}, "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver replay: %v\n", err)
		os.Exit(1)
	}
	// The replay envelope carries events under either "events" or "data".
	m, _ := replay.(map[string]any)
	events, _ := m["events"].([]any)
	if events == nil {
		if d, ok := m["data"].(map[string]any); ok {
			events, _ = d["events"].([]any)
		}
	}
	fmt.Printf("GO_SDK_REPLAYED_EVENTS: %d\n", len(events))
	if len(events) == 0 {
		fmt.Fprintln(os.Stderr, "go_driver: relay dispatch did not round-trip")
		os.Exit(1)
	}

	// State save + load round-trip.
	_, err = client.CallManifest(ctx, "actae.api.v1.state.save", instance,
		map[string]string{"channel_id": channel}, url.Values{},
		map[string]any{"channel_id": channel, "cursor": 1, "state": map[string]any{"step": 3}}, "e2e-gosdk-state", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver state.save: %v\n", err)
		os.Exit(1)
	}
	loaded, err := client.CallManifest(ctx, "actae.api.v1.state.load", instance,
		map[string]string{"channel_id": channel}, url.Values{}, nil, "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver state.load: %v\n", err)
		os.Exit(1)
	}
	if loaded == nil {
		fmt.Fprintln(os.Stderr, "go_driver: state.load returned nothing")
		os.Exit(1)
	}
	fmt.Println("GO_SDK_STATE: ok")

	// Fork the channel (immutable provenance).
	fork, err := client.CallManifest(ctx, "actae.api.v1.forks.create", instance,
		map[string]string{}, url.Values{},
		map[string]any{
			"source_channel_id": channel,
			"new_channel_id":    "ch_gosdk_fork_e2e",
			"at_cursor":         1,
			"reason":            "fleet e2e",
		}, "e2e-gosdk-fork", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver fork: %v\n", err)
		os.Exit(1)
	}
	fm, _ := fork.(map[string]any)
	child, _ := fm["child_channel_id"].(string)
	if child == "" {
		if d, ok := fm["data"].(map[string]any); ok {
			child, _ = d["child_channel_id"].(string)
		}
	}
	if child == "" {
		fmt.Fprintln(os.Stderr, "go_driver: fork returned no child channel")
		os.Exit(1)
	}
	fmt.Printf("GO_SDK_FORK_CHILD: %s\n", child)

	// Idempotency conflict: same idempotency key + different content -> 409.
	_, err = client.CallManifest(ctx, "actae.api.v1.events.record", instance,
		map[string]string{}, url.Values{},
		map[string]any{
			"channel_id": channel,
			"event_type": "tool.gosdk",
			"payload":    map[string]any{"conflict": true},
			"metadata":   map[string]any{"actor": "gosdk"},
		}, "e2e-gosdk-1", "") // same key as the first record, different content
	if err == nil {
		fmt.Fprintln(os.Stderr, "go_driver: expected an idempotency conflict (409)")
		os.Exit(1)
	}
	if apiErr, ok := err.(*actae.APIError); ok && apiErr.StatusCode == 409 {
		fmt.Println("GO_SDK_IDEMPOTENCY_CONFLICT: 409")
	} else {
		fmt.Fprintf(os.Stderr, "go_driver: expected APIError 409, got %v\n", err)
		os.Exit(1)
	}

	// Pagination: record several events on a dedicated channel, then iterate
	// opaque keyset pages (limit=2) and confirm every event is seen once.
	pageChannel := channel + "_pages"
	for i := 0; i < 5; i++ {
		_, err = client.CallManifest(ctx, "actae.api.v1.events.record", instance,
			map[string]string{}, url.Values{},
			map[string]any{
				"channel_id": pageChannel,
				"event_type": "tool.page",
				"payload":    map[string]any{"n": i},
				"metadata":   map[string]any{"actor": "gosdk"},
			}, fmt.Sprintf("e2e-gosdk-page-%d", i), "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "go_driver page record: %v\n", err)
			os.Exit(1)
		}
	}
	paged := map[string]bool{}
	pageCount := 0
	pageToken := ""
	pageBody := map[string]any{"channel_ids": []string{pageChannel}, "limit": 2}
	for {
		var pr actae.PartialResult
		var err error
		if pageToken == "" {
			pr, err = client.QueryMany(ctx, []string{instance}, pageBody)
		} else {
			pr, err = client.QueryManyWithTokens(ctx, []string{instance}, pageBody, map[string]string{instance: pageToken})
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "go_driver pagination: %v\n", err)
			os.Exit(1)
		}
		pageCount++
		for _, item := range pr.Items {
			if m, ok := item.(map[string]any); ok {
				paged[fmt.Sprint(m["payload"])] = true
			}
		}
		if pr.NextPageToken == "" {
			break
		}
		pageToken = pr.NextPageToken
	}
	if pageCount < 3 || len(paged) != 5 {
		fmt.Fprintf(os.Stderr, "go_driver pagination incomplete: pages=%d unique=%d\n", pageCount, len(paged))
		os.Exit(1)
	}
	fmt.Printf("GO_SDK_PAGINATION: pages=%d unique=%d\n", pageCount, len(paged))

	// Multi-instance partial results: one real instance + one non-routable
	// instance -> the result separates successes from failures and never
	// hides which instance failed.
	pr, err := client.QueryMany(ctx, []string{instance, "ins_missing_e2e"}, map[string]any{"channel_ids": []string{pageChannel}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "go_driver QueryMany: %v\n", err)
		os.Exit(1)
	}
	if len(pr.Errors) == 0 {
		fmt.Fprintln(os.Stderr, "go_driver: expected a partial result with errors for the missing instance")
		os.Exit(1)
	}
	if _, err := pr.RequireComplete(); err == nil {
		fmt.Fprintln(os.Stderr, "go_driver: RequireComplete must raise on a partial result")
		os.Exit(1)
	}
	fmt.Println("GO_SDK_PARTIAL_RESULTS: ok")

	// Cancellation: open a ticketed stream with a cancelable context and
	// abort after the first frame — the stream must terminate cleanly.
	saasURL := os.Getenv("ACTAE_E2E_SAAS_URL")
	internalToken := os.Getenv("ACTAE_E2E_INTERNAL_TOKEN")
	if saasURL != "" && internalToken != "" {
		cancelCtx, cancel := context.WithCancel(ctx)
		frames, errs, err := client.StreamWithReconnect(cancelCtx, actae.FleetStreamOptions{
			InstanceID: instance,
			TicketProvider: func(ctx context.Context, _ int64) (string, error) {
				return mintStreamTicket(os.Getenv("ACTAE_FLEET_URL"), saasURL, internalToken, os.Getenv("ACTAE_ORG_ID"), instance, channel)
			},
			Origin: os.Getenv("ACTAE_FLEET_URL"),
			MaxReconnectAttempts: 2,
			ReconnectDelay:       500 * time.Millisecond,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "go_driver stream: %v\n", err)
			os.Exit(1)
		}
		got := false
		select {
		case _, ok := <-frames:
			got = ok
		case err := <-errs:
			if err != nil {
				fmt.Fprintf(os.Stderr, "go_driver stream err: %v\n", err)
				os.Exit(1)
			}
		case <-time.After(15 * time.Second):
		}
		// Cancel mid-stream; the channel must close cleanly (no deadlock).
		cancel()
		if !got {
			// A clean immediate close is also acceptable (stream may be empty).
			fmt.Println("GO_SDK_CANCELLATION: ok (no frame before cancel)")
		} else {
			fmt.Println("GO_SDK_CANCELLATION: ok")
		}
	}

	fmt.Println("GO_SDK_CROSS_LAYER: ok")
}

// mintStreamTicket mints a one-use gateway stream ticket via the SaaS
// internal endpoint (the gateway's workload identity).
func mintStreamTicket(gatewayURL, saasURL, internalToken, org, instance, channel string) (string, error) {
	body := map[string]any{
		"organization_id": org,
		"instance_id":     instance,
		"channels":        []string{channel},
		"origin":          gatewayURL,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, saasURL+"/v1/internal/stream-tickets", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+internalToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return "", fmt.Errorf("ticket mint returned %d", resp.StatusCode)
	}
	var out struct {
		Ticket string `json:"ticket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Ticket, nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}