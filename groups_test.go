package actae

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupSessionDurableWaitReleaseAndPromote(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/execution-groups/g/members/research/messages":
			if polls.Add(1) < 2 {
				_, _ = w.Write([]byte(`{"messages":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"messages":[{"message_id":"m1","group_id":"g","from_member_id":"router","to_member_id":"research","type":"done","payload":{},"source_event_id":"s","delivery_event_id":"d","status":"delivered","created_at":"2026-01-01T00:00:00Z"}]}`))
		case "/api/v1/execution-groups/g/members/research/claim":
			_, _ = w.Write([]byte(`{"lease":{"group_id":"g","member_id":"research","owner_id":"worker","generation":2,"lease_until":"2099-01-01T00:00:00Z"}}`))
		case "/api/v1/execution-groups/g/members/research/release":
			_, _ = w.Write([]byte(`{"status":"released"}`))
		case "/api/v1/execution-group-forks/g/members/research/promote":
			_, _ = w.Write([]byte(`{"fork":{"fork_group_id":"g","member_policies":{"research":"reactive"}}}`))
		default:
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "not_found"})
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientOptions{APIKey: "key", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	group, err := client.ExecutionGroup("g")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	message, err := group.WaitFor(ctx, "research", "done", nil, time.Millisecond)
	if err != nil || message.MessageID != "m1" {
		t.Fatalf("wait: %#v %v", message, err)
	}
	member, _ := group.Member("research")
	if _, err = member.Claim(ctx, "worker", 30); err != nil {
		t.Fatal(err)
	}
	if err = member.Release(ctx); err != nil {
		t.Fatal(err)
	}
	receipt, err := group.Promote(ctx, "research")
	if err != nil || receipt["fork_group_id"] != "g" {
		t.Fatalf("promote: %#v %v", receipt, err)
	}
}

func TestGroupMessageFromWebSocketEvent(t *testing.T) {
	event := Event{ID: "delivery", EventType: "message.received", Payload: map[string]any{
		"message_id": "m1", "group_id": "g", "from": map[string]any{"member": "router"},
		"to": map[string]any{"member": "a"}, "type": "task.done", "payload": map[string]any{"ok": true},
	}}
	message, ok := groupMessageFromEvent(event)
	if !ok || message.MessageID != "m1" || message.ToMemberID != "a" || message.DeliveryEventID != "delivery" {
		t.Fatalf("unexpected websocket group message: %#v", message)
	}
}
