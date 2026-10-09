package actae

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestFleetClientUsesCanonicalRoutes(t *testing.T) {
	var got []struct{ method, path string }
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, struct{ method, path string }{r.Method, r.URL.Path})
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/fleet/token" {
			_, _ = w.Write([]byte(`{"access_token":"fleet"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{Endpoint: s.URL, OrganizationToken: "org", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = c.Query(ctx, "i-1", url.Values{"limit": {"10"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Replay(ctx, "i-1", "run", url.Values{"cursor": {"3"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SaveState(ctx, "i-1", "run", 7, map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LatestState(ctx, "i-1", "run"); err != nil {
		t.Fatal(err)
	}
	want := []struct{ method, path string }{{"POST", "/api/v1/fleet/token"}, {"POST", "/v1/organizations/org-1/events/query"}, {"POST", "/v1/organizations/org-1/instances/i-1/ops/actae.api.v1.events.replay"}, {"PUT", "/v1/organizations/org-1/instances/i-1/state/run"}, {"POST", "/v1/organizations/org-1/instances/i-1/ops/actae.api.v1.state.load"}}
	if len(got) != len(want) {
		t.Fatalf("requests = %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestFleetClientRejectsUnconfirmedDelete(t *testing.T) {
	c, err := NewFleetClient(FleetClientOptions{Endpoint: "https://fleet.example", OrganizationToken: "org"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Call(context.Background(), http.MethodDelete, "/api/v1/channels/run", "i-1", nil, nil, "", ""); err == nil {
		t.Fatal("expected confirmation error")
	}
}

func TestFleetStreamRequiresTicket(t *testing.T) {
	c, err := NewFleetClient(FleetClientOptions{Endpoint: "https://fleet.example", OrganizationToken: "org"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.StreamWithReconnect(context.Background(), FleetStreamOptions{}); err == nil {
		t.Fatal("expected ticket validation")
	}
}

func TestFleetQueryManyUsesTopLevelPageTokens(t *testing.T) {
	var captured map[string]any
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/fleet/token" {
			_, _ = w.Write([]byte(`{"access_token":"fleet"}`))
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[{"id":"e1"}],"page":{"next_page_tokens":{"ins-a":"tok-a","ins-b":"tok-b"},"has_more":true}}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{Endpoint: s.URL, OrganizationToken: "org", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.QueryManyWithTokens(context.Background(), []string{"ins-a", "ins-b"},
		map[string]any{"limit": 25}, map[string]string{"ins-a": "prev-a", "ins-b": "prev-b"})
	if err != nil {
		t.Fatal(err)
	}
	// Selectors are `instances`, filters are `query`, continuation is the
	// top-level `page_tokens` map.
	if captured["instances"] == nil {
		t.Fatal("missing instances selector")
	}
	query, ok := captured["query"].(map[string]any)
	if !ok || query["limit"] != float64(25) {
		t.Fatalf("query = %v", captured["query"])
	}
	pt, ok := captured["page_tokens"].(map[string]any)
	if !ok || pt["ins-a"] != "prev-a" {
		t.Fatalf("page_tokens = %v", captured["page_tokens"])
	}
	if _, insideQuery := query["page_token"]; insideQuery {
		t.Fatal("page_token must not live inside query")
	}
	// The full per-instance continuation map is preserved, not collapsed.
	if result.PageTokens["ins-a"] != "tok-a" || result.PageTokens["ins-b"] != "tok-b" {
		t.Fatalf("PageTokens = %v", result.PageTokens)
	}
}

func TestFleetQueryPagesUsesTopLevelToken(t *testing.T) {
	var captured []map[string]any
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/fleet/token" {
			_, _ = w.Write([]byte(`{"access_token":"fleet"}`))
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		captured = append(captured, body)
		w.Header().Set("Content-Type", "application/json")
		page := body["page_tokens"]
		if page == nil {
			_, _ = w.Write([]byte(`{"events":[{"id":"p1"}],"page":{"next_page_tokens":{"ins-1":"tok-1"},"has_more":true}}`))
		} else {
			_, _ = w.Write([]byte(`{"events":[{"id":"p2"}],"page":{"next_page_tokens":{},"has_more":false}}`))
		}
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{Endpoint: s.URL, OrganizationToken: "org", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	err = c.QueryPages(context.Background(), "ins-1", url.Values{"limit": {"2"}}, func(p FleetPage) error {
		ids = append(ids, p.Items[0].(map[string]any)["id"].(string))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "p1" || ids[1] != "p2" {
		t.Fatalf("ids = %v", ids)
	}
	// The continuation must be top-level page_tokens, never in query.
	second := captured[1]
	if _, ok := second["page_tokens"]; !ok {
		t.Fatalf("second request missing page_tokens: %v", captured)
	}
	if _, ok := second["query"].(map[string]any)["page_token"]; ok {
		t.Fatalf("page_token leaked into query: %v", captured)
	}
}
