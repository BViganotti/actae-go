package actae

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func specPath(name string) string { return filepath.Join("..", "..", "spec", "api-parity", "v1", name) }

func TestGeneratedManifestMatchesSource(t *testing.T) {
	var source struct {
		Operations []struct{ ID, Method, Path, Action string } `json:"operations"`
	}
	raw, err := os.ReadFile(specPath("manifest.json"))
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("monorepo spec unavailable (public mirror): %v", err)
		}
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	if len(source.Operations) != 76 || len(ManifestOperations) != len(source.Operations) {
		t.Fatalf("operation count drift: source=%d generated=%d", len(source.Operations), len(ManifestOperations))
	}
	for _, want := range source.Operations {
		got, ok := ManifestOperationByID(want.ID)
		if !ok {
			t.Fatalf("missing generated operation %s", want.ID)
		}
		if got.Method != want.Method || got.Path != want.Path || got.Action != want.Action {
			t.Errorf("%s drift: %#v vs %#v", want.ID, got, want)
		}
	}
}

func TestSharedGoldenVectors(t *testing.T) {
	var fixture struct {
		Vectors []struct {
			Scope       string         `json:"scope"`
			Type        string         `json:"type"`
			Channel     string         `json:"channel"`
			Step        string         `json:"step"`
			Canonical   string         `json:"canonical"`
			OperationID string         `json:"operation_id"`
			Payload     map[string]any `json:"payload"`
			Metadata    map[string]any `json:"metadata"`
		} `json:"deterministic_operation_keys"`
	}
	raw, err := os.ReadFile(specPath("golden_vectors.json"))
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("monorepo spec unavailable (public mirror): %v", err)
		}
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, v := range fixture.Vectors {
		if got := CanonicalStepContent(v.Payload, v.Metadata); got != v.Canonical {
			t.Errorf("canonical drift for %s: %q", v.Channel, got)
		}
		if got := DeterministicOperationKey(v.Scope, v.Type, v.Channel, v.Step, v.Canonical); got != v.OperationID {
			t.Errorf("operation id drift for %s: %s", v.Channel, got)
		}
	}
}

func TestCallManifestUsesTypedGatewayDispatch(t *testing.T) {
	var gotPath string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"fleet"}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{Endpoint: s.URL, OrganizationToken: "org", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CallManifest(context.Background(), "actae.api.v1.events.replay", "inst-1", map[string]string{"channel_id": "run"}, url.Values{"limit": {"2"}}, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.events.replay" {
		t.Fatalf("path = %s", gotPath)
	}
}

func TestTokenExchangeSendsRequiredBody(t *testing.T) {
	var body map[string]any
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/fleet/token" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"fleet-tok","expires_in":300}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{
		Endpoint:          s.URL,
		OrganizationToken: "org",
		OrganizationID:    "org-1",
		RequestedScopes:   []string{"events:read"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := c.token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "fleet-tok" {
		t.Fatalf("token = %s", tok)
	}
	if body["purpose"] != "fleet_access" {
		t.Fatalf("purpose = %v", body["purpose"])
	}
	if body["organization_id"] != "org-1" {
		t.Fatalf("organization_id = %v", body["organization_id"])
	}
	scopes, _ := body["scopes"].([]any)
	if len(scopes) != 1 || scopes[0] != "events:read" {
		t.Fatalf("scopes = %v", body["scopes"])
	}
}

func TestTokenExchangeDefaultScopesIsLeastPrivilege(t *testing.T) {
	var body map[string]any
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"t","expires_in":300}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{
		Endpoint:          s.URL,
		OrganizationToken: "org",
		OrganizationID:    "org-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.token(context.Background()); err != nil {
		t.Fatal(err)
	}
	scopes, _ := body["scopes"].([]any)
	if len(scopes) == 0 {
		t.Fatalf("default scopes must not be empty")
	}
	for _, s := range scopes {
		if !containsStr(DefaultFleetScopes, s.(string)) {
			t.Fatalf("scope %v not in default", s)
		}
	}
}

func containsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func TestEveryManifestOperationHasFleetBinding(t *testing.T) {
	var paths []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/fleet/token" {
			_, _ = w.Write([]byte(`{"access_token":"fleet"}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer s.Close()
	c, err := NewFleetClient(FleetClientOptions{Endpoint: s.URL, OrganizationToken: "org", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	fleetSupported := 0
	for id, op := range ManifestOperations {
		params := map[string]string{}
		confirm := ""
		for i := 0; i < len(op.Path); {
			start := strings.Index(op.Path[i:], "{")
			if start < 0 {
				break
			}
			start += i
			end := strings.Index(op.Path[start:], "}")
			if end < 0 {
				t.Fatalf("malformed manifest path %s", op.Path)
			}
			end += start
			name := op.Path[start+1 : end]
			value := "value-" + name
			params[name] = value
			if confirm == "" {
				confirm = value
			}
			i = end + 1
		}
		cTarget := ""
		if op.Confirmation {
			cTarget = confirm
		}
		if !op.FleetSupported {
			_, err := c.CallManifest(context.Background(), id, "inst-1", params, nil, map[string]any{}, "stable-key", cTarget)
			if err == nil {
				t.Fatalf("%s: expected UnsupportedOperation error for non-fleet op", id)
			}
			continue
		}
		fleetSupported++
		if _, err := c.CallManifest(context.Background(), id, "inst-1", params, nil, map[string]any{}, "stable-key", cTarget); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	if len(paths) != fleetSupported+1 { // + token exchange
		t.Fatalf("dispatch count = %d, want %d (fleet-supported only)", len(paths), fleetSupported+1)
	}
}
