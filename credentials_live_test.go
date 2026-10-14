package actae

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLiveCredentialsFile proves the zero-config path against a real instance:
// the client is configured *only* by a written client.json (env credentials are
// cleared), and it records an event. Opt-in — skipped unless ACTAE_URL and
// ACTAE_API_KEY are set (the same gate as the other live suites). Run with:
//
//	ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... go test -run TestLiveCredentialsFile ./...
func TestLiveCredentialsFile(t *testing.T) {
	url := os.Getenv("ACTAE_URL")
	key := os.Getenv("ACTAE_API_KEY")
	if url == "" || key == "" {
		t.Skip("ACTAE_URL/ACTAE_API_KEY not set")
	}

	dir := t.TempDir()
	body, err := json.Marshal(map[string]string{"endpoint": url, "api_key": key})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}

	// Make the file the only credential source.
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	t.Setenv("ACTAE_API_KEY", "")
	t.Setenv("ACTAE_URL", "")
	t.Setenv("ACTAE_WS_URL", "")

	client, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv from file: %v", err)
	}
	channel := fmt.Sprintf("sdk-creds-%d", time.Now().UnixNano())
	if _, err := client.Record(
		context.Background(), channel, "sdk.credentials",
		map[string]any{"ok": true}, RecordOptions{Actor: "go-live"},
	); err != nil {
		t.Fatalf("Record using file credentials: %v", err)
	}
}
