package actae

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCredentials(t *testing.T, dir, body string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "client.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestLoadClientCredentials(t *testing.T) {
	dir := t.TempDir()
	writeCredentials(t, dir, `{"schema":"actae.client/v1","endpoint":"http://127.0.0.1:8002/","api_key":"sk-abc"}`)
	t.Setenv("ACTAE_CONFIG_DIR", dir)

	endpoint, key, ok := loadClientCredentials()
	if !ok {
		t.Fatal("expected credentials to load")
	}
	if endpoint != "http://127.0.0.1:8002" {
		t.Errorf("endpoint = %q", endpoint)
	}
	if key != "sk-abc" {
		t.Errorf("key = %q", key)
	}
}

func TestLoadClientCredentialsMissing(t *testing.T) {
	t.Setenv("ACTAE_CONFIG_DIR", t.TempDir())
	if _, _, ok := loadClientCredentials(); ok {
		t.Error("expected missing file to be absent")
	}
}

func TestLoadClientCredentialsInvalid(t *testing.T) {
	cases := []string{
		`{not json`,
		`[]`,
		`{"endpoint":"ftp://x","api_key":"sk-1"}`,
		`{"endpoint":"http://","api_key":"sk-1"}`,
		`{"endpoint":"http://127.0.0.1:8002","api_key":""}`,
		`{"endpoint":"http://127.0.0.1:8002","api_key":"sk a b"}`,
		`{"endpoint":"http://127.0.0.1:8002"}`,
	}
	for _, body := range cases {
		dir := t.TempDir()
		writeCredentials(t, dir, body)
		t.Setenv("ACTAE_CONFIG_DIR", dir)
		if _, _, ok := loadClientCredentials(); ok {
			t.Errorf("expected invalid payload to be absent: %s", body)
		}
	}
}

func TestLoadClientCredentialsRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.json")
	if err := os.WriteFile(real, []byte(`{"endpoint":"http://evil","api_key":"sk-evil"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(dir, "client.json")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	if _, _, ok := loadClientCredentials(); ok {
		t.Error("must refuse to follow a symlinked credentials file")
	}
}

func TestLoadClientCredentialsSingleSecondaryFallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, "client-8003.json"),
		[]byte(`{"endpoint":"http://127.0.0.1:8003","api_key":"sk-secondary"}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	endpoint, key, ok := loadClientCredentials()
	if !ok || endpoint != "http://127.0.0.1:8003" || key != "sk-secondary" {
		t.Fatalf("secondary fallback failed: %q %q %v", endpoint, key, ok)
	}
}

func TestLoadClientCredentialsMultipleSecondariesAmbiguous(t *testing.T) {
	dir := t.TempDir()
	for _, port := range []string{"8003", "8004"} {
		if err := os.WriteFile(
			filepath.Join(dir, "client-"+port+".json"),
			[]byte(`{"endpoint":"http://127.0.0.1:`+port+`","api_key":"sk-x"}`),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	if _, _, ok := loadClientCredentials(); ok {
		t.Error("multiple secondaries must be ambiguous (ignored)")
	}
}

func TestLoadClientCredentialsDefaultWinsOverSecondary(t *testing.T) {
	dir := t.TempDir()
	writeCredentials(t, dir, `{"endpoint":"http://127.0.0.1:8002","api_key":"sk-default"}`)
	if err := os.WriteFile(
		filepath.Join(dir, "client-8003.json"),
		[]byte(`{"endpoint":"http://127.0.0.1:8003","api_key":"sk-secondary"}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	if _, key, _ := loadClientCredentials(); key != "sk-default" {
		t.Errorf("default should win: %q", key)
	}
}

func TestNewClientFromEnvUsesFile(t *testing.T) {
	dir := t.TempDir()
	writeCredentials(t, dir, `{"endpoint":"http://127.0.0.1:8002","api_key":"sk-from-file"}`)
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	t.Setenv("ACTAE_API_KEY", "")
	t.Setenv("ACTAE_URL", "")

	c, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	if c.apiKey != "sk-from-file" || c.endpoint != "http://127.0.0.1:8002" {
		t.Errorf("file not applied: %q %q", c.apiKey, c.endpoint)
	}
}

func TestNewClientFromEnvEnvWinsOverFile(t *testing.T) {
	dir := t.TempDir()
	writeCredentials(t, dir, `{"endpoint":"http://127.0.0.1:8002","api_key":"sk-from-file"}`)
	t.Setenv("ACTAE_CONFIG_DIR", dir)
	t.Setenv("ACTAE_API_KEY", "sk-from-env")
	t.Setenv("ACTAE_URL", "http://env-host:9000")

	c, err := NewClientFromEnv(ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	if c.apiKey != "sk-from-env" || c.endpoint != "http://env-host:9000" {
		t.Errorf("env should win: %q %q", c.apiKey, c.endpoint)
	}
}

func TestNewClientFromEnvNoCredentialsIsActionable(t *testing.T) {
	t.Setenv("ACTAE_CONFIG_DIR", t.TempDir())
	t.Setenv("ACTAE_API_KEY", "")
	t.Setenv("ACTAE_URL", "")
	_, err := NewClientFromEnv(ClientOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "actae login") {
		t.Errorf("error should mention `actae login`: %q", got)
	}
}
