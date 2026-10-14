package actae

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// clientCredentials is the subset of ~/.actae/client.json the SDK reads. The
// Rust server writes the full document (schema/instance/created_at); unknown
// fields are ignored for forward compatibility.
type clientCredentials struct {
	Endpoint string `json:"endpoint"`
	APIKey   string `json:"api_key"`
}

// configDir returns the per-user Actae config directory (ACTAE_CONFIG_DIR,
// default ~/.actae) — the same directory the Rust server writes client.json to.
func configDir() string {
	if dir := os.Getenv("ACTAE_CONFIG_DIR"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".actae")
}

// validEndpoint reports whether endpoint is an http(s) URL with a host.
func validEndpoint(endpoint string) bool {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		return false
	}
	if len(endpoint) <= len("https://") {
		return false
	}
	for _, r := range endpoint {
		if r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// validAPIKey reports whether key is a plausible API key (non-empty, no
// whitespace or control characters).
func validAPIKey(key string) bool {
	if key == "" {
		return false
	}
	for _, r := range key {
		if r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// loadCredentialsFile parses one credentials file. A missing, unreadable,
// symlinked, malformed, or invalid file is treated as absent.
func loadCredentialsFile(path string) (endpoint, apiKey string, ok bool) {
	// Refuse to follow a symlink (defense in depth, parity with the Rust writer).
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink != 0 {
		return "", "", false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", false
	}
	var creds clientCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return "", "", false
	}
	creds.Endpoint = strings.TrimSpace(creds.Endpoint)
	creds.Endpoint = strings.TrimRight(creds.Endpoint, "/")
	if !validEndpoint(creds.Endpoint) || !validAPIKey(creds.APIKey) {
		return "", "", false
	}
	return creds.Endpoint, creds.APIKey, true
}

// loadClientCredentials reads ~/.actae/client.json (written by `actae` on first
// boot or `actae login`). When it is absent but exactly one per-instance
// `client-<port>.json` exists (a machine running more than one instance), that
// file is used; multiple are ambiguous and ignored.
func loadClientCredentials() (endpoint, apiKey string, ok bool) {
	dir := configDir()
	if dir == "" {
		return "", "", false
	}
	if e, k, ok := loadCredentialsFile(filepath.Join(dir, "client.json")); ok {
		return e, k, true
	}
	matches, err := filepath.Glob(filepath.Join(dir, "client-*.json"))
	if err != nil || len(matches) != 1 {
		return "", "", false
	}
	return loadCredentialsFile(matches[0])
}
