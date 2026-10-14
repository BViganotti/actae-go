package actae

// Parity tests mirroring the Python SDK's test_request_helper.py and
// test_sdk_edge_cases.py suites: content-type matrix, error-code matrix,
// URL/TLS construction, payload edge cases, and normalize edge cases.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Content-type matrix (mirrors test_request_helper.py)
// ---------------------------------------------------------------------------

func TestContentTypeMatrix(t *testing.T) {
	routes := map[string]routeHandler{
		"GET /text-plain": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("plain"))
		},
		"GET /text-html": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte("<html>"))
		},
		"GET /application-text": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Header().Set("Content-Type", "application/text")
			w.Write([]byte("app-text"))
		},
		"GET /unknown-type": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write([]byte(`{"ok": true}`))
		},
		"GET /no-content-type": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"ok": true}`))
		},
	}
	ts := newTestServer(t, routes)
	c := newTestClient(t, ts)
	ctx := context.Background()

	if v, err := c.do(ctx, http.MethodGet, "/text-plain", nil, nil, nil); err != nil || v != "plain" {
		t.Errorf("text/plain = %v %v", v, err)
	}
	if v, err := c.do(ctx, http.MethodGet, "/text-html", nil, nil, nil); err != nil || v != "<html>" {
		t.Errorf("text/html = %v %v", v, err)
	}
	if v, err := c.do(ctx, http.MethodGet, "/application-text", nil, nil, nil); err != nil || v != "app-text" {
		t.Errorf("application/text = %v %v", v, err)
	}
	if v, err := c.do(ctx, http.MethodGet, "/unknown-type", nil, nil, nil); err != nil {
		t.Errorf("unknown content type: %v", err)
	} else if m, ok := v.(map[string]any); !ok || m["ok"] != true {
		t.Errorf("unknown content type parsed as %T", v)
	}
	if v, err := c.do(ctx, http.MethodGet, "/no-content-type", nil, nil, nil); err != nil {
		t.Errorf("no content type: %v", err)
	} else if m, ok := v.(map[string]any); !ok || m["ok"] != true {
		t.Errorf("no content type parsed as %T", v)
	}
}

// ---------------------------------------------------------------------------
// Error-code matrix (mirrors test_request_helper.py)
// ---------------------------------------------------------------------------

func TestErrorCodeMatrix(t *testing.T) {
	cases := []struct {
		status int
		body   string
		check  func(err error) bool
	}{
		{400, `{"error": "bad"}`, func(e error) bool { return apiStatus(e) == 400 }},
		// Python SDK parity: HTTP 401 surfaces as AuthError, not APIError.
		{401, `{"error": "unauthorized"}`, func(e error) bool { _, ok := e.(*AuthError); return ok }},
		{404, `{"error": "missing"}`, func(e error) bool { return apiStatus(e) == 404 }},
		{500, `{"error": "boom"}`, func(e error) bool { return apiStatus(e) == 500 }},
		{503, `{"error": "unavailable"}`, func(e error) bool { return apiStatus(e) == 503 }},
		{429, `{"error": "slow", "retry_after_seconds": 5}`, func(e error) bool {
			rl, ok := e.(*RateLimitError)
			return ok && rl.RetryAfterSeconds == 5
		}},
		// text error body still maps to APIError
		{502, `gateway error`, func(e error) bool {
			ae, ok := e.(*APIError)
			return ok && ae.StatusCode == 502 && strings.Contains(ae.Error(), "gateway error")
		}},
	}
	for _, c := range cases {
		ts := newTestServer(t, map[string]routeHandler{
			"GET /err": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
				w.WriteHeader(c.status)
				w.Write([]byte(c.body))
			},
		})
		client := newTestClient(t, ts)
		_, err := client.do(context.Background(), http.MethodGet, "/err", nil, nil, nil)
		if err == nil {
			t.Fatalf("status %d: expected error", c.status)
		}
		if !c.check(err) {
			t.Errorf("status %d: got %T (%v)", c.status, err, err)
		}
	}
}

func apiStatus(err error) int {
	if ae, ok := err.(*APIError); ok {
		return ae.StatusCode
	}
	return -1
}

// ---------------------------------------------------------------------------
// URL / headers / timeout construction (mirrors test_request_helper.py)
// ---------------------------------------------------------------------------

func TestEndpointTrailingSlash(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			if r.URL.Path != "/api/v1/channels" {
				t.Errorf("path = %q", r.URL.Path)
			}
			w.Write([]byte(`{"channels": []}`))
		},
	})
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: ts.server.URL + "/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListChannels(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCustomHeadersMerged(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/channels": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			if r.Header.Get("x-custom") != "yes" {
				t.Error("custom header missing")
			}
			if r.Header.Get("x-api-key") != "sk-test-123" {
				t.Error("api key header missing alongside custom")
			}
			w.Write([]byte(`{"channels": []}`))
		},
	})
	c := newTestClient(t, ts)
	h := http.Header{"x-custom": {"yes"}}
	if _, err := c.do(context.Background(), http.MethodGet, "/api/v1/channels", nil, nil, h); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultTimeout(t *testing.T) {
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://localhost:1"})
	if err != nil {
		t.Fatal(err)
	}
	if c.timeout != 30*1000*1000*1000 {
		t.Errorf("default timeout = %v", c.timeout)
	}
}

func TestEmptyAndNullJSONResponses(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /empty": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(""))
		},
		"GET /list": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`[]`))
		},
		"GET /null": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`null`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	if v, err := c.do(ctx, http.MethodGet, "/empty", nil, nil, nil); err != nil || v != nil {
		t.Errorf("empty body = %v %v", v, err)
	}
	if v, err := c.do(ctx, http.MethodGet, "/list", nil, nil, nil); err != nil {
		t.Errorf("list body: %v", err)
	} else if _, ok := v.([]any); !ok {
		t.Errorf("list body parsed as %T", v)
	}
	if v, err := c.do(ctx, http.MethodGet, "/null", nil, nil, nil); err != nil || v != nil {
		t.Errorf("null body = %v %v", v, err)
	}
}

// ---------------------------------------------------------------------------
// TLS construction (mirrors test_request_helper.py TLS tests)
// ---------------------------------------------------------------------------

func TestTLSConfigDefaultsToNil(t *testing.T) {
	cfg, err := buildTLSConfig(ClientOptions{})
	if err != nil || cfg != nil {
		t.Errorf("expected (nil, nil), got %v %v", cfg, err)
	}
}

func TestTLSPrebuiltConfigPassedThrough(t *testing.T) {
	prebuilt := &tls.Config{InsecureSkipVerify: true}
	cfg, err := buildTLSConfig(ClientOptions{TLSConfig: prebuilt})
	if err != nil || cfg != prebuilt {
		t.Errorf("expected prebuilt config back, got %v %v", cfg, err)
	}
}

// writeCertFiles generates a real self-signed CA and leaf cert/key pair and
// writes them to temp files, returning (caPath, certPath, keyPath).
func writeCertFiles(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()

	mkCA := func() (*x509.Certificate, *rsa.PrivateKey) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: "test-ca"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(24 * time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return cert, key
	}

	caCert, caKey := mkCA()

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writePEM := func(p, typ string, der []byte) {
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePEM(caPath, "CERTIFICATE", caCert.Raw)
	writePEM(certPath, "CERTIFICATE", leafDER)
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(keyPath, "PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}

func TestTLSMissingFilesError(t *testing.T) {
	if _, err := buildTLSConfig(ClientOptions{CACert: "/nonexistent/ca.pem"}); err == nil {
		t.Error("expected error for missing CA file")
	}
	if _, err := buildTLSConfig(ClientOptions{ClientCert: "/nonexistent/cert.pem", ClientKey: "/nonexistent/key.pem"}); err == nil {
		t.Error("expected error for missing client cert")
	}
}

func TestTLSCACertLoadsRootCAs(t *testing.T) {
	ca, _, _ := writeCertFiles(t)
	cfg, err := buildTLSConfig(ClientOptions{CACert: ca})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("RootCAs not set")
	}
}

func TestTLSClientCertLoads(t *testing.T) {
	_, cert, key := writeCertFiles(t)
	cfg, err := buildTLSConfig(ClientOptions{ClientCert: cert, ClientKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil || len(cfg.Certificates) != 1 {
		t.Fatal("Certificates not set")
	}
}

func TestTLSCAAndClientCertCombined(t *testing.T) {
	ca, cert, key := writeCertFiles(t)
	cfg, err := buildTLSConfig(ClientOptions{CACert: ca, ClientCert: cert, ClientKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 {
		t.Fatal("combined TLS config incomplete")
	}
}

func TestTLSCAInvalidPEM(t *testing.T) {
	bad := writeTempFile(t, "bad-ca.pem", "not a pem")
	if _, err := buildTLSConfig(ClientOptions{CACert: bad}); err == nil {
		t.Error("expected error for invalid CA PEM")
	}
}

func TestHTTPClientInstallsTLSConfig(t *testing.T) {
	ca, cert, key := writeCertFiles(t)
	c, err := NewClient(ClientOptions{
		APIKey:     "k",
		Endpoint:   "https://actae.example.com",
		CACert:     ca,
		ClientCert: cert,
		ClientKey:  key,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := c.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", c.httpClient.Transport)
	}
	if tr.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig not installed on HTTP transport")
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Fatal("RootCAs not set on HTTP transport")
	}
	if len(tr.TLSClientConfig.Certificates) != 1 {
		t.Fatalf("client certificates = %d, want 1", len(tr.TLSClientConfig.Certificates))
	}
}

func TestHTTPClientPrebuiltTLSConfigInstalled(t *testing.T) {
	prebuilt := &tls.Config{InsecureSkipVerify: true}
	c, err := NewClient(ClientOptions{
		APIKey:    "k",
		Endpoint:  "https://actae.example.com",
		TLSConfig: prebuilt,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := c.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", c.httpClient.Transport)
	}
	if tr.TLSClientConfig != prebuilt {
		t.Fatal("prebuilt TLSConfig not installed on HTTP transport")
	}
}

func TestHTTPClientOverrideKeepsItsOwnTransport(t *testing.T) {
	ca, _, _ := writeCertFiles(t)
	override := &http.Client{}
	c, err := NewClient(ClientOptions{
		APIKey:     "k",
		Endpoint:   "https://actae.example.com",
		CACert:     ca,
		HTTPClient: override,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.httpClient != override {
		t.Fatal("HTTPClient override not used")
	}
	if c.httpClient.Transport != nil {
		t.Fatal("TLS options must not be applied to an overridden HTTP client")
	}
}

func TestHTTPClientNoTLSDefaultsToNilTransport(t *testing.T) {
	c, err := NewClient(ClientOptions{APIKey: "k", Endpoint: "http://localhost:8002"})
	if err != nil {
		t.Fatal(err)
	}
	if c.httpClient.Transport != nil {
		t.Fatalf("transport = %T, want nil (plain HTTP default)", c.httpClient.Transport)
	}
}

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// ---------------------------------------------------------------------------
// Payload and cursor edge cases (mirrors test_sdk_edge_cases.py)
// ---------------------------------------------------------------------------

func TestRecordPayloadEdgeCases(t *testing.T) {
	payloads := []any{
		map[string]any{},
		nil,
		map[string]any{"unicode": "héllo wörld ✓", "controls": "a\nb\tc"},
		map[string]any{"big": strings.Repeat("x", 100000)},
		map[string]any{"nested": map[string]any{"deep": map[string]any{"list": []any{1, "two", true, nil, 3.14}}}},
		[]any{1, 2, 3},
		"just a string",
	}
	for i, payload := range payloads {
		ts := newTestServer(t, map[string]routeHandler{
			"POST /api/v1/events/record": func(w http.ResponseWriter, r *http.Request, body map[string]any) {
				// echo the payload back so round-trip is exercised
				w.Write([]byte(`{"event": {"id": "e", "channel_id": "c", "type": "t", "payload": ` + mustJSON(body["payload"]) + `, "actor": "a", "cursor": 1, "timestamp": "t"}}`))
			},
		})
		c := newTestClient(t, ts)
		ev, err := c.Record(context.Background(), "c", "t", payload, RecordOptions{Actor: "a"})
		if err != nil {
			t.Fatalf("payload %d: %v", i, err)
		}
		if payload != nil && ev.Payload == nil {
			t.Errorf("payload %d: lost in round-trip", i)
		}
	}
}

func TestGetCursorMissingReturnsNil(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/events/cursor/empty": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"message": "no events"}`))
		},
		"GET /api/v1/events/cursor/zero": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"latest_cursor": 0}`))
		},
		"GET /api/v1/events/cursor/huge": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"latest_cursor": 9223372036854775807}`))
		},
	})
	c := newTestClient(t, ts)
	ctx := context.Background()
	cur, err := c.GetCursor(ctx, "empty")
	if err != nil || cur != nil {
		t.Errorf("missing cursor = %v %v", cur, err)
	}
	cur, err = c.GetCursor(ctx, "zero")
	if err != nil || cur == nil || *cur != 0 {
		t.Errorf("zero cursor = %v %v", cur, err)
	}
	cur, err = c.GetCursor(ctx, "huge")
	if err != nil || cur == nil || *cur != 9223372036854775807 {
		t.Errorf("huge cursor = %v %v", cur, err)
	}
}

func TestReplayLimitDefaults(t *testing.T) {
	var gotLimit string
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/events/replay/c": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			gotLimit = r.URL.Query().Get("limit")
			w.Write([]byte(`{"events": []}`))
		},
	})
	c := newTestClient(t, ts)
	if _, err := c.Replay(context.Background(), "c", ReplayOptions{}); err != nil {
		t.Fatal(err)
	}
	if gotLimit != "100" {
		t.Errorf("default limit = %q, want 100", gotLimit)
	}
	if _, err := c.Replay(context.Background(), "c", ReplayOptions{Limit: -5}); err != nil {
		t.Fatal(err)
	}
	if gotLimit != "100" {
		t.Errorf("negative limit = %q, want default 100", gotLimit)
	}
}

func TestChannelIDGrammar(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /api/v1/events/cursor/valid.channel_1": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"latest_cursor": 1}`))
		},
	})
	c := newTestClient(t, ts)
	// Valid grammar: [A-Za-z0-9._:-]
	if _, err := c.GetCursor(context.Background(), "valid.channel_1"); err != nil {
		t.Fatalf("valid channel: %v", err)
	}
	// Reserved characters must fail fast on the client (audit item 10) —
	// the previous behavior let slashes into the URL and created channels
	// that could never be re-addressed over HTTP/WS.
	for _, id := range []string{"channel/with/slashes", "has space", "has\"quote", "path//double", ""} {
		if _, err := c.GetCursor(context.Background(), id); err == nil {
			t.Fatalf("expected grammar rejection for %q", id)
		}
	}
}

func TestEscapePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://h/api/v1/events/replay/chan 1", "http://h/api/v1/events/replay/chan%201"},
		{"http://h/api/v1/events/cursor/c", "http://h/api/v1/events/cursor/c"},
		{"http://h/a b?q=1 2", "http://h/a%20b?q=1 2"},
		{"not a url", "not a url"},
		{"http://h", "http://h"},
		{"http://h/", "http://h/"},
		{"http://h/a%2Fb", "http://h/a%2Fb"}, // pre-escaped preserved
	}
	for _, c := range cases {
		if got := escapePath(c.in); got != c.want {
			t.Errorf("escapePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Normalize edge cases (mirrors test_client_ws.py normalize tests)
// ---------------------------------------------------------------------------

func TestNormalizeEdgeCases(t *testing.T) {
	if got := normalizeMsg(map[string]any{}); len(got) != 0 {
		t.Errorf("empty dict: %v", got)
	}
	unknown := map[string]any{"Mystery": map[string]any{"a": 1}}
	if got := normalizeMsg(unknown); got["type"] != nil {
		t.Errorf("unknown tag should not add type: %v", got)
	}
	lower := map[string]any{"broadcast": map[string]any{"topic": "t"}}
	if got := normalizeMsg(lower); got["type"] != nil {
		t.Errorf("case-sensitive: %v", got)
	}
	noType := map[string]any{"topic": "t"}
	if got := normalizeMsg(noType); got["type"] != nil {
		t.Errorf("missing type: %v", got)
	}
}

func TestDispatchSubscriptionPayloadTopic(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	// subscription confirmation with topic only in payload (no top-level topic)
	ts.sendAll(`{"Subscription": {"event": "subscribed", "cursor": 3, "payload": {"topic": "payload-topic", "cursor": 3}}}`)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if topics := c.SubscribedTopics(); topics["payload-topic"] == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("payload-based topic not subscribed: %v", c.SubscribedTopics())
}

func TestReadinessAllDegraded(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /readyz": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"status": "unavailable", "readiness_checks": {"database_ready": false, "capacity_available": false}}`))
		},
	})
	c := newTestClient(t, ts)
	rr, err := c.ReadinessCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rr.Status != "unavailable" || rr.DatabaseReady || rr.CapacityAvailable {
		t.Errorf("degraded readiness parsed wrong: %+v", rr)
	}
}

func TestHealthNoComponents(t *testing.T) {
	ts := newTestServer(t, map[string]routeHandler{
		"GET /healthz": func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
			w.Write([]byte(`{"status": "ok"}`))
		},
	})
	c := newTestClient(t, ts)
	hs, err := c.HealthCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if hs.Status != "ok" || len(hs.Components) != 0 {
		t.Errorf("no-components health parsed wrong: %+v", hs)
	}
}

func TestPublishEmptyPayload(t *testing.T) {
	ts := newWSTestServer(t)
	c := newWSClient(t, ts)
	defer c.Disconnect()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev, err := c.Publish(context.Background(), "empty-topic", map[string]any{})
	if err != nil {
		t.Fatalf("publish empty payload: %v", err)
	}
	if ev.Cursor != 11 { // server test ack cursor
		t.Errorf("ack cursor = %d", ev.Cursor)
	}
	if ev.Payload == nil {
		t.Error("empty payload lost")
	}
}
