package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/connector"
	"github.com/redis/go-redis/v9"
)

func TestOTELInitFailureContinues(t *testing.T) {
	t.Setenv("OTEL_STDOUT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", writeCert(t))
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdown := installTracer(ctx, logger, "127.0.0.1:9")
	if shutdown != nil {
		t.Fatal("setup failure returned a shutdown func")
	}
	if !strings.Contains(buf.String(), "otel init failed, continuing without traces") {
		t.Fatalf("log %q", buf.String())
	}

	dir := t.TempDir()
	key := make([]byte, 32)
	store, err := connector.Open(ctx, "file:"+filepath.Join(dir, "test.db"), "", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	h := connector.NewHandlers(store, nil, slog.New(slog.DiscardHandler))
	h.SetBearer("canary-token")
	mux := http.NewServeMux()
	h.Register(mux)
	req := httptest.NewRequest(http.MethodPost, "/peers", strings.NewReader(`{"key":"alice","address":"127.0.0.1:9001","state":"available"}`))
	req.Header.Set("Authorization", "Bearer canary-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"ok":true}` {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
}

func writeCert(t *testing.T) string {
	t.Helper()
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cert.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRefusedExpirySkipsListen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	called := false
	err := run(nil, func(name string) (string, bool) {
		if name == "SURFACE_SILENCE_WINDOW" {
			return "0s", true
		}
		return "", false
	}, logger, func(*http.Server) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected startup to be refused")
	}
	msg := err.Error()
	if !strings.Contains(msg, "SURFACE_SILENCE_WINDOW") || !strings.Contains(msg, "retired") {
		t.Fatalf("error %q", msg)
	}
	if strings.Contains(msg, "0s") {
		t.Fatalf("error contains the raw value: %q", msg)
	}
	if called {
		t.Fatal("listen was called")
	}
}

func TestConnectorHelp(t *testing.T) {
	text := connector.ConnectorHelp()
	for _, want := range []string{
		"--listen", ":8080",
		"--request-lifetime", "10m",
		"--heartbeat-interval", "30s",
		"--maintenance-interval", "1s",
		"--source-allowance", "60",
		"--key-allowance",
		"--max-key-chars", "128",
		"--max-address-chars", "256",
		"--max-id-chars", "64",
		"--max-body-bytes", "8192",
		"--cache-addr",
		"--secret-store-addr",
		"--trace-endpoint",
		"--config",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("help missing %q\n%s", want, text)
		}
	}
	if strings.Contains(text, "connector_token") {
		t.Fatalf("help listed a secret: %s", text)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	called := false
	err := run([]string{"--help"}, func(string) (string, bool) { return "", false }, logger, func(*http.Server) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("listen was called")
	}
}

func TestReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listened := false
	listen := func(*http.Server) error {
		listened = true
		return nil
	}
	lookup := func(string) (string, bool) { return "", false }

	if err := run([]string{"--ready"}, lookup, logger, listen); err == nil {
		t.Fatal("connection refused returned success")
	}
	if listened {
		t.Fatal("listen was called")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	status := http.StatusOK
	body := "no\n"
	var sawHealth bool
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		sawHealth = true
		mu.Lock()
		code, text := status, body
		mu.Unlock()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(text))
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	waitReadyProbe(t)
	if err := run([]string{"--ready"}, lookup, logger, listen); err == nil {
		t.Fatal("body no returned success")
	}
	mu.Lock()
	status = http.StatusServiceUnavailable
	body = "ok\n"
	mu.Unlock()
	if err := run([]string{"--ready"}, lookup, logger, listen); err == nil {
		t.Fatal("status 503 returned success")
	}
	mu.Lock()
	status = http.StatusOK
	body = "ok\n"
	mu.Unlock()
	if err := run([]string{"--ready"}, lookup, logger, listen); err != nil {
		t.Fatal(err)
	}
	if !sawHealth {
		t.Fatal("ready did not request /health")
	}
	if listened {
		t.Fatal("listen was called")
	}

	help := captureConnectorStdout(t, func() {
		if err := run([]string{"--help"}, lookup, logger, listen); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(help, "--ready") || !strings.Contains(help, "--punch") {
		t.Fatalf("help %s", help)
	}
	if listened {
		t.Fatal("listen was called")
	}
}

func waitReadyProbe(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1:8080/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("health probe did not start")
}

func captureConnectorStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestDropPrunedPeerCacheLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	keys := []string{"bare-peer-key-9f", "second-peer-key"}
	dropPrunedPeerCache(context.Background(), nil, logger, keys)
	text := buf.String()
	if !strings.Contains(text, "pruned peers") || !strings.Contains(text, "count=2") {
		t.Fatalf("log %q", text)
	}
	for _, key := range keys {
		if strings.Contains(text, key) {
			t.Fatalf("log included peer key %q in %q", key, text)
		}
	}
	quiet := buf.Len()
	dropPrunedPeerCache(context.Background(), nil, logger, nil)
	if buf.Len() != quiet {
		t.Fatalf("empty prune logged %q", buf.String()[quiet:])
	}
	dropPrunedPeerCache(context.Background(), nil, nil, keys)
}

func TestDropPrunedPeerCacheDeletesKeys(t *testing.T) {
	addr := os.Getenv("SURFACE_TEST_REDIS")
	if addr == "" {
		t.Skip("SURFACE_TEST_REDIS unset")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	cache := connector.NewRedis(addr, "")
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	cached := "prelisten-cached-" + suffix
	missing := "prelisten-missing-" + suffix
	address := "203.0.113.50:50"
	t.Cleanup(func() {
		_ = client.Del(ctx, "peer:"+cached, "peer:"+missing).Err()
	})
	if err := client.Set(ctx, "peer:"+cached, `{"key":"`+cached+`","address":"`+address+`","state":"available","last_seen":1}`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	dropPrunedPeerCache(ctx, cache, logger, []string{cached, missing})

	n, err := client.Exists(ctx, "peer:"+cached).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("pre-listen prune left the peer cache key")
	}
	text := buf.String()
	if !strings.Contains(text, "pruned peers") || !strings.Contains(text, "count=2") {
		t.Fatalf("log %q", text)
	}
	if strings.Contains(text, cached) || strings.Contains(text, missing) || strings.Contains(text, address) {
		t.Fatalf("log included a private value: %s", text)
	}
}
