package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer lets a test read slog output while a heartbeat goroutine writes it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestSurfaceHelp(t *testing.T) {
	text := surfaceHelp()
	for _, want := range []string{
		"surface <connector-url> <my-key> [listen-addr]",
		"--heartbeat-interval", "30s",
		"--resume-window", "60s",
		"--pending-poll", "2s",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("help missing %q\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--") && strings.Contains(strings.Fields(line)[0], "token") {
			t.Fatalf("flag name contains token: %s", line)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := run([]string{"--help"}, func(string) (string, bool) { return "", false }, logger, strings.NewReader(""))
	if !errors.Is(err, errHelp) {
		t.Fatalf("help: %v", err)
	}
}

func TestSurfaceExitsWithoutToken(t *testing.T) {
	var posts int
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/peers" {
			posts++
			gotAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := run([]string{srv.URL, "peer-key", "127.0.0.1:0"}, func(string) (string, bool) { return "", false }, logger, strings.NewReader(""))
	if err == nil || err.Error() != "SURFACE_CONNECTOR_TOKEN is required" {
		t.Fatalf("empty token: %v", err)
	}
	if posts != 0 {
		t.Fatalf("empty token posted %d", posts)
	}

	err = run([]string{srv.URL, "peer-key", "127.0.0.1:0"}, func(k string) (string, bool) {
		if k == "SURFACE_CONNECTOR_TOKEN" {
			return "surface-test-bearer", true
		}
		return "", false
	}, logger, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("posts %d", posts)
	}
	if gotAuth != "Bearer surface-test-bearer" {
		t.Fatalf("auth %q", gotAuth)
	}
}

func TestHeartbeatRepeatsPresence(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	a := &app{
		base:       srv.URL,
		myKey:      "busy-key",
		listenAddr: "203.0.113.10:10",
		token:      "surface-test-bearer",
		presence:   "busy",
		ctx:        context.Background(),
	}
	if err := a.register("busy"); err != nil {
		t.Fatal(err)
	}
	if err := a.heartbeatOnce(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), bodies...)
	gotAuth := append([]string(nil), auths...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("posts %d", len(got))
	}
	for _, body := range got {
		if !strings.Contains(body, `"state":"busy"`) || strings.Contains(body, "available") {
			t.Fatalf("body %s", body)
		}
		if !strings.Contains(body, "busy-key") || !strings.Contains(body, "203.0.113.10:10") {
			t.Fatalf("body %s", body)
		}
	}
	for _, auth := range gotAuth {
		if auth != "Bearer surface-test-bearer" {
			t.Fatalf("auth %q", auth)
		}
	}
	a.inChat = true
	a.base = "http://127.0.0.1:1"
	if err := a.heartbeatOnce(); err == nil {
		t.Fatal("expected refresh error")
	}
	if !a.inChat || a.chatConn != nil {
		t.Fatal("failed refresh changed the chat")
	}
}

func TestLogsOmitKeyAndAddress(t *testing.T) {
	const (
		key   = "peer-key-DO-NOT-LOG"
		addr  = "192.0.2.10:9001"
		token = "bearer-DO-NOT-LOG"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/pending" {
			_, _ = w.Write([]byte("[]"))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	err := run([]string{srv.URL, key, addr}, func(k string) (string, bool) {
		if k == "SURFACE_CONNECTOR_TOKEN" {
			return token, true
		}
		return "", false
	}, logger, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dead := "http://127.0.0.1:1"
	a := &app{
		base:       dead,
		myKey:      key,
		listenAddr: addr,
		token:      token,
		heartbeat:  20 * time.Millisecond,
		logger:     logger,
		ctx:        ctx,
		presence:   "available",
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.heartbeatLoop()
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	text := buf.String()
	if !strings.Contains(text, "registered") || !strings.Contains(text, "heartbeat") {
		t.Fatalf("log missing class: %s", text)
	}
	for _, secret := range []string{key, addr, token, dead} {
		if strings.Contains(text, secret) {
			t.Fatalf("log contains %q\n%s", secret, text)
		}
	}
}

func TestRequestReadableFollowsStatus(t *testing.T) {
	const proof = "proof-DO-NOT-LOG"
	var status int
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/requests/req-1" {
			t.Errorf("path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer surface-test-bearer" {
			t.Errorf("auth %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	var buf bytes.Buffer
	a := &app{
		base:   srv.URL,
		token:  "surface-test-bearer",
		ctx:    context.Background(),
		logger: slog.New(slog.NewTextHandler(&buf, nil)),
	}
	status = http.StatusNotFound
	body = "request not found\n"
	if a.requestReadable("req-1") {
		t.Fatal("missing request was readable")
	}
	status = http.StatusOK
	body = `{"status":"accepted","admission_proof":"` + proof + `"}`
	if !a.requestReadable("req-1") {
		t.Fatal("accepted request was refused")
	}
	status = http.StatusOK
	body = `{"status":"pending"}`
	if a.requestReadable("req-1") {
		t.Fatal("pending request was readable")
	}
	if strings.Contains(buf.String(), proof) {
		t.Fatalf("log contains proof: %s", buf.String())
	}
}
