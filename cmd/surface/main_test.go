package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

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
		"--max-chats", "4",
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
		listenAddr: mustAddr(t, "203.0.113.10:10"),
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
	a.base = "http://127.0.0.1:1"
	if err := a.heartbeatOnce(); err == nil {
		t.Fatal("expected refresh error")
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
		listenAddr: mustAddr(t, addr),
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

func TestSurfacePathFlags(t *testing.T) {
	help := surfaceHelp()
	if !strings.Contains(help, "--relay") || !strings.Contains(help, "--punch") {
		t.Fatalf("help\n%s", help)
	}
	def, err := parseSurface([]string{"http://connector.example:8080", "alice"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if def.relay != "" || def.punch != "connector.example:8081" {
		t.Fatalf("defaults relay %q punch %q", def.relay, def.punch)
	}
	if def.maxChats != 4 {
		t.Fatalf("default maxChats %d", def.maxChats)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "surface.env")
	if err := os.WriteFile(path, []byte("SURFACE_RELAY=file-relay:1\nSURFACE_PUNCH=10.1.1.1:8081\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lookup := func(k string) (string, bool) {
		switch k {
		case "SURFACE_RELAY":
			return "env-relay:1", true
		case "SURFACE_PUNCH":
			return "10.2.2.2:9", true
		default:
			return "", false
		}
	}
	fromEnv, err := parseSurface([]string{"--config", path, "http://127.0.0.1:8080", "alice"}, lookup)
	if err != nil || fromEnv.relay != "env-relay:1" || fromEnv.punch != "10.2.2.2:9" {
		t.Fatalf("env %+v %v", fromEnv, err)
	}
	fromFlag, err := parseSurface([]string{"--config", path, "--relay", "flag-relay:1", "--punch", "10.3.3.3:9", "http://127.0.0.1:8080", "alice"}, lookup)
	if err != nil || fromFlag.relay != "flag-relay:1" || fromFlag.punch != "10.3.3.3:9" {
		t.Fatalf("flag %+v %v", fromFlag, err)
	}
	blank, err := parseSurface([]string{"--relay", " \t ", "--punch", "  ", "http://connector.example:8080", "alice"}, nil)
	if err != nil || blank.relay != "" || blank.punch != "" {
		t.Fatalf("blank %+v %v", blank, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var runErr error
	text := captureStdout(t, func() {
		runErr = run([]string{srv.URL, "alice", "127.0.0.1:0"}, func(k string) (string, bool) {
			if k == "SURFACE_CONNECTOR_TOKEN" {
				return "dev", true
			}
			return "", false
		}, logger, strings.NewReader("quit\n"))
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, word := range []string{"lookup", "connect", "accept", "reject", "quit"} {
		if !strings.Contains(text, word) {
			t.Fatalf("commands missing %s\n%s", word, text)
		}
	}
}

func TestMaxChatsConfig(t *testing.T) {
	def, err := parseSurface([]string{"http://connector.example:8080", "alice"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if def.maxChats != 4 {
		t.Fatalf("default %d", def.maxChats)
	}

	fromFlag, err := parseSurface([]string{"--max-chats", "2", "http://connector.example:8080", "alice"}, nil)
	if err != nil || fromFlag.maxChats != 2 {
		t.Fatalf("flag %+v %v", fromFlag, err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "surface.env")
	if err := os.WriteFile(path, []byte("SURFACE_MAX_CHATS=5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := parseSurface([]string{"--config", path, "http://127.0.0.1:8080", "alice"}, nil)
	if err != nil || fromFile.maxChats != 5 {
		t.Fatalf("file %+v %v", fromFile, err)
	}

	lookup := func(k string) (string, bool) {
		if k == "SURFACE_MAX_CHATS" {
			return "3", true
		}
		return "", false
	}
	fromEnv, err := parseSurface([]string{"--config", path, "http://127.0.0.1:8080", "alice"}, lookup)
	if err != nil || fromEnv.maxChats != 3 {
		t.Fatalf("env %+v %v", fromEnv, err)
	}
	flagWins, err := parseSurface([]string{"--config", path, "--max-chats", "8", "http://127.0.0.1:8080", "alice"}, lookup)
	if err != nil || flagWins.maxChats != 8 {
		t.Fatalf("flag over env %+v %v", flagWins, err)
	}

	for _, raw := range []string{"0", "-1", "nope"} {
		_, err := parseSurface([]string{"--max-chats", raw, "http://connector.example:8080", "alice"}, nil)
		if err == nil || !strings.Contains(err.Error(), "SURFACE_MAX_CHATS not a positive integer") {
			t.Fatalf("raw %q: %v", raw, err)
		}
	}
	_, err = parseSurface([]string{"--config", path, "http://127.0.0.1:8080", "alice"}, func(k string) (string, bool) {
		if k == "SURFACE_MAX_CHATS" {
			return "0", true
		}
		return "", false
	})
	if err == nil || !strings.Contains(err.Error(), "SURFACE_MAX_CHATS not a positive integer") {
		t.Fatalf("env zero: %v", err)
	}
}

func TestConnectSkippedAtCapacity(t *testing.T) {
	var connects int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/connect" {
			connects++
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{
		base:     srv.URL,
		myKey:    "bob",
		token:    "dev",
		ctx:      ctx,
		cancel:   cancel,
		maxChats: 1,
	}
	a.initRegistry()
	_ = attachPipe(t, a.registry, "req-a", "alice")
	if err := a.handleLine("connect carol"); err != nil {
		t.Fatal(err)
	}
	if connects != 0 {
		t.Fatalf("connect posted %d times", connects)
	}
}

func TestAcceptSkippedAtCapacity(t *testing.T) {
	var accepts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/accept") {
			accepts++
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{base: srv.URL, token: "dev", ctx: ctx, cancel: cancel, maxChats: 1}
	a.initRegistry()
	_ = attachPipe(t, a.registry, "req-a", "alice")
	if err := a.handleLine("accept req-b"); err != nil {
		t.Fatal(err)
	}
	if accepts != 0 {
		t.Fatalf("accept posted %d times", accepts)
	}
}

func TestAcceptWhileFocusedHitsConnector(t *testing.T) {
	var accepts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/accept") {
			accepts++
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{base: srv.URL, token: "dev", ctx: ctx, cancel: cancel, maxChats: 4}
	a.initRegistry()
	_ = attachPipe(t, a.registry, "req-a", "alice")
	_ = a.handleLine("accept req-b")
	if accepts != 1 {
		t.Fatalf("accept posted %d times", accepts)
	}
}

func TestHandleLineQuitEndsOnlyFocused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{ctx: ctx, cancel: cancel, maxChats: 4, resumeWindow: time.Minute}
	a.initRegistry()
	go a.pumpEvents()
	first := attachPipe(t, a.registry, "req-a", "alice")
	second := attachPipe(t, a.registry, "req-b", "bob")
	if err := a.handleLine("/quit"); err != nil {
		t.Fatal(err)
	}
	waitGone(t, a.registry, second.ID())
	if !a.registry.Has(first.ID()) {
		t.Fatal("quit closed the unfocused conversation")
	}
}

func TestConnectReportsRateLimit(t *testing.T) {
	prev := acceptPoll
	acceptPoll = time.Millisecond
	t.Cleanup(func() { acceptPoll = prev })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/peers/alice":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"key":"alice","address":"127.0.0.1:9001","state":"available"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/connect":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"request_id":"req-1","status":"pending"}`))
		default:
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		}
	}))
	defer srv.Close()

	a := &app{
		base:       srv.URL,
		myKey:      "bob",
		listenAddr: mustAddr(t, "127.0.0.1:9002"),
		token:      "dev",
		ctx:        context.Background(),
	}
	err := a.doConnect("alice")
	if err == nil || err.Error() != "too many requests" {
		t.Fatalf("connect: %v", err)
	}
}

func TestAcceptReportsRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	a := &app{base: srv.URL, token: "dev", ctx: context.Background()}
	if _, err := a.postAction("req-1", "accept"); err == nil || err.Error() != "too many requests" {
		t.Fatalf("accept: %v", err)
	}
}

func TestPendingRateLimitIsNotADecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	var buf lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	a := &app{
		base:        srv.URL,
		myKey:       "alice",
		token:       "dev",
		pendingPoll: 5 * time.Millisecond,
		logger:      slog.New(slog.NewTextHandler(&buf, nil)),
		ctx:         ctx,
	}
	go a.pollPending()
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(buf.String(), "too many requests") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	text := buf.String()
	if strings.Contains(text, "decode pending") {
		t.Fatalf("rate limit logged as a decode error: %s", text)
	}
	if !strings.Contains(text, "too many requests") {
		t.Fatalf("log: %s", text)
	}
}
