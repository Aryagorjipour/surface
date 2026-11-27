package main

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/Aryagorjipour/surface/internal/mailbox"
	tea "github.com/charmbracelet/bubbletea"
)

func TestMailboxDuration(t *testing.T) {
	help := surfaceHelp()
	if !strings.Contains(help, "--mailbox") || !strings.Contains(help, "default unset") {
		t.Fatalf("help\n%s", help)
	}
	def, err := parseSurface([]string{"http://connector.example:8080", "alice"}, nil)
	if err != nil || def.mailbox != 0 {
		t.Fatalf("unset %+v %v", def.mailbox, err)
	}
	blank, err := parseSurface([]string{"http://connector.example:8080", "alice"}, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return " \t ", true
		}
		return "", false
	})
	if err != nil || blank.mailbox != 0 {
		t.Fatalf("blank %+v %v", blank.mailbox, err)
	}
	_, err = parseSurface([]string{"--mailbox", "0s", "http://connector.example:8080", "alice"}, nil)
	if err == nil || !strings.Contains(err.Error(), "SURFACE_MAILBOX") || strings.Contains(err.Error(), "0s") {
		t.Fatalf("zero: %v", err)
	}
	got, err := parseSurface([]string{"--mailbox", "24h", "http://connector.example:8080", "alice"}, nil)
	if err != nil || got.mailbox != 24*time.Hour {
		t.Fatalf("flag %+v %v", got.mailbox, err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "surface.env")
	if err := os.WriteFile(path, []byte("SURFACE_MAILBOX=1h\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := parseSurface([]string{"--config", path, "http://connector.example:8080", "alice"}, func(string) (string, bool) {
		return "", false
	})
	if err != nil || fromFile.mailbox != time.Hour {
		t.Fatalf("file %+v %v", fromFile.mailbox, err)
	}
	fromEnv, err := parseSurface([]string{"--config", path, "http://connector.example:8080", "alice"}, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return "2h", true
		}
		return "", false
	})
	if err != nil || fromEnv.mailbox != 2*time.Hour {
		t.Fatalf("env %+v %v", fromEnv.mailbox, err)
	}
	fromFlag, err := parseSurface([]string{"--config", path, "--mailbox", "3h", "http://connector.example:8080", "alice"}, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return "2h", true
		}
		return "", false
	})
	if err != nil || fromFlag.mailbox != 3*time.Hour {
		t.Fatalf("flag wins %+v %v", fromFlag.mailbox, err)
	}
}

func TestOwnsIncludesResumeGap(t *testing.T) {
	r := NewRegistry(context.Background(), 4, time.Minute)
	if r.Owns("bob") {
		t.Fatal("empty registry owns bob")
	}
	r.byID[ConvID("a")] = &Conversation{id: "a", peer: "bob", carrier: carrierResumeGap{}}
	r.byID[ConvID("b")] = &Conversation{id: "b", peer: "bob", carrier: carrierLive{}}
	if !r.Owns("bob") {
		t.Fatal("resume gap was invisible")
	}
	if got := r.IDForPeer("bob"); got != "" {
		t.Fatalf("IDForPeer %q", got)
	}
	if r.Owns("carol") {
		t.Fatal("owns carol")
	}
}

func TestMailboxCommands(t *testing.T) {
	ctx := context.Background()
	a := &app{
		ctx:    ctx,
		myKey:  "alice",
		deskCh: make(chan tea.Msg, 4),
		base:   "http://127.0.0.1:1",
		token:  "tok",
	}
	if err := a.handleLine("pubkey"); err != nil {
		t.Fatal(err)
	}
	if got := (<-a.deskCh).(note).text; got != "unknown command" {
		t.Fatalf("unset pubkey %q", got)
	}

	dir := t.TempDir()
	box, err := mailbox.Open(mailbox.Config{
		Dir: dir, Me: "alice", TTL: time.Hour, Endpoint: a.base, Bearer: a.token,
		Busy: func(peer string) bool { return a.registry.Owns(peer) },
	})
	if err != nil {
		t.Fatal(err)
	}
	a.box = box
	a.initRegistry()
	if err := a.handleLine("pubkey"); err != nil {
		t.Fatal(err)
	}
	if got := (<-a.deskCh).(note).text; !strings.HasPrefix(got, "x25519:") {
		t.Fatalf("pubkey %q", got)
	}
	if err := a.handleLine("leave bob secret phrase"); err != nil {
		t.Fatal(err)
	}
	got := (<-a.deskCh).(note).text
	if got != "leave refused no-key" || strings.Contains(got, "bob") || strings.Contains(got, "secret") {
		t.Fatalf("no key %q", got)
	}

	peer, err := mailbox.Open(mailbox.Config{
		Dir: filepath.Join(t.TempDir(), "bob"), Me: "bob", TTL: time.Hour, Endpoint: a.base, Bearer: a.token,
		Busy: func(string) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", peer.Public().String()); err != nil {
		t.Fatal(err)
	}
	a.registry.byID[ConvID("gap")] = &Conversation{id: "gap", peer: "bob", carrier: carrierResumeGap{}}
	if err := a.handleLine("leave bob secret phrase"); err != nil {
		t.Fatal(err)
	}
	got = (<-a.deskCh).(note).text
	if got != "leave refused busy" || strings.Contains(got, "bob") || strings.Contains(got, "secret") {
		t.Fatalf("busy %q", got)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("outbox %d", len(entries))
	}
}

func TestCollectLogsClassOnly(t *testing.T) {
	var buf bytes.Buffer
	a := &app{
		ctx:    context.Background(),
		myKey:  "alice",
		base:   "http://127.0.0.1:1",
		token:  "tok",
		logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	box, err := mailbox.Open(mailbox.Config{
		Dir: t.TempDir(), Me: "alice", TTL: time.Hour, Endpoint: a.base, Bearer: a.token,
		Busy: func(string) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	a.box = box
	a.collect()
	text := buf.String()
	if !strings.Contains(text, "op=collect") || !strings.Contains(text, "class=unavailable") {
		t.Fatalf("log %q", text)
	}
	if strings.Contains(text, "alice") || strings.Contains(text, "http://") {
		t.Fatalf("log leaked %q", text)
	}
}

func TestHeartbeatCollectsWhenOpen(t *testing.T) {
	var mu sync.Mutex
	var gets int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/mailbox/") {
			mu.Lock()
			gets++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	a := &app{
		base: srv.URL, myKey: "alice", token: "tok", ctx: context.Background(), presence: "available",
	}
	if err := a.heartbeatOnce(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if gets != 0 {
		t.Fatalf("unset gets %d", gets)
	}
	mu.Unlock()
	box, err := mailbox.Open(mailbox.Config{
		Dir: t.TempDir(), Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	a.box = box
	if err := a.heartbeatOnce(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gets != 1 {
		t.Fatalf("gets %d", gets)
	}
}

func TestMailboxDuringBootstrap(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(4 * time.Second)
	setDialTimeout(40 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, copied := startSplicer(t)
	dialer, listener := startPathPair(t, base, "dev", "box-alice", "box-bob", "", relayAddr, "", relayAddr)
	rewritePeer(t, base, "dev", listener.app.myKey, "127.0.0.1:1")
	var mu sync.Mutex
	stored := map[string][]byte{}
	boxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			stored[r.URL.Path] = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			var rows []map[string]string
			for path, raw := range stored {
				var stage struct {
					Body string `json:"body"`
				}
				if json.Unmarshal(raw, &stage) == nil && stage.Body != "" {
					id := path[strings.LastIndex(path, "/")+1:]
					rows = append(rows, map[string]string{"id": id, "body": stage.Body})
				}
			}
			_ = json.NewEncoder(w).Encode(rows)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer boxSrv.Close()
	openBox := func(a *app, me string) *mailbox.Box {
		box, err := mailbox.Open(mailbox.Config{
			Dir: t.TempDir(), Me: me, TTL: time.Hour, Endpoint: boxSrv.URL, Bearer: "dev",
			Busy: func(string) bool { return false },
		})
		if err != nil {
			t.Fatal(err)
		}
		a.box = box
		return box
	}
	alice := openBox(dialer.app, "alice")
	bob := openBox(listener.app, "bob")
	if err := alice.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if err := bob.Hold("alice", alice.Public().String()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
	leave := captureStdout(t, func() {
		if err := dialer.app.handleLine("leave bob bootstrap-secret"); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(leave, "staged") || bytes.Contains(copied.Bytes(), []byte("bootstrap-secret")) {
		t.Fatalf("leave %q relay %d", leave, copied.Len())
	}
	waitPathLine(t, dialer.buf, phraseRelayStopped, 6*time.Second)
	again := captureStdout(t, func() {
		if err := dialer.app.handleLine("leave bob after-secret"); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(again, "staged") {
		t.Fatalf("after %q", again)
	}
	var got []string
	if err := bob.Collect(context.Background(), func(d mailbox.Delivered) error {
		got = append(got, d.Text)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "bootstrap-secret") || !strings.Contains(joined, "after-secret") {
		t.Fatalf("read %q", joined)
	}
	if bytes.Contains(copied.Bytes(), []byte("bootstrap-secret")) || bytes.Contains(copied.Bytes(), []byte("after-secret")) {
		t.Fatal("mailbox bytes crossed the relay")
	}
}

func TestRegisterCollectsWhenMailboxSet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name string
		ttl  string
		want int
	}{
		{name: "unset", want: 0},
		{name: "set", ttl: "24h", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			var mu sync.Mutex
			var gets int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/mailbox/") {
					mu.Lock()
					gets++
					mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte("[]"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer srv.Close()
			err := run([]string{srv.URL, "alice", "127.0.0.1:0"}, func(k string) (string, bool) {
				switch k {
				case "SURFACE_CONNECTOR_TOKEN":
					return "tok", true
				case "SURFACE_MAILBOX":
					if tc.ttl == "" {
						return "", false
					}
					return tc.ttl, true
				default:
					return "", false
				}
			}, logger, strings.NewReader(""))
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if gets != tc.want {
				t.Fatalf("gets %d", gets)
			}
			_, statErr := os.Stat(filepath.Join("surface-mailbox", "alice"))
			if tc.want == 0 && !os.IsNotExist(statErr) {
				t.Fatal("unset created the mailbox directory")
			}
			if tc.want == 1 && statErr != nil {
				t.Fatal(statErr)
			}
		})
	}
}
