package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

func TestAdmitPrintsConnectedLines(t *testing.T) {
	if connectedPrompt(true) != "connected — type messages (/quit to leave)" {
		t.Fatalf("dialer prompt %q", connectedPrompt(true))
	}
	if connectedPrompt(false) != ">>> peer connected — type messages (/quit to leave)" {
		t.Fatalf("listener prompt %q", connectedPrompt(false))
	}

	raw, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	const (
		id     = "req-1"
		caller = "caller"
		target = "target"
	)
	g := &Gate{}
	client, server := wsPair(t)
	ctx := context.Background()
	var ready *session.Session
	out := captureStdout(t, func() {
		errCh := make(chan error, 1)
		go func() {
			errCh <- g.acceptAdmitted(ctx, server, id, caller, target, raw, func(s *session.Session) {
				ready = s
				_ = connectedPrompt(false)
			})
		}()
		dialer, err := g.dialAdmitted(ctx, client, id, caller, target, raw)
		if err != nil {
			t.Error(err)
		}
		if err := <-errCh; err != nil {
			t.Error(err)
		}
		if dialer == nil || ready == nil {
			t.Fatal("missing session")
		}
		frame, err := session.Seal(dialer, "surface-round-trip", 0)
		if err != nil {
			t.Error(err)
		}
		plain, display, err := session.Open(ready, frame)
		if err != nil || !display || plain != "surface-round-trip" {
			t.Errorf("usable %q %v %v", plain, display, err)
		}
	})
	if strings.Contains(out, connectedPrompt(true)) || strings.Contains(out, connectedPrompt(false)) {
		t.Fatalf("admit printed %q", out)
	}

	bad, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	client, server = wsPair(t)
	called := false
	errCh := make(chan error, 1)
	go func() {
		errCh <- g.acceptAdmitted(ctx, server, id, caller, target, bad, func(*session.Session) {
			called = true
		})
	}()
	_, err = g.dialAdmitted(ctx, client, id, caller, target, raw)
	if err == nil {
		t.Fatal("wrong proof dialed")
	}
	if strings.Contains(err.Error(), connectedPrompt(true)) || strings.Contains(err.Error(), connectedPrompt(false)) {
		t.Fatalf("error text %q", err.Error())
	}
	got := <-errCh
	if got == nil || called {
		t.Fatalf("listener err %v called %v", got, called)
	}
	if strings.Contains(got.Error(), connectedPrompt(true)) || strings.Contains(got.Error(), connectedPrompt(false)) {
		t.Fatalf("listener error text %q", got.Error())
	}

	client, server = wsPair(t)
	errCh = make(chan error, 1)
	go func() {
		errCh <- g.acceptAdmitted(ctx, server, id, caller, target, raw, func(*session.Session) {})
	}()
	_, dialErr := g.dialAdmitted(ctx, client, id, caller, target, raw)
	if err := <-errCh; err == nil && dialErr == nil {
		t.Fatal("second admit succeeded")
	}
	if g.session(id) == nil || session.Finished(g.session(id)) {
		t.Fatal("first session was cleared")
	}
}

func TestChatResumeSkipsPrompt(t *testing.T) {
	raw, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	const (
		id     = "req-9"
		caller = "caller"
		target = "target"
	)
	g := &Gate{}
	g.remember(id, caller, target, raw)
	client, server := wsPair(t)
	ctx := context.Background()
	readyN := 0
	errCh := make(chan error, 1)
	go func() {
		_, err := g.take(ctx, server, time.Time{}, time.Time{}, func(*session.Session) {
			readyN++
		})
		errCh <- err
	}()
	// take() on an admission needs the invite path. Use accept through take by dialing.
	dialer, err := g.dialAdmitted(ctx, client, id, caller, target, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if readyN != 1 {
		t.Fatalf("ready %d", readyN)
	}
	_ = client.Close(websocket.StatusGoingAway, "")
	_ = server.Close(websocket.StatusGoingAway, "")

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Second)
	client, server = wsPair(t)
	listenSess := g.session(id)
	errCh = make(chan error, 1)
	disp := make(chan string, 1)
	go func() {
		_, err := g.take(ctx, server, now, deadline, func(*session.Session) {
			readyN++
		})
		errCh <- err
		if err != nil {
			return
		}
		_, data, err := server.Read(ctx)
		if err != nil {
			errCh <- err
			return
		}
		plain, display, err := session.Open(listenSess, data)
		if err != nil || !display {
			disp <- ""
			return
		}
		disp <- plain
	}()
	if err := pushResume(ctx, client, dialer, now, deadline, []string{"queued-line"}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	select {
	case plain := <-disp:
		if plain != "queued-line" {
			t.Fatalf("displayed %q", plain)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued line was not displayed")
	}
	if readyN != 1 {
		t.Fatalf("ready called again: %d", readyN)
	}
}

func TestHandshakeRefusesUnreadableRequest(t *testing.T) {
	raw, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	const (
		id     = "req-stale"
		caller = "caller"
		target = "target"
	)
	ctx := context.Background()
	g := &Gate{}
	checked := false
	g.readable = func(got string) bool {
		checked = true
		return false
	}
	g.remember(id, caller, target, raw)
	client, server := wsPair(t)
	ready := false
	errCh := make(chan error, 1)
	go func() {
		_, err := g.take(ctx, server, time.Time{}, time.Time{}, func(*session.Session) {
			ready = true
		})
		errCh <- err
	}()
	_, dialErr := g.dialAdmitted(ctx, client, id, caller, target, raw)
	if dialErr == nil {
		t.Fatal("expired request completed the handshake")
	}
	if err := <-errCh; err == nil || ready || !checked {
		t.Fatalf("listener err %v ready %v checked %v", err, ready, checked)
	}
	if g.session(id) != nil {
		t.Fatal("expired request spent the proof")
	}
	if strings.Contains(dialErr.Error(), "connected —") {
		t.Fatalf("error text %q", dialErr.Error())
	}

	g = &Gate{}
	g.readable = func(string) bool { return true }
	g.remember(id, caller, target, raw)
	client, server = wsPair(t)
	errCh = make(chan error, 1)
	go func() {
		_, err := g.take(ctx, server, time.Time{}, time.Time{}, func(*session.Session) {})
		errCh <- err
	}()
	if _, err := g.dialAdmitted(ctx, client, id, caller, target, raw); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil || g.session(id) == nil {
		t.Fatalf("readable handshake %v", err)
	}

	revived := &Gate{}
	revived.readable = func(string) bool {
		t.Error("restarted listener consulted the connector")
		return true
	}
	client, server = wsPair(t)
	errCh = make(chan error, 1)
	go func() {
		_, err := revived.take(ctx, server, time.Time{}, time.Time{}, func(*session.Session) {})
		errCh <- err
	}()
	if _, err := revived.dialAdmitted(ctx, client, id, caller, target, raw); err == nil {
		t.Fatal("restarted listener admitted the old proof")
	}
	if err := <-errCh; err == nil || revived.session(id) != nil {
		t.Fatalf("restart err %v session %v", err, revived.session(id))
	}
}

func TestFailedOpenEndsBothSides(t *testing.T) {
	dialer, listener := admittedPair(t)
	other, _ := admittedPair(t)
	foreign, err := session.Seal(other, "not-the-line", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	left := &app{
		ctx:          ctx,
		resumeWindow: time.Minute,
		sess:         dialer,
		inChat:       true,
		dialer:       true,
	}
	right := &app{
		ctx:          ctx,
		resumeWindow: time.Minute,
		sess:         listener,
		inChat:       true,
	}
	client, server := wsPair(t)
	left.chatConn = client
	right.chatConn = server

	leftDone := make(chan bool, 1)
	rightDone := make(chan bool, 1)
	out := captureStdout(t, func() {
		go func() { leftDone <- left.readLoop(client) }()
		go func() { rightDone <- right.readLoop(server) }()
		if err := client.Write(ctx, websocket.MessageBinary, foreign); err != nil {
			t.Error(err)
		}
		waitDone(t, leftDone, "dialer")
		waitDone(t, rightDone, "listener")
	})
	if strings.Contains(out, "not-the-line") {
		t.Fatalf("displayed undecrypted bytes: %q", out)
	}
	if strings.Contains(out, "left chat") {
		t.Fatalf("failed open printed quit line: %q", out)
	}
	if strings.Count(out, "chat ended") < 2 {
		t.Fatalf("both sides should end: %q", out)
	}
	if !left.resumeUntil.IsZero() || !right.resumeUntil.IsZero() {
		t.Fatal("failed open started the resume window")
	}
	if !session.Finished(dialer) || !session.Finished(listener) {
		t.Fatal("session still resumable")
	}
	if _, err := session.ResumeOffer(dialer); err == nil {
		t.Fatal("dialer resumed after a failed open")
	}
}

func TestNetworkCloseStillResumes(t *testing.T) {
	dialer, _ := admittedPair(t)
	ctx := context.Background()
	client, server := wsPair(t)
	a := &app{
		ctx:          ctx,
		resumeWindow: time.Minute,
		sess:         dialer,
		chatConn:     client,
		inChat:       true,
		dialer:       true,
	}
	_ = server.Close(websocket.StatusGoingAway, "")
	if !a.readLoop(client) {
		t.Fatal("network close did not arm resume")
	}
	if a.resumeUntil.IsZero() || session.Finished(dialer) {
		t.Fatal("network close finished the session")
	}
}

func admittedPair(t *testing.T) (*session.Session, *session.Session) {
	t.Helper()
	raw, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	const (
		id     = "req-open"
		caller = "caller"
		target = "target"
	)
	g := &Gate{}
	g.remember(id, caller, target, raw)
	client, server := wsPair(t)
	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		_, err := g.take(ctx, server, time.Time{}, time.Time{}, func(*session.Session) {})
		errCh <- err
	}()
	dialer, err := g.dialAdmitted(ctx, client, id, caller, target, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	listener := g.session(id)
	if dialer == nil || listener == nil {
		t.Fatal("missing session")
	}
	_ = client.CloseNow()
	_ = server.CloseNow()
	return dialer, listener
}

func waitDone(t *testing.T, done <-chan bool, side string) {
	t.Helper()
	select {
	case armed := <-done:
		if armed {
			t.Errorf("%s armed resume", side)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("%s did not end", side)
	}
}

func wsPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	got := make(chan *websocket.Conn, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		got <- c
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(websocket.StatusNormalClosure, "") })
	var server *websocket.Conn
	select {
	case server = <-got:
	case <-ctx.Done():
		t.Fatal("server accept")
	}
	t.Cleanup(func() { _ = server.Close(websocket.StatusNormalClosure, "") })
	return client, server
}

func captureStdout(t *testing.T, fn func()) string {
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
