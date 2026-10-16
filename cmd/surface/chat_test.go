package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestWebsocketAcceptSpan(t *testing.T) {
	rec := installSurfaceSpans(t)
	const (
		caller = "canary-peer-key"
		target = "target"
		addr   = "203.0.113.10:9001"
	)
	raw, text, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	g := &Gate{}
	g.remember("req-ok", caller, target, raw)
	client, server := wsPair(t)
	errCh := make(chan error, 1)
	readySpans := make(chan int, 1)
	out := captureStdout(t, func() {
		go func() {
			_, err := g.take(ctx, server, time.Now(), time.Time{}, func(*session.Session) {
				readySpans <- len(rec.Ended())
				fmt.Println(connectedPrompt(false))
			})
			errCh <- err
		}()
		dialer, err := g.dialAdmitted(ctx, client, "req-ok", caller, target, raw)
		if err != nil {
			t.Errorf("dial %v", err)
			return
		}
		if err := <-errCh; err != nil {
			t.Error(err)
		}
		select {
		case n := <-readySpans:
			if n != 1 {
				t.Errorf("span count at the connected line: %d", n)
			}
		default:
			t.Error("connected line ran before the span ended")
		}
		if _, err := session.Seal(dialer, "surface-round-trip", 0); err != nil {
			t.Error(err)
		}
	})
	if !exactLine(out, connectedPrompt(false)) {
		t.Fatalf("listener prompt missing from %q", out)
	}
	if exactLine(out, connectedPrompt(true)) {
		t.Fatalf("dialer prompt printed by the listener path %q", out)
	}
	if len(rec.Ended()) != 1 {
		t.Fatalf("completed handshake spans %d", len(rec.Ended()))
	}
	assertWebsocketSpan(t, rec.Ended()[0], "accepted")
	assertSpanClean(t, rec.Ended()[0], raw, text, caller, addr)

	bad, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	g.remember("req-bad", caller, target, bad)
	client, server = wsPair(t)
	refusedOut := captureStdout(t, func() {
		errCh = make(chan error, 1)
		go func() {
			_, err := g.take(ctx, server, time.Now(), time.Time{}, func(*session.Session) {
				fmt.Println(connectedPrompt(false))
				fmt.Println(connectedPrompt(true))
			})
			errCh <- err
		}()
		_, _ = g.dialAdmitted(ctx, client, "req-bad", caller, target, raw)
		<-errCh
	})
	if exactLine(refusedOut, connectedPrompt(true)) || exactLine(refusedOut, connectedPrompt(false)) {
		t.Fatalf("refused handshake printed %q", refusedOut)
	}
	if len(rec.Ended()) != 2 {
		t.Fatalf("spans after refusal %d", len(rec.Ended()))
	}
	assertWebsocketSpan(t, rec.Ended()[1], "refused")
	assertSpanClean(t, rec.Ended()[1], raw, text, caller, addr)

	client, server = wsPair(t)
	missing, _, err := session.Offer("req-miss", caller, target, make([]byte, session.ProofBytes))
	if err != nil {
		t.Fatal(err)
	}
	missCh := make(chan error, 1)
	go func(conn *websocket.Conn) {
		_, err := g.take(ctx, conn, time.Now(), time.Time{}, func(*session.Session) {
			fmt.Println(connectedPrompt(false))
		})
		missCh <- err
	}(server)
	if err := client.Write(ctx, websocket.MessageBinary, missing); err != nil {
		t.Fatal(err)
	}
	_, _, _ = client.Read(ctx)
	select {
	case missErr := <-missCh:
		if missErr == nil {
			t.Fatal("missing proof was accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing proof hung")
	}
	if len(rec.Ended()) != 3 {
		t.Fatalf("spans after missing proof %d", len(rec.Ended()))
	}
	assertWebsocketSpan(t, rec.Ended()[2], "refused")

	g.remember("req-later", caller, target, raw)
	client, server = wsPair(t)
	later := make(chan error, 1)
	go func() {
		_, err := g.take(ctx, server, time.Now(), time.Time{}, func(*session.Session) {})
		later <- err
	}()
	if _, err := g.dialAdmitted(ctx, client, "req-later", caller, target, raw); err != nil {
		t.Fatal(err)
	}
	if err := <-later; err != nil {
		t.Fatal(err)
	}
	if len(rec.Ended()) != 4 {
		t.Fatalf("later handshake spans %d", len(rec.Ended()))
	}
	assertWebsocketSpan(t, rec.Ended()[3], "accepted")
}

func exactLine(out, want string) bool {
	for _, line := range strings.Split(out, "\n") {
		if line == want {
			return true
		}
	}
	return false
}

func installSurfaceSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func assertWebsocketSpan(t *testing.T, sp sdktrace.ReadOnlySpan, status string) {
	t.Helper()
	if sp.Name() != "websocket accept" {
		t.Fatalf("span name %q", sp.Name())
	}
	attrs := sp.Attributes()
	if len(attrs) != 2 {
		t.Fatalf("attributes %#v", attrs)
	}
	var endpoint, got string
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "endpoint":
			if attr.Value.Type() != attribute.STRING {
				t.Fatalf("endpoint type %s", attr.Value.Type())
			}
			endpoint = attr.Value.AsString()
		case "status":
			if attr.Value.Type() != attribute.STRING {
				t.Fatalf("status type %s", attr.Value.Type())
			}
			got = attr.Value.AsString()
		default:
			t.Fatalf("unexpected attribute %s", attr.Key)
		}
	}
	if endpoint != "websocket accept" || got != status {
		t.Fatalf("endpoint %q status %q", endpoint, got)
	}
}

func assertSpanClean(t *testing.T, sp sdktrace.ReadOnlySpan, proof []byte, proofText, key, addr string) {
	t.Helper()
	text := sp.Name()
	for _, attr := range sp.Attributes() {
		text += " " + string(attr.Key) + "=" + attr.Value.String()
	}
	for _, secret := range []string{string(proof), proofText, key, addr, "surface-round-trip"} {
		if secret != "" && strings.Contains(text, secret) {
			t.Fatalf("span included private text")
		}
	}
}

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
