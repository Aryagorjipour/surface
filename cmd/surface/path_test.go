package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/connector"
	"github.com/coder/websocket"
)

func TestPathHarnesses(t *testing.T) {
	if attemptBudget != 30*time.Second {
		t.Fatalf("budget %s", attemptBudget)
	}
	if dialTimeout != 2*time.Second {
		t.Fatalf("dial timeout %s", dialTimeout)
	}
	if acceptPoll != time.Second {
		t.Fatalf("poll %s", acceptPoll)
	}
	setAttemptBudget(2500 * time.Millisecond)
	setDialTimeout(800 * time.Millisecond)
	prevPoll := acceptPoll
	acceptPoll = 20 * time.Millisecond
	t.Cleanup(func() {
		setAttemptBudget(30 * time.Second)
		setDialTimeout(2 * time.Second)
		acceptPoll = prevPoll
	})

	base, handlers := startPathConnector(t)
	punchAddr, packets := startPathPunch(t, handlers)
	const token = "dev"

	t.Run("lab", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "lab-alice", "lab-bob", punchAddr, "", punchAddr, "")
		before := packets.Load()
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, connectedPrompt(true), 2*time.Second)
		waitPathLine(t, listener.buf, connectedPrompt(false), 2*time.Second)
		if !exactLine(dialer.buf.String(), "accepted — dialing "+listener.app.listenAddr) {
			t.Fatalf("dialer lines %q", dialer.buf.String())
		}
		assertPathConnected(t, dialer.buf, listener.buf)
		stdout := exchangePath(t, dialer.app, listener.app)
		if strings.Count(stdout, "peer: surface-round-trip") < 1 {
			t.Fatalf("stdout %q", stdout)
		}
		time.Sleep(200 * time.Millisecond)
		if packets.Load() != before {
			t.Fatalf("punch packets %d", packets.Load()-before)
		}
	})

	t.Run("punch", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "punch-alice", "punch-bob", punchAddr, "", punchAddr, "")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
		waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
		assertPathConnected(t, dialer.buf, listener.buf)
		if !exactLine(dialer.buf.String(), "accepted — dialing 127.0.0.1:1") {
			t.Fatalf("dialed %q", dialer.buf.String())
		}
		stdout := exchangePath(t, dialer.app, listener.app)
		if strings.Count(stdout, "peer: surface-round-trip") < 1 {
			t.Fatalf("stdout %q", stdout)
		}
		body := getPath(t, base, token, "/peers/"+listener.app.myKey)
		var peer struct {
			Address string `json:"address"`
		}
		if err := json.Unmarshal(body, &peer); err != nil {
			t.Fatal(err)
		}
		if peer.Address != "127.0.0.1:1" {
			t.Fatalf("lookup address %q body %s", peer.Address, body)
		}
		pending := getPath(t, base, token, "/pending?key="+listener.app.myKey)
		if bytes.Contains(pending, []byte("127.0.0.1:1")) && bytes.Contains(bytes.ToLower(pending), []byte("ticket")) {
			t.Fatalf("pending %s", pending)
		}
	})

	t.Run("relay", func(t *testing.T) {
		relayAddr, copied := startSplicer(t)
		dialer, listener := startPathPair(t, base, token, "relay-alice", "relay-bob", "", relayAddr, "", relayAddr)
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
		waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
		assertPathConnected(t, dialer.buf, listener.buf)
		stdout := exchangePath(t, dialer.app, listener.app)
		if strings.Count(stdout, "peer: surface-round-trip") < 1 {
			t.Fatalf("stdout %q", stdout)
		}
		if bytes.Contains(copied.Bytes(), []byte("surface-round-trip")) {
			t.Fatal("spliced bytes contain surface-round-trip")
		}
	})

	t.Run("blocked unset", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "block-alice", "block-bob", "", "", "", "")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		_, proof := acceptPath(t, base, token, listener)
		assertPathFailed(t, dialer, listener)
		if got := pathProof(t, base, token, listener.lastID); got != proof || pathStatus(t, base, token, listener.lastID) != "accepted" {
			t.Fatalf("status %s proof %q want accepted %q", pathStatus(t, base, token, listener.lastID), got, proof)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws://"+listener.app.listenAddr, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.CloseNow()
	})

	t.Run("blocked one sided", func(t *testing.T) {
		relayAddr, _ := startSplicer(t)
		dialer, listener := startPathPair(t, base, token, "one-alice", "one-bob", "", relayAddr, "", "")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		assertPathFailed(t, dialer, listener)
	})

	t.Run("blocked mismatched", func(t *testing.T) {
		left, _ := startSplicer(t)
		right, _ := startSplicer(t)
		dialer, listener := startPathPair(t, base, token, "mis-alice", "mis-bob", "", left, "", right)
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		assertPathFailed(t, dialer, listener)
	})

	t.Run("blocked not accepting", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "dead-alice", "dead-bob", "", "127.0.0.1:1", "", "127.0.0.1:1")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		assertPathFailed(t, dialer, listener)
	})

	t.Run("reject", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "rej-alice", "rej-bob", "", "", "", "")
		stdout := captureStdout(t, func() {
			go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
			id := waitPathPending(t, base, token, listener.app.myKey)
			if err := listener.app.handleLine("reject " + id); err != nil {
				t.Fatal(err)
			}
			waitPathLine(t, listener.buf, "rejected", 2*time.Second)
			time.Sleep(300 * time.Millisecond)
		})
		if !exactLine(listener.buf.String(), "rejected") {
			t.Fatalf("listener %q", listener.buf.String())
		}
		if exactLine(listener.buf.String(), "traversal failed") || exactLine(dialer.buf.String(), "traversal failed") || strings.Contains(stdout, "traversal failed") {
			t.Fatalf("unexpected failure listener %q dialer %q stdout %q", listener.buf.String(), dialer.buf.String(), stdout)
		}
		if !strings.Contains(stdout, "connect error: rejected") {
			t.Fatalf("stdout %q", stdout)
		}
	})

	t.Run("quit", func(t *testing.T) {
		dialer, listener := startPathPair(t, base, token, "quit-alice", "quit-bob", "", "", "", "")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, "accepted — dialing 127.0.0.1:1", 2*time.Second)
		if err := dialer.app.handleLine("quit"); err != nil && !errors.Is(err, errStop) {
			t.Fatal(err)
		}
		waitPathLine(t, listener.buf, "traversal failed", 4*time.Second)
		time.Sleep(200 * time.Millisecond)
		if exactLine(dialer.buf.String(), "traversal failed") {
			t.Fatalf("dialer printed failure %q", dialer.buf.String())
		}
		if exactLine(dialer.buf.String(), connectedPrompt(true)) || exactLine(listener.buf.String(), connectedPrompt(false)) {
			t.Fatalf("connected dialer %q listener %q", dialer.buf.String(), listener.buf.String())
		}
	})

	t.Run("punch before relay", func(t *testing.T) {
		relayAddr, accepts := listenRelayCount(t)
		dialer, listener := startPathPair(t, base, token, "both-alice", "both-bob", punchAddr, relayAddr, punchAddr, relayAddr)
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
		waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
		if !exactLine(dialer.buf.String(), "accepted — dialing 127.0.0.1:1") {
			t.Fatalf("dialed %q", dialer.buf.String())
		}
		time.Sleep(200 * time.Millisecond)
		if n := accepts.Load(); n != 0 {
			t.Fatalf("relay accepted %d", n)
		}
	})

	t.Run("relay after punch miss", func(t *testing.T) {
		hole := startUDPBlackhole(t)
		missPunch, _ := startPathPunchNoted(t, handlers, hole)
		relayAddr, accepts := startCountingSplicer(t)
		dialer, listener := startPathPair(t, base, token, "miss-alice", "miss-bob", missPunch, relayAddr, missPunch, relayAddr)
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		acceptPath(t, base, token, listener)
		waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
		waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
		if !exactLine(dialer.buf.String(), "accepted — dialing 127.0.0.1:1") {
			t.Fatalf("dialed %q", dialer.buf.String())
		}
		if n := accepts.Load(); n < 2 {
			t.Fatalf("relay accepted %d", n)
		}
	})

	t.Run("budget from accept", func(t *testing.T) {
		prevBudget := attemptBudget
		prevDial := dialTimeout
		prevPoll := acceptPoll
		setAttemptBudget(500 * time.Millisecond)
		setDialTimeout(40 * time.Millisecond)
		acceptPoll = 400 * time.Millisecond
		t.Cleanup(func() {
			setAttemptBudget(prevBudget)
			setDialTimeout(prevDial)
			acceptPoll = prevPoll
		})
		dialer, listener := startPathPair(t, base, token, "bound-alice", "bound-bob", "", "", "", "")
		rewritePeer(t, base, token, listener.app.myKey, "127.0.0.1:1")
		go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
		id := waitPathPending(t, base, token, listener.app.myKey)
		listener.lastID = id
		// The dialer sleeps before its first status read, so this accept lands in that sleep.
		time.Sleep(30 * time.Millisecond)
		if err := listener.app.handleLine("accept " + id); err != nil {
			t.Fatal(err)
		}
		acceptedAt := time.Now()
		const slack = 80 * time.Millisecond
		limit := attemptBudget + slack
		dialerAt := waitPathLineAt(t, dialer.buf, "traversal failed", 3*time.Second)
		listenerAt := waitPathLineAt(t, listener.buf, "traversal failed", 3*time.Second)
		if d := dialerAt.Sub(acceptedAt); d > limit {
			t.Fatalf("dialer outcome %s after accept, budget %s", d, attemptBudget)
		}
		if d := listenerAt.Sub(acceptedAt); d > limit {
			t.Fatalf("listener outcome %s after accept, budget %s", d, attemptBudget)
		}
		if exactLine(dialer.buf.String(), connectedPrompt(true)) || exactLine(listener.buf.String(), connectedPrompt(false)) {
			t.Fatalf("connected dialer %q listener %q", dialer.buf.String(), listener.buf.String())
		}
	})
}

type pathApp struct {
	app    *app
	buf    *lockedBuffer
	lastID string
}

func startPathConnector(t *testing.T) (string, *connector.Handlers) {
	t.Helper()
	dir := t.TempDir()
	key := bytes.Repeat([]byte("k"), 32)
	store, err := connector.Open(context.Background(), "file:"+filepath.Join(dir, "path.db"), "", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := connector.NewHandlers(store, nil, logger)
	h.SetBearer("dev")
	cfg := connector.DefaultConfig()
	cfg.SourceAllowance = 100000
	cfg.KeyAllowance = 100000
	h.SetLimits(connector.NewLimits(cfg))
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL, h
}

func startPathPunch(t *testing.T, h *connector.Handlers) (string, *atomic.Int64) {
	t.Helper()
	return startPathPunchNoted(t, h, "")
}

func startPathPunchNoted(t *testing.T, h *connector.Handlers, noted string) (string, *atomic.Int64) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var packets atomic.Int64
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, src, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			packets.Add(1)
			if n == 16 && src != nil {
				addr := src.String()
				if noted != "" {
					addr = noted
				}
				h.NotePunch(append([]byte(nil), buf[:n]...), addr)
			}
		}
	}()
	return conn.LocalAddr().String(), &packets
}

func startUDPBlackhole(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 64*1024)
		for {
			if _, _, err := conn.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()
	return conn.LocalAddr().String()
}

type acceptCounter struct {
	net.Listener
	n *atomic.Int64
}

func (a *acceptCounter) Accept() (net.Conn, error) {
	conn, err := a.Listener.Accept()
	if err == nil {
		a.n.Add(1)
	}
	return conn, err
}

func startCountingSplicer(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var n atomic.Int64
	go serveSplice(&acceptCounter{Listener: ln, n: &n}, &lockedBuffer{})
	return ln.Addr().String(), &n
}

func startPathPair(t *testing.T, base, token, dialerKey, listenerKey, dialerPunch, dialerRelay, listenerPunch, listenerRelay string) (*pathApp, *pathApp) {
	t.Helper()
	dialer := startPathSurface(t, base, token, dialerKey, dialerPunch, dialerRelay)
	listener := startPathSurface(t, base, token, listenerKey, listenerPunch, listenerRelay)
	return dialer, listener
}

func startPathSurface(t *testing.T, base, token, key, punch, relay string) *pathApp {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	buf := &lockedBuffer{}
	a := &app{
		base:         base,
		myKey:        key,
		listenAddr:   addr,
		token:        token,
		heartbeat:    time.Hour,
		resumeWindow: time.Minute,
		pendingPoll:  20 * time.Millisecond,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		ctx:          ctx,
		cancel:       cancel,
		presence:     "available",
		fromKey:      map[string]string{},
		punchAddr:    punch,
		relayAddr:    relay,
		out:          buf,
	}
	a.gate.readable = a.requestReadable
	go a.serveWS()
	waitPathListen(t, addr)
	if err := a.register("available"); err != nil {
		t.Fatal(err)
	}
	go a.pollPending()
	return &pathApp{app: a, buf: buf}
}

func waitPathListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not listening on %s", addr)
}

func acceptPath(t *testing.T, base, token string, listener *pathApp) (string, string) {
	t.Helper()
	id := waitPathPending(t, base, token, listener.app.myKey)
	listener.lastID = id
	if err := listener.app.handleLine("accept " + id); err != nil {
		t.Fatal(err)
	}
	return id, pathProof(t, base, token, id)
}

func waitPathPending(t *testing.T, base, token, key string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		body := getPath(t, base, token, "/pending?key="+key)
		var rows []struct {
			ID string `json:"request_id"`
		}
		if err := json.Unmarshal(body, &rows); err == nil && len(rows) > 0 && rows[0].ID != "" {
			return rows[0].ID
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("no pending request")
	return ""
}

func pathProof(t *testing.T, base, token, id string) string {
	t.Helper()
	var body struct {
		Proof string `json:"admission_proof"`
	}
	if err := json.Unmarshal(getPath(t, base, token, "/requests/"+id), &body); err != nil {
		t.Fatal(err)
	}
	if body.Proof == "" {
		t.Fatal("missing proof")
	}
	return body.Proof
}

func pathStatus(t *testing.T, base, token, id string) string {
	t.Helper()
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(getPath(t, base, token, "/requests/"+id), &body); err != nil {
		t.Fatal(err)
	}
	return body.Status
}

func getPath(t *testing.T, base, token, path string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status %d body %s", path, resp.StatusCode, body)
	}
	return body
}

func rewritePeer(t *testing.T, base, token, key, addr string) {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"key": key, "address": addr, "state": "available"})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/peers", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rewrite %d %s", resp.StatusCode, body)
	}
}

func waitPathLine(t *testing.T, buf *lockedBuffer, line string, d time.Duration) {
	t.Helper()
	waitPathLineAt(t, buf, line, d)
}

func waitPathLineAt(t *testing.T, buf *lockedBuffer, line string, d time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if exactLine(buf.String(), line) {
			return time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("missing %q in %q", line, buf.String())
	return time.Time{}
}

func listenRelayCount(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var accepts atomic.Int64
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), &accepts
}

func assertPathConnected(t *testing.T, dialer, listener *lockedBuffer) {
	t.Helper()
	if !exactLine(dialer.String(), connectedPrompt(true)) || !exactLine(listener.String(), connectedPrompt(false)) {
		t.Fatalf("dialer %q listener %q", dialer.String(), listener.String())
	}
	if exactLine(dialer.String(), "traversal failed") || exactLine(listener.String(), "traversal failed") {
		t.Fatalf("failed dialer %q listener %q", dialer.String(), listener.String())
	}
}

func assertPathFailed(t *testing.T, dialer, listener *pathApp) {
	t.Helper()
	waitPathLine(t, dialer.buf, "traversal failed", 4*time.Second)
	waitPathLine(t, listener.buf, "traversal failed", 4*time.Second)
	if exactLine(dialer.buf.String(), connectedPrompt(true)) || exactLine(listener.buf.String(), connectedPrompt(false)) {
		t.Fatalf("connected dialer %q listener %q", dialer.buf.String(), listener.buf.String())
	}
	for _, side := range []*pathApp{dialer, listener} {
		for _, line := range strings.Split(side.buf.String(), "\n") {
			if line != "traversal failed" {
				continue
			}
			if strings.Contains(line, side.app.myKey) || strings.Contains(line, "127.0.0.1") {
				t.Fatalf("failure line %q", line)
			}
		}
	}
}

func exchangePath(t *testing.T, dialer, listener *app) string {
	t.Helper()
	return captureStdout(t, func() {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) && (!dialer.chatting() || !listener.chatting()) {
			time.Sleep(10 * time.Millisecond)
		}
		if !dialer.chatting() || !listener.chatting() {
			t.Fatal("not chatting")
		}
		if err := dialer.handleLine("surface-round-trip"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
	})
}
