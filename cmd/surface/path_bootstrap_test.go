package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

func TestAttemptBudgetIs20s(t *testing.T) {
	if loadAttemptBudget() != 20*time.Second {
		t.Fatalf("budget %s", loadAttemptBudget())
	}
}

func TestRelayCutoverKeepsLines(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(8 * time.Second)
	setDialTimeout(40 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})

	base, _ := startPathConnector(t)
	relayAddr, copied := startSplicer(t)
	dialer, listener := startPathPair(t, base, "dev", "cut-alice", "cut-bob", "", relayAddr, "", relayAddr)
	rewritePeer(t, base, "dev", listener.app.myKey, "127.0.0.1:1")
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	id := acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
	waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
	if !dialer.app.registry.Bootstrap(ConvID(id)) || !listener.app.registry.Bootstrap(ConvID(id)) {
		t.Fatal("relay chat should stay up while the direct handshake fails")
	}
	if err := dialer.app.handleLine("hello-relay"); err != nil {
		t.Fatal(err)
	}
	waitLine(t, listener.app, "hello-relay")
	before := copied.Len()
	if before == 0 {
		t.Fatal("relay carried no bytes")
	}

	rewritePeer(t, base, "dev", listener.app.myKey, listener.app.listenAddr.String())
	waitDirect(t, dialer.app, listener.app, ConvID(id))
	dialerConv, _ := dialer.app.registry.Focused()
	if dialerConv == nil || dialerConv.ID() != ConvID(id) || !hasLine(dialerConv.Lines(), "hello-relay") {
		t.Fatalf("lines %v id %v", dialerConv, id)
	}
	if listener.app.registry.Len() != 1 || !listener.app.registry.Has(ConvID(id)) {
		t.Fatal("direct path opened a second chat")
	}
	mark := copied.Len()
	if err := dialer.app.handleLine("hello-direct"); err != nil {
		t.Fatal(err)
	}
	waitLine(t, listener.app, "hello-direct")
	if copied.Len() != mark || bytes.Contains(copied.Bytes()[mark:], []byte("hello-direct")) {
		t.Fatal("direct line still crossed the relay")
	}
	if err := offerAgain(t, dialer.app, listener.app, id); err == nil {
		t.Fatal("second extra handshake was accepted")
	}
	if listener.app.registry.Len() != 1 {
		t.Fatalf("len %d", listener.app.registry.Len())
	}
}

func TestRelayBudgetRemovesChat(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(2 * time.Second)
	setDialTimeout(30 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, _ := startSplicer(t)
	dialer, listener := startPathPair(t, base, "dev", "bud-alice", "bud-bob", "", relayAddr, "", relayAddr)
	rewritePeer(t, base, "dev", listener.app.myKey, "127.0.0.1:1")
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	id := acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, phraseRelayStopped, 4*time.Second)
	waitPathLine(t, listener.buf, phraseRelayStopped, 4*time.Second)
	if dialer.app.registry.Has(ConvID(id)) || listener.app.registry.Has(ConvID(id)) {
		t.Fatal("bootstrap chat still open")
	}
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	next := acceptPath(t, base, "dev", listener)
	if next == id {
		t.Fatal("retry reused the request")
	}
}

func TestNoRelayFailsWithoutDial(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(5 * time.Second)
	setDialTimeout(30 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, accepts := listenRelayCount(t)
	_ = relayAddr
	dialer, listener := startPathPair(t, base, "dev", "none-alice", "none-bob", "", "", "", "")
	rewritePeer(t, base, "dev", listener.app.myKey, "127.0.0.1:1")
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, phraseNoRelay, 3*time.Second)
	waitPathLine(t, listener.buf, phraseNoRelay, 3*time.Second)
	if accepts.Load() != 0 {
		t.Fatalf("relay dials %d", accepts.Load())
	}
}

func TestDirectResumeSkipsRelay(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(8 * time.Second)
	setDialTimeout(200 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, accepts := listenRelayCount(t)
	dialer, listener := startPathPair(t, base, "dev", "res-alice", "res-bob", "", relayAddr, "", relayAddr)
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	id := acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
	waitPathLine(t, listener.buf, connectedPrompt(false), 3*time.Second)
	if dialer.app.registry.Bootstrap(ConvID(id)) {
		t.Fatal("direct chat marked bootstrap")
	}
	focused, ok := dialer.app.registry.Focused()
	if !ok {
		t.Fatal("no chat")
	}
	focused.stateMu.Lock()
	live, isLive := focused.carrier.(carrierLive)
	focused.stateMu.Unlock()
	if !isLive || live.conn == nil {
		t.Fatal("no carrier")
	}
	_ = live.conn.CloseNow()
	time.Sleep(300 * time.Millisecond)
	if accepts.Load() != 0 {
		t.Fatalf("relay dials %d", accepts.Load())
	}
	if !dialer.app.registry.Has(ConvID(id)) {
		t.Fatal("resume dropped the chat")
	}
}

func TestTwoChatsStopOnlyOneRelay(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(6 * time.Second)
	setDialTimeout(40 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, _ := startSplicer(t)
	dialer := startPathSurface(t, base, "dev", "two-alice", "", relayAddr, "127.0.0.1:0")
	bob := startPathSurface(t, base, "dev", "two-bob", "", relayAddr, "127.0.0.1:0")
	carol := startPathSurface(t, base, "dev", "two-carol", "", relayAddr, "127.0.0.1:0")
	rewritePeer(t, base, "dev", bob.app.myKey, "127.0.0.1:1")
	rewritePeer(t, base, "dev", carol.app.myKey, "127.0.0.1:1")
	go func() { _ = dialer.app.handleLine("connect " + bob.app.myKey) }()
	bobID := acceptPath(t, base, "dev", bob)
	waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
	go func() { _ = dialer.app.handleLine("connect " + carol.app.myKey) }()
	carolID := acceptPath(t, base, "dev", carol)
	waitUntil(t, 3*time.Second, func() bool {
		return dialer.app.registry.Bootstrap(ConvID(carolID))
	})
	rewritePeer(t, base, "dev", bob.app.myKey, bob.app.listenAddr.String())
	waitUntil(t, 4*time.Second, func() bool {
		return dialer.app.registry.Has(ConvID(bobID)) && !dialer.app.registry.Bootstrap(ConvID(bobID))
	})
	if !dialer.app.registry.Bootstrap(ConvID(carolID)) {
		t.Fatal("second relay stopped with the first chat")
	}
	waitPathLine(t, carol.buf, phraseRelayStopped, 8*time.Second)
	if !dialer.app.registry.Has(ConvID(bobID)) || dialer.app.registry.Bootstrap(ConvID(bobID)) {
		t.Fatal("first chat did not stay direct")
	}
}

func TestOneSidedDirectSocketKeepsRelay(t *testing.T) {
	prevBudget, prevDial, prevPoll := loadAttemptBudget(), loadDialTimeout(), loadAcceptPoll()
	setAttemptBudget(8 * time.Second)
	setDialTimeout(40 * time.Millisecond)
	setAcceptPoll(20 * time.Millisecond)
	t.Cleanup(func() {
		setAttemptBudget(prevBudget)
		setDialTimeout(prevDial)
		setAcceptPoll(prevPoll)
	})
	base, _ := startPathConnector(t)
	relayAddr, _ := startSplicer(t)
	dialer, listener := startPathPair(t, base, "dev", "side-alice", "side-bob", "", relayAddr, "", relayAddr)
	rewritePeer(t, base, "dev", listener.app.myKey, "127.0.0.1:1")
	go func() { _ = dialer.app.handleLine("connect " + listener.app.myKey) }()
	id := acceptPath(t, base, "dev", listener)
	waitPathLine(t, dialer.buf, connectedPrompt(true), 3*time.Second)
	conn, err := net.Dial("tcp", listener.app.listenAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(200 * time.Millisecond)
	if !dialer.app.registry.Bootstrap(ConvID(id)) || !listener.app.registry.Has(ConvID(id)) {
		t.Fatal("one-sided socket ended the relay chat")
	}
}

func waitLine(t *testing.T, a *app, text string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		focused, ok := a.registry.Focused()
		if ok && hasLine(focused.Lines(), text) {
			return
		}
		for _, line := range a.registry.StatusLines() {
			_ = line
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("missing line %q", text)
}

func hasLine(lines []string, text string) bool {
	for _, line := range lines {
		if bytes.Contains([]byte(line), []byte(text)) {
			return true
		}
	}
	return false
}

func waitDirect(t *testing.T, dialer, listener *app, id ConvID) {
	t.Helper()
	waitUntil(t, 5*time.Second, func() bool {
		return dialer.registry.Has(id) && !dialer.registry.Bootstrap(id) && listener.registry.Has(id) && !listener.registry.Bootstrap(id)
	})
}

func waitUntil(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func offerAgain(t *testing.T, dialer, listener *app, id string) error {
	t.Helper()
	var body struct {
		Identity string `json:"identity"`
	}
	if err := json.Unmarshal(getPath(t, dialer.base, dialer.token, "/peers/"+listener.myKey), &body); err != nil {
		t.Fatal(err)
	}
	pub, err := session.ParsePublic(body.Identity)
	if err != nil {
		t.Fatal(err)
	}
	key, err := session.DeriveHandshakeKey(dialer.identity, pub, id)
	if err != nil {
		t.Fatal(err)
	}
	msg, _, err := session.Offer(id, dialer.myKey, listener.myKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws://"+listener.listenAddr.String(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(ctx, websocket.MessageBinary, msg); err != nil {
		return err
	}
	_, _, err = conn.Read(ctx)
	return err
}

func (b *lockedBuffer) Len() int {
	if b == nil {
		return 0
	}
	return len(b.Bytes())
}
