package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

type pipeConn struct {
	in     chan []byte
	closed chan struct{}
	once   sync.Once
	peer   *pipeConn
}

func pipePair() (*pipeConn, *pipeConn) {
	a := &pipeConn{in: make(chan []byte, 8), closed: make(chan struct{})}
	b := &pipeConn{in: make(chan []byte, 8), closed: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (p *pipeConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-p.closed:
		return 0, nil, errors.New("closed")
	case b := <-p.in:
		return websocket.MessageBinary, b, nil
	}
}

func (p *pipeConn) Write(ctx context.Context, _ websocket.MessageType, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return errors.New("closed")
	case <-p.peer.closed:
		return errors.New("closed")
	case p.peer.in <- append([]byte(nil), data...):
		return nil
	}
}

func (p *pipeConn) Close(websocket.StatusCode, string) error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

func (p *pipeConn) CloseNow() error {
	return p.Close(0, "")
}

func testRegistry(t *testing.T, max int) *Registry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewRegistry(ctx, max, time.Minute)
}

func attachPipe(t *testing.T, r *Registry, id ConvID, peer string) *Conversation {
	t.Helper()
	dialer, _ := admittedPair(t)
	conn, other := pipePair()
	t.Cleanup(func() {
		_ = conn.CloseNow()
		_ = other.CloseNow()
	})
	c, err := r.Attach(AttachReq{ID: id, Peer: peer, Sess: dialer, Conn: conn})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func waitConvEvent(t *testing.T, events <-chan ConvEvent, kind EvKind, id ConvID) ConvEvent {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Kind == kind && (id == "" || ev.ID == id) {
				return ev
			}
		case <-deadline:
			t.Fatalf("timeout waiting for kind %d id %s", kind, id)
			return ConvEvent{}
		}
	}
}

func waitGone(t *testing.T, r *Registry, id ConvID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !r.Has(id) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("conversation %s still live", id)
}

func TestRegistryTwoAttaches(t *testing.T) {
	r := testRegistry(t, 4)
	a := attachPipe(t, r, "req-a", "alice")
	b := attachPipe(t, r, "req-b", "bob")
	if !r.Has(a.ID()) || !r.Has(b.ID()) {
		t.Fatal("both attaches should stay live")
	}
	if r.Len() != 2 {
		t.Fatalf("len %d", r.Len())
	}
	focused, ok := r.Focused()
	if !ok || focused.ID() != b.ID() {
		t.Fatalf("new attach should take focus, got %+v", focused)
	}
}

func TestRegistryCapRefuseLeavesFirstLive(t *testing.T) {
	r := testRegistry(t, 1)
	first := attachPipe(t, r, "req-a", "alice")
	dialer, _ := admittedPair(t)
	conn, other := pipePair()
	t.Cleanup(func() {
		_ = conn.CloseNow()
		_ = other.CloseNow()
	})
	_, err := r.Attach(AttachReq{ID: "req-b", Peer: "bob", Sess: dialer, Conn: conn})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("attach: %v", err)
	}
	if !r.Has(first.ID()) || r.Len() != 1 {
		t.Fatal("capacity refuse must leave the first conversation live")
	}
	focused, ok := r.Focused()
	if !ok || focused.ID() != first.ID() {
		t.Fatal("focus moved after a refused attach")
	}
}

func TestRegistryResumeDoesNotStealFocus(t *testing.T) {
	r := testRegistry(t, 4)
	aLeft, aRight := pipePair()
	bLeft, bRight := pipePair()
	t.Cleanup(func() {
		_ = aLeft.CloseNow()
		_ = aRight.CloseNow()
		_ = bLeft.CloseNow()
		_ = bRight.CloseNow()
	})
	sessA, _ := admittedPair(t)
	sessB, _ := admittedPair(t)
	if _, err := r.Attach(AttachReq{ID: "req-a", Peer: "alice", Sess: sessA, Conn: aLeft}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Attach(AttachReq{ID: "req-b", Peer: "bob", Sess: sessB, Conn: bLeft}); err != nil {
		t.Fatal(err)
	}
	_ = aLeft.CloseNow()
	waitConvEvent(t, r.Events(), EvBroke, "req-a")
	resumeLeft, resumeRight := pipePair()
	t.Cleanup(func() {
		_ = resumeLeft.CloseNow()
		_ = resumeRight.CloseNow()
	})
	if err := r.DeliverResume("req-a", resumeLeft, sessA); err != nil {
		t.Fatal(err)
	}
	waitConvEvent(t, r.Events(), EvResumed, "req-a")
	focused, ok := r.Focused()
	if !ok || focused.ID() != "req-b" {
		t.Fatalf("resume stole focus: %+v", focused)
	}
	if !r.Has("req-a") || !r.Has("req-b") {
		t.Fatal("resume dropped a conversation")
	}
}

func TestRegistryFocusEmitsNoEnd(t *testing.T) {
	r := testRegistry(t, 4)
	a := attachPipe(t, r, "req-a", "alice")
	b := attachPipe(t, r, "req-b", "bob")
	if err := r.Focus(a.ID()); err != nil {
		t.Fatal(err)
	}
	if err := r.Focus(b.ID()); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-r.Events():
		t.Fatalf("focus emitted %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	if !r.Has(a.ID()) || !r.Has(b.ID()) {
		t.Fatal("focus closed a conversation")
	}
}

func TestRegistryQuitEndsOnlyFocused(t *testing.T) {
	r := testRegistry(t, 4)
	a := attachPipe(t, r, "req-a", "alice")
	b := attachPipe(t, r, "req-b", "bob")
	b.Quit()
	ev := waitConvEvent(t, r.Events(), EvEnded, b.ID())
	if ev.Text != "quit" {
		t.Fatalf("ended text %q", ev.Text)
	}
	waitGone(t, r, b.ID())
	if !r.Has(a.ID()) {
		t.Fatal("quit closed the unfocused conversation")
	}
	focused, ok := r.Focused()
	if !ok || focused.ID() != a.ID() {
		t.Fatalf("focus after quit %+v %v", focused, ok)
	}
}

func TestTwoChatsExchangeIndependently(t *testing.T) {
	leftReg := testRegistry(t, 4)
	rightReg := testRegistry(t, 4)
	d1, l1, c1, s1 := handshakePair(t, "req-1")
	d2, l2, c2, s2 := handshakePair(t, "req-2")
	if _, err := leftReg.Attach(AttachReq{ID: "req-1", Peer: "bob", Sess: d1, Conn: c1, Dialer: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := rightReg.Attach(AttachReq{ID: "req-1", Peer: "alice", Sess: l1, Conn: s1}); err != nil {
		t.Fatal(err)
	}
	if _, err := leftReg.Attach(AttachReq{ID: "req-2", Peer: "carol", Sess: d2, Conn: c2, Dialer: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := rightReg.Attach(AttachReq{ID: "req-2", Peer: "alice", Sess: l2, Conn: s2}); err != nil {
		t.Fatal(err)
	}
	if err := leftReg.Focus("req-1"); err != nil {
		t.Fatal(err)
	}
	focused, _ := leftReg.Focused()
	if err := focused.SendText("alpha"); err != nil {
		t.Fatal(err)
	}
	ev := waitConvEvent(t, rightReg.Events(), EvPeerLine, "req-1")
	if ev.Text != "alpha" {
		t.Fatalf("req-1 got %q", ev.Text)
	}
	if err := leftReg.Focus("req-2"); err != nil {
		t.Fatal(err)
	}
	focused, _ = leftReg.Focused()
	if err := focused.SendText("beta"); err != nil {
		t.Fatal(err)
	}
	ev = waitConvEvent(t, rightReg.Events(), EvPeerLine, "req-2")
	if ev.Text != "beta" {
		t.Fatalf("req-2 got %q", ev.Text)
	}
	if !leftReg.Has("req-1") || !rightReg.Has("req-1") {
		t.Fatal("focus switch dropped req-1")
	}
	if err := rightReg.Focus("req-1"); err != nil {
		t.Fatal(err)
	}
	focused, _ = rightReg.Focused()
	if err := focused.SendText("gamma"); err != nil {
		t.Fatal(err)
	}
	ev = waitConvEvent(t, leftReg.Events(), EvPeerLine, "req-1")
	if ev.Text != "gamma" {
		t.Fatalf("inactive req-1 got %q", ev.Text)
	}
}

func TestFailedOpenEndsBothSides(t *testing.T) {
	dialer, listener, client, server := handshakePair(t, "req-open")
	other, _ := admittedPair(t)
	foreign, err := session.Seal(other, "not-the-line", 0)
	if err != nil {
		t.Fatal(err)
	}
	left := testRegistry(t, 4)
	right := testRegistry(t, 4)
	if _, err := left.Attach(AttachReq{ID: "req-open", Peer: "right", Sess: dialer, Conn: client, Dialer: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Attach(AttachReq{ID: "req-open", Peer: "left", Sess: listener, Conn: server}); err != nil {
		t.Fatal(err)
	}
	if err := client.Write(context.Background(), websocket.MessageBinary, foreign); err != nil {
		t.Fatal(err)
	}
	waitConvEvent(t, left.Events(), EvEnded, "req-open")
	waitConvEvent(t, right.Events(), EvEnded, "req-open")
	waitGone(t, left, "req-open")
	waitGone(t, right, "req-open")
	if !session.Finished(dialer) || !session.Finished(listener) {
		t.Fatal("session still resumable")
	}
	if _, err := session.ResumeOffer(dialer); err == nil {
		t.Fatal("dialer resumed after a failed open")
	}
}

func TestNetworkCloseStillResumes(t *testing.T) {
	dialer, _, client, server := handshakePair(t, "req-net")
	r := testRegistry(t, 4)
	if _, err := r.Attach(AttachReq{ID: "req-net", Peer: "peer", Sess: dialer, Conn: client, Dialer: true, Addr: "127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	_ = server.Close(websocket.StatusGoingAway, "")
	waitConvEvent(t, r.Events(), EvBroke, "req-net")
	if !r.Has("req-net") || session.Finished(dialer) {
		t.Fatal("network close finished the session")
	}
	if !r.ExpectingResume() {
		t.Fatal("network close did not arm resume")
	}
}
