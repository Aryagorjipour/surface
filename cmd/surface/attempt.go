package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

var (
	attemptBudget = 30 * time.Second
	dialTimeout   = 2 * time.Second
	acceptPoll    = time.Second
	punchEvery    = 100 * time.Millisecond
)

func setAttemptBudget(d time.Duration) { attemptBudget = d }

func setDialTimeout(d time.Duration) { dialTimeout = d }

// attempt dials the registered address first. Punch runs to completion before
// a relay socket opens. The listener keeps the WebSocket accept loop running
// beside whichever fallback is active. The dialer's bound starts from the
// accept response, which can be one poll older than this call.
func (a *app) attempt(dialer bool, addr, id, caller, target string, proof []byte) error {
	budget := attemptBudget
	if dialer && acceptPoll < budget {
		budget -= acceptPoll
	}
	ctx, cancel := context.WithDeadline(a.ctx, time.Now().Add(budget))
	defer cancel()
	defer a.postDone(id)

	if dialer {
		if a.dialDirect(ctx, addr, id, caller, target, proof) {
			return nil
		}
	} else if a.waitConnected(ctx, dialTimeout) {
		return nil
	}
	if ctx.Err() != nil {
		return a.failAttempt(ctx)
	}
	go a.fallbacks(ctx, id, caller, target, proof, dialer)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if a.chatting() {
			cancel()
			return nil
		}
		select {
		case <-ctx.Done():
			return a.failAttempt(ctx)
		case <-tick.C:
		}
	}
}

func (a *app) fallbacks(ctx context.Context, id, caller, target string, proof []byte, dialer bool) {
	if strings.TrimSpace(a.punchAddr) != "" {
		a.punchPath(ctx, id, caller, target, proof, dialer)
	}
	if a.chatting() || ctx.Err() != nil {
		return
	}
	if strings.TrimSpace(a.relayAddr) != "" {
		a.relayPath(ctx, id, caller, target, proof, dialer)
	}
}

func (a *app) failAttempt(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		a.say("traversal failed")
		a.markPhase(phaseLobby)
	}
	return nil
}

func (a *app) chatting() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inChat
}

func (a *app) waitConnected(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if a.chatting() {
			return true
		}
		select {
		case <-ctx.Done():
			return a.chatting()
		case <-timer.C:
			return a.chatting()
		case <-tick.C:
		}
	}
}

func (a *app) dialDirect(ctx context.Context, addr, id, caller, target string, proof []byte) bool {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, _, err := websocket.Dial(dialCtx, "ws://"+addr, nil)
	cancel()
	if err != nil {
		return false
	}
	sess, err := a.gate.dialAdmitted(ctx, c, id, caller, target, proof)
	if err != nil {
		_ = c.Close(websocket.StatusPolicyViolation, "")
		return false
	}
	a.say(connectedPrompt(true))
	if !a.beginChat(c, sess, true, addr) {
		_ = c.Close(websocket.StatusNormalClosure, "")
	}
	return true
}

func (a *app) postDone(id string) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/requests/"+id+"/punch", strings.NewReader(`{"done":true}`))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	a.drain(resp.Body)
}

func (a *app) postTicket(ctx context.Context, id, ticket string) string {
	body := `{"ticket":"` + ticket + `"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/requests/"+id+"/punch", strings.NewReader(body))
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var out struct {
		Peer string `json:"peer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return ""
	}
	return out.Peer
}

func (a *app) punchPath(ctx context.Context, id, caller, target string, proof []byte, dialer bool) {
	if a.chatting() {
		return
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return
	}
	keep := false
	defer func() {
		if !keep {
			_ = conn.Close()
		}
	}()
	dest, err := net.ResolveUDPAddr("udp", a.punchAddr)
	if err != nil {
		return
	}
	ticket := make([]byte, 16)
	if _, err := rand.Read(ticket); err != nil {
		return
	}
	text := hex.EncodeToString(ticket)
	var peer string
	tick := time.NewTicker(punchEvery)
	defer tick.Stop()
	for peer == "" {
		if a.chatting() || ctx.Err() != nil {
			return
		}
		_, _ = conn.WriteToUDP(ticket, dest)
		peer = a.postTicket(ctx, id, text)
		if peer != "" {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
	remote, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		return
	}
	// Ticket posts use the attempt context until a peer is known. The datagram
	// exchange stops at half of the time still left when a relay is set, so
	// that relay can run before the budget ends.
	handCtx, stop := a.datagramCtx(ctx)
	defer stop()
	if dialer {
		keep = a.udpDial(handCtx, conn, remote, id, caller, target, proof)
		return
	}
	keep = a.udpAccept(handCtx, conn, remote, id, caller, target, proof)
}

func (a *app) datagramCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	noop := func() {}
	if strings.TrimSpace(a.relayAddr) == "" {
		return ctx, noop
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return ctx, noop
	}
	left := time.Until(deadline)
	if left <= 0 {
		return ctx, noop
	}
	return context.WithTimeout(ctx, left/2)
}

func (a *app) udpDial(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, id, caller, target string, proof []byte) bool {
	msg, st, err := session.Offer(id, caller, target, proof)
	if err != nil {
		return false
	}
	link := &udpLink{conn: conn, peer: remote}
	tick := time.NewTicker(punchEvery)
	defer tick.Stop()
	for {
		if a.chatting() || ctx.Err() != nil {
			return false
		}
		if _, err := conn.WriteToUDP(msg, remote); err != nil {
			return false
		}
		readCtx, cancel := context.WithTimeout(ctx, punchEvery)
		_, reply, err := link.Read(readCtx)
		cancel()
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			case <-tick.C:
				continue
			}
		}
		sess, err := session.FinishOffer(reply, st)
		if err != nil {
			continue
		}
		if a.beginChat(link, sess, true, remote.String()) {
			a.say(connectedPrompt(true))
			return true
		}
		return false
	}
}

func (a *app) udpAccept(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, id, caller, target string, proof []byte) bool {
	link := &udpLink{conn: conn, peer: remote}
	var cache offerCache
	for {
		if a.chatting() || ctx.Err() != nil {
			return false
		}
		_, msg, err := link.Read(ctx)
		if err != nil {
			return false
		}
		reply, sess, reused, refused := a.takeUDPOffer(&cache, msg, id, caller, target, proof)
		if refused || reply == nil {
			continue
		}
		if _, err := conn.WriteToUDP(reply, remote); err != nil {
			return false
		}
		if reused || sess == nil {
			continue
		}
		if a.beginChat(link, sess, false, remote.String()) {
			a.say("\n" + connectedPrompt(false))
			return true
		}
		return false
	}
}

func (a *app) takeUDPOffer(cache *offerCache, msg []byte, id, caller, target string, proof []byte) (reply []byte, sess *session.Session, reused bool, refused bool) {
	var opened *session.Session
	out, reused, refused := cache.decide(msg, func(in []byte) ([]byte, error) {
		reply, next, err := a.acceptDirect(in, id, caller, target, proof)
		opened = next
		return reply, err
	})
	return out, opened, reused, refused
}

func (a *app) acceptDirect(msg []byte, id, caller, target string, proof []byte) ([]byte, *session.Session, error) {
	a.gate.mu.Lock()
	_, spent := a.gate.live[id]
	a.gate.mu.Unlock()
	reply, sess, err := session.AcceptOffer(msg, id, caller, target, proof, spent)
	if err != nil {
		return nil, nil, err
	}
	a.gate.mu.Lock()
	if a.gate.live == nil {
		a.gate.live = map[string]*session.Session{}
	}
	if _, exists := a.gate.live[id]; exists {
		a.gate.mu.Unlock()
		return nil, nil, fmt.Errorf("refused")
	}
	a.gate.live[id] = sess
	a.gate.mu.Unlock()
	return reply, sess, nil
}

func (a *app) relayPath(ctx context.Context, id, caller, target string, proof []byte, dialer bool) {
	if a.chatting() {
		return
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", a.relayAddr)
	if err != nil {
		return
	}
	link := newRelayLink(conn)
	owned := true
	defer func() {
		if owned {
			_ = link.CloseNow()
		}
	}()
	if err := writeRelayHeader(conn, id); err != nil {
		return
	}
	if dialer {
		msg, st, err := session.Offer(id, caller, target, proof)
		if err != nil {
			return
		}
		if err := link.Write(ctx, websocket.MessageBinary, msg); err != nil {
			return
		}
		_, reply, err := link.Read(ctx)
		if err != nil {
			return
		}
		sess, err := session.FinishOffer(reply, st)
		if err != nil {
			return
		}
		if a.beginChat(link, sess, true, "") {
			owned = false
			a.say(connectedPrompt(true))
		}
		return
	}
	_, msg, err := link.Read(ctx)
	if err != nil {
		return
	}
	reply, sess, err := a.acceptDirect(msg, id, caller, target, proof)
	if err != nil {
		return
	}
	if err := link.Write(ctx, websocket.MessageBinary, reply); err != nil {
		return
	}
	if a.beginChat(link, sess, false, "") {
		owned = false
		a.say("\n" + connectedPrompt(false))
	}
}
