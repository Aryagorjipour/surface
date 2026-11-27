package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Aryagorjipour/surface/internal/hostport"
	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

var (
	attemptBudget = 20 * time.Second
	dialTimeout   = 2 * time.Second
	acceptPoll    = time.Second
	punchEvery    = 100 * time.Millisecond
	wsClient      = &http.Client{Transport: happyTransport()}
)

func happyTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = happyDial
	return tr
}

func happyDial(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout:       30 * time.Second,
		KeepAlive:     30 * time.Second,
		FallbackDelay: 300 * time.Millisecond,
	}
	return dialer.DialContext(ctx, network, addr)
}

func directURL(addr hostport.Addr, secure bool) string {
	scheme := "ws"
	if secure {
		scheme = "wss"
	}
	return scheme + "://" + addr.String()
}

func dialPeer(ctx context.Context, addr hostport.Addr, secure bool, roots *x509.CertPool) (*websocket.Conn, error) {
	client := wsClient
	if secure {
		tr := happyTransport()
		tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		client = &http.Client{Transport: tr}
	}
	conn, _, err := websocket.Dial(ctx, directURL(addr, secure), &websocket.DialOptions{
		HTTPClient: client,
	})
	return conn, err
}

func dialWebSocket(ctx context.Context, addr hostport.Addr) (*websocket.Conn, error) {
	return dialPeer(ctx, addr, false, nil)
}

func setAttemptBudget(d time.Duration) { attemptBudget = d }

func setDialTimeout(d time.Duration) { dialTimeout = d }

func (a *app) attempt(dialer bool, addr, id, caller, target string, proof session.HandshakeKey) error {
	budget := attemptBudget
	if dialer && acceptPoll < budget {
		budget -= acceptPoll
	}
	ctx, cancel := context.WithDeadline(a.ctx, time.Now().Add(budget))
	started := make(chan struct{})
	go func() {
		defer cancel()
		defer a.postDone(id)
		a.runAttempt(ctx, dialer, addr, id, caller, target, proof, started)
	}()
	select {
	case <-started:
		return nil
	case <-ctx.Done():
		return nil
	case <-a.ctx.Done():
		return nil
	}
}

func (a *app) runAttempt(ctx context.Context, dialer bool, addr, id, caller, target string, proof session.HandshakeKey, started chan struct{}) {
	var once sync.Once
	release := func() { once.Do(func() { close(started) }) }
	defer release()
	if dialer {
		if a.dialDirect(ctx, addr, id, caller, target, proof) {
			return
		}
	} else if a.waitDirect(ctx, dialTimeout, id) {
		return
	}
	if ctx.Err() != nil {
		_ = a.failAttempt(ctx, id)
		return
	}
	done := make(chan struct{})
	go func() {
		a.fallbacks(ctx, id, caller, target, proof, dialer)
		close(done)
	}()
	var nextDial time.Time
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if a.directReady(id) {
			return
		}
		if a.onBootstrap(id) {
			release()
		}
		select {
		case <-ctx.Done():
			_ = a.failAttempt(ctx, id)
			return
		case <-done:
			done = nil
			if a.directReady(id) {
				return
			}
			if !a.onBootstrap(id) && strings.TrimSpace(a.relayAddr) == "" {
				_ = a.failAttempt(ctx, id)
				return
			}
		case <-tick.C:
			if dialer && a.onBootstrap(id) && !time.Now().Before(nextDial) {
				nextDial = time.Now().Add(50 * time.Millisecond)
				if a.dialDirect(ctx, a.directAddr(target, addr), id, caller, target, proof) {
					return
				}
			}
		}
	}
}

func (a *app) fallbacks(ctx context.Context, id, caller, target string, proof session.HandshakeKey, dialer bool) {
	if strings.TrimSpace(a.punchAddr) != "" {
		a.punchPath(ctx, id, caller, target, proof, dialer)
	}
	if a.chatting(id) || ctx.Err() != nil {
		return
	}
	if strings.TrimSpace(a.relayAddr) != "" {
		a.relayPath(ctx, id, caller, target, proof, dialer)
	}
}

const (
	phraseRelayStopped = "direct connection was not established in time; the temporary relay has stopped"
	phraseNoRelay      = "direct connection was not established"
)

func (a *app) failAttempt(ctx context.Context, id string) error {
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		if a.registry != nil {
			_ = a.registry.DropBootstrap(ConvID(id))
		}
		return nil
	}
	dropped := false
	if a.registry != nil {
		dropped = a.registry.DropBootstrap(ConvID(id))
	}
	if a.directReady(id) {
		return nil
	}
	phrase := phraseNoRelay
	if dropped || strings.TrimSpace(a.relayAddr) != "" {
		phrase = phraseRelayStopped
	}
	a.say(phrase)
	a.refreshLimitWarning()
	if a.registry == nil || a.registry.Len() == 0 {
		a.markPhase(phaseLobby)
	}
	return nil
}

func (a *app) directReady(id string) bool {
	if a.registry == nil || !a.registry.Has(ConvID(id)) {
		return false
	}
	return !a.registry.Bootstrap(ConvID(id))
}

func (a *app) onBootstrap(id string) bool {
	return a.registry != nil && a.registry.Bootstrap(ConvID(id))
}

func (a *app) directAddr(target, fallback string) string {
	if strings.TrimSpace(target) == "" {
		return fallback
	}
	peer, err := a.lookup(target)
	if err != nil || peer == nil || strings.TrimSpace(peer.Address) == "" {
		return fallback
	}
	return peer.Address
}

func (a *app) waitDirect(ctx context.Context, d time.Duration, id string) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if a.directReady(id) {
			return true
		}
		select {
		case <-ctx.Done():
			return a.directReady(id)
		case <-timer.C:
			return a.directReady(id)
		case <-tick.C:
		}
	}
}

func (a *app) chatting(id string) bool {
	if a.registry == nil {
		return false
	}
	return a.registry.Has(ConvID(id))
}

func (a *app) waitConnected(ctx context.Context, d time.Duration, id string) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if a.chatting(id) {
			return true
		}
		select {
		case <-ctx.Done():
			return a.chatting(id)
		case <-timer.C:
			return a.chatting(id)
		case <-tick.C:
		}
	}
}

func (a *app) dialDirect(ctx context.Context, raw, id, caller, target string, proof session.HandshakeKey) bool {
	addr, err := hostport.Parse(raw)
	if err != nil {
		return false
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := dialPeer(dialCtx, addr, a.directSecure, a.directRoots)
	cancel()
	if err != nil {
		return false
	}
	sess, err := a.gate.dialAdmitted(ctx, c, id, caller, target, proof)
	if err != nil {
		_ = c.Close(websocket.StatusPolicyViolation, "")
		return false
	}
	if a.onBootstrap(id) {
		if err := a.registry.Migrate(ConvID(id), c, sess); err != nil {
			_ = c.Close(websocket.StatusPolicyViolation, "")
			return false
		}
		a.say(connectedPrompt(true))
		return true
	}
	a.say(connectedPrompt(true))
	if !a.beginChat(c, sess, true, addr, ConvID(id), target, false, time.Time{}) {
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

func (a *app) punchPath(ctx context.Context, id, caller, target string, proof session.HandshakeKey, dialer bool) {
	if a.chatting(id) {
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
		if a.chatting(id) || ctx.Err() != nil {
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

func (a *app) udpDial(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, id, caller, target string, proof session.HandshakeKey) bool {
	msg, st, err := session.Offer(id, caller, target, proof)
	if err != nil {
		return false
	}
	link := &udpLink{conn: conn, peer: remote}
	tick := time.NewTicker(punchEvery)
	defer tick.Stop()
	for {
		if a.chatting(id) || ctx.Err() != nil {
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
		addr, err := hostport.Parse(remote.String())
		if err != nil {
			return false
		}
		if a.beginChat(link, sess, true, addr, ConvID(id), target, false, time.Time{}) {
			a.say(connectedPrompt(true))
			return true
		}
		return false
	}
}

func (a *app) udpAccept(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, id, caller, target string, proof session.HandshakeKey) bool {
	link := &udpLink{conn: conn, peer: remote}
	var cache offerCache
	for {
		if a.chatting(id) || ctx.Err() != nil {
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
		addr, err := hostport.Parse(remote.String())
		if err != nil {
			return false
		}
		if a.beginChat(link, sess, false, addr, ConvID(id), caller, false, time.Time{}) {
			a.say("\n" + connectedPrompt(false))
			return true
		}
		return false
	}
}

func (a *app) takeUDPOffer(cache *offerCache, msg []byte, id, caller, target string, proof session.HandshakeKey) (reply []byte, sess *session.Session, reused bool, refused bool) {
	var opened *session.Session
	out, reused, refused := cache.decide(msg, func(in []byte) ([]byte, error) {
		reply, next, err := a.acceptDirect(in, id, caller, target, proof)
		opened = next
		return reply, err
	})
	return out, opened, reused, refused
}

func (a *app) acceptDirect(msg []byte, id, caller, target string, proof session.HandshakeKey) ([]byte, *session.Session, error) {
	reply, sess, err := session.AcceptOffer(msg, id, caller, target, proof, a.gate.alreadyAdmitted(id))
	if err != nil {
		return nil, nil, err
	}
	if err := a.gate.admit(id, sess); err != nil {
		return nil, nil, err
	}
	return reply, sess, nil
}

func (a *app) relayPath(ctx context.Context, id, caller, target string, proof session.HandshakeKey, dialer bool) {
	if a.chatting(id) {
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
		if a.beginChat(link, sess, true, hostport.Addr{}, ConvID(id), target, true, deadlineOf(ctx)) {
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
	if a.beginChat(link, sess, false, hostport.Addr{}, ConvID(id), caller, true, deadlineOf(ctx)) {
		owned = false
		a.say("\n" + connectedPrompt(false))
	}
}

func deadlineOf(ctx context.Context) time.Time {
	if ctx == nil {
		return time.Time{}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	return deadline
}
