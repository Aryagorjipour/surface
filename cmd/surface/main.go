package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/Aryagorjipour/surface/internal/shared"
	"github.com/coder/websocket"
)

type peer struct {
	Key     string `json:"key"`
	Address string `json:"address"`
	State   string `json:"state"`
}

type connectResp struct {
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

type app struct {
	base         string
	myKey        string
	listenAddr   string
	token        string
	heartbeat    time.Duration
	resumeWindow time.Duration
	pendingPoll  time.Duration
	logger       *slog.Logger
	ctx          context.Context
	cancel       context.CancelFunc

	mu          sync.Mutex
	presence    string
	inChat      bool
	chatConn    byteConn
	sess        *session.Session
	dialer      bool
	peerAddr    string
	quitting    bool
	resumeUntil time.Time
	queued      []string
	fromKey     map[string]string
	gate        Gate
	punchAddr   string
	relayAddr   string
	out         io.Writer
}

func main() {
	logger := shared.NewLogger("surface")
	err := run(os.Args[1:], os.LookupEnv, logger, os.Stdin)
	if errors.Is(err, errHelp) {
		fmt.Print(surfaceHelp())
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(args []string, lookup func(string) (string, bool), logger *slog.Logger, stdin io.Reader) error {
	cfg, err := parseSurface(args, lookup)
	if err != nil {
		return err
	}
	if len(cfg.rest) < 2 {
		return errors.New("usage: surface <connector-url> <my-key> [listen-addr]")
	}
	token, _ := lookup("SURFACE_CONNECTOR_TOKEN")
	if token == "" {
		return errors.New("SURFACE_CONNECTOR_TOKEN is required")
	}
	listenAddr := "127.0.0.1:9001"
	if len(cfg.rest) > 2 {
		listenAddr = cfg.rest[2]
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	endpoint, _ := lookup("OTEL_EXPORTER_OTLP_ENDPOINT")
	shutdown, traceErr := shared.InitOTEL(ctx, "surface", logger, endpoint)
	if traceErr != nil {
		logger.Warn("otel init failed, continuing without traces", "err", traceErr)
	} else {
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				logger.Warn("otel shutdown", "err", err)
			}
		}()
	}
	a := &app{
		base:         strings.TrimRight(cfg.rest[0], "/"),
		myKey:        cfg.rest[1],
		listenAddr:   listenAddr,
		token:        token,
		heartbeat:    cfg.heartbeat,
		resumeWindow: cfg.resume,
		pendingPoll:  cfg.pending,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		presence:     "available",
		fromKey:      map[string]string{},
		punchAddr:    cfg.punch,
		relayAddr:    cfg.relay,
	}
	a.gate.readable = a.requestReadable
	go a.serveWS()
	if err := a.register("available"); err != nil {
		return err
	}
	logger.Info("registered")
	go a.pollPending()
	go a.heartbeatLoop()

	fmt.Println("commands: lookup <key> | connect <key> | accept <id> | reject <id> | quit")
	fmt.Println("(in chat: just type messages; /quit leaves chat)")

	sc := bufio.NewScanner(stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := a.handleLine(line); err != nil {
			if errors.Is(err, errStop) {
				return nil
			}
			return err
		}
	}
	return sc.Err()
}

func (a *app) handleLine(line string) error {
	a.mu.Lock()
	chatting := a.inChat
	conn := a.chatConn
	sess := a.sess
	a.mu.Unlock()

	if chatting {
		if line == "/quit" {
			a.quitChat()
			return nil
		}
		if conn == nil {
			a.mu.Lock()
			a.queued = append(a.queued, line)
			a.mu.Unlock()
			return nil
		}
		frame, err := session.Seal(sess, line, session.Highest(sess))
		if err != nil {
			fmt.Println("send failed:", err)
			return nil
		}
		if err := conn.Write(a.ctx, websocket.MessageBinary, frame); err != nil {
			fmt.Println("send failed:", err)
		}
		return nil
	}

	parts := strings.Fields(line)
	switch parts[0] {
	case "quit", "exit":
		a.cancel()
		return errStop
	case "lookup":
		if len(parts) < 2 {
			fmt.Println("usage: lookup <key>")
			return nil
		}
		p, err := a.lookup(parts[1])
		if err != nil {
			fmt.Println("lookup error:", err)
			return nil
		}
		fmt.Printf("peer: %+v\n", p)
	case "connect":
		if len(parts) < 2 {
			fmt.Println("usage: connect <key>")
			return nil
		}
		if err := a.doConnect(parts[1]); err != nil {
			fmt.Println("connect error:", err)
		}
	case "accept":
		if len(parts) < 2 {
			fmt.Println("usage: accept <request-id>")
			return nil
		}
		proofText, err := a.postAction(parts[1], "accept")
		if err != nil {
			fmt.Println("accept error:", err)
			return nil
		}
		raw, err := session.DecodeProof(proofText)
		if err != nil {
			fmt.Println("accept error:", err)
			return nil
		}
		from := a.callerFor(parts[1])
		a.gate.remember(parts[1], from, a.myKey, raw)
		a.say("accepted — waiting for peer to dial")
		go func() {
			_ = a.attempt(false, "", parts[1], from, a.myKey, raw)
		}()
	case "reject":
		if len(parts) < 2 {
			fmt.Println("usage: reject <request-id>")
			return nil
		}
		if _, err := a.postAction(parts[1], "reject"); err != nil {
			fmt.Println("reject error:", err)
		} else {
			a.say("rejected")
		}
	default:
		fmt.Println("unknown command")
	}
	return nil
}

var errStop = errors.New("stop")

func (a *app) quitChat() {
	a.mu.Lock()
	a.quitting = true
	conn := a.chatConn
	sess := a.sess
	a.inChat = false
	a.chatConn = nil
	a.queued = nil
	a.resumeUntil = time.Time{}
	a.mu.Unlock()
	if sess != nil && conn != nil {
		if frame, err := session.End(sess); err == nil {
			_ = conn.Write(a.ctx, websocket.MessageBinary, frame)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
	fmt.Println("left chat")
}

func (a *app) drain(body io.ReadCloser) {
	if body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		a.logger.Warn("drain response")
	}
	if err := body.Close(); err != nil {
		a.logger.Warn("close response")
	}
}

func (a *app) do(method, url string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(a.ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

func (a *app) register(state string) error {
	a.mu.Lock()
	a.presence = state
	a.mu.Unlock()
	body, _ := json.Marshal(map[string]string{
		"key": a.myKey, "address": a.listenAddr, "state": state,
	})
	resp, err := a.do(http.MethodPost, a.base+"/peers", body)
	if err != nil {
		return err
	}
	a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (a *app) heartbeatOnce() error {
	a.mu.Lock()
	state := a.presence
	a.mu.Unlock()
	if state == "" {
		state = "available"
	}
	return a.register(state)
}

func (a *app) heartbeatLoop() {
	tick := time.NewTicker(a.heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-tick.C:
			if err := a.heartbeatOnce(); err != nil && a.logger != nil {
				a.logger.Warn("heartbeat")
			}
		}
	}
}

func (a *app) lookup(key string) (*peer, error) {
	resp, err := a.do(http.MethodGet, a.base+"/peers/"+key, nil)
	if err != nil {
		return nil, err
	}
	defer a.drain(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("not found")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var p peer
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (a *app) pollPending() {
	tick := time.NewTicker(a.pendingPoll)
	defer tick.Stop()
	seen := map[string]bool{}
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-tick.C:
			resp, err := a.do(http.MethodGet, a.base+"/pending?key="+a.myKey, nil)
			if err != nil {
				continue
			}
			var reqs []map[string]any
			err = json.NewDecoder(resp.Body).Decode(&reqs)
			a.drain(resp.Body)
			if err != nil {
				if a.logger != nil {
					a.logger.Warn("decode pending")
				}
				continue
			}
			for _, r := range reqs {
				id, _ := r["request_id"].(string)
				from, _ := r["from_key"].(string)
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				a.mu.Lock()
				a.fromKey[id] = from
				a.mu.Unlock()
				fmt.Printf("\n>>> incoming request from %s\n>>> id: %s\n>>> type: accept %s   or   reject %s\n> ", from, id, id, id)
			}
		}
	}
}

func (a *app) postAction(id, action string) (string, error) {
	resp, err := a.do(http.MethodPost, a.base+"/requests/"+id+"/"+action, nil)
	if err != nil {
		return "", err
	}
	defer a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
		Proof  string `json:"admission_proof"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil && action == "accept" {
		return "", err
	}
	return body.Proof, nil
}

func (a *app) doConnect(target string) error {
	p, err := a.lookup(target)
	if err != nil {
		return err
	}
	if p.State != "available" {
		return fmt.Errorf("target state %s", p.State)
	}

	body, _ := json.Marshal(map[string]string{
		"from_key": a.myKey, "to_key": target, "from_address": a.listenAddr,
	})
	resp, err := a.do(http.MethodPost, a.base+"/connect", body)
	if err != nil {
		return err
	}
	defer a.drain(resp.Body)
	var cr connectResp
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return err
	}
	fmt.Println("request created:", cr.RequestID)

	for i := 0; i < 30; i++ {
		time.Sleep(acceptPoll)
		resp, err := a.do(http.MethodGet, a.base+"/requests/"+cr.RequestID, nil)
		if err != nil {
			continue
		}
		var status struct {
			Status string `json:"status"`
			Proof  string `json:"admission_proof"`
		}
		err = json.NewDecoder(resp.Body).Decode(&status)
		a.drain(resp.Body)
		if err != nil {
			if a.logger != nil {
				a.logger.Warn("decode request status")
			}
			continue
		}
		if status.Status == "accepted" {
			raw, err := session.DecodeProof(status.Proof)
			if err != nil {
				return err
			}
			a.say("accepted — dialing", p.Address)
			return a.attempt(true, p.Address, cr.RequestID, a.myKey, target, raw)
		}
		if status.Status == "rejected" {
			return fmt.Errorf("rejected")
		}
	}
	return fmt.Errorf("timeout waiting for accept")
}

func (a *app) serveWS() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		a.onIncoming(c)
	})
	srv := &http.Server{Addr: a.listenAddr, Handler: mux}
	go func() {
		<-a.ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil && a.logger != nil {
			a.logger.Error("ws shutdown")
		}
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed && a.logger != nil {
		a.logger.Error("ws listen failed")
	}
}

func (a *app) onIncoming(c byteConn) {
	a.mu.Lock()
	if a.chatConn != nil {
		a.mu.Unlock()
		_ = c.Close(websocket.StatusPolicyViolation, "")
		return
	}
	until := a.resumeUntil
	a.mu.Unlock()
	var admitted bool
	ready := func(sess *session.Session) {
		admitted = true
		if a.beginChat(c, sess, false, "") {
			a.say("\n" + connectedPrompt(false))
		}
	}
	sess, err := a.gate.take(a.ctx, c, time.Now(), until, ready)
	if err != nil || admitted || sess == nil {
		return
	}
	a.resumeAttached(c, sess)
}

func (a *app) say(args ...any) {
	if a == nil || a.out == nil {
		fmt.Println(args...)
		return
	}
	_, _ = fmt.Fprintln(a.out, args...)
}

func (a *app) callerFor(id string) string {
	a.mu.Lock()
	from := a.fromKey[id]
	a.mu.Unlock()
	if from != "" {
		return from
	}
	resp, err := a.do(http.MethodGet, a.base+"/requests/"+id, nil)
	if err != nil {
		return ""
	}
	defer a.drain(resp.Body)
	var body struct {
		From string `json:"from_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return ""
	}
	a.mu.Lock()
	a.fromKey[id] = body.From
	a.mu.Unlock()
	return body.From
}

func (a *app) beginChat(c byteConn, sess *session.Session, dialer bool, addr string) bool {
	a.mu.Lock()
	if a.inChat {
		a.mu.Unlock()
		return false
	}
	a.inChat = true
	a.chatConn = c
	a.sess = sess
	a.dialer = dialer
	a.peerAddr = addr
	a.quitting = false
	a.resumeUntil = time.Time{}
	a.mu.Unlock()
	go a.chatLoop(c)
	return true
}

func (a *app) resumeAttached(c byteConn, sess *session.Session) {
	a.mu.Lock()
	a.inChat = true
	a.chatConn = c
	a.sess = sess
	a.resumeUntil = time.Time{}
	a.mu.Unlock()
	go a.chatLoop(c)
}

func (a *app) chatLoop(c byteConn) {
	for {
		if !a.readLoop(c) {
			return
		}
		a.mu.Lock()
		dialer := a.dialer
		addr := a.peerAddr
		until := a.resumeUntil
		a.mu.Unlock()
		if !dialer {
			a.watchResume(until)
			return
		}
		next, ok := a.retryDial(addr)
		if !ok {
			return
		}
		c = next
	}
}

func (a *app) readLoop(c byteConn) bool {
	for {
		_, data, err := c.Read(a.ctx)
		if err != nil {
			a.mu.Lock()
			quit := a.quitting
			current := a.chatConn == c || a.chatConn == nil
			a.mu.Unlock()
			if quit || !current {
				return false
			}
			return a.armBreak(c)
		}
		a.mu.Lock()
		sess := a.sess
		quit := a.quitting
		a.mu.Unlock()
		text, display, openErr := session.Open(sess, data)
		if openErr != nil {
			a.tellFinished(c, sess)
			a.endSession(quit)
			return false
		}
		if session.Finished(sess) {
			a.endSession(quit)
			return false
		}
		if display {
			fmt.Printf("peer: %s\n> ", text)
		}
	}
}

func (a *app) tellFinished(c byteConn, sess *session.Session) {
	if frame := session.FailureNotice(sess); len(frame) > 0 && c != nil {
		_ = c.Write(a.ctx, websocket.MessageBinary, frame)
	}
	if c != nil {
		_ = c.CloseNow()
	}
}

func (a *app) requestReadable(id string) bool {
	resp, err := a.do(http.MethodGet, a.base+"/requests/"+id, nil)
	if err != nil {
		return false
	}
	defer a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var status struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return false
	}
	return status.Status == "accepted"
}

func (a *app) endSession(quit bool) {
	a.mu.Lock()
	a.inChat = false
	a.chatConn = nil
	a.resumeUntil = time.Time{}
	a.mu.Unlock()
	if !quit {
		fmt.Println("\nchat ended")
	}
}

func (a *app) armBreak(c byteConn) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.quitting {
		return false
	}
	if a.sess != nil && session.Finished(a.sess) {
		a.inChat = false
		a.chatConn = nil
		a.mu.Unlock()
		fmt.Println("\nchat ended")
		a.mu.Lock()
		return false
	}
	if a.chatConn != nil && a.chatConn != c {
		return false
	}
	a.chatConn = nil
	a.resumeUntil = time.Now().Add(a.resumeWindow)
	return true
}

func (a *app) watchResume(until time.Time) {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-a.ctx.Done():
		return
	case <-timer.C:
		a.mu.Lock()
		if a.quitting || a.chatConn != nil || !a.resumeUntil.Equal(until) {
			a.mu.Unlock()
			return
		}
		a.inChat = false
		a.resumeUntil = time.Time{}
		a.mu.Unlock()
		fmt.Println("\nchat ended")
	}
}

func (a *app) retryDial(addr string) (byteConn, bool) {
	for {
		a.mu.Lock()
		until := a.resumeUntil
		quit := a.quitting
		sess := a.sess
		a.mu.Unlock()
		if quit || sess == nil {
			return nil, false
		}
		if !time.Now().Before(until) {
			a.expireResume()
			return nil, false
		}
		time.Sleep(time.Second)
		a.mu.Lock()
		until = a.resumeUntil
		quit = a.quitting
		sess = a.sess
		lines := append([]string(nil), a.queued...)
		a.mu.Unlock()
		if quit || sess == nil {
			return nil, false
		}
		if !time.Now().Before(until) {
			a.expireResume()
			return nil, false
		}
		c, _, err := websocket.Dial(a.ctx, "ws://"+addr, nil)
		if err != nil {
			continue
		}
		err = pushResume(a.ctx, c, sess, time.Now(), until, lines)
		if err != nil {
			_ = c.Close(websocket.StatusPolicyViolation, "")
			continue
		}
		a.mu.Lock()
		if len(a.queued) >= len(lines) {
			a.queued = append([]string(nil), a.queued[len(lines):]...)
		} else {
			a.queued = nil
		}
		a.chatConn = c
		a.resumeUntil = time.Time{}
		a.mu.Unlock()
		return c, true
	}
}

func (a *app) expireResume() {
	a.mu.Lock()
	if a.quitting || a.chatConn != nil {
		a.mu.Unlock()
		return
	}
	a.inChat = false
	a.resumeUntil = time.Time{}
	a.mu.Unlock()
	fmt.Println("\nchat ended")
}
