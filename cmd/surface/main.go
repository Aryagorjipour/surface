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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/Aryagorjipour/surface/internal/mailbox"
	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/Aryagorjipour/surface/internal/shared"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/coder/websocket"
	"github.com/mattn/go-isatty"
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

	mu        sync.Mutex
	presence  string
	fromKey   map[string]string
	gate      Gate
	registry  *Registry
	maxChats  int
	punchAddr string
	relayAddr string
	out       io.Writer
	deskCh    chan tea.Msg
	box       *mailbox.Box
}

func main() {
	logger := shared.NewLogger("surface")
	if stdinTerminal(os.Stdin) {
		out := discardWhenTerminal(os.Stderr, isatty.IsTerminal(os.Stderr.Fd()))
		logger = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", "surface")
	}
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
		maxChats:     cfg.maxChats,
	}
	a.initRegistry()
	if err := a.openBox(cfg.mailbox); err != nil {
		return err
	}
	a.gate.readable = a.requestReadable
	go a.pumpEvents()
	go a.serveWS()
	if err := a.register("available"); err != nil {
		return err
	}
	logger.Info("registered")
	if stdinTerminal(stdin) {
		a.deskCh = make(chan tea.Msg, 256)
	}
	a.collect()
	go a.pollPending()
	go a.heartbeatLoop()
	if a.deskCh != nil {
		return a.serveDesk(stdin, os.Stdout)
	}

	fmt.Println("commands: lookup <key> | connect <key> | accept <id> | reject <id> | quit")
	fmt.Println("(in chat: type messages; /chats /focus /quit; lobby verbs still work)")

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
	a.initRegistry()
	if a.box != nil {
		handled, err := a.dispatchMailbox(line)
		if err != nil || handled {
			return err
		}
	}
	if cmd, ok := parseChatCommand(line); ok {
		return a.dispatchChatCommand(cmd)
	}
	verb, rest, lobby := parseLobbyVerb(line)
	if lobby {
		return a.handleLobbyVerb(verb, rest)
	}
	if focused, ok := a.registry.Focused(); ok {
		if err := focused.SendText(line); err != nil {
			a.println("send failed:", err)
		}
		return nil
	}
	a.println("unknown command")
	return nil
}

func parseChatCommand(line string) (chatCommand, bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return chatCommand{}, false
	}
	switch parts[0] {
	case "/chats":
		return chatCommand{kind: chatCmdChats}, true
	case "/focus":
		ref := ""
		if len(parts) > 1 {
			ref = parts[1]
		}
		return chatCommand{kind: chatCmdFocus, ref: ref}, true
	case "/quit":
		return chatCommand{kind: chatCmdQuit}, true
	default:
		return chatCommand{}, false
	}
}

func parseLobbyVerb(line string) (string, []string, bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil, false
	}
	switch parts[0] {
	case "quit", "exit", "lookup", "connect", "accept", "reject":
		return parts[0], parts[1:], true
	default:
		return "", nil, false
	}
}

type chatCmdKind uint8

const (
	chatCmdChats chatCmdKind = iota
	chatCmdFocus
	chatCmdQuit
)

type chatCommand struct {
	kind chatCmdKind
	ref  string
}

func (a *app) dispatchChatCommand(cmd chatCommand) error {
	switch cmd.kind {
	case chatCmdChats:
		lines := a.registry.StatusLines()
		if len(lines) == 0 {
			a.println("(no chats)")
			break
		}
		for _, line := range lines {
			a.println(line)
		}
	case chatCmdFocus:
		if err := a.registry.FocusRef(cmd.ref); err != nil {
			a.println("focus error:", err)
		}
	case chatCmdQuit:
		if c, ok := a.registry.Focused(); ok {
			c.Quit()
		}
	}
	return nil
}

func (a *app) handleLobbyVerb(verb string, rest []string) error {
	switch verb {
	case "quit", "exit":
		a.cancel()
		return errStop
	case "lookup":
		if len(rest) < 1 {
			a.println("usage: lookup <key>")
			return nil
		}
		p, err := a.lookup(rest[0])
		if err != nil {
			a.println("lookup error:", err)
			return nil
		}
		if a.deskCh != nil {
			a.note(note{kind: noteLookup, key: p.Key, state: p.State, address: p.Address, text: fmt.Sprintf("peer: %+v", p)})
		} else {
			fmt.Printf("peer: %+v\n", p)
		}
	case "connect":
		if len(rest) < 1 {
			a.println("usage: connect <key>")
			return nil
		}
		if a.registry.AtCapacity() {
			a.println("at capacity")
			a.markPhase(phaseLobby)
			return nil
		}
		if err := a.doConnect(rest[0]); err != nil {
			a.println("connect error:", err)
			a.markPhase(phaseLobby)
		}
	case "accept":
		if len(rest) < 1 {
			a.println("usage: accept <request-id>")
			return nil
		}
		_ = a.acceptRequest(rest[0])
	case "reject":
		if len(rest) < 1 {
			a.println("usage: reject <request-id>")
			return nil
		}
		_ = a.rejectRequest(rest[0])
	default:
		a.println("unknown command")
	}
	return nil
}

var errStop = errors.New("stop")

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
	a.collect()
	a.mu.Lock()
	state := a.presence
	a.mu.Unlock()
	if state == "" {
		state = "available"
	}
	return a.register(state)
}

func (a *app) openBox(ttl time.Duration) error {
	if a == nil || ttl <= 0 {
		return nil
	}
	box, err := mailbox.Open(mailbox.Config{
		Dir:      filepath.Join("surface-mailbox", a.myKey),
		Me:       a.myKey,
		TTL:      ttl,
		Endpoint: a.base,
		Bearer:   a.token,
		Busy:     func(peer string) bool { return a.registry.Owns(peer) },
	})
	if err != nil {
		return err
	}
	a.box = box
	return nil
}

func (a *app) dispatchMailbox(line string) (bool, error) {
	if a == nil || a.box == nil {
		return false, nil
	}
	verb, rest, ok := parseMailboxVerb(line)
	if !ok {
		return false, nil
	}
	return true, a.handleMailbox(verb, rest)
}

func parseMailboxVerb(line string) (string, []string, bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil, false
	}
	switch parts[0] {
	case "pubkey", "hold", "leave":
		return parts[0], parts[1:], true
	default:
		return "", nil, false
	}
}

func (a *app) handleMailbox(verb string, rest []string) error {
	switch verb {
	case "pubkey":
		if len(rest) != 0 {
			a.println(mailbox.Class(mailbox.ErrInvalid))
			return nil
		}
		a.println(a.box.Public().String())
	case "hold":
		if len(rest) != 2 {
			a.println(mailbox.Class(mailbox.ErrInvalid))
			return nil
		}
		if err := a.box.Hold(rest[0], rest[1]); err != nil {
			a.println(mailbox.Class(err))
			return nil
		}
	case "leave":
		if len(rest) < 2 {
			a.println("leave refused", mailbox.Class(mailbox.ErrInvalid))
			return nil
		}
		_, err := a.box.Leave(a.ctx, rest[0], strings.Join(rest[1:], " "))
		if err != nil {
			a.println("leave refused", mailbox.Class(err))
			return nil
		}
		a.println("staged")
	}
	return nil
}

func (a *app) collect() {
	if a == nil || a.box == nil {
		return
	}
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	err := a.box.Collect(ctx, func(d mailbox.Delivered) error {
		a.say("left:", d.From, d.Text)
		return nil
	})
	if err != nil && a.logger != nil {
		a.logger.Warn("mailbox", "op", "collect", "class", mailbox.Class(err))
	}
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
			if resp.StatusCode == http.StatusTooManyRequests {
				a.drain(resp.Body)
				if a.logger != nil {
					a.logger.Warn("too many requests")
				}
				continue
			}
			if resp.StatusCode != http.StatusOK {
				a.drain(resp.Body)
				if a.logger != nil {
					a.logger.Warn("decode pending")
				}
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
				if a.deskCh != nil {
					a.note(note{kind: noteInvite, key: from, id: id})
				} else {
					fmt.Printf("\n>>> incoming request from %s\n>>> id: %s\n>>> type: accept %s   or   reject %s\n> ", from, id, id, id)
				}
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
	if resp.StatusCode == http.StatusTooManyRequests {
		return "", errors.New("too many requests")
	}
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
	a.initRegistry()
	if a.registry.AtCapacity() {
		return ErrAtCapacity
	}
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
	if resp.StatusCode == http.StatusTooManyRequests {
		return errors.New("too many requests")
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var cr connectResp
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return err
	}
	a.println("request created:", cr.RequestID)

	for i := 0; i < 30; i++ {
		time.Sleep(acceptPoll)
		resp, err := a.do(http.MethodGet, a.base+"/requests/"+cr.RequestID, nil)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			a.drain(resp.Body)
			return errors.New("too many requests")
		}
		if resp.StatusCode != http.StatusOK {
			a.drain(resp.Body)
			if a.logger != nil {
				a.logger.Warn("decode request status")
			}
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
	a.initRegistry()
	var admitted bool
	ready := func(sess *session.Session) {
		admitted = true
		id := a.gate.idOf(sess)
		peer := a.callerFor(id)
		if a.beginChat(c, sess, false, "", ConvID(id), peer) {
			a.say("\n" + connectedPrompt(false))
			return
		}
		_ = c.Close(websocket.StatusPolicyViolation, "at capacity")
	}
	sess, err := a.gate.take(a.ctx, c, time.Now(), time.Time{}, ready)
	if err != nil || admitted || sess == nil {
		return
	}
	id := a.gate.idOf(sess)
	_ = a.registry.DeliverResume(ConvID(id), c, sess)
}

func (a *app) say(args ...any) {
	if a != nil && a.deskCh != nil {
		a.note(note{kind: noteToast, level: toastInfo, text: lineText(args...)})
		return
	}
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

func (a *app) beginChat(c byteConn, sess *session.Session, dialer bool, addr string, id ConvID, peer string) bool {
	a.initRegistry()
	_, err := a.registry.Attach(AttachReq{
		ID:     id,
		Peer:   peer,
		Sess:   sess,
		Conn:   c,
		Dialer: dialer,
		Addr:   addr,
	})
	if err != nil {
		return false
	}
	a.markPhase(phaseChat)
	return true
}

func (a *app) initRegistry() {
	if a.registry != nil {
		return
	}
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	max := a.maxChats
	if max < 1 {
		max = 4
	}
	a.registry = NewRegistry(ctx, max, a.resumeWindow)
	a.gate.until = a.registry.ResumeUntil
}

func (a *app) pumpEvents() {
	if a.registry == nil {
		return
	}
	for {
		select {
		case <-a.ctx.Done():
			return
		case ev, ok := <-a.registry.Events():
			if !ok {
				return
			}
			a.handleConvEvent(ev)
		}
	}
}

func (a *app) handleConvEvent(ev ConvEvent) {
	switch ev.Kind {
	case EvPeerLine:
		if a.deskCh != nil {
			a.note(note{kind: notePeer, text: ev.Text, id: string(ev.ID), key: ev.Peer})
		} else {
			fmt.Printf("peer: %s\n> ", ev.Text)
		}
	case EvSelfLine:
		if a.deskCh != nil {
			a.note(note{kind: noteSelf, text: ev.Text, id: string(ev.ID), key: ev.Peer})
		}
	case EvEnded:
		if ev.Text == "quit" {
			a.println("left chat")
		} else {
			a.println("\nchat ended")
		}
		if a.registry.Len() == 0 {
			a.markPhase(phaseLobby)
		} else {
			a.markPhase(phaseChat)
		}
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
