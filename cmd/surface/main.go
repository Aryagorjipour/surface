package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"log/slog"

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
	base       string
	myKey      string
	listenAddr string
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc

	mu       sync.Mutex
	inChat   bool
	chatConn *websocket.Conn
}

func main() {
	logger := shared.NewLogger("surface")
	if len(os.Args) < 3 {
		fmt.Println("usage: surface <connector-url> <my-key> [listen-addr]")
		os.Exit(1)
	}
	base := strings.TrimRight(os.Args[1], "/")
	myKey := os.Args[2]
	listenAddr := "127.0.0.1:9001"
	if len(os.Args) > 3 {
		listenAddr = os.Args[3]
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := &app{
		base:       base,
		myKey:      myKey,
		listenAddr: listenAddr,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}

	go a.serveWS()

	if err := a.register("available"); err != nil {
		logger.Error("register failed", "err", err)
		os.Exit(1)
	}
	logger.Info("registered", "key", myKey, "addr", listenAddr)

	go a.pollPending()

	fmt.Println("commands: lookup <key> | connect <key> | accept <id> | reject <id> | quit")
	fmt.Println("(in chat: just type messages; /quit leaves chat)")

	sc := bufio.NewScanner(os.Stdin)
	if sc.Err() != nil {
		logger.Error("BUFIO scanner failed", "key", myKey, "addr", listenAddr)
		fmt.Println("Request failed, please try again")
		return
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		a.mu.Lock()
		chatting := a.inChat
		conn := a.chatConn
		a.mu.Unlock()

		if chatting && conn != nil {
			if line == "/quit" {
				if err := conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
					a.logger.Warn("close chat", "err", err)
				}
				a.mu.Lock()
				a.inChat = false
				a.chatConn = nil
				a.mu.Unlock()
				fmt.Println("left chat")
				continue
			}
			if err := conn.Write(a.ctx, websocket.MessageText, []byte(line)); err != nil {
				fmt.Println("send failed:", err)
			}
			continue
		}

		parts := strings.Fields(line)
		switch parts[0] {
		case "quit", "exit":
			cancel()
			return
		case "lookup":
			if len(parts) < 2 {
				fmt.Println("usage: lookup <key>")
				continue
			}
			p, err := a.lookup(parts[1])
			if err != nil {
				fmt.Println("lookup error:", err)
				continue
			}
			fmt.Printf("peer: %+v\n", p)
		case "connect":
			if len(parts) < 2 {
				fmt.Println("usage: connect <key>")
				continue
			}
			if err := a.doConnect(parts[1]); err != nil {
				fmt.Println("connect error:", err)
			}
		case "accept":
			if len(parts) < 2 {
				fmt.Println("usage: accept <request-id>")
				continue
			}
			if err := a.postAction(parts[1], "accept"); err != nil {
				fmt.Println("accept error:", err)
			} else {
				fmt.Println("accepted — waiting for peer to dial")
			}
		case "reject":
			if len(parts) < 2 {
				fmt.Println("usage: reject <request-id>")
				continue
			}
			if err := a.postAction(parts[1], "reject"); err != nil {
				fmt.Println("reject error:", err)
			} else {
				fmt.Println("rejected")
			}
		default:
			fmt.Println("unknown command")
		}
	}
}

func (a *app) drain(body io.ReadCloser) {
	if body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		a.logger.Warn("drain response", "err", err)
	}
	if err := body.Close(); err != nil {
		a.logger.Warn("close response", "err", err)
	}
}

func (a *app) register(state string) error {
	body, _ := json.Marshal(map[string]string{
		"key": a.myKey, "address": a.listenAddr, "state": state,
	})
	resp, err := http.Post(a.base+"/peers", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (a *app) lookup(key string) (*peer, error) {
	resp, err := http.Get(a.base + "/peers/" + key)
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
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	seen := map[string]bool{}
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-tick.C:
			resp, err := http.Get(a.base + "/pending?key=" + a.myKey)
			if err != nil {
				continue
			}
			var reqs []map[string]any
			err = json.NewDecoder(resp.Body).Decode(&reqs)
			a.drain(resp.Body)
			if err != nil {
				a.logger.Warn("decode pending", "err", err)
				continue
			}
			for _, r := range reqs {
				id, _ := r["request_id"].(string)
				from, _ := r["from_key"].(string)
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				fmt.Printf("\n>>> incoming request from %s\n>>> id: %s\n>>> type: accept %s   or   reject %s\n> ", from, id, id, id)
			}
		}
	}
}

func (a *app) postAction(id, action string) error {
	resp, err := http.Post(a.base+"/requests/"+id+"/"+action, "application/json", nil)
	if err != nil {
		return err
	}
	a.drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
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
	resp, err := http.Post(a.base+"/connect", "application/json", bytes.NewReader(body))
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
		time.Sleep(1 * time.Second)
		resp, err := http.Get(a.base + "/requests/" + cr.RequestID)
		if err != nil {
			continue
		}
		var status map[string]any
		err = json.NewDecoder(resp.Body).Decode(&status)
		a.drain(resp.Body)
		if err != nil {
			a.logger.Warn("decode request status", "err", err)
			continue
		}
		st, _ := status["status"].(string)
		if st == "accepted" {
			fmt.Println("accepted — dialing", p.Address)
			return a.dialAndChat(p.Address)
		}
		if st == "rejected" {
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
		fmt.Println("\n>>> peer connected — type messages (/quit to leave)")
		a.enterChat(c)
	})
	srv := &http.Server{Addr: a.listenAddr, Handler: mux}
	go func() {
		<-a.ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			a.logger.Error("ws shutdown", "err", err)
		}
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		a.logger.Error("ws listen failed", "err", err)
	}
}

func (a *app) dialAndChat(addr string) error {
	c, _, err := websocket.Dial(a.ctx, "ws://"+addr, nil)
	if err != nil {
		return err
	}
	fmt.Println("connected — type messages (/quit to leave)")
	a.enterChat(c)
	return nil
}

func (a *app) enterChat(c *websocket.Conn) {
	a.mu.Lock()
	a.inChat = true
	a.chatConn = c
	a.mu.Unlock()

	go func() {
		defer func() {
			if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
				a.logger.Warn("close chat", "err", err)
			}
			a.mu.Lock()
			if a.chatConn == c {
				a.inChat = false
				a.chatConn = nil
			}
			a.mu.Unlock()
			fmt.Println("\nchat ended")
		}()
		for {
			_, data, err := c.Read(a.ctx)
			if err != nil {
				return
			}
			fmt.Printf("peer: %s\n> ", string(data))
		}
	}()
}
