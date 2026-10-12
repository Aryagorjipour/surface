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
	defer cancel()

	go serveWS(ctx, listenAddr, logger)

	if err := register(base, myKey, listenAddr, "available"); err != nil {
		logger.Error("register failed", "err", err)
		os.Exit(1)
	}
	logger.Info("registered", "key", myKey, "addr", listenAddr)

	go pollPending(ctx, base, myKey)

	fmt.Println("commands: lookup <key> | connect <key> | accept <id> | reject <id> | quit")
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
			p, err := lookup(base, parts[1])
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
			if err := doConnect(ctx, base, myKey, listenAddr, parts[1]); err != nil {
				fmt.Println("connect error:", err)
			}
		case "accept":
			if len(parts) < 2 {
				fmt.Println("usage: accept <request-id>")
				continue
			}
			if err := postAction(base, parts[1], "accept"); err != nil {
				fmt.Println("accept error:", err)
			} else {
				fmt.Println("accepted")
			}
		case "reject":
			if len(parts) < 2 {
				fmt.Println("usage: reject <request-id>")
				continue
			}
			if err := postAction(base, parts[1], "reject"); err != nil {
				fmt.Println("reject error:", err)
			} else {
				fmt.Println("rejected")
			}
		default:
			fmt.Println("unknown command")
		}
	}
}

func register(base, key, addr, state string) error {
	body, _ := json.Marshal(map[string]string{
		"key": key, "address": addr, "state": state,
	})
	resp, err := http.Post(base+"/peers", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func lookup(base, key string) (*peer, error) {
	resp, err := http.Get(base + "/peers/" + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
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

func pollPending(ctx context.Context, base, myKey string) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	seen := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			resp, err := http.Get(base + "/pending?key=" + myKey)
			if err != nil {
				continue
			}
			var reqs []map[string]any
			json.NewDecoder(resp.Body).Decode(&reqs)
			resp.Body.Close()
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

func postAction(base, id, action string) error {
	resp, err := http.Post(base+"/requests/"+id+"/"+action, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func doConnect(ctx context.Context, base, myKey, myAddr, target string) error {
	p, err := lookup(base, target)
	if err != nil {
		return err
	}
	if p.State != "available" {
		return fmt.Errorf("target state %s", p.State)
	}

	body, _ := json.Marshal(map[string]string{
		"from_key": myKey, "to_key": target, "from_address": myAddr,
	})
	resp, err := http.Post(base+"/connect", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var cr connectResp
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return err
	}
	fmt.Println("request created:", cr.RequestID)

	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		resp, err := http.Get(base + "/requests/" + cr.RequestID)
		if err != nil {
			continue
		}
		var status map[string]any
		json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()
		st, _ := status["status"].(string)
		if st == "accepted" {
			fmt.Println("accepted — dialing", p.Address)
			return dialAndChat(ctx, p.Address)
		}
		if st == "rejected" {
			return fmt.Errorf("rejected")
		}
	}
	return fmt.Errorf("timeout waiting for accept")
}

func serveWS(ctx context.Context, addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		fmt.Println("\n>>> peer connected")
		chatLoop(ctx, c)
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("ws listen failed", "err", err)
	}
}

func dialAndChat(ctx context.Context, addr string) error {
	c, _, err := websocket.Dial(ctx, "ws://"+addr, nil)
	if err != nil {
		return err
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	fmt.Println("connected — type messages (empty line ignored)")
	chatLoop(ctx, c)
	return nil
}

func chatLoop(ctx context.Context, c *websocket.Conn) {
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		if sc.Err() != nil {
			fmt.Println("Request failed, please try again")
		}
		for sc.Scan() {
			msg := sc.Text()
			if strings.TrimSpace(msg) == "" {
				continue
			}
			if err := c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
				return
			}
		}
	}()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			fmt.Println("\n chat closed:", err)
			return
		}
		fmt.Printf("peer: %s\n> ", string(data))
	}
}
