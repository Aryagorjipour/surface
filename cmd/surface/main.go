package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Aryagorjipour/surface/internal/shared"
)

func main() {
	logger := shared.NewLogger("surface")
	if len(os.Args) < 2 {
		fmt.Println("usage: surface <connector-url>")
	}
	base := os.Args[1]

	body := map[string]string{
		"key":     "arya",
		"address": "127.0.0.1:9001",
		"state":   "available",
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+"/peers", "application/json", bytes.NewReader(b))
	if err != nil {
		logger.Error("register failed", "err", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	logger.Info("registered", "status", resp.StatusCode)

	resp, err = http.Get(base + "/peers/bob")
	if err != nil {
		logger.Error("lookup failed", "err", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	var peer map[string]any
	json.NewDecoder(resp.Body).Decode(&peer)
	logger.Info("lookup result", "status", resp.StatusCode, "peer", peer)

	time.Sleep(2 * time.Second)
}
