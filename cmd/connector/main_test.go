package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/connector"
	"github.com/redis/go-redis/v9"
)

func TestRefusedExpirySkipsListen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	called := false
	err := run(func(name string) (string, bool) {
		if name == "SURFACE_SILENCE_WINDOW" {
			return "0s", true
		}
		return "", false
	}, logger, func(*http.Server) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected startup to be refused")
	}
	msg := err.Error()
	if !strings.Contains(msg, "SURFACE_SILENCE_WINDOW") || !strings.Contains(msg, "not a positive duration") {
		t.Fatalf("error %q", msg)
	}
	if strings.Contains(msg, "0s") {
		t.Fatalf("error contains the raw value: %q", msg)
	}
	if called {
		t.Fatal("listen was called")
	}
}

func TestDropPrunedPeerCacheLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	keys := []string{"bare-peer-key-9f", "second-peer-key"}
	dropPrunedPeerCache(context.Background(), nil, logger, keys)
	text := buf.String()
	if !strings.Contains(text, "pruned peers") || !strings.Contains(text, "count=2") {
		t.Fatalf("log %q", text)
	}
	for _, key := range keys {
		if strings.Contains(text, key) {
			t.Fatalf("log included peer key %q in %q", key, text)
		}
	}
	quiet := buf.Len()
	dropPrunedPeerCache(context.Background(), nil, logger, nil)
	if buf.Len() != quiet {
		t.Fatalf("empty prune logged %q", buf.String()[quiet:])
	}
	dropPrunedPeerCache(context.Background(), nil, nil, keys)
}

func TestDropPrunedPeerCacheDeletesKeys(t *testing.T) {
	addr := os.Getenv("SURFACE_TEST_REDIS")
	if addr == "" {
		t.Skip("SURFACE_TEST_REDIS unset")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	cache := connector.NewRedis(addr, "")
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	cached := "prelisten-cached-" + suffix
	missing := "prelisten-missing-" + suffix
	address := "203.0.113.50:50"
	t.Cleanup(func() {
		_ = client.Del(ctx, "peer:"+cached, "peer:"+missing).Err()
	})
	if err := client.Set(ctx, "peer:"+cached, `{"key":"`+cached+`","address":"`+address+`","state":"available","last_seen":1}`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	dropPrunedPeerCache(ctx, cache, logger, []string{cached, missing})

	n, err := client.Exists(ctx, "peer:"+cached).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("pre-listen prune left the peer cache key")
	}
	text := buf.String()
	if !strings.Contains(text, "pruned peers") || !strings.Contains(text, "count=2") {
		t.Fatalf("log %q", text)
	}
	if strings.Contains(text, cached) || strings.Contains(text, missing) || strings.Contains(text, address) {
		t.Fatalf("log included a private value: %s", text)
	}
}
