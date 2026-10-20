package connector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	url := "file:" + dbPath

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	ctx := context.Background()
	store, err := Open(ctx, url, "", key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := os.Remove(dbPath); err != nil {
			t.Error(err)
		}
	}()

	err = store.UpsertPeer(ctx, "alice", "192.168.1.10:8765", "available", "pub-alice")
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	addr, state, identity, seen, err := store.GetPeer(ctx, "alice")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if addr != "192.168.1.10:8765" || state != "available" || identity != "pub-alice" {
		t.Errorf("got addr=%s state=%s identity=%s", addr, state, identity)
	}
	if time.Since(seen) > 5*time.Second {
		t.Errorf("last_seen too old: %v", seen)
	}

	err = store.UpsertPeer(ctx, "alice", "10.0.0.2:9000", "busy", "pub-alice")
	if err != nil {
		t.Fatal(err)
	}
	addr, state, identity, _, err = store.GetPeer(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "10.0.0.2:9000" || state != "busy" || identity != "pub-alice" {
		t.Errorf("update failed: %s %s %s", addr, state, identity)
	}
}

func TestFileStoreConcurrentUpserts(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("peer-%d", i)
			if err := store.UpsertPeer(ctx, key, "127.0.0.1:9001", "available", ""); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, _, _, _, err := store.GetPeer(ctx, fmt.Sprintf("peer-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPruneDropsStalePeers(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	now := t0
	store.SetNow(func() time.Time { return now })

	if err := store.UpsertPeer(ctx, "old-peer", "203.0.113.8:8", "available", ""); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(5 * time.Second)
	if err := store.UpsertPeer(ctx, "fresh-peer", "203.0.113.9:9", "busy", ""); err != nil {
		t.Fatal(err)
	}
	window := 7 * time.Second
	now = t0.Add(8 * time.Second)
	keys, err := store.Prune(ctx, now, window)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "old-peer" {
		t.Fatalf("pruned %#v", keys)
	}
	addr, _, _, _, err := store.GetPeer(ctx, "old-peer")
	if err == nil || err.Error() != "peer not found" || addr != "" {
		t.Fatalf("stale get addr=%q err=%v", addr, err)
	}
	addr, state, _, _, err := store.GetPeer(ctx, "fresh-peer")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "203.0.113.9:9" || state != "busy" {
		t.Fatalf("fresh peer %s %s", addr, state)
	}

	if _, err := store.Prune(ctx, now, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := store.GetPeer(ctx, "old-peer"); err == nil || err.Error() != "peer not found" {
		t.Fatalf("24h prune restored the row: %v", err)
	}

	now = t0.Add(9 * time.Second)
	if err := store.UpsertPeer(ctx, "old-peer", "203.0.113.10:10", "available", "pub-old"); err != nil {
		t.Fatal(err)
	}
	addr, state, _, _, err = store.GetPeer(ctx, "old-peer")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "203.0.113.10:10" || state != "available" {
		t.Fatalf("re-register %s %s", addr, state)
	}
}

func TestCountLivePeersOmitsStaleRows(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	now := t0
	store.SetNow(func() time.Time { return now })
	window := 10 * time.Second

	n, err := store.CountLivePeers(ctx, now, window)
	if err != nil || n != 0 {
		t.Fatalf("empty %d %v", n, err)
	}
	if err := store.UpsertPeer(ctx, "live-peer", "198.51.100.10:4242", "available", ""); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(-30 * time.Second)
	if err := store.UpsertPeer(ctx, "stale-peer", "198.51.100.11:4243", "available", ""); err != nil {
		t.Fatal(err)
	}
	now = t0
	n, err = store.CountLivePeers(ctx, now, window)
	if err != nil || n != 1 {
		t.Fatalf("live %d %v", n, err)
	}
	now = t0.Add(-30 * time.Second)
	n, err = store.CountLivePeers(ctx, now, window)
	if err != nil || n != 2 {
		t.Fatalf("rewound %d %v", n, err)
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	ct, nonce, err := encrypt(key, "secret-address")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := decrypt(key, ct, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if pt != "secret-address" {
		t.Errorf("got %q", pt)
	}
}
