package connector

import (
	"context"
	"os"
	"path/filepath"
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
	defer store.Close()
	defer os.Remove(dbPath)

	err = store.UpsertPeer(ctx, "alice", "192.168.1.10:8765", "available")
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	addr, state, seen, err := store.GetPeer(ctx, "alice")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if addr != "192.168.1.10:8765" || state != "available" {
		t.Errorf("got addr=%s state=%s", addr, state)
	}
	if time.Since(seen) > 5*time.Second {
		t.Errorf("last_seen too old: %v", seen)
	}

	err = store.UpsertPeer(ctx, "alice", "10.0.0.2:9000", "busy")
	if err != nil {
		t.Fatal(err)
	}
	addr, state, _, err = store.GetPeer(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "10.0.0.2:9000" || state != "busy" {
		t.Errorf("update failed: %s %s", addr, state)
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
