package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	url := "file:" + filepath.Join(dir, "test.db")
	key := make([]byte, 32)
	store, err := Open(context.Background(), url, "", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestUpsertAndGetPeer(t *testing.T) {
	store := testStore(t)
	h := &Handlers{store: store}
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"key":"alice","address":"127.0.0.1:8765","state":"available"}`
	req := httptest.NewRequest(http.MethodPost, "/peers", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert status %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/peers/alice", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status %d body %s", rec.Code, rec.Body.String())
	}

	var got peerResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Address != "127.0.0.1:8765" || got.State != "available" {
		t.Errorf("got %+v", got)
	}
}

func TestGetPeerNotFound(t *testing.T) {
	store := testStore(t)
	h := &Handlers{store: store}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/peers/missing", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
