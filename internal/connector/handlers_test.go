package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
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

func TestRejectMalformed(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux)

	longKey := strings.Repeat("k", 129)
	longAddr := strings.Repeat("a", 257)
	maxKey := strings.Repeat("k", 128)
	maxAddr := strings.Repeat("a", 256)

	rec := postMux(mux, "/peers", peerJSON(longKey, "127.0.0.1:1", "available"))
	requirePhrase(t, rec, http.StatusBadRequest, "key too long")
	if _, _, _, err := store.GetPeer(context.Background(), longKey); err == nil {
		t.Fatal("long key was stored")
	}

	rec = postMux(mux, "/peers", peerJSON("addr-key", longAddr, "available"))
	requirePhrase(t, rec, http.StatusBadRequest, "address too long")
	rec = getReq(mux, "/peers/addr-key", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")

	rec = postMux(mux, "/peers", peerJSON("state-key", "127.0.0.1:1", "Available"))
	requirePhrase(t, rec, http.StatusBadRequest, "invalid state")
	rec = getReq(mux, "/peers/state-key", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")

	payload := bytes.Repeat([]byte("x"), DefaultConfig().MaxBodyBytes+100)
	counter := &countReader{r: bytes.NewReader(payload)}
	req := httptest.NewRequest(http.MethodPost, "/peers", counter)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	requirePhrase(t, rec, http.StatusBadRequest, "request too large")
	if counter.n > DefaultConfig().MaxBodyBytes+1 || counter.n >= len(payload) {
		t.Fatalf("body read %d of %d", counter.n, len(payload))
	}

	rec = postMux(mux, "/peers", "{")
	requirePhrase(t, rec, http.StatusBadRequest, "invalid json")
	rec = postMux(mux, "/peers", `{"key":"only"}`)
	requirePhrase(t, rec, http.StatusBadRequest, "key, address, state required")

	rec = postMux(mux, "/peers", peerJSON(maxKey, maxAddr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("exact ceilings: %d %s", rec.Code, rec.Body.String())
	}

	for _, tc := range []struct {
		body   string
		phrase string
	}{
		{connectJSON(longKey, "target", "127.0.0.1:1"), "key too long"},
		{connectJSON("from", longKey, "127.0.0.1:1"), "key too long"},
		{connectJSON("from", "target", longAddr), "address too long"},
	} {
		rec = postMux(mux, "/connect", tc.body)
		requirePhrase(t, rec, http.StatusBadRequest, tc.phrase)
		if len(h.byID) != 0 {
			t.Fatalf("connect stored a request: %d", len(h.byID))
		}
	}

	rec = getReq(mux, "/peers/"+longKey, "")
	requirePhrase(t, rec, http.StatusBadRequest, "key too long")
	rec = getReq(mux, "/pending?key="+longKey, "")
	requirePhrase(t, rec, http.StatusBadRequest, "key too long")

	longID := strings.Repeat("i", 65)
	h.mu.Lock()
	h.byID[longID] = &ConnectRequest{ID: longID, Status: "pending"}
	h.mu.Unlock()
	for _, target := range []string{
		"/requests/" + longID + "/accept",
		"/requests/" + longID + "/reject",
		"/requests/" + longID,
	} {
		method := http.MethodGet
		if strings.HasSuffix(target, "/accept") || strings.HasSuffix(target, "/reject") {
			method = http.MethodPost
		}
		rec = callMux(mux, method, target, "", "")
		requirePhrase(t, rec, http.StatusBadRequest, "identifier too long")
	}
	h.mu.RLock()
	if h.byID[longID].Status != "pending" {
		t.Fatalf("long id was looked up, status %s", h.byID[longID].Status)
	}
	h.mu.RUnlock()
}

func TestConnectBusyStaysConflict(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux)

	for _, tc := range []struct{ key, state string }{
		{"busy-peer", "busy"},
		{"offline-peer", "offline"},
	} {
		rec := postMux(mux, "/peers", peerJSON(tc.key, "127.0.0.1:9", tc.state))
		if rec.Code != http.StatusOK {
			t.Fatalf("register %s: %d %s", tc.state, rec.Code, rec.Body.String())
		}
		rec = postMux(mux, "/connect", connectJSON("caller", tc.key, "127.0.0.1:8"))
		requirePhrase(t, rec, http.StatusConflict, "target not connectable")
	}
}

func TestSourceAllowanceHTTP(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg := DefaultConfig()
	cfg.SourceAllowance = 2
	cfg.KeyAllowance = 100
	lim := NewLimits(cfg)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	lim.SetNow(func() time.Time { return now })
	h.SetLimits(lim)
	mux := http.NewServeMux()
	h.Register(mux)

	const src = "198.51.100.20:1000"
	rec := callMux(mux, http.MethodPost, "/peers", peerJSON("alice", "127.0.0.1:8765", "available"), src)
	if rec.Code != http.StatusOK {
		t.Fatalf("first status %d %s", rec.Code, rec.Body.String())
	}
	rec = callMux(mux, http.MethodGet, "/peers/alice", "", src)
	if rec.Code != http.StatusOK {
		t.Fatalf("second status %d %s", rec.Code, rec.Body.String())
	}
	rec = callMux(mux, http.MethodPost, "/peers", "{", src)
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")

	rec = callMux(mux, http.MethodGet, "/peers/alice", "", "203.0.113.9:1000")
	if rec.Code == http.StatusTooManyRequests {
		t.Fatal("second source was limited with the first")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("second source status %d", rec.Code)
	}

	now = now.Add(60*time.Second + time.Nanosecond)
	rec = callMux(mux, http.MethodGet, "/peers/alice", "", src)
	if rec.Code != http.StatusOK {
		t.Fatalf("after window status %d %s", rec.Code, rec.Body.String())
	}
}

func TestRejectionLogBare(t *testing.T) {
	store := testStore(t)
	cap := &slogCapture{}
	h := NewHandlers(store, nil, slog.New(cap))
	cfg := DefaultConfig()
	cfg.SourceAllowance = 1
	lim := NewLimits(cfg)
	h.SetLimits(lim)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		secretKey  = "leaked-peer-key-9f3a"
		secretAddr = "leaked-address-9f3a"
		secretIP   = "198.51.100.77"
	)
	rec := callMux(mux, http.MethodPost, "/peers", peerJSON(secretKey, secretAddr, "Available"), secretIP+":4444")
	requirePhrase(t, rec, http.StatusBadRequest, "invalid state")
	rec = callMux(mux, http.MethodPost, "/peers", "{", secretIP+":4444")
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")

	records := cap.all()
	if len(records) != 2 {
		t.Fatalf("records %d", len(records))
	}
	want := []struct{ op, class string }{
		{"register", "invalid state"},
		{"register", "too many requests"},
	}
	for i, rec := range records {
		if rec.Level != slog.LevelInfo {
			t.Fatalf("record %d level %s", i, rec.Level)
		}
		attrs := map[string]string{}
		rec.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		if len(attrs) != 2 || attrs["op"] != want[i].op || attrs["class"] != want[i].class {
			t.Fatalf("record %d attrs %#v", i, attrs)
		}
		text := rec.Message + attrs["op"] + attrs["class"]
		for _, secret := range []string{secretKey, secretAddr, "Available", secretIP} {
			if strings.Contains(text, secret) {
				t.Fatalf("record contains %s", secret)
			}
		}
	}

	t.Run("nil logger", func(t *testing.T) {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("nil logger panicked: %v", recovered)
			}
		}()
		bare := NewHandlers(store, nil, nil)
		bareMux := http.NewServeMux()
		bare.Register(bareMux)
		rec := postMux(bareMux, "/peers", "{")
		requirePhrase(t, rec, http.StatusBadRequest, "invalid json")
	})
}

func TestKeyAllowanceHTTP(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg := DefaultConfig()
	cfg.KeyAllowance = 2
	cfg.SourceAllowance = 100
	h.SetLimits(NewLimits(cfg))
	mux := http.NewServeMux()
	h.Register(mux)

	rec := getReq(mux, "/peers/solo", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	rec = getReq(mux, "/peers/solo", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	saved := h.store
	h.store = nil
	rec = getReq(mux, "/peers/solo", "")
	h.store = saved
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")

	rec = getReq(mux, "/peers/other-key", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")

	rec = postMux(mux, "/peers", peerJSON("bob", "127.0.0.1:1", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register bob %d %s", rec.Code, rec.Body.String())
	}
	rec = getReq(mux, "/peers/bob", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("lookup bob %d", rec.Code)
	}
	rec = postMux(mux, "/connect", connectJSON("alice", "bob", "127.0.0.1:2"))
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")
	if len(h.byID) != 0 {
		t.Fatal("exhausted connect created a request")
	}
	rec = getReq(mux, "/peers/alice", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	rec = getReq(mux, "/peers/alice", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")

	longKey := strings.Repeat("k", 129)
	for range 3 {
		rec = getReq(mux, "/peers/"+longKey, "")
		requirePhrase(t, rec, http.StatusBadRequest, "key too long")
	}

	for i := range 3 {
		rec = postMux(mux, "/peers", peerJSON("reg-key", "127.0.0.1:3", "available"))
		if i < 2 && rec.Code != http.StatusOK {
			t.Fatalf("register %d: %d %s", i, rec.Code, rec.Body.String())
		}
		if i == 2 {
			requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")
		}
	}

	for i := range 3 {
		rec = getReq(mux, "/pending?key=pend-key", "")
		if i < 2 && rec.Code != http.StatusOK {
			t.Fatalf("pending %d: %d %s", i, rec.Code, rec.Body.String())
		}
		if i == 2 {
			requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")
		}
	}

	h.mu.Lock()
	h.byID["accept-id"] = &ConnectRequest{ID: "accept-id", Status: "pending"}
	h.mu.Unlock()
	for range 3 {
		rec = callMux(mux, http.MethodPost, "/requests/accept-id/accept", "", "")
		if rec.Code == http.StatusTooManyRequests {
			t.Fatal("accept consumed a key allowance")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("accept %d %s", rec.Code, rec.Body.String())
		}
	}
}

type countReader struct {
	n int
	r io.Reader
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

type slogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (s *slogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (s *slogCapture) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	s.records = append(s.records, r.Clone())
	s.mu.Unlock()
	return nil
}

func (s *slogCapture) WithAttrs([]slog.Attr) slog.Handler { return s }

func (s *slogCapture) WithGroup(string) slog.Handler { return s }

func (s *slogCapture) all() []slog.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]slog.Record, len(s.records))
	copy(out, s.records)
	return out
}

func peerJSON(key, address, state string) string {
	b, err := json.Marshal(map[string]string{"key": key, "address": address, "state": state})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func connectJSON(fromKey, toKey, fromAddress string) string {
	b, err := json.Marshal(map[string]string{
		"from_key": fromKey, "to_key": toKey, "from_address": fromAddress,
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func postMux(mux *http.ServeMux, target, body string) *httptest.ResponseRecorder {
	return callMux(mux, http.MethodPost, target, body, "")
}

func getReq(mux *http.ServeMux, target, addr string) *httptest.ResponseRecorder {
	return callMux(mux, http.MethodGet, target, "", addr)
}

func callMux(mux *http.ServeMux, method, target, body, addr string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, r)
	if addr != "" {
		req.RemoteAddr = addr
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func requirePhrase(t *testing.T, rec *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	got := strings.TrimRight(rec.Body.String(), "\n")
	if rec.Code != status || got != want {
		t.Fatalf("status %d body %q, want %d %q", rec.Code, got, status, want)
	}
}
