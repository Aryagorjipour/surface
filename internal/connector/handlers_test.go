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
	h.byID[longID] = &ConnectRequest{ID: longID, Status: "pending", expiresAt: time.Now().Add(time.Hour)}
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

	pastID := strings.Repeat("p", 65)
	h.mu.Lock()
	h.byID[pastID] = &ConnectRequest{ID: pastID, Status: "pending", expiresAt: time.Unix(1, 0)}
	h.mu.Unlock()
	for _, target := range []string{
		"/requests/" + pastID + "/accept",
		"/requests/" + pastID + "/reject",
		"/requests/" + pastID,
	} {
		method := http.MethodGet
		if strings.HasSuffix(target, "/accept") || strings.HasSuffix(target, "/reject") {
			method = http.MethodPost
		}
		rec = callMux(mux, method, target, "", "")
		requirePhrase(t, rec, http.StatusBadRequest, "identifier too long")
	}
	h.mu.RLock()
	if h.byID[pastID] == nil || h.byID[pastID].Status != "pending" {
		t.Fatal("past long id was dropped or its status changed")
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
	h.byID["accept-id"] = &ConnectRequest{ID: "accept-id", Status: "pending", expiresAt: time.Now().Add(time.Hour)}
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

func TestPendingExpires(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.DiscardHandler))
	var now time.Time
	now = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	const life = 5 * time.Second
	h.SetLifetime(life)
	mux := http.NewServeMux()
	h.Register(mux)

	rec := postMux(mux, "/peers", peerJSON("target", "203.0.113.1:1", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}

	idA := mustConnect(t, mux, "caller", "target", "203.0.113.2:2")
	deadlineA := now.Add(life)
	now = deadlineA
	if !pendingHas(pendingBodies(t, mux, "target"), idA, "pending") {
		t.Fatal("request missing at its deadline")
	}
	requireStatus(t, mux, idA, "pending")

	now = deadlineA.Add(time.Nanosecond)
	rec = getReq(mux, "/pending?key=target", "")
	requirePhrase(t, rec, http.StatusOK, "[]")
	for _, target := range []string{"/requests/" + idA, "/requests/" + idA + "/accept", "/requests/" + idA + "/reject"} {
		method := http.MethodGet
		if strings.Contains(target, "/accept") || strings.Contains(target, "/reject") {
			method = http.MethodPost
		}
		rec = callMux(mux, method, target, "", "")
		requirePhrase(t, rec, http.StatusNotFound, "request not found")
	}
	h.mu.RLock()
	_, still := h.byID[idA]
	h.mu.RUnlock()
	if still {
		t.Fatal("expired request stayed in memory")
	}

	now = time.Date(2026, 10, 16, 12, 30, 0, 0, time.UTC)
	idOld := mustConnect(t, mux, "caller", "target", "203.0.113.2:2")
	now = now.Add(time.Second)
	idNew := mustConnect(t, mux, "caller", "target", "203.0.113.2:2")
	now = time.Date(2026, 10, 16, 12, 30, 0, 0, time.UTC).Add(life + time.Nanosecond)
	pending := pendingBodies(t, mux, "target")
	if pendingHas(pending, idOld, "pending") || !pendingHas(pending, idNew, "pending") {
		t.Fatalf("newer request did not stay listed: %#v", pending)
	}

	now = time.Date(2026, 10, 16, 13, 0, 0, 0, time.UTC)
	idC := mustConnect(t, mux, "caller", "target", "203.0.113.2:2")
	now = now.Add(time.Second)
	rec = postMux(mux, "/requests/"+idC+"/accept", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"accepted"}`)
	if pendingHas(pendingBodies(t, mux, "target"), idC, "pending") {
		t.Fatal("accepted request stayed on the pending list")
	}
	h.mu.RLock()
	acceptedDeadline := h.byID[idC].expiresAt
	h.mu.RUnlock()
	now = acceptedDeadline.Add(-time.Second)
	requireStatus(t, mux, idC, "accepted")
	h.mu.RLock()
	if !h.byID[idC].expiresAt.Equal(acceptedDeadline) {
		t.Fatal("status read moved the accept deadline")
	}
	h.mu.RUnlock()
	now = acceptedDeadline
	requireStatus(t, mux, idC, "accepted")
	h.mu.RLock()
	if !h.byID[idC].expiresAt.Equal(acceptedDeadline) {
		t.Fatal("status read at the deadline moved it")
	}
	h.mu.RUnlock()
	now = acceptedDeadline.Add(time.Nanosecond)
	rec = getReq(mux, "/requests/"+idC, "")
	requirePhrase(t, rec, http.StatusNotFound, "request not found")

	now = time.Date(2026, 10, 16, 14, 0, 0, 0, time.UTC)
	idD := mustConnect(t, mux, "caller", "target", "203.0.113.2:2")
	now = now.Add(time.Second)
	rec = postMux(mux, "/requests/"+idD+"/reject", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"rejected"}`)
	if pendingHas(pendingBodies(t, mux, "target"), idD, "pending") {
		t.Fatal("rejected request stayed on the pending list")
	}
	h.mu.RLock()
	rejectedDeadline := h.byID[idD].expiresAt
	h.mu.RUnlock()
	now = rejectedDeadline.Add(-time.Second)
	requireStatus(t, mux, idD, "rejected")
	h.mu.RLock()
	if !h.byID[idD].expiresAt.Equal(rejectedDeadline) {
		t.Fatal("status read moved the reject deadline")
	}
	h.mu.RUnlock()
	now = rejectedDeadline
	requireStatus(t, mux, idD, "rejected")
	now = rejectedDeadline.Add(time.Nanosecond)
	rec = getReq(mux, "/requests/"+idD, "")
	requirePhrase(t, rec, http.StatusNotFound, "request not found")
}

func TestStalePeerNotFound(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.DiscardHandler))
	var now time.Time
	now = time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	const window = 4 * time.Second
	h.SetSilenceWindow(window)
	h.SetLifetime(30 * time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, tc := range []struct{ key, state string }{
		{"busy-peer", "busy"},
		{"offline-peer", "offline"},
	} {
		rec := postMux(mux, "/peers", peerJSON(tc.key, "203.0.113.4:4", tc.state))
		if rec.Code != http.StatusOK {
			t.Fatalf("register %s: %d %s", tc.state, rec.Code, rec.Body.String())
		}
		rec = getReq(mux, "/peers/"+tc.key, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("lookup %s: %d %s", tc.state, rec.Code, rec.Body.String())
		}
		got := strings.TrimRight(rec.Body.String(), "\n")
		if strings.Contains(got, "last_seen") {
			t.Fatalf("lookup body includes last_seen: %s", got)
		}
		var body peerResponse
		if err := json.Unmarshal([]byte(got), &body); err != nil {
			t.Fatal(err)
		}
		if body.State != tc.state || body.Address != "203.0.113.4:4" {
			t.Fatalf("presence %+v", body)
		}
	}

	registeredAt := now
	rec := postMux(mux, "/peers", peerJSON("silent", "203.0.113.5:5", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register silent %d %s", rec.Code, rec.Body.String())
	}
	now = registeredAt.Add(time.Second)
	rec = getReq(mux, "/peers/silent", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("lookup inside window %d %s", rec.Code, rec.Body.String())
	}
	now = registeredAt.Add(window + time.Second)
	rec = getReq(mux, "/peers/silent", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	rec = getReq(mux, "/peers/silent", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	rec = postMux(mux, "/connect", connectJSON("caller", "silent", "203.0.113.8:8"))
	requirePhrase(t, rec, http.StatusNotFound, "target not found")
	if len(h.byID) != 0 {
		t.Fatal("connect to a stale peer stored a request")
	}

	now = registeredAt
	rec = postMux(mux, "/peers", peerJSON("untouched", "203.0.113.6:6", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register untouched %d %s", rec.Code, rec.Body.String())
	}
	now = registeredAt.Add(window + time.Second)
	h.pruneOnce(context.Background())
	h.SetSilenceWindow(24 * time.Hour)
	addr, _, _, err := store.GetPeer(context.Background(), "untouched")
	if err == nil || err.Error() != "peer not found" || addr != "" {
		t.Fatalf("untouched after prune: addr %q err %v", addr, err)
	}
	h.SetSilenceWindow(window)

	rec = postMux(mux, "/peers", peerJSON("silent", "203.0.113.7:7", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("reregister %d %s", rec.Code, rec.Body.String())
	}
	rec = getReq(mux, "/peers/silent", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "203.0.113.7:7") {
		t.Fatalf("reregister lookup %d %s", rec.Code, rec.Body.String())
	}

	now = time.Date(2026, 10, 16, 11, 0, 0, 0, time.UTC)
	rec = postMux(mux, "/peers", peerJSON("live", "203.0.113.9:9", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register live %d %s", rec.Code, rec.Body.String())
	}
	liveID := mustConnect(t, mux, "caller", "live", "203.0.113.8:8")
	now = now.Add(window + time.Second)
	h.pruneOnce(context.Background())
	h.SetSilenceWindow(24 * time.Hour)
	rec = getReq(mux, "/peers/live", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	requireStatus(t, mux, liveID, "pending")
	if !pendingHas(pendingBodies(t, mux, "live"), liveID, "pending") {
		t.Fatal("prune dropped a request that was still inside its lifetime")
	}
	h.mu.RLock()
	if h.byID[liveID] == nil || h.byID[liveID].Status != "pending" {
		t.Fatal("prune accepted or rejected the request")
	}
	h.mu.RUnlock()
}

func TestFreshConnectStillWorks(t *testing.T) {
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	h.Register(mux)

	for _, key := range []string{"fresh-a", "fresh-b"} {
		rec := postMux(mux, "/peers", peerJSON(key, "127.0.0.1:9", "available"))
		if rec.Code != http.StatusOK {
			t.Fatalf("register %s: %d %s", key, rec.Code, rec.Body.String())
		}
	}
	rec := postMux(mux, "/connect", connectJSON("fresh-a", "fresh-b", "127.0.0.1:8"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("connect %d %s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Status != "pending" || created.ID == "" {
		t.Fatalf("created %+v", created)
	}
	rec = postMux(mux, "/requests/"+created.ID+"/accept", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"accepted"}`)
	for _, key := range []string{"fresh-a", "fresh-b"} {
		rec = getReq(mux, "/peers/"+key, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("lookup %s: %d %s", key, rec.Code, rec.Body.String())
		}
		var body peerResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Key != key || body.State != "available" || body.Address != "127.0.0.1:9" {
			t.Fatalf("lookup %+v", body)
		}
	}
}

func TestOperatorDurations(t *testing.T) {
	cfg, err := ParseExpiryConfig(func(name string) (string, bool) {
		switch name {
		case "SURFACE_REQUEST_LIFETIME":
			return "2s", true
		case "SURFACE_SILENCE_WINDOW":
			return "3s", true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	store := testStore(t)
	h := NewHandlers(store, nil, slog.New(slog.DiscardHandler))
	h.SetLifetime(cfg.RequestLifetime)
	h.SetSilenceWindow(cfg.SilenceWindow)
	var now time.Time
	now = time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	mux := http.NewServeMux()
	h.Register(mux)

	rec := postMux(mux, "/peers", peerJSON("op-peer", "203.0.113.3:3", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	id := mustConnect(t, mux, "op-caller", "op-peer", "203.0.113.4:4")
	now = now.Add(3*time.Second + time.Second)
	rec = getReq(mux, "/pending?key=op-peer", "")
	requirePhrase(t, rec, http.StatusOK, "[]")
	rec = getReq(mux, "/requests/"+id, "")
	requirePhrase(t, rec, http.StatusNotFound, "request not found")
	rec = getReq(mux, "/peers/op-peer", "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
}

func TestExpiryLogsBare(t *testing.T) {
	store := testStore(t)
	capture := &slogCapture{}
	h := NewHandlers(store, nil, slog.New(capture))
	var now time.Time
	now = time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	h.SetLifetime(time.Second)
	h.SetSilenceWindow(time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		peerKey  = "bare-peer-key-9f"
		peerAddr = "203.0.113.77:77"
		secret   = "test-secret-do-not-log"
	)
	rec := postMux(mux, "/peers", peerJSON(peerKey, peerAddr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	id := mustConnect(t, mux, "bare-caller", peerKey, secret)
	now = now.Add(2 * time.Second)
	before := len(capture.all())
	h.pruneOnce(context.Background())
	records := capture.all()
	if len(records) <= before {
		t.Fatal("prune removed rows and logged nothing")
	}
	var sawRequests, sawPeers bool
	for _, record := range records[before:] {
		text := recordText(record)
		if strings.Contains(text, peerAddr) || strings.Contains(text, peerKey) || strings.Contains(text, id) || strings.Contains(text, secret) {
			t.Fatalf("expiry log included a private value: %s", text)
		}
		count := attrInt(record, "count")
		switch record.Message {
		case "pruned requests":
			if count <= 0 {
				t.Fatalf("request count %d", count)
			}
			sawRequests = true
		case "pruned peers":
			if count <= 0 {
				t.Fatalf("peer count %d", count)
			}
			sawPeers = true
		}
	}
	if !sawRequests || !sawPeers {
		t.Fatalf("missing prune records requests=%v peers=%v", sawRequests, sawPeers)
	}
	quiet := len(capture.all())
	h.pruneOnce(context.Background())
	if len(capture.all()) != quiet {
		t.Fatal("a prune that removed nothing wrote a record")
	}
}

func mustConnect(t *testing.T, mux *http.ServeMux, from, to, addr string) string {
	t.Helper()
	rec := postMux(mux, "/connect", connectJSON(from, to, addr))
	if rec.Code != http.StatusCreated {
		t.Fatalf("connect %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.ID == "" || body.Status != "pending" {
		t.Fatalf("connect body %+v", body)
	}
	return body.ID
}

func pendingBodies(t *testing.T, mux *http.ServeMux, key string) []struct {
	ID     string `json:"request_id"`
	Status string `json:"status"`
} {
	t.Helper()
	rec := getReq(mux, "/pending?key="+key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("pending %d %s", rec.Code, rec.Body.String())
	}
	body := strings.TrimRight(rec.Body.String(), "\n")
	var items []struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(body), &items); err != nil {
		t.Fatalf("pending json %q: %v", body, err)
	}
	return items
}

func pendingHas(items []struct {
	ID     string `json:"request_id"`
	Status string `json:"status"`
}, id, status string) bool {
	for _, item := range items {
		if item.ID == id && item.Status == status {
			return true
		}
	}
	return false
}

func requireStatus(t *testing.T, mux *http.ServeMux, id, status string) {
	t.Helper()
	rec := getReq(mux, "/requests/"+id, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != status {
		t.Fatalf("status %q, want %q", body.Status, status)
	}
}

func recordText(record slog.Record) string {
	var b strings.Builder
	b.WriteString(record.Message)
	record.Attrs(func(a slog.Attr) bool {
		b.WriteByte(' ')
		b.WriteString(a.String())
		return true
	})
	return b.String()
}

func attrInt(record slog.Record, key string) int64 {
	var n int64
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			n = a.Value.Int64()
		}
		return true
	})
	return n
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
