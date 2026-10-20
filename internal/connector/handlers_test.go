package connector

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/Aryagorjipour/surface/internal/session"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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
	h.SetBearer(testBearer)
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"key":"alice","address":"127.0.0.1:8765","state":"available"}`
	req := httptest.NewRequest(http.MethodPost, "/peers", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+testBearer)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert status %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/peers/alice", nil)
	req.Header.Set("Authorization", "Bearer "+testBearer)
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
	h.SetBearer(testBearer)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/peers/missing", nil)
	req.Header.Set("Authorization", "Bearer "+testBearer)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestRejectMalformed(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux)

	longKey := strings.Repeat("k", 129)
	longAddr := strings.Repeat("a", 257)
	maxKey := strings.Repeat("k", 128)
	maxAddr := strings.Repeat("a", 254) + ":1"

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
	req.Header.Set("Authorization", "Bearer "+testBearer)
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
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	h := newTestHandlers(store, nil, slog.New(cap))
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
		bare := newTestHandlers(store, nil, nil)
		bareMux := http.NewServeMux()
		bare.Register(bareMux)
		rec := postMux(bareMux, "/peers", "{")
		requirePhrase(t, rec, http.StatusBadRequest, "invalid json")
	})
}

func TestKeyAllowanceHTTP(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	var now time.Time
	now = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	const life = 5 * time.Second
	h.SetLifetime(life)
	h.SetSilenceWindow(24 * time.Hour)
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
	requireAccepted(t, rec)
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
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
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
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
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
	requireAccepted(t, rec)
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
		if name == "SURFACE_REQUEST_LIFETIME" {
			return "2s", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	h.SetLifetime(cfg.RequestLifetime)
	h.SetSilenceWindow(3 * time.Second)
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
	h := newTestHandlers(store, nil, slog.New(capture))
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
		fromAddr = "203.0.113.98:98"
	)
	rec := postMux(mux, "/peers", peerJSON(peerKey, peerAddr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	id := mustConnect(t, mux, "bare-caller", peerKey, fromAddr)
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
		if strings.Contains(text, peerAddr) || strings.Contains(text, peerKey) || strings.Contains(text, id) || strings.Contains(text, fromAddr) {
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

const testBearer = "surface-test-bearer"

func newTestHandlers(store *Store, cache *RedisCache, logger *slog.Logger) *Handlers {
	h := NewHandlers(store, cache, logger)
	h.SetBearer(testBearer)
	return h
}

func callMux(mux *http.ServeMux, method, target, body, addr string) *httptest.ResponseRecorder {
	return callMuxToken(mux, method, target, body, addr, testBearer)
}

func callMuxToken(mux *http.ServeMux, method, target, body, addr, token string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, r)
	if addr != "" {
		req.RemoteAddr = addr
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func requireAccepted(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Status string `json:"status"`
		Proof  string `json:"admission_proof"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "accepted" || body.Proof == "" {
		t.Fatalf("body %s", rec.Body.String())
	}
	return body.Proof
}

func requirePhrase(t *testing.T, rec *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	got := strings.TrimRight(rec.Body.String(), "\n")
	if rec.Code != status || got != want {
		t.Fatalf("status %d body %q, want %d %q", rec.Code, got, status, want)
	}
}

func TestRestoreLoadedRequest(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	now := time.Date(2026, 10, 16, 15, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	h.SetLifetime(10 * time.Minute)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		idA   = "restore-a"
		idB   = "restore-b"
		addrA = "203.0.113.10:10"
	)
	deadline := now.Add(10 * time.Minute)
	h.applyStored([]storedRequest{
		{
			ConnectRequest: ConnectRequest{
				ID: idA, FromKey: "caller-a", FromAddress: addrA,
				ToKey: "target-a", Status: "pending", Created: now,
			},
			ExpiresAt: deadline,
		},
		{
			ConnectRequest: ConnectRequest{
				ID: idB, FromKey: "caller-b", FromAddress: "203.0.113.11:11",
				ToKey: "target-b", Status: "pending", Created: now,
			},
			ExpiresAt: deadline,
		},
	})

	rec := getReq(mux, "/pending?key=target-a", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("pending %d %s", rec.Code, rec.Body.String())
	}
	body := strings.TrimRight(rec.Body.String(), "\n")
	for _, want := range []string{
		`"request_id":"` + idA + `"`,
		`"from_key":"caller-a"`,
		`"from_address":"` + addrA + `"`,
		`"to_key":"target-a"`,
		`"status":"pending"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("pending body %s missing %s", body, want)
		}
	}
	if strings.Contains(body, idB) {
		t.Fatalf("pending body included the other target: %s", body)
	}
	if strings.Contains(body, "expires_at") {
		t.Fatalf("pending body contains expires_at: %s", body)
	}

	rec = getReq(mux, "/requests/"+idA, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	statusBody := strings.TrimRight(rec.Body.String(), "\n")
	for _, want := range []string{
		`"request_id":"` + idA + `"`,
		`"from_key":"caller-a"`,
		`"from_address":"` + addrA + `"`,
		`"to_key":"target-a"`,
		`"status":"pending"`,
	} {
		if !strings.Contains(statusBody, want) {
			t.Fatalf("status body %s missing %s", statusBody, want)
		}
	}
	if strings.Contains(statusBody, "expires_at") {
		t.Fatalf("status body contains expires_at: %s", statusBody)
	}
}

func TestRestoreDecision(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	now := time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	const life = 10 * time.Minute
	h.SetLifetime(life)
	mux := http.NewServeMux()
	h.Register(mux)

	acceptedAt := now.Add(2 * time.Hour)
	rejectedAt := now.Add(3 * time.Hour)
	created := now.Add(-time.Minute)
	h.applyStored([]storedRequest{
		{
			ConnectRequest: ConnectRequest{
				ID: "acc-id", FromKey: "caller-a", FromAddress: "203.0.113.10:10",
				ToKey: "target-a", Status: "accepted", Created: created,
			},
			ExpiresAt: acceptedAt,
		},
		{
			ConnectRequest: ConnectRequest{
				ID: "rej-id", FromKey: "caller-b", FromAddress: "203.0.113.11:11",
				ToKey: "target-a", Status: "rejected", Created: created,
			},
			ExpiresAt: rejectedAt,
		},
		{
			ConnectRequest: ConnectRequest{
				ID: "past-id", FromKey: "caller-c", FromAddress: "203.0.113.12:12",
				ToKey: "target-a", Status: "pending", Created: created,
			},
			ExpiresAt: now.Add(-time.Nanosecond),
		},
		{
			ConnectRequest: ConnectRequest{
				ID: "acc-zero", FromKey: "caller-d", FromAddress: "203.0.113.13:13",
				ToKey: "target-a", Status: "accepted", Created: created,
			},
		},
		{
			ConnectRequest: ConnectRequest{
				ID: "rej-zero", FromKey: "caller-e", FromAddress: "203.0.113.14:14",
				ToKey: "target-a", Status: "rejected", Created: created,
			},
		},
		{
			ConnectRequest: ConnectRequest{
				ID: "legacy-id", FromKey: "caller-f", FromAddress: "203.0.113.15:15",
				ToKey: "target-b", Status: "pending", Created: created,
			},
		},
	})

	rec := getReq(mux, "/requests/acc-id", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("accepted status %d %s", rec.Code, rec.Body.String())
	}
	acceptedBody := strings.TrimRight(rec.Body.String(), "\n")
	if !strings.Contains(acceptedBody, `"status":"accepted"`) {
		t.Fatalf("accepted body %s", acceptedBody)
	}
	if strings.Contains(acceptedBody, "expires_at") {
		t.Fatalf("status body contains expires_at: %s", acceptedBody)
	}
	pendingA := strings.TrimRight(getReq(mux, "/pending?key=target-a", "").Body.String(), "\n")
	if strings.Contains(pendingA, "acc-id") {
		t.Fatalf("accepted id stayed on the pending list: %s", pendingA)
	}
	h.mu.RLock()
	accepted := h.byID["acc-id"]
	h.mu.RUnlock()
	if accepted == nil || !accepted.expiresAt.Equal(acceptedAt) || accepted.expiresAt.Equal(now.Add(life)) {
		t.Fatalf("accepted deadline = fill time plus lifetime, got %#v", accepted)
	}

	rec = getReq(mux, "/requests/rej-id", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("rejected status %d %s", rec.Code, rec.Body.String())
	}
	rejectedBody := strings.TrimRight(rec.Body.String(), "\n")
	if !strings.Contains(rejectedBody, `"status":"rejected"`) || strings.Contains(rejectedBody, "expires_at") {
		t.Fatalf("rejected body %s", rejectedBody)
	}
	if strings.Contains(pendingA, "rej-id") {
		t.Fatalf("rejected id stayed on the pending list: %s", pendingA)
	}

	h.mu.RLock()
	_, past := h.byID["past-id"]
	_, accZero := h.byID["acc-zero"]
	_, rejZero := h.byID["rej-zero"]
	legacy := h.byID["legacy-id"]
	h.mu.RUnlock()
	if past || accZero || rejZero {
		t.Fatal("a past request or a decision without a deadline was inserted")
	}
	if legacy == nil || !legacy.expiresAt.Equal(created.Add(life)) {
		t.Fatalf("legacy deadline %#v, want %s", legacy, created.Add(life))
	}
	rec = getReq(mux, "/requests/legacy-id", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("legacy status %d %s", rec.Code, rec.Body.String())
	}
	legacyBody := strings.TrimRight(rec.Body.String(), "\n")
	if !strings.Contains(legacyBody, `"status":"pending"`) || strings.Contains(legacyBody, "expires_at") {
		t.Fatalf("legacy body %s", legacyBody)
	}
	pendingB := strings.TrimRight(getReq(mux, "/pending?key=target-b", "").Body.String(), "\n")
	if !strings.Contains(pendingB, "legacy-id") {
		t.Fatalf("legacy pending body %s", pendingB)
	}
}

func TestRestartWithoutStore(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	h.Register(mux)

	rec := postMux(mux, "/peers", peerJSON("target", "203.0.113.1:1", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	rec = postMux(mux, "/connect", connectJSON("caller", "target", "203.0.113.2:2"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("connect %d %s", rec.Code, rec.Body.String())
	}
	body := strings.TrimRight(rec.Body.String(), "\n")
	var created struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || !strings.Contains(body, `"status":"pending"`) {
		t.Fatalf("connect body %s", body)
	}
	if !pendingHas(pendingBodies(t, mux, "target"), created.ID, "pending") {
		t.Fatal("pending list omitted the new request")
	}
	rec = postMux(mux, "/requests/"+created.ID+"/accept", "")
	requireAccepted(t, rec)

	h2 := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	if err := h2.LoadRequests(context.Background()); err != nil {
		t.Fatal(err)
	}
	mux2 := http.NewServeMux()
	h2.Register(mux2)
	rec = getReq(mux2, "/pending?key=target", "")
	requirePhrase(t, rec, http.StatusOK, "[]")
	for _, target := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/requests/" + created.ID},
		{http.MethodPost, "/requests/" + created.ID + "/accept"},
		{http.MethodPost, "/requests/" + created.ID + "/reject"},
	} {
		rec = callMux(mux2, target.method, target.path, "", "")
		requirePhrase(t, rec, http.StatusNotFound, "request not found")
	}
}

func TestRestoreLogsBare(t *testing.T) {
	const (
		callerAddr = "203.0.113.50:50"
		callerKey  = "log-caller-key"
		targetKey  = "log-target-key"
		requestID  = "log-request-id"
		secret     = "test-secret-do-not-log"
	)
	now := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	const life = 10 * time.Minute
	readable := storedRequest{
		ConnectRequest: ConnectRequest{
			ID: requestID, FromKey: callerKey, FromAddress: callerAddr,
			ToKey: targetKey, Status: "pending", Created: now,
		},
		ExpiresAt: now.Add(life),
	}
	skipped := storedRequest{
		ConnectRequest: ConnectRequest{
			ID: requestID, FromKey: callerKey, FromAddress: secret,
			ToKey: targetKey, Status: "accepted", Created: now,
		},
	}

	store := testStore(t)
	restoredLog := &slogCapture{}
	h := newTestHandlers(store, nil, slog.New(restoredLog))
	h.SetNow(func() time.Time { return now })
	h.SetLifetime(life)
	h.applyStored([]storedRequest{readable})
	if !sawLog(restoredLog.all(), "restored requests", true) {
		t.Fatal("a readable pending fill did not log restored requests")
	}
	if sawLog(restoredLog.all(), "pending restore skipped", false) {
		t.Fatal("a fill that skipped nothing logged a skip")
	}
	assertBare(t, restoredLog.all(), callerAddr, callerKey, targetKey, requestID, secret)

	skipLog := &slogCapture{}
	h2 := NewHandlers(store, nil, slog.New(skipLog))
	h2.SetNow(func() time.Time { return now })
	h2.SetLifetime(life)
	h2.applyStored([]storedRequest{skipped})
	if !sawLog(skipLog.all(), "pending restore skipped", true) {
		t.Fatal("a decision without a deadline did not log a skip")
	}
	h2.mu.RLock()
	_, inserted := h2.byID[requestID]
	h2.mu.RUnlock()
	if inserted {
		t.Fatal("a decision without a deadline was inserted")
	}
	assertBare(t, skipLog.all(), callerAddr, callerKey, targetKey, requestID, secret)

	emptyLog := &slogCapture{}
	h3 := NewHandlers(store, nil, slog.New(emptyLog))
	h3.SetNow(func() time.Time { return now })
	h3.applyStored(nil)
	if sawLog(emptyLog.all(), "restored requests", false) {
		t.Fatal("a fill of no records logged restored requests")
	}

	failLog := &slogCapture{}
	h4 := NewHandlers(store, NewRedis("127.0.0.1:1", secret), slog.New(failLog))
	h4.mu.Lock()
	h4.byID = map[string]*ConnectRequest{"seed": {ID: requestID, FromKey: callerKey, FromAddress: callerAddr, ToKey: targetKey}}
	h4.byToKey = map[string][]string{targetKey: {requestID}}
	h4.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h4.LoadRequests(ctx); err == nil {
		t.Fatal("canceled fill returned nil")
	}
	h4.mu.RLock()
	emptyMaps := len(h4.byID) == 0 && len(h4.byToKey) == 0
	h4.mu.RUnlock()
	if !emptyMaps {
		t.Fatal("failed fill left a partial map")
	}
	records := failLog.all()
	if len(records) != 1 || records[0].Message != "pending store unavailable, continuing without restore" {
		t.Fatalf("failed fill logs %#v", records)
	}
	if records[0].NumAttrs() != 0 {
		t.Fatalf("failed fill log has attributes")
	}
	assertBare(t, records, callerAddr, callerKey, targetKey, requestID, secret)
}

func sawLog(records []slog.Record, message string, needCount bool) bool {
	for _, record := range records {
		if record.Message != message {
			continue
		}
		if needCount && attrInt(record, "count") <= 0 {
			continue
		}
		return true
	}
	return false
}

func TestAcceptReturnsAdmissionProof(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		caller = "proof-caller"
		target = "proof-target"
		addr   = "203.0.113.40:40"
	)
	rec := postMux(mux, "/peers", peerJSON(target, "203.0.113.41:41", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	rec = postMux(mux, "/peers", peerJSON(caller, addr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register caller %d %s", rec.Code, rec.Body.String())
	}
	id := mustConnect(t, mux, caller, target, addr)

	rec = getReq(mux, "/requests/"+id, "")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "admission_proof") || strings.Contains(rec.Body.String(), "expires_at") {
		t.Fatalf("pending status %d %s", rec.Code, rec.Body.String())
	}
	pending := getReq(mux, "/pending?key="+target, "")
	if strings.Contains(pending.Body.String(), "admission_proof") {
		t.Fatalf("pending listed a proof: %s", pending.Body.String())
	}
	lookup := getReq(mux, "/peers/"+target, "")
	if lookup.Code != http.StatusOK || strings.Contains(lookup.Body.String(), "admission_proof") || strings.Contains(lookup.Body.String(), "last_seen") {
		t.Fatalf("lookup %d %s", lookup.Code, lookup.Body.String())
	}

	rec = postMux(mux, "/requests/"+id+"/accept", "")
	proof := requireAccepted(t, rec)
	raw, err := base64.StdEncoding.DecodeString(proof)
	if err != nil || len(raw) != session.ProofBytes {
		t.Fatalf("proof %q err %v len %d", proof, err, len(raw))
	}
	rec = getReq(mux, "/requests/"+id, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), proof) || strings.Contains(rec.Body.String(), "expires_at") {
		t.Fatalf("accepted status %s", rec.Body.String())
	}
	again := getReq(mux, "/requests/"+id, "")
	if !strings.Contains(again.Body.String(), proof) {
		t.Fatalf("second status changed the proof: %s", again.Body.String())
	}
	if strings.Contains(getReq(mux, "/pending?key="+target, "").Body.String(), "admission_proof") {
		t.Fatal("accepted proof appeared on the pending list")
	}

	rejectedID := mustConnect(t, mux, caller, target, addr)
	rec = postMux(mux, "/requests/"+rejectedID+"/reject", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"rejected"}`)
	if strings.Contains(rec.Body.String(), "admission_proof") {
		t.Fatalf("reject minted a proof: %s", rec.Body.String())
	}

	kept := "kept-proof-value"
	now := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	// The deadline is compared with the handler clock, not the wall clock.
	h.SetNow(func() time.Time { return now })
	h.applyStored([]storedRequest{{
		ConnectRequest: ConnectRequest{
			ID: "stored-accepted", FromKey: caller, FromAddress: addr, ToKey: target,
			Status: "accepted", Created: now, AdmissionProof: kept,
		},
		ExpiresAt: now.Add(time.Minute),
	}})
	rec = getReq(mux, "/requests/stored-accepted", "")
	if !strings.Contains(rec.Body.String(), kept) || strings.Contains(rec.Body.String(), "expires_at") {
		t.Fatalf("stored status %s", rec.Body.String())
	}
	if strings.Contains(getReq(mux, "/pending?key="+target, "").Body.String(), kept) {
		t.Fatal("stored proof appeared on the pending list")
	}
}

func TestBearerOrder(t *testing.T) {
	store := testStore(t)
	capture := &slogCapture{}
	h := NewHandlers(store, nil, slog.New(capture))
	h.SetBearer(testBearer)
	cfg := DefaultConfig()
	cfg.SourceAllowance = 2
	cfg.KeyAllowance = 1
	h.SetLimits(NewLimits(cfg))
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		src   = "203.0.113.80:80"
		key   = "hidden-peer"
		addr  = "203.0.113.81:81"
		wrong = "wrong-bearer-value"
	)
	longKey := strings.Repeat("k", 129)
	body := peerJSON(key, addr, "available")
	rec := callMuxToken(mux, http.MethodPost, "/peers", body, src, "")
	requirePhrase(t, rec, http.StatusUnauthorized, "unauthorized")
	if strings.Contains(rec.Body.String(), testBearer) {
		t.Fatalf("body echoed the token: %s", rec.Body.String())
	}
	rec = callMuxToken(mux, http.MethodGet, "/peers/"+key, "", "198.51.100.80:80", testBearer)
	requirePhrase(t, rec, http.StatusNotFound, "not found")

	rec = callMuxToken(mux, http.MethodPost, "/peers", body, src, "")
	requirePhrase(t, rec, http.StatusUnauthorized, "unauthorized")
	rec = callMuxToken(mux, http.MethodPost, "/peers", body, src, "")
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")

	rec = callMuxToken(mux, http.MethodPost, "/peers", body, "198.51.100.81:81", wrong)
	requirePhrase(t, rec, http.StatusUnauthorized, "unauthorized")
	if strings.Contains(rec.Body.String(), wrong) || strings.Contains(rec.Body.String(), testBearer) {
		t.Fatalf("body echoed a token: %s", rec.Body.String())
	}

	rec = callMuxToken(mux, http.MethodPost, "/peers", peerJSON(longKey, addr, "available"), "198.51.100.82:82", testBearer)
	requirePhrase(t, rec, http.StatusBadRequest, "key too long")

	okKey := "allowed-once"
	rec = callMuxToken(mux, http.MethodPost, "/peers", peerJSON(okKey, addr, "available"), "198.51.100.83:83", testBearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	rec = callMuxToken(mux, http.MethodPost, "/peers", peerJSON(okKey, addr, "busy"), "198.51.100.84:84", testBearer)
	requirePhrase(t, rec, http.StatusTooManyRequests, "too many requests")

	for _, record := range capture.all() {
		text := recordText(record)
		if strings.Contains(text, testBearer) || strings.Contains(text, wrong) {
			t.Fatalf("log contained a token: %s", text)
		}
	}
}

func TestLivenessExactBoundary(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	start := time.Date(2026, 10, 16, 19, 0, 0, 0, time.UTC)
	now := start
	h.SetNow(func() time.Time { return now })
	window := 3 * time.Second
	h.SetSilenceWindow(window)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		key  = "live-peer"
		addr = "203.0.113.90:90"
	)
	rec := postMux(mux, "/peers", peerJSON(key, addr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	now = start.Add(window)
	rec = getReq(mux, "/peers/"+key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("exact window %d %s", rec.Code, rec.Body.String())
	}
	var body peerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Key != key || body.Address != addr || body.State != "available" || strings.Contains(rec.Body.String(), "last_seen") {
		t.Fatalf("lookup %+v %s", body, rec.Body.String())
	}
	now = start.Add(window + time.Second)
	rec = getReq(mux, "/peers/"+key, "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	rec = postMux(mux, "/connect", connectJSON("live-caller", key, "203.0.113.91:91"))
	requirePhrase(t, rec, http.StatusNotFound, "target not found")
	if strings.Contains(getReq(mux, "/pending?key="+key, "").Body.String(), "live-caller") {
		t.Fatal("a missed peer created a request")
	}

	now = start.Add(time.Hour)
	busy := "busy-peer"
	rec = postMux(mux, "/peers", peerJSON(busy, addr, "busy"))
	if rec.Code != http.StatusOK {
		t.Fatalf("busy register %d %s", rec.Code, rec.Body.String())
	}
	rec = postMux(mux, "/peers", peerJSON(busy, addr, "busy"))
	if rec.Code != http.StatusOK {
		t.Fatalf("busy refresh %d %s", rec.Code, rec.Body.String())
	}
	rec = getReq(mux, "/peers/"+busy, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.State != "busy" || body.Address != addr {
		t.Fatalf("busy lookup %+v", body)
	}
	rec = postMux(mux, "/connect", connectJSON("live-caller", busy, "203.0.113.91:91"))
	requirePhrase(t, rec, http.StatusConflict, "target not connectable")
}

func TestActionSpans(t *testing.T) {
	rec := installSpanRecorder(t)
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	const (
		key   = "canary-peer-key"
		addr  = "203.0.113.10:9001"
		token = "canary-token"
	)
	h.SetBearer(token)
	mux := http.NewServeMux()
	h.Register(mux)

	before := 0
	step := func(pattern string, status int, got *httptest.ResponseRecorder) {
		t.Helper()
		if got.Code != status {
			t.Fatalf("%s status %d body %s", pattern, got.Code, got.Body.String())
		}
		ended := rec.Ended()
		if len(ended) != before+1 {
			t.Fatalf("%s produced %d new spans", pattern, len(ended)-before)
		}
		assertActionSpan(t, ended[len(ended)-1], pattern, status)
		before = len(ended)
	}

	step("POST /peers", http.StatusOK, callMuxToken(mux, http.MethodPost, "/peers", peerJSON(key, addr, "available"), "", token))
	step("GET /peers/{key}", http.StatusOK, callMuxToken(mux, http.MethodGet, "/peers/"+key, "", "", token))
	connected := callMuxToken(mux, http.MethodPost, "/connect", connectJSON("carol", key, addr), "", token)
	step("POST /connect", http.StatusCreated, connected)
	var created struct {
		ID string `json:"request_id"`
	}
	if err := json.Unmarshal(connected.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatalf("connect body %s", connected.Body.String())
	}
	step("GET /pending", http.StatusOK, callMuxToken(mux, http.MethodGet, "/pending?key="+key, "", "", token))
	step("POST /requests/{id}/accept", http.StatusOK, callMuxToken(mux, http.MethodPost, "/requests/"+created.ID+"/accept", "", "", token))
	step("POST /requests/{id}/reject", http.StatusOK, callMuxToken(mux, http.MethodPost, "/requests/"+created.ID+"/reject", "", "", token))
	step("GET /requests/{id}", http.StatusOK, callMuxToken(mux, http.MethodGet, "/requests/"+created.ID, "", "", token))

	unauthorized := callMuxToken(mux, http.MethodPost, "/peers", peerJSON(key, addr, "available"), "192.0.2.20:20", "")
	requirePhrase(t, unauthorized, http.StatusUnauthorized, "unauthorized")
	step("POST /peers", http.StatusUnauthorized, unauthorized)

	limited := testStore(t)
	h2 := newTestHandlers(limited, nil, slog.New(slog.DiscardHandler))
	h2.SetBearer(token)
	cfg := DefaultConfig()
	cfg.SourceAllowance = 1
	h2.SetLimits(NewLimits(cfg))
	mux2 := http.NewServeMux()
	h2.Register(mux2)
	filled := callMuxToken(mux2, http.MethodPost, "/peers", peerJSON("other-key", "127.0.0.1:1", "available"), "198.51.100.9:9", token)
	if filled.Code != http.StatusOK {
		t.Fatalf("allowance fill %d %s", filled.Code, filled.Body.String())
	}
	before = len(rec.Ended())
	denied := callMuxToken(mux2, http.MethodPost, "/peers", peerJSON(key, addr, "available"), "198.51.100.9:9", token)
	requirePhrase(t, denied, http.StatusTooManyRequests, "too many requests")
	step("POST /peers", http.StatusTooManyRequests, denied)

	for _, sp := range rec.Ended() {
		text := sp.Name()
		for _, attr := range sp.Attributes() {
			text += " " + string(attr.Key) + "=" + attr.Value.String()
		}
		for _, secret := range []string{key, addr, token, "surface-round-trip"} {
			if strings.Contains(text, secret) {
				t.Fatalf("span included %q: %s", secret, text)
			}
		}
	}
}

func TestHealth(t *testing.T) {
	rec := installSpanRecorder(t)
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.0.2.20:20"
	got := httptest.NewRecorder()
	mux.ServeHTTP(got, req)
	if got.Code != http.StatusOK || got.Body.String() != "ok\n" {
		t.Fatalf("health %d %q", got.Code, got.Body.String())
	}
	missing := callMux(mux, http.MethodGet, "/peers/canary-peer-key", "", "192.0.2.21:21")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("health created a peer %d %s", missing.Code, missing.Body.String())
	}

	before := len(rec.Ended())
	queried := httptest.NewRequest(http.MethodGet, "/health?canary-peer-key=1&addr=203.0.113.10:9001", nil)
	queried.RemoteAddr = "192.0.2.22:22"
	qrec := httptest.NewRecorder()
	mux.ServeHTTP(qrec, queried)
	if qrec.Code != http.StatusOK || qrec.Body.String() != "ok\n" {
		t.Fatalf("query health %d %q", qrec.Code, qrec.Body.String())
	}
	ended := rec.Ended()
	if len(ended) != before+1 {
		t.Fatalf("health spans %d", len(ended)-before)
	}
	assertActionSpan(t, ended[len(ended)-1], "GET /health", http.StatusOK)
	text := ended[len(ended)-1].Name()
	for _, attr := range ended[len(ended)-1].Attributes() {
		text += " " + string(attr.Key) + "=" + attr.Value.String()
	}
	for _, secret := range []string{"canary-peer-key", "203.0.113.10:9001"} {
		if strings.Contains(text, secret) || strings.Contains(qrec.Body.String(), secret) {
			t.Fatalf("health leaked %q span %s body %q", secret, text, qrec.Body.String())
		}
	}

	beforePath := len(rec.Ended())
	extra := httptest.NewRequest(http.MethodGet, "/health/canary-peer-key?addr=203.0.113.10:9001", nil)
	extra.RemoteAddr = "192.0.2.24:24"
	erec := httptest.NewRecorder()
	mux.ServeHTTP(erec, extra)
	if erec.Code != http.StatusOK || erec.Body.String() != "ok\n" {
		t.Fatalf("extra health %d %q", erec.Code, erec.Body.String())
	}
	ended = rec.Ended()
	if len(ended) != beforePath+1 {
		t.Fatalf("extra health spans %d", len(ended)-beforePath)
	}
	assertActionSpan(t, ended[len(ended)-1], "GET /health", http.StatusOK)
	text = ended[len(ended)-1].Name()
	for _, attr := range ended[len(ended)-1].Attributes() {
		text += " " + string(attr.Key) + "=" + attr.Value.String()
	}
	for _, secret := range []string{"canary-peer-key", "203.0.113.10:9001"} {
		if strings.Contains(text, secret) || strings.Contains(erec.Body.String(), secret) {
			t.Fatalf("extra health leaked %q span %s body %q", secret, text, erec.Body.String())
		}
	}
	stillMissing := callMux(mux, http.MethodGet, "/peers/canary-peer-key", "", "192.0.2.25:25")
	if stillMissing.Code != http.StatusNotFound {
		t.Fatalf("extra health created a peer %d %s", stillMissing.Code, stillMissing.Body.String())
	}

	limited := testStore(t)
	h2 := newTestHandlers(limited, nil, slog.New(slog.DiscardHandler))
	cfg := DefaultConfig()
	cfg.SourceAllowance = 1
	h2.SetLimits(NewLimits(cfg))
	mux2 := http.NewServeMux()
	h2.Register(mux2)
	filled := callMuxToken(mux2, http.MethodPost, "/peers", peerJSON("other-key", "127.0.0.1:1", "available"), "198.51.100.9:9", testBearer)
	if filled.Code != http.StatusOK {
		t.Fatalf("allowance fill %d %s", filled.Code, filled.Body.String())
	}
	over := httptest.NewRequest(http.MethodGet, "/health", nil)
	over.RemoteAddr = "198.51.100.9:9"
	overRec := httptest.NewRecorder()
	mux2.ServeHTTP(overRec, over)
	if overRec.Code != http.StatusOK || overRec.Body.String() != "ok\n" {
		t.Fatalf("exhausted health %d %q", overRec.Code, overRec.Body.String())
	}

	unauthorized := callMuxToken(mux, http.MethodPost, "/peers", peerJSON("canary-peer-key", "203.0.113.10:9001", "available"), "192.0.2.23:23", "")
	requirePhrase(t, unauthorized, http.StatusUnauthorized, "unauthorized")
}

func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func assertActionSpan(t *testing.T, sp sdktrace.ReadOnlySpan, pattern string, status int) {
	t.Helper()
	if sp.Name() != pattern {
		t.Fatalf("span name %q want %q", sp.Name(), pattern)
	}
	attrs := sp.Attributes()
	if len(attrs) != 2 {
		t.Fatalf("%s attributes %#v", pattern, attrs)
	}
	var endpoint string
	var sawStatus bool
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "endpoint":
			if attr.Value.Type() != attribute.STRING {
				t.Fatalf("endpoint type %s", attr.Value.Type())
			}
			endpoint = attr.Value.AsString()
		case "status":
			if attr.Value.Type() != attribute.INT64 {
				t.Fatalf("status type %s", attr.Value.Type())
			}
			sawStatus = true
			if attr.Value.AsInt64() != int64(status) {
				t.Fatalf("status %d want %d", attr.Value.AsInt64(), status)
			}
		default:
			t.Fatalf("unexpected attribute %s", attr.Key)
		}
	}
	if endpoint != pattern || !sawStatus {
		t.Fatalf("endpoint %q status present %v", endpoint, sawStatus)
	}
}

func assertBare(t *testing.T, records []slog.Record, banned ...string) {
	t.Helper()
	for _, record := range records {
		text := recordText(record)
		for _, secret := range banned {
			if secret != "" && strings.Contains(text, secret) {
				t.Fatalf("restore log included a private value: %s", text)
			}
		}
	}
}
