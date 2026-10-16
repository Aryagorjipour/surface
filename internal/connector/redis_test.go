package connector

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisExpiry(t *testing.T) {
	addr := os.Getenv("SURFACE_TEST_REDIS")
	if addr == "" {
		t.Skip("SURFACE_TEST_REDIS unset")
	}
	cache := NewRedis(addr, "")
	ctx := context.Background()
	if err := cache.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})

	store := testStore(t)
	h := NewHandlers(store, cache, slog.New(slog.DiscardHandler))
	h.SetLifetime(time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	target := "redis-target-" + suffix
	caller := "redis-caller-" + suffix
	staleKey := "redis-stale-" + suffix
	staleAddr := "198.51.100.8:8"
	t.Cleanup(func() {
		_ = cache.client.Del(ctx, "pending:"+target, "peer:"+staleKey).Err()
	})

	rec := postMux(mux, "/peers", peerJSON(target, "203.0.113.20:20", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register target %d %s", rec.Code, rec.Body.String())
	}
	rec = postMux(mux, "/peers", peerJSON(caller, "203.0.113.21:21", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register caller %d %s", rec.Code, rec.Body.String())
	}

	idA := mustConnect(t, mux, caller, target, "203.0.113.21:21")
	t.Cleanup(func() { _ = cache.client.Del(ctx, "req:"+idA).Err() })
	h.SetLifetime(30 * time.Second)
	idB := mustConnect(t, mux, caller, target, "203.0.113.21:21")
	t.Cleanup(func() { _ = cache.client.Del(ctx, "req:"+idB).Err() })

	time.Sleep(1200 * time.Millisecond)

	n, err := cache.client.Exists(ctx, "req:"+idA).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("req key for the 1s request is still present")
	}
	n, err = cache.client.Exists(ctx, "req:"+idB).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("req key inside the 30s lifetime is missing")
	}
	ids, err := cache.PendingIDs(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if containsID(ids, idA) || !containsID(ids, idB) {
		t.Fatalf("pending set %#v", ids)
	}

	rec = postMux(mux, "/requests/"+idB+"/accept", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"accepted"}`)
	ttl, err := cache.client.TTL(ctx, "req:"+idB).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 20*time.Second || ttl > 40*time.Second {
		t.Fatalf("accepted ttl %s, want about 30s", ttl)
	}

	h.SetSilenceWindow(5 * time.Second)
	old := time.Now().Add(-time.Hour).Unix()
	if err := cache.SetPeer(ctx, staleKey, peerResponse{Key: staleKey, Address: staleAddr, State: "available"}, old, time.Minute); err != nil {
		t.Fatal(err)
	}
	rec = getReq(mux, "/peers/"+staleKey, "")
	requirePhrase(t, rec, http.StatusNotFound, "not found")
	if strings.Contains(rec.Body.String(), staleAddr) {
		t.Fatalf("lookup returned stale cache address %s", rec.Body.String())
	}
	n, err = cache.client.Exists(ctx, "peer:"+staleKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("stale peer cache key was not deleted")
	}
}

func TestConnectDropsStalePeerCache(t *testing.T) {
	addr := os.Getenv("SURFACE_TEST_REDIS")
	if addr == "" {
		t.Skip("SURFACE_TEST_REDIS unset")
	}
	cache := NewRedis(addr, "")
	ctx := context.Background()
	if err := cache.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})

	store := testStore(t)
	capture := &slogCapture{}
	h := NewHandlers(store, cache, slog.New(capture))
	var now time.Time
	now = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	h.SetNow(func() time.Time { return now })
	h.SetSilenceWindow(time.Second)
	h.SetLifetime(time.Hour)
	mux := http.NewServeMux()
	h.Register(mux)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	staleKey := "connect-stale-" + suffix
	freshKey := "connect-fresh-" + suffix
	const (
		staleAddr = "203.0.113.77:77"
		secret    = "test-secret-do-not-log"
	)
	t.Cleanup(func() {
		_ = cache.client.Del(ctx, "peer:"+staleKey, "peer:"+freshKey, "pending:"+freshKey).Err()
	})

	rec := postMux(mux, "/peers", peerJSON(staleKey, staleAddr, "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register stale %d %s", rec.Code, rec.Body.String())
	}
	rec = getReq(mux, "/peers/"+staleKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("lookup stale %d %s", rec.Code, rec.Body.String())
	}
	n, err := cache.client.Exists(ctx, "peer:"+staleKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("fresh lookup did not cache the peer")
	}

	now = now.Add(2 * time.Second)
	rec = postMux(mux, "/connect", connectJSON("connect-caller", staleKey, secret))
	requirePhrase(t, rec, http.StatusNotFound, "target not found")
	if strings.Contains(rec.Body.String(), staleAddr) || strings.Contains(rec.Body.String(), staleKey) {
		t.Fatalf("connect body %s", rec.Body.String())
	}
	if len(h.byID) != 0 {
		t.Fatal("connect to a stale peer stored a request")
	}
	n, err = cache.client.Exists(ctx, "peer:"+staleKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("connect left the stale peer cache key")
	}
	for _, record := range capture.all() {
		text := recordText(record)
		if strings.Contains(text, staleKey) || strings.Contains(text, staleAddr) || strings.Contains(text, secret) {
			t.Fatalf("connect log included a private value: %s", text)
		}
	}

	rec = postMux(mux, "/peers", peerJSON(freshKey, "203.0.113.78:78", "available"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register fresh %d %s", rec.Code, rec.Body.String())
	}
	rec = getReq(mux, "/peers/"+freshKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("lookup fresh %d %s", rec.Code, rec.Body.String())
	}
	rec = postMux(mux, "/connect", connectJSON("connect-caller", freshKey, "203.0.113.1:1"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("fresh connect %d %s", rec.Code, rec.Body.String())
	}
	n, err = cache.client.Exists(ctx, "peer:"+freshKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("connect deleted a fresh peer cache key")
	}
}

func TestRedisRestart(t *testing.T) {
	addr := os.Getenv("SURFACE_TEST_REDIS")
	if addr == "" {
		t.Skip("SURFACE_TEST_REDIS unset")
	}
	cache := NewRedis(addr, "")
	ctx := context.Background()
	if err := cache.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})

	store := testStore(t)
	const life = time.Minute
	now := time.Date(2026, 10, 16, 16, 0, 0, 0, time.UTC)
	newHandlers := func(clock *time.Time) *Handlers {
		h := NewHandlers(store, cache, slog.New(slog.DiscardHandler))
		h.SetNow(func() time.Time { return *clock })
		h.SetLifetime(life)
		return h
	}
	h := newHandlers(&now)
	mux := http.NewServeMux()
	h.Register(mux)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	targetA := "restart-a-" + suffix
	targetB := "restart-b-" + suffix
	caller := "restart-caller-" + suffix
	const fromAddr = "203.0.113.40:40"
	t.Cleanup(func() {
		_ = cache.client.Del(ctx, "pending:"+targetA, "pending:"+targetB).Err()
	})

	for _, key := range []string{targetA, targetB, caller} {
		rec := postMux(mux, "/peers", peerJSON(key, "203.0.113.20:20", "available"))
		if rec.Code != http.StatusOK {
			t.Fatalf("register %s %d %s", key, rec.Code, rec.Body.String())
		}
	}
	idA := mustConnect(t, mux, caller, targetA, fromAddr)
	idB := mustConnect(t, mux, caller, targetB, fromAddr)
	t.Cleanup(func() {
		_ = cache.client.Del(ctx, "req:"+idA, "req:"+idB).Err()
	})

	raw, err := cache.client.Get(ctx, "req:"+idA).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "expires_at") {
		t.Fatalf("stored body missing expires_at: %s", raw)
	}

	h2 := newHandlers(&now)
	if err := h2.LoadRequests(ctx); err != nil {
		t.Fatal(err)
	}
	mux2 := http.NewServeMux()
	h2.Register(mux2)

	rec := getReq(mux2, "/pending?key="+targetA, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("pending %d %s", rec.Code, rec.Body.String())
	}
	body := strings.TrimRight(rec.Body.String(), "\n")
	for _, want := range []string{
		`"request_id":"` + idA + `"`,
		`"from_key":"` + caller + `"`,
		`"from_address":"` + fromAddr + `"`,
		`"to_key":"` + targetA + `"`,
		`"status":"pending"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("pending body %s missing %s", body, want)
		}
	}
	if strings.Contains(body, idB) {
		t.Fatalf("pending list included the other target: %s", body)
	}
	rec = getReq(mux2, "/requests/"+idA, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	statusBody := strings.TrimRight(rec.Body.String(), "\n")
	for _, want := range []string{
		`"request_id":"` + idA + `"`,
		`"from_key":"` + caller + `"`,
		`"from_address":"` + fromAddr + `"`,
		`"to_key":"` + targetA + `"`,
		`"status":"pending"`,
	} {
		if !strings.Contains(statusBody, want) {
			t.Fatalf("status body %s missing %s", statusBody, want)
		}
	}
	if strings.Contains(statusBody, "expires_at") {
		t.Fatalf("status body contains expires_at: %s", statusBody)
	}

	rec = postMux(mux2, "/requests/"+idA+"/accept", "")
	requirePhrase(t, rec, http.StatusOK, `{"status":"accepted"}`)

	h3now := now
	h3 := newHandlers(&h3now)
	if err := h3.LoadRequests(ctx); err != nil {
		t.Fatal(err)
	}
	mux3 := http.NewServeMux()
	h3.Register(mux3)
	rec = getReq(mux3, "/requests/"+idA, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("restored decision %d %s", rec.Code, rec.Body.String())
	}
	decisionBody := strings.TrimRight(rec.Body.String(), "\n")
	if !strings.Contains(decisionBody, `"status":"accepted"`) || strings.Contains(decisionBody, "expires_at") {
		t.Fatalf("decision body %s", decisionBody)
	}
	pendingAfter := strings.TrimRight(getReq(mux3, "/pending?key="+targetA, "").Body.String(), "\n")
	if strings.Contains(pendingAfter, idA) {
		t.Fatalf("accepted id returned on the pending list: %s", pendingAfter)
	}

	h3.mu.RLock()
	deadline := h3.byID[idA].expiresAt
	h3.mu.RUnlock()
	h3now = deadline.Add(time.Nanosecond)
	rec = getReq(mux3, "/requests/"+idA, "")
	requirePhrase(t, rec, http.StatusNotFound, "request not found")
	n, err := cache.client.Exists(ctx, "req:"+idA).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("req key survived the deadline")
	}
	ids, err := cache.PendingIDs(ctx, targetA)
	if err != nil {
		t.Fatal(err)
	}
	if containsID(ids, idA) {
		t.Fatalf("pending set still has %s: %#v", idA, ids)
	}

	h4now := h3now
	h4 := newHandlers(&h4now)
	if err := h4.LoadRequests(ctx); err != nil {
		t.Fatal(err)
	}
	mux4 := http.NewServeMux()
	h4.Register(mux4)
	rec = getReq(mux4, "/requests/"+idA, "")
	requirePhrase(t, rec, http.StatusNotFound, "request not found")
	rec = getReq(mux4, "/pending?key="+targetA, "")
	requirePhrase(t, rec, http.StatusOK, "[]")
}

func containsID(ids []string, id string) bool {
	for _, item := range ids {
		if item == id {
			return true
		}
	}
	return false
}
