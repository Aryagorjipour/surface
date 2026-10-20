package connector

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCensusReportsCountsWithoutIdentifiers(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.DiscardHandler))
	t0 := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	now := t0
	h.SetNow(func() time.Time { return now })
	h.SetSilenceWindow(10 * time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		liveKey   = "census-live-key"
		liveAddr  = "198.51.100.10:4242"
		staleKey  = "census-stale-key"
		staleAddr = "198.51.100.11:4243"
		fromKey   = "census-from-key"
		fromAddr  = "198.51.100.12:4244"
	)
	banned := []string{liveKey, liveAddr, staleKey, staleAddr, fromKey, fromAddr}

	disabled := callMux(mux, http.MethodGet, "/operator/peers", "", "")
	requirePhrase(t, disabled, http.StatusNotFound, "not found")
	requirePhrase(t, callMuxToken(mux, http.MethodGet, "/operator/peers", "", "", ""), http.StatusUnauthorized, "unauthorized")
	requirePhrase(t, callMuxToken(mux, http.MethodGet, "/operator/pending", "", "", ""), http.StatusUnauthorized, "unauthorized")

	h.SetCensus(true)
	requirePhrase(t, callMuxToken(mux, http.MethodGet, "/operator/peers", "", "", ""), http.StatusUnauthorized, "unauthorized")
	requirePhrase(t, callMuxToken(mux, http.MethodGet, "/operator/pending", "", "", "wrong-bearer-value"), http.StatusUnauthorized, "unauthorized")
	assertCount(t, callMux(mux, http.MethodGet, "/operator/peers", "", ""), 0)
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 0)

	if rec := postMux(mux, "/peers", peerJSON(liveKey, liveAddr, "available")); rec.Code != http.StatusOK {
		t.Fatalf("live register %d %s", rec.Code, rec.Body.String())
	}
	now = t0.Add(-30 * time.Second)
	if rec := postMux(mux, "/peers", peerJSON(staleKey, staleAddr, "available")); rec.Code != http.StatusOK {
		t.Fatalf("stale register %d %s", rec.Code, rec.Body.String())
	}
	now = t0
	rec := postMux(mux, "/connect", connectJSON(fromKey, liveKey, fromAddr))
	if rec.Code != http.StatusCreated {
		t.Fatalf("connect %d %s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID string `json:"request_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatalf("request id %q %v", created.ID, err)
	}

	assertCount(t, callMux(mux, http.MethodGet, "/operator/peers", "", ""), 1, banned...)
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 1, banned...)

	now = t0.Add(h.requestLifetime() + time.Second)
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 0, banned...)
	now = t0
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 1, banned...)

	accept := callMux(mux, http.MethodPost, "/requests/"+created.ID+"/accept", "", "")
	if accept.Code != http.StatusOK {
		t.Fatalf("accept %d %s", accept.Code, accept.Body.String())
	}
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 0, banned...)

	now = t0.Add(-30 * time.Second)
	assertCount(t, callMux(mux, http.MethodGet, "/operator/peers", "", ""), 2, banned...)

	h.SetCensus(false)
	requirePhrase(t, callMux(mux, http.MethodGet, "/operator/peers", "", ""), http.StatusNotFound, "not found")
	requirePhrase(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), http.StatusNotFound, "not found")
}

func assertCount(t *testing.T, rec *httptest.ResponseRecorder, want int, banned ...string) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type %s", rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	for _, secret := range banned {
		if strings.Contains(body, secret) {
			t.Fatalf("body contains %q", secret)
		}
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) != 1 {
		t.Fatalf("fields %d", len(raw))
	}
	var n int
	if err := json.Unmarshal(raw["count"], &n); err != nil || n != want {
		t.Fatalf("count %s want %d", raw["count"], want)
	}
}

func TestCensusMissingStore(t *testing.T) {
	h := newTestHandlers(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.SetCensus(true)
	mux := http.NewServeMux()
	h.Register(mux)
	requirePhrase(t, callMux(mux, http.MethodGet, "/operator/peers", "", ""), http.StatusInternalServerError, "store error")
	assertCount(t, callMux(mux, http.MethodGet, "/operator/pending", "", ""), 0)
}
