package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type ConnectRequest struct {
	ID          string    `json:"request_id"`
	FromKey     string    `json:"from_key"`
	FromAddress string    `json:"from_address"`
	ToKey       string    `json:"to_key"`
	Status      string    `json:"status"`
	Created     time.Time `json:"created"`
	expiresAt   time.Time `json:"-"`
}

type Handlers struct {
	store      *Store
	logger     *slog.Logger
	mu         sync.RWMutex
	byID       map[string]*ConnectRequest
	byToKey    map[string][]string
	cache      *RedisCache
	limits     *Limits
	limitsOnce sync.Once
	now        func() time.Time
	lifetime   time.Duration
	silence    time.Duration
}

func NewHandlers(store *Store, cache *RedisCache, logger *slog.Logger) *Handlers {
	return &Handlers{
		store:   store,
		cache:   cache,
		logger:  logger,
		byID:    make(map[string]*ConnectRequest),
		byToKey: make(map[string][]string),
	}
}

// SetLimits installs process limits before the first request.
func (h *Handlers) SetLimits(l *Limits) {
	h.limits = l
}

// SetNow replaces the clock used for request deadlines and peer staleness.
func (h *Handlers) SetNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	h.now = now
	if h.store != nil {
		h.store.SetNow(now)
	}
}

// SetLifetime installs the request lifetime. Zero keeps the 10-minute default.
func (h *Handlers) SetLifetime(d time.Duration) {
	h.lifetime = d
}

// SetSilenceWindow installs the silence window on the handlers and the store.
// Open does not choose the window. Zero keeps the 24-hour default.
func (h *Handlers) SetSilenceWindow(d time.Duration) {
	h.silence = d
	if h.store != nil {
		h.store.SetSilenceWindow(d)
	}
}

func (h *Handlers) clock() time.Time {
	if h.now == nil {
		return time.Now()
	}
	return h.now()
}

func (h *Handlers) requestLifetime() time.Duration {
	if h.lifetime > 0 {
		return h.lifetime
	}
	return DefaultExpiryConfig().RequestLifetime
}

func (h *Handlers) silenceWindow() time.Duration {
	if h.silence > 0 {
		return h.silence
	}
	return DefaultExpiryConfig().SilenceWindow
}

func (h *Handlers) lim() *Limits {
	h.limitsOnce.Do(func() {
		if h.limits == nil {
			h.limits = NewLimits(DefaultConfig())
		}
	})
	return h.limits
}

func (h *Handlers) Register(mux *http.ServeMux) {
	h.route(mux, "POST /peers", "register", h.handleUpsertPeer)
	h.route(mux, "GET /peers/{key}", "lookup", h.handleGetPeer)
	h.route(mux, "POST /connect", "connect", h.handleConnect)
	h.route(mux, "GET /pending", "pending", h.handlePending)
	h.route(mux, "POST /requests/{id}/accept", "accept", h.handleAccept)
	h.route(mux, "POST /requests/{id}/reject", "reject", h.handleReject)
	h.route(mux, "GET /requests/{id}", "status", h.handleRequestStatus)
}

func (h *Handlers) route(mux *http.ServeMux, pattern, op string, fn http.HandlerFunc) {
	traced := otelhttp.NewHandler(fn, pattern)
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if !h.lim().AllowSource(SourceHost(r.RemoteAddr)) {
			h.reject(w, http.StatusTooManyRequests, op, "too many requests")
			return
		}
		traced.ServeHTTP(w, ScrubForTrace(r, pattern))
	})
}

func (h *Handlers) reject(w http.ResponseWriter, status int, op, class string) {
	if h.logger != nil {
		h.logger.Info("refused", "op", op, "class", class)
	}
	http.Error(w, class, status)
}

func (h *Handlers) readBody(w http.ResponseWriter, r *http.Request, op string) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(h.lim().cfg.MaxBodyBytes))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			h.reject(w, http.StatusBadRequest, op, "request too large")
			return nil, false
		}
		h.reject(w, http.StatusBadRequest, op, "invalid json")
		return nil, false
	}
	return body, true
}

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, v any) {
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error("encode response", "err", err)
	}
}

type peerRequest struct {
	Key     string `json:"key"`
	Address string `json:"address"`
	State   string `json:"state"`
}

type peerResponse struct {
	Key     string `json:"key"`
	Address string `json:"address"`
	State   string `json:"state"`
}

func (h *Handlers) handleUpsertPeer(w http.ResponseWriter, r *http.Request) {
	body, ok := h.readBody(w, r, "register")
	if !ok {
		return
	}
	var req peerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.reject(w, http.StatusBadRequest, "register", "invalid json")
		return
	}
	if fault, ok := ValidateRegister(h.lim().cfg, req.Key, req.Address, req.State); !ok {
		h.reject(w, fault.Status, "register", fault.Phrase)
		return
	}
	if !h.lim().AllowKey(req.Key) {
		h.reject(w, http.StatusTooManyRequests, "register", "too many requests")
		return
	}

	if err := h.store.UpsertPeer(r.Context(), req.Key, req.Address, req.State); err != nil {
		h.logger.Error("upsert failed", "err", err)
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	_ = h.cache.DelPeer(r.Context(), req.Key)

	h.writeJSON(w, 0, map[string]bool{"ok": true})
}

func (h *Handlers) handleGetPeer(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		h.reject(w, http.StatusBadRequest, "lookup", "missing key")
		return
	}
	if fault, ok := ValidateKey(h.lim().cfg, key); !ok {
		h.reject(w, fault.Status, "lookup", fault.Phrase)
		return
	}
	if !h.lim().AllowKey(key) {
		h.reject(w, http.StatusTooManyRequests, "lookup", "too many requests")
		return
	}

	if h.cache != nil {
		if p, err := h.cache.GetPeer(r.Context(), key); err == nil && p != nil {
			if p.LastSeen == 0 || PeerStale(p.LastSeen, h.clock(), h.silenceWindow()) {
				_ = h.cache.DelPeer(r.Context(), key)
			} else {
				h.writeJSON(w, 0, peerResponse{Key: p.Key, Address: p.Address, State: p.State})
				return
			}
		}
	}

	addr, state, seen, err := h.store.GetPeer(r.Context(), key)
	if err != nil {
		if err.Error() == "peer not found" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		h.logger.Error("get peer failed", "err", err)
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	_ = h.cache.SetPeer(r.Context(), key, peerResponse{Key: key, Address: addr, State: state}, seen.Unix(), 30*time.Second)

	h.writeJSON(w, 0, peerResponse{Key: key, Address: addr, State: state})
}

func (h *Handlers) handleConnect(w http.ResponseWriter, r *http.Request) {
	body, ok := h.readBody(w, r, "connect")
	if !ok {
		return
	}
	var req ConnectRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.reject(w, http.StatusBadRequest, "connect", "invalid json")
		return
	}
	if fault, ok := ValidateConnect(h.lim().cfg, req.FromKey, req.ToKey, req.FromAddress); !ok {
		h.reject(w, fault.Status, "connect", fault.Phrase)
		return
	}
	if !h.lim().AllowKey(req.FromKey, req.ToKey) {
		h.reject(w, http.StatusTooManyRequests, "connect", "too many requests")
		return
	}

	_, state, _, err := h.store.GetPeer(r.Context(), req.ToKey)
	if err != nil {
		if err.Error() == "peer not found" {
			_ = h.cache.DelPeer(r.Context(), req.ToKey)
		}
		http.Error(w, "target not found", http.StatusNotFound)
		return
	}
	if state != "available" {
		http.Error(w, "target not connectable", http.StatusConflict)
		return
	}

	created := h.clock()
	lifetime := h.requestLifetime()
	cr := &ConnectRequest{
		ID:          uuid.NewString(),
		FromKey:     req.FromKey,
		FromAddress: req.FromAddress,
		ToKey:       req.ToKey,
		Status:      "pending",
		Created:     created,
		expiresAt:   RequestDeadline(created, time.Time{}, false, lifetime),
	}

	_ = h.cache.SetRequest(r.Context(), cr, lifetime)

	h.mu.Lock()
	h.byID[cr.ID] = cr
	h.byToKey[req.ToKey] = append(h.byToKey[req.ToKey], cr.ID)
	h.mu.Unlock()

	h.writeJSON(w, http.StatusCreated, map[string]string{
		"request_id": cr.ID,
		"status":     cr.Status,
	})
}

func (h *Handlers) handlePending(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		h.reject(w, http.StatusBadRequest, "pending", "key query param required")
		return
	}
	if fault, ok := ValidateKey(h.lim().cfg, key); !ok {
		h.reject(w, fault.Status, "pending", fault.Phrase)
		return
	}
	if !h.lim().AllowKey(key) {
		h.reject(w, http.StatusTooManyRequests, "pending", "too many requests")
		return
	}

	now := h.clock()
	h.mu.Lock()
	ids := append([]string(nil), h.byToKey[key]...)
	out := make([]ConnectRequest, 0)
	for _, id := range ids {
		cr := h.byID[id]
		if cr == nil {
			continue
		}
		if !RequestReadable(now, cr.expiresAt) {
			h.removeLocked(id)
			continue
		}
		if cr.Status == "pending" {
			out = append(out, *cr)
		}
	}
	h.mu.Unlock()

	h.writeJSON(w, 0, out)
}

func (h *Handlers) handleAccept(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if fault, ok := ValidateID(h.lim().cfg, id); !ok {
		h.reject(w, fault.Status, "accept", fault.Phrase)
		return
	}

	h.mu.Lock()
	cr, ok := h.byID[id]
	if !ok || !RequestReadable(h.clock(), cr.expiresAt) {
		if ok {
			h.removeLocked(id)
		}
		h.mu.Unlock()
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	now := h.clock()
	cr.Status = "accepted"
	cr.expiresAt = RequestDeadline(cr.Created, now, true, h.requestLifetime())
	h.mu.Unlock()

	_ = h.cache.UpdateRequestStatus(r.Context(), id, "accepted", h.requestLifetime())

	h.writeJSON(w, 0, map[string]string{"status": "accepted"})
}

func (h *Handlers) handleReject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if fault, ok := ValidateID(h.lim().cfg, id); !ok {
		h.reject(w, fault.Status, "reject", fault.Phrase)
		return
	}

	h.mu.Lock()
	cr, ok := h.byID[id]
	if !ok || !RequestReadable(h.clock(), cr.expiresAt) {
		if ok {
			h.removeLocked(id)
		}
		h.mu.Unlock()
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	now := h.clock()
	cr.Status = "rejected"
	cr.expiresAt = RequestDeadline(cr.Created, now, true, h.requestLifetime())
	h.mu.Unlock()

	_ = h.cache.UpdateRequestStatus(r.Context(), id, "rejected", h.requestLifetime())

	h.logger.Info("request rejected", "id", id)
	h.writeJSON(w, 0, map[string]string{"status": "rejected"})
}

func (h *Handlers) handleRequestStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if fault, ok := ValidateID(h.lim().cfg, id); !ok {
		h.reject(w, fault.Status, "status", fault.Phrase)
		return
	}

	h.mu.Lock()
	cr, ok := h.byID[id]
	if !ok || !RequestReadable(h.clock(), cr.expiresAt) {
		if ok {
			h.removeLocked(id)
		}
		h.mu.Unlock()
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	copy := *cr
	h.mu.Unlock()

	h.writeJSON(w, 0, &copy)
}

func (h *Handlers) removeLocked(id string) {
	cr := h.byID[id]
	delete(h.byID, id)
	if cr == nil {
		return
	}
	ids := h.byToKey[cr.ToKey]
	dst := 0
	for _, other := range ids {
		if other != id {
			ids[dst] = other
			dst++
		}
	}
	if dst == 0 {
		delete(h.byToKey, cr.ToKey)
		return
	}
	h.byToKey[cr.ToKey] = ids[:dst]
}

// pruneOnce drops expired in-process requests and stale peer rows.
// It does not accept or reject a request, and it does not log keys.
func (h *Handlers) pruneOnce(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := h.clock()
	h.mu.Lock()
	targets := make([]string, 0, len(h.byToKey))
	for key := range h.byToKey {
		targets = append(targets, key)
	}
	var drop []string
	for id, cr := range h.byID {
		if cr == nil || !RequestReadable(now, cr.expiresAt) {
			drop = append(drop, id)
		}
	}
	for _, id := range drop {
		h.removeLocked(id)
	}
	h.mu.Unlock()

	if len(drop) > 0 && h.logger != nil {
		h.logger.Info("pruned requests", "count", len(drop))
	}

	var peerRemoved int
	if h.store != nil {
		keys, err := h.store.Prune(ctx, now, h.silenceWindow())
		if err != nil {
			if h.logger != nil {
				h.logger.Error("prune failed", "err", err)
			}
		} else {
			peerRemoved = len(keys)
			for _, key := range keys {
				_ = h.cache.DelPeer(ctx, key)
			}
		}
	}
	if peerRemoved > 0 && h.logger != nil {
		h.logger.Info("pruned peers", "count", peerRemoved)
	}
	if h.cache != nil {
		for _, key := range targets {
			_, _ = h.cache.PendingIDs(ctx, key)
		}
	}
}

// StartPrune deletes expired requests and stale peers until ctx is cancelled.
// The returned function waits until the loop has stopped.
func (h *Handlers) StartPrune(ctx context.Context, every time.Duration) func() {
	if every <= 0 {
		every = time.Second
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.pruneOnce(context.Background())
			}
		}
	}()
	return func() { <-done }
}
