package connector

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type ConnectRequest struct {
	ID             string    `json:"request_id"`
	FromKey        string    `json:"from_key"`
	FromAddress    string    `json:"from_address"`
	ToKey          string    `json:"to_key"`
	Status         string    `json:"status"`
	Created        time.Time `json:"created"`
	AdmissionProof string    `json:"admission_proof,omitempty"`
	expiresAt      time.Time `json:"-"`
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
	bearer     string
	census     bool
	punchMu    sync.Mutex
	punch      *punchBook
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

// SetBearer installs the deployment credential checked on every route.
func (h *Handlers) SetBearer(token string) {
	if h == nil {
		return
	}
	h.bearer = token
}

// SetCensus turns the operator count routes on. A later call still applies.
func (h *Handlers) SetCensus(on bool) {
	if h == nil {
		return
	}
	h.census = on
}

// SetSilenceWindow installs the liveness window on the handlers and the store.
// Open does not choose the window. Zero keeps the 90-second default.
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
	h.route(mux, "POST /requests/{id}/punch", "punch", h.handlePunch)
	h.route(mux, "GET /operator/peers", "census-peers", h.handleCensusPeers)
	h.route(mux, "GET /operator/pending", "census-pending", h.handleCensusPending)
	mux.HandleFunc("GET /health", h.handleHealth)
	// An extra path still uses this handler, so the span stays GET /health.
	mux.HandleFunc("GET /health/", h.handleHealth)
}

func (h *Handlers) route(mux *http.ServeMux, pattern, op string, fn http.HandlerFunc) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		h.withSpan(pattern, w, r, func(w http.ResponseWriter, r *http.Request) {
			if !h.lim().AllowSource(SourceHost(r.RemoteAddr)) {
				h.reject(w, http.StatusTooManyRequests, op, "too many requests")
				return
			}
			if !bearerOK(h.bearer, r.Header.Get("Authorization")) {
				h.reject(w, http.StatusUnauthorized, op, "unauthorized")
				return
			}
			fn(w, r)
		})
	})
}

// withSpan records one span named with the route template. The only attributes
// are endpoint and the HTTP status written to the client.
func (h *Handlers) withSpan(pattern string, w http.ResponseWriter, r *http.Request, fn func(http.ResponseWriter, *http.Request)) {
	ctx, span := otel.Tracer("github.com/Aryagorjipour/surface/connector").Start(r.Context(), pattern)
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		span.SetAttributes(
			attribute.String("endpoint", pattern),
			attribute.Int("status", sw.status),
		)
		span.End()
	}()
	fn(sw, r.WithContext(ctx))
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status = http.StatusOK
		s.wrote = true
	}
	return s.ResponseWriter.Write(b)
}

// handleHealth is outside the bearer check and the source allowance.
// The body is the word ok and a newline. It does not register a peer.
func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.withSpan("GET /health", w, r, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}

// bearerOK accepts only Authorization: Bearer after the source allowance.
// An empty expected token rejects every route. A missing header is rejected
// without comparing the token. A present token is compared as SHA-256 digests.
func bearerOK(expected, header string) bool {
	if expected == "" {
		return false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	presented := header[len(prefix):]
	sumE := sha256.Sum256([]byte(expected))
	sumP := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(sumE[:], sumP[:]) == 1
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

	_ = h.cache.SetPeer(r.Context(), key, peerResponse{Key: key, Address: addr, State: state}, seen.Unix(), h.silenceWindow())

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

type countResponse struct {
	Count int `json:"count"`
}

func (h *Handlers) handleCensusPeers(w http.ResponseWriter, r *http.Request) {
	h.serveCount(w, r, "count peers", func() (int, error) {
		return h.store.CountLivePeers(r.Context(), h.clock(), h.silenceWindow())
	})
}

func (h *Handlers) handleCensusPending(w http.ResponseWriter, r *http.Request) {
	h.serveCount(w, r, "count pending", func() (int, error) {
		return h.pendingCount(), nil
	})
}

func (h *Handlers) serveCount(w http.ResponseWriter, r *http.Request, op string, count func() (int, error)) {
	if !h.census {
		h.reject(w, http.StatusNotFound, op, "not found")
		return
	}
	n, err := count()
	if err != nil {
		if h.logger != nil {
			h.logger.Error(op, "err", err)
		}
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	h.writeJSON(w, 0, countResponse{Count: n})
}

func (h *Handlers) pendingCount() int {
	now := h.clock()
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, cr := range h.byID {
		if cr != nil && cr.Status == "pending" && RequestReadable(now, cr.expiresAt) {
			n++
		}
	}
	return n
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
	var dropped []droppedRequest
	for _, id := range ids {
		cr := h.byID[id]
		if cr == nil {
			continue
		}
		if !RequestReadable(now, cr.expiresAt) {
			if item, ok := h.removeLocked(id); ok {
				dropped = append(dropped, item)
			}
			continue
		}
		if cr.Status == "pending" {
			out = append(out, *cr)
		}
	}
	h.mu.Unlock()
	h.forgetRequests(r.Context(), dropped)

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
		item, removed := droppedRequest{}, false
		if ok {
			item, removed = h.removeLocked(id)
		}
		h.mu.Unlock()
		if removed {
			h.forgetRequests(r.Context(), []droppedRequest{item})
		}
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	now := h.clock()
	life := h.requestLifetime()
	if cr.Status == "pending" && cr.AdmissionProof == "" {
		_, text, err := session.NewProof()
		if err != nil {
			h.mu.Unlock()
			http.Error(w, "store error", http.StatusInternalServerError)
			return
		}
		cr.AdmissionProof = text
	}
	cr.Status = "accepted"
	cr.expiresAt = RequestDeadline(cr.Created, now, true, life)
	stored := *cr
	h.mu.Unlock()

	_ = h.cache.SetRequest(r.Context(), &stored, life)

	h.writeJSON(w, 0, struct {
		Status         string `json:"status"`
		AdmissionProof string `json:"admission_proof,omitempty"`
	}{Status: "accepted", AdmissionProof: stored.AdmissionProof})
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
		item, removed := droppedRequest{}, false
		if ok {
			item, removed = h.removeLocked(id)
		}
		h.mu.Unlock()
		if removed {
			h.forgetRequests(r.Context(), []droppedRequest{item})
		}
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	now := h.clock()
	life := h.requestLifetime()
	cr.Status = "rejected"
	cr.expiresAt = RequestDeadline(cr.Created, now, true, life)
	stored := *cr
	h.mu.Unlock()

	_ = h.cache.SetRequest(r.Context(), &stored, life)

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
		item, removed := droppedRequest{}, false
		if ok {
			item, removed = h.removeLocked(id)
		}
		h.mu.Unlock()
		if removed {
			h.forgetRequests(r.Context(), []droppedRequest{item})
		}
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	copy := *cr
	h.mu.Unlock()

	h.writeJSON(w, 0, &copy)
}

type droppedRequest struct {
	id    string
	toKey string
}

func (h *Handlers) removeLocked(id string) (droppedRequest, bool) {
	cr := h.byID[id]
	delete(h.byID, id)
	h.clearPunch(id)
	if cr == nil {
		return droppedRequest{}, false
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
	} else {
		h.byToKey[cr.ToKey] = ids[:dst]
	}
	return droppedRequest{id: id, toKey: cr.ToKey}, true
}

func (h *Handlers) forgetRequests(ctx context.Context, dropped []droppedRequest) {
	if h.cache == nil || len(dropped) == 0 {
		return
	}
	for _, item := range dropped {
		_ = h.cache.DelRequest(ctx, item.id, item.toKey)
	}
}

// applyStored copies still-readable stored requests into the maps.
// A past deadline is returned so the caller can delete it after releasing h.mu.
// It does not call Redis.
func (h *Handlers) applyStored(records []storedRequest) (inserted, skipped int, drops []droppedRequest) {
	now := h.clock()
	life := h.requestLifetime()
	ready := make([]*ConnectRequest, 0, len(records))
	for i := range records {
		rec := &records[i]
		deadline, keep, skip, drop := classifyStored(rec, now, life)
		if skip {
			skipped++
			continue
		}
		if drop {
			if rec.ID != "" {
				drops = append(drops, droppedRequest{id: rec.ID, toKey: rec.ToKey})
			}
			continue
		}
		if !keep {
			continue
		}
		cp := new(ConnectRequest)
		*cp = rec.ConnectRequest
		cp.expiresAt = deadline
		ready = append(ready, cp)
	}
	h.mu.Lock()
	if h.byID == nil {
		h.byID = make(map[string]*ConnectRequest)
	}
	if h.byToKey == nil {
		h.byToKey = make(map[string][]string)
	}
	for _, cr := range ready {
		if _, exists := h.byID[cr.ID]; exists {
			h.byID[cr.ID] = cr
			continue
		}
		h.byID[cr.ID] = cr
		h.byToKey[cr.ToKey] = append(h.byToKey[cr.ToKey], cr.ID)
		inserted++
	}
	h.mu.Unlock()
	if inserted > 0 && h.logger != nil {
		h.logger.Info("restored requests", "count", inserted)
	}
	if skipped > 0 && h.logger != nil {
		h.logger.Info("pending restore skipped", "count", skipped)
	}
	return inserted, skipped, drops
}

func classifyStored(rec *storedRequest, now time.Time, life time.Duration) (deadline time.Time, keep, skip, drop bool) {
	switch rec.Status {
	case "pending":
		deadline = rec.ExpiresAt
		if deadline.IsZero() {
			deadline = RequestDeadline(rec.Created, time.Time{}, false, life)
		}
	case "accepted", "rejected":
		if rec.ExpiresAt.IsZero() {
			return time.Time{}, false, true, false
		}
		deadline = rec.ExpiresAt
	default:
		return time.Time{}, false, false, false
	}
	if !RequestReadable(now, deadline) {
		return deadline, false, false, true
	}
	return deadline, true, false, false
}

// LoadRequests fills byID and byToKey from the shared pending store.
// A nil cache does not scan. A failed scan leaves both maps empty.
func (h *Handlers) LoadRequests(ctx context.Context) error {
	if h == nil || h.cache == nil || h.cache.client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, skipped, err := h.cache.fetchStored(ctx)
	if err != nil {
		h.mu.Lock()
		h.byID = make(map[string]*ConnectRequest)
		h.byToKey = make(map[string][]string)
		h.mu.Unlock()
		if h.logger != nil {
			h.logger.Info("pending store unavailable, continuing without restore")
		}
		return err
	}
	_, _, drops := h.applyStored(records)
	h.forgetRequests(ctx, drops)
	if skipped > 0 && h.logger != nil {
		h.logger.Info("pending restore skipped", "count", skipped)
	}
	return nil
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
	var dropped []droppedRequest
	for _, id := range drop {
		if item, ok := h.removeLocked(id); ok {
			dropped = append(dropped, item)
		}
	}
	h.mu.Unlock()
	h.forgetRequests(ctx, dropped)

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
