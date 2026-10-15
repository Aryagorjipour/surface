package connector

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"log/slog"

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
}

type Handlers struct {
	store   *Store
	logger  *slog.Logger
	mu      sync.RWMutex
	byID    map[string]*ConnectRequest
	byToKey map[string][]string
	cache   *RedisCache
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

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.Handle("POST /peers", otelhttp.NewHandler(http.HandlerFunc(h.handleUpsertPeer), "POST /peers"))
	mux.Handle("GET /peers/{key}", otelhttp.NewHandler(http.HandlerFunc(h.handleGetPeer), "GET /peers/{key}"))
	mux.Handle("POST /connect", otelhttp.NewHandler(http.HandlerFunc(h.handleConnect), "POST /connect"))
	mux.Handle("GET /pending", otelhttp.NewHandler(http.HandlerFunc(h.handlePending), "GET /pending"))
	mux.Handle("POST /requests/{id}/accept", otelhttp.NewHandler(http.HandlerFunc(h.handleAccept), "POST /requests/{id}/accept"))
	mux.Handle("POST /requests/{id}/reject", otelhttp.NewHandler(http.HandlerFunc(h.handleReject), "POST /requests/{id}/reject"))
	mux.Handle("GET /requests/{id}", otelhttp.NewHandler(http.HandlerFunc(h.handleRequestStatus), "GET /requests/{id}"))
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
	var req peerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Key == "" || req.Address == "" || req.State == "" {
		http.Error(w, "key, address, state required", http.StatusBadRequest)
		return
	}
	if req.State != "available" && req.State != "busy" && req.State != "offline" {
		http.Error(w, "invalid state", http.StatusBadRequest)
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
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	if h.cache != nil {
		if p, err := h.cache.GetPeer(r.Context(), key); err == nil {
			h.writeJSON(w, 0, p)
			return
		}
	}

	addr, state, _, err := h.store.GetPeer(r.Context(), key)
	if err != nil {
		if err.Error() == "peer not found" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		h.logger.Error("get peer failed", "err", err)
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	_ = h.cache.SetPeer(r.Context(), key, peerResponse{Key: key, Address: addr, State: state}, 30*time.Second)

	h.writeJSON(w, 0, peerResponse{Key: key, Address: addr, State: state})
}

func (h *Handlers) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req ConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.FromKey == "" || req.ToKey == "" || req.FromAddress == "" {
		http.Error(w, "from_key, to_key, from_address required", http.StatusBadRequest)
		return
	}

	_, state, _, err := h.store.GetPeer(r.Context(), req.ToKey)
	if err != nil {
		http.Error(w, "target not found", http.StatusNotFound)
		return
	}
	if state != "available" {
		http.Error(w, "target not connectable", http.StatusConflict)
		return
	}

	cr := &ConnectRequest{
		ID:          uuid.NewString(),
		FromKey:     req.FromKey,
		FromAddress: req.FromAddress,
		ToKey:       req.ToKey,
		Status:      "pending",
		Created:     time.Now(),
	}

	_ = h.cache.SetRequest(r.Context(), cr, 10*time.Minute)

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
		http.Error(w, "key query param required", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	ids := h.byToKey[key]
	var out []ConnectRequest
	for _, id := range ids {
		if cr, ok := h.byID[id]; ok && cr.Status == "pending" {
			out = append(out, *cr)
		}
	}
	h.mu.RUnlock()

	h.writeJSON(w, 0, out)
}

func (h *Handlers) handleAccept(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.mu.Lock()
	cr, ok := h.byID[id]
	if !ok {
		h.mu.Unlock()
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	cr.Status = "accepted"
	h.mu.Unlock()

	_ = h.cache.UpdateRequestStatus(r.Context(), id, "accepted")

	h.writeJSON(w, 0, map[string]string{"status": "accepted"})
}

func (h *Handlers) handleReject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.mu.Lock()
	cr, ok := h.byID[id]
	if !ok {
		h.mu.Unlock()
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	cr.Status = "rejected"
	h.mu.Unlock()

	_ = h.cache.UpdateRequestStatus(r.Context(), id, "rejected")

	h.logger.Info("request rejected", "id", id)
	h.writeJSON(w, 0, map[string]string{"status": "rejected"})
}

func (h *Handlers) handleRequestStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.mu.RLock()
	cr, ok := h.byID[id]
	h.mu.RUnlock()
	if !ok {
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}

	h.writeJSON(w, 0, cr)
}
