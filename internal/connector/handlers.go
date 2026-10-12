package connector

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ConnectRequest struct {
	ID          string    `json:request_id`
	FromKey     string    `json:from_key`
	FromAddress string    `json:from_address`
	ToKey       string    `json:to_key`
	Status      string    `json:status`
	Created     time.Time `json:created`
}

type Handlers struct {
	store   *Store
	logger  *slog.Logger
	mu      sync.RWMutex
	pending map[string][]ConnectRequest
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

type connectRequest struct {
	FromKey     string `json:"from_key"`
	ToKey       string `json:"to_key"`
	FromAddress string `json:"from_address"`
}

type connectResponse struct {
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

func NewHandlers(store *Store, logger *slog.Logger) *Handlers {
	return &Handlers{
		store:   store,
		logger:  logger,
		pending: make(map[string][]ConnectRequest),
	}
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /peers", h.handleUpsertPeer)
	mux.HandleFunc("GET /peers/{key}", h.handleGetPeer)
	mux.HandleFunc("POST /connect", h.handleConnect)
	mux.HandleFunc("GET /pending", h.handlePending)
	mux.HandleFunc("POST /requests/{id}/accept", h.handleAccept)
	mux.HandleFunc("POST /requests/{id}/reject", h.handleReject)
	mux.HandleFunc("GET /requests/{id}", h.handleRequestStatus)
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
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (h *Handlers) handleGetPeer(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	addr, state, _, err := h.store.GetPeer(r.Context(), key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(peerResponse{
		Key:     key,
		Address: addr,
		State:   state,
	})
}

func (h *Handlers) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req connectRequest
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

	id := uuid.NewString()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(connectResponse{
		RequestID: id,
		Status:    "pending",
	})
}

func (h *Handlers) handlePending(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key query param required", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	list := h.pending[key]
	h.mu.RUnlock()

	var out []ConnectRequest
	for _, connectRequest := range list {
		if connectRequest.Status == "pending" {
			out = append(out, connectRequest)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (h *Handlers) findRequest(id string) (*ConnectRequest, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for toKey, list := range h.pending {
		for i := range list {
			if list[i].ID == id {
				return &list[i], toKey
			}
		}
	}
	return nil, ""
}

func (h *Handlers) handleAccept(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cr, toKey := h.findRequest(id)
	if cr == nil {
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}

	h.mu.Lock()
	for i := range h.pending[toKey] {
		if h.pending[toKey][i].ID == id {
			h.pending[toKey][i].Status = "accepted"
			break
		}
	}
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func (h *Handlers) handleReject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cr, toKey := h.findRequest(id)
	if cr == nil {
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}

	h.mu.Lock()
	for i := range h.pending[toKey] {
		if h.pending[toKey][i].ID == id {
			h.pending[toKey][i].Status = "rejected"
			break
		}
	}
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}

func (h *Handlers) handleRequestStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cr, _ := h.findRequest(id)
	if cr == nil {
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cr)
}
