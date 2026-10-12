package connector

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type Handlers struct {
	store  *Store
	logger *slog.Logger
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

func NewHandlers(store *Store) *Handlers {
	return &Handlers{store: store}
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /peers", h.handleUpsertPeer)
	mux.HandleFunc("GET /peers/{key}", h.handleGetPeer)
	mux.HandleFunc("POST /connect", h.handleConnect)
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
