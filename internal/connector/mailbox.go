package connector

import (
	"context"
	"net/http"
	"time"

	"github.com/Aryagorjipour/surface/internal/mailbox"
)

// MountMailbox installs the drop routes when ttl is positive.
// A non-positive ttl drops the mailbox table and mounts nothing.
// The returned function waits until expiry has stopped. It is nil when ttl is not positive.
func (h *Handlers) MountMailbox(ctx context.Context, mux *http.ServeMux, ttl, every time.Duration) (func(), error) {
	if h == nil || h.store == nil || mux == nil {
		return nil, mailbox.ErrInvalid
	}
	s := h.store
	if s.db == nil {
		return nil, mailbox.ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ttl <= 0 {
		if err := mailbox.Remove(ctx, s.db); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err := mailbox.Migrate(ctx, s.db); err != nil {
		return nil, err
	}
	shelf, err := mailbox.NewShelf(s.db, ttl, func() time.Time { return h.clock() })
	if err != nil {
		return nil, err
	}
	h.route(mux, "PUT /mailbox/{id}", "stage", h.stageDrop(shelf))
	h.route(mux, "GET /mailbox/{slot}", "pull", h.pullSlot(shelf))
	h.route(mux, "DELETE /mailbox/{id}", "delete", h.deleteDrop(shelf))
	return shelf.RunExpire(ctx, every), nil
}

func (h *Handlers) stageDrop(shelf *mailbox.Shelf) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, ok := h.readBody(w, r, "stage")
		if !ok {
			return
		}
		id, err := mailbox.ParseDropID(r.PathValue("id"))
		if err != nil {
			h.reject(w, mailbox.Status(err), "stage", mailbox.Class(err))
			return
		}
		slot, blob, err := mailbox.DecodeStage(body)
		if err != nil {
			h.reject(w, mailbox.Status(err), "stage", mailbox.Class(err))
			return
		}
		if err := shelf.Stage(r.Context(), id, slot, blob); err != nil {
			h.reject(w, mailbox.Status(err), "stage", mailbox.Class(err))
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (h *Handlers) pullSlot(shelf *mailbox.Shelf) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slot, err := mailbox.ParseSlot(r.PathValue("slot"))
		if err != nil {
			h.reject(w, mailbox.Status(err), "pull", mailbox.Class(err))
			return
		}
		rows, err := shelf.Pull(r.Context(), slot, h.clock())
		if err != nil {
			h.reject(w, mailbox.Status(err), "pull", mailbox.Class(err))
			return
		}
		out, err := mailbox.EncodePull(rows)
		if err != nil {
			h.reject(w, http.StatusInternalServerError, "pull", "unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)
	}
}

func (h *Handlers) deleteDrop(shelf *mailbox.Shelf) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := mailbox.ParseDropID(r.PathValue("id"))
		if err != nil {
			h.reject(w, mailbox.Status(err), "delete", mailbox.Class(err))
			return
		}
		if err := shelf.Delete(r.Context(), id); err != nil {
			h.reject(w, mailbox.Status(err), "delete", mailbox.Class(err))
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
