package connector

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

const punchTicketBytes = 16

// punchObs is one accepted request's tickets. It is process-local.
type punchObs struct {
	order  []string
	source map[string]string
}

type punchBook struct {
	mu     sync.Mutex
	byReq  map[string]*punchObs
	closed map[string]struct{}
	udp    map[string]string
}

func (h *Handlers) punches() *punchBook {
	if h.punch == nil {
		h.punch = &punchBook{
			byReq:  map[string]*punchObs{},
			closed: map[string]struct{}{},
			udp:    map[string]string{},
		}
	}
	return h.punch
}

// NotePunch records the UDP source of a 16-byte ticket. Any other payload is ignored.
// The address is the packet source. A later HTTP body cannot replace it.
func (h *Handlers) NotePunch(payload []byte, addr string) {
	if h == nil || len(payload) != punchTicketBytes {
		return
	}
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	key := hex.EncodeToString(payload)
	h.punchMu.Lock()
	defer h.punchMu.Unlock()
	book := h.punches()
	if _, ok := book.udp[key]; !ok {
		book.udp[key] = addr
	}
	for id, note := range book.byReq {
		if _, done := book.closed[id]; done || note == nil {
			continue
		}
		if cur, ok := note.source[key]; ok && cur == "" {
			note.source[key] = book.udp[key]
		}
	}
}

// clearPunch drops the observation and the closed mark. Request expiry calls it.
func (h *Handlers) clearPunch(id string) {
	if h == nil || id == "" {
		return
	}
	h.punchMu.Lock()
	defer h.punchMu.Unlock()
	h.dropPunchLocked(id, true)
}

// finishPunch removes tickets and remembers that the request is closed until expiry.
func (h *Handlers) finishPunch(id string) {
	if h == nil || id == "" {
		return
	}
	h.punchMu.Lock()
	defer h.punchMu.Unlock()
	h.dropPunchLocked(id, false)
	book := h.punches()
	book.closed[id] = struct{}{}
}

func (h *Handlers) dropPunchLocked(id string, forgetClosed bool) {
	book := h.punches()
	if note := book.byReq[id]; note != nil {
		for _, ticket := range note.order {
			delete(book.udp, ticket)
		}
	}
	delete(book.byReq, id)
	if forgetClosed {
		delete(book.closed, id)
	}
}

func (h *Handlers) handlePunch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if fault, ok := ValidateID(h.lim().cfg, id); !ok {
		h.reject(w, fault.Status, "punch", fault.Phrase)
		return
	}
	h.mu.Lock()
	cr, ok := h.byID[id]
	readable := ok && cr != nil && RequestReadable(h.clock(), cr.expiresAt)
	status := ""
	if readable {
		status = cr.Status
	}
	h.mu.Unlock()
	if !readable {
		h.clearPunch(id)
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}
	if status != "accepted" {
		h.reject(w, http.StatusConflict, "punch", "request not accepted")
		return
	}
	body, ok := h.readBody(w, r, "punch")
	if !ok {
		return
	}
	var in struct {
		Done   bool   `json:"done"`
		Ticket string `json:"ticket"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		h.reject(w, http.StatusBadRequest, "punch", "invalid ticket")
		return
	}
	if in.Done {
		h.finishPunch(id)
		h.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if !validPunchTicket(in.Ticket) {
		h.reject(w, http.StatusBadRequest, "punch", "invalid ticket")
		return
	}
	peer, closed, third := h.observeTicket(id, in.Ticket)
	if third {
		h.reject(w, http.StatusConflict, "punch", "traversal closed")
		return
	}
	if closed {
		h.writeJSON(w, http.StatusOK, map[string]string{"peer": ""})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"peer": peer})
}

func validPunchTicket(ticket string) bool {
	if len(ticket) != punchTicketBytes*2 {
		return false
	}
	for _, c := range ticket {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// observeTicket attaches a ticket or reports the other side's UDP source.
// A closed request stores nothing. A third distinct ticket is refused and not stored.
func (h *Handlers) observeTicket(id, ticket string) (peer string, closed bool, third bool) {
	h.punchMu.Lock()
	defer h.punchMu.Unlock()
	book := h.punches()
	if _, ok := book.closed[id]; ok {
		return "", true, false
	}
	note := book.byReq[id]
	if note == nil {
		note = &punchObs{source: map[string]string{}}
		book.byReq[id] = note
	}
	if _, ok := note.source[ticket]; ok {
		return otherPunchPeer(note, ticket), false, false
	}
	if len(note.order) >= 2 {
		return "", false, true
	}
	note.order = append(note.order, ticket)
	note.source[ticket] = book.udp[ticket]
	return otherPunchPeer(note, ticket), false, false
}

func otherPunchPeer(note *punchObs, ticket string) string {
	if note == nil {
		return ""
	}
	for _, other := range note.order {
		if other != ticket {
			return note.source[other]
		}
	}
	return ""
}
