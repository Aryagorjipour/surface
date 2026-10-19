package main

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

// ConvID is a Connector request_id. One actor per id for the life of that chat.
type ConvID string

type convFocus struct {
	id  ConvID
	set bool
}

var (
	ErrAtCapacity      = errors.New("max concurrent chats reached")
	ErrAlreadyAttached = errors.New("conversation already attached")
	ErrUnknownConv     = errors.New("unknown conversation")
)

type Registry struct {
	mu     sync.Mutex
	byID   map[ConvID]*Conversation
	focus  convFocus
	max    int
	events chan ConvEvent
	parent context.Context
	window time.Duration
}

func NewRegistry(parent context.Context, maxChats int, resumeWindow time.Duration) *Registry {
	if parent == nil {
		parent = context.Background()
	}
	if maxChats < 1 {
		maxChats = 4
	}
	if resumeWindow <= 0 {
		resumeWindow = 60 * time.Second
	}
	return &Registry{
		byID:   make(map[ConvID]*Conversation),
		max:    maxChats,
		events: make(chan ConvEvent, 256),
		parent: parent,
		window: resumeWindow,
	}
}

func (r *Registry) Attach(req AttachReq) (*Conversation, error) {
	if req.ID == "" || req.Sess == nil || req.Conn == nil {
		return nil, errors.New("invalid attach")
	}
	r.mu.Lock()
	if _, ok := r.byID[req.ID]; ok {
		r.mu.Unlock()
		return nil, ErrAlreadyAttached
	}
	if len(r.byID) >= r.max {
		r.mu.Unlock()
		return nil, ErrAtCapacity
	}
	ctx, cancel := context.WithCancel(r.parent)
	c := &Conversation{
		id:      req.ID,
		peer:    req.Peer,
		inbox:   make(chan convCmd, 32),
		ctx:     ctx,
		cancel:  cancel,
		sess:    req.Sess,
		carrier: carrierLive{conn: req.Conn},
		dialer:  req.Dialer,
		addr:    req.Addr,
		window:  r.window,
		gen:     1,
		reg:     r,
	}
	r.byID[req.ID] = c
	r.focus = convFocus{id: req.ID, set: true}
	r.mu.Unlock()
	go c.run()
	return c, nil
}

// Focus changes only the pointer. It never closes a carrier or emits EvEnded.
func (r *Registry) Focus(id ConvID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byID[id]
	if !ok {
		return ErrUnknownConv
	}
	r.focus = convFocus{id: id, set: true}
	c.stateMu.Lock()
	c.unread = 0
	c.stateMu.Unlock()
	return nil
}

func (r *Registry) FocusRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ErrUnknownConv
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.byID[ConvID(ref)]; ok {
		r.focus = convFocus{id: c.id, set: true}
		c.stateMu.Lock()
		c.unread = 0
		c.stateMu.Unlock()
		return nil
	}
	var matches []*Conversation
	for _, c := range r.byID {
		if c.peer == ref || strings.HasPrefix(c.peer, ref) || strings.HasPrefix(string(c.id), ref) {
			matches = append(matches, c)
		}
	}
	if len(matches) != 1 {
		return ErrUnknownConv
	}
	r.focus = convFocus{id: matches[0].id, set: true}
	matches[0].stateMu.Lock()
	matches[0].unread = 0
	matches[0].stateMu.Unlock()
	return nil
}

func (r *Registry) Focused() (*Conversation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.focus.set {
		return nil, false
	}
	c, ok := r.byID[r.focus.id]
	return c, ok && c != nil
}

func (r *Registry) AtCapacity() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID) >= r.max
}

func (r *Registry) Has(id ConvID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.byID[id]
	return ok
}

func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}

func (r *Registry) IDForPeer(peer string) ConvID {
	r.mu.Lock()
	defer r.mu.Unlock()
	var found ConvID
	n := 0
	for id, c := range r.byID {
		if c.peer == peer {
			found = id
			n++
		}
	}
	if n == 1 {
		return found
	}
	return ""
}

// ExpectingResume reports whether any actor is in a resume gap.
// Callers must not use this to peek frames; Gate.take still runs first.
func (r *Registry) ExpectingResume() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.byID {
		if c.inResume() {
			return true
		}
	}
	return false
}

func (r *Registry) ResumeUntil(id string) time.Time {
	r.mu.Lock()
	c := r.byID[ConvID(id)]
	r.mu.Unlock()
	if c == nil {
		return time.Time{}
	}
	return c.resumeDeadline()
}

func (r *Registry) DeliverResume(id ConvID, conn byteConn, sess *session.Session) error {
	r.mu.Lock()
	c := r.byID[id]
	r.mu.Unlock()
	if c == nil {
		if conn != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, "")
		}
		return ErrUnknownConv
	}
	c.DeliverResume(conn, sess)
	return nil
}

func (r *Registry) StatusLines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.byID))
	for id, c := range r.byID {
		mark := "  "
		if r.focus.set && r.focus.id == id {
			mark = " *"
		}
		resume, unread := c.statusBits()
		state := "live"
		if resume {
			state = "resume"
		}
		line := mark + " " + string(id) + " " + c.peer + " " + state
		if unread > 0 {
			line += " " + strconv.Itoa(unread)
		}
		out = append(out, line)
	}
	sort.Strings(out)
	return out
}

func (r *Registry) Events() <-chan ConvEvent {
	return r.events
}

func (r *Registry) noteUnread(id ConvID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.focus.set && r.focus.id == id {
		return
	}
	c := r.byID[id]
	if c == nil {
		return
	}
	c.stateMu.Lock()
	c.unread++
	c.stateMu.Unlock()
}

func (r *Registry) remove(id ConvID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byID, id)
	if !r.focus.set || r.focus.id != id {
		return
	}
	r.focus = convFocus{}
	for other := range r.byID {
		r.focus = convFocus{id: other, set: true}
		return
	}
}
