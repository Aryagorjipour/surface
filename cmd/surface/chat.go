package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func connectedPrompt(dialer bool) string {
	if dialer {
		return "connected — type messages (/quit to leave)"
	}
	return ">>> peer connected — type messages (/quit to leave)"
}

type Gate struct {
	mu       sync.Mutex
	live     map[string]*session.Session
	invites  map[string]invite
	readable func(requestID string) bool
	until    func(requestID string) time.Time
}

type invite struct {
	caller string
	target string
	key    session.HandshakeKey
}

func (g *Gate) remember(requestID, callerKey, targetKey string, key session.HandshakeKey) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.invites == nil {
		g.invites = make(map[string]invite)
	}
	g.invites[requestID] = invite{caller: callerKey, target: targetKey, key: key}
}

func (g *Gate) session(requestID string) *session.Session {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.live[requestID]
}

func (g *Gate) idOf(sess *session.Session) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, s := range g.live {
		if s == sess {
			return id
		}
	}
	return ""
}

func (g *Gate) dialAdmitted(ctx context.Context, conn byteConn, requestID, callerKey, targetKey string, key session.HandshakeKey) (*session.Session, error) {
	msg, st, err := session.Offer(requestID, callerKey, targetKey, key)
	if err != nil {
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, msg); err != nil {
		return nil, err
	}
	_, reply, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	return session.FinishOffer(reply, st)
}

func (g *Gate) acceptAdmitted(ctx context.Context, conn byteConn, requestID, callerKey, targetKey string, key session.HandshakeKey, ready func(*session.Session)) error {
	_, msg, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	return g.finishAdmit(ctx, conn, msg, requestID, callerKey, targetKey, key, ready)
}

func (g *Gate) finishAdmit(ctx context.Context, conn byteConn, msg []byte, requestID, callerKey, targetKey string, key session.HandshakeKey, ready func(*session.Session)) error {
	_, span := startAcceptSpan(ctx)
	status := "refused"
	defer endAcceptSpan(&span, &status)
	g.mu.Lock()
	_, spent := g.live[requestID]
	check := g.readable
	g.mu.Unlock()
	if check != nil && !check(requestID) {
		_ = conn.Close(websocket.StatusPolicyViolation, "")
		return fmt.Errorf("refused")
	}
	reply, sess, err := session.AcceptOffer(msg, requestID, callerKey, targetKey, key, spent)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "")
		return err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, reply); err != nil {
		return err
	}
	g.mu.Lock()
	if g.live == nil {
		g.live = make(map[string]*session.Session)
	}
	g.live[requestID] = sess
	g.mu.Unlock()
	status = "accepted"
	endAcceptSpan(&span, &status)
	if ready != nil {
		ready(sess)
	}
	return nil
}

func (g *Gate) take(ctx context.Context, conn byteConn, now, deadline time.Time, ready func(*session.Session)) (*session.Session, error) {
	_, msg, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if len(msg) == 0 {
		return refuseAccept(ctx, conn)
	}
	switch msg[0] {
	case 0x01:
		id, caller, err := session.OfferNames(msg)
		if err != nil {
			return refuseAccept(ctx, conn)
		}
		g.mu.Lock()
		inv, ok := g.invites[id]
		g.mu.Unlock()
		if !ok || inv.caller != caller {
			return refuseAccept(ctx, conn)
		}
		if err := g.finishAdmit(ctx, conn, msg, id, caller, inv.target, inv.key, ready); err != nil {
			return nil, err
		}
		return g.session(id), nil
	case 0x05:
		id, _, _, _, err := resumeID(msg)
		if err != nil {
			recordRefused(ctx)
			_ = conn.Close(websocket.StatusPolicyViolation, "")
			return nil, err
		}
		sess := g.session(id)
		d := deadline
		if g.until != nil {
			if u := g.until(id); !u.IsZero() {
				d = u
			}
		}
		reply, err := session.AcceptResume(sess, msg, now, d)
		if err != nil {
			recordRefused(ctx)
			_ = conn.Close(websocket.StatusPolicyViolation, "")
			return nil, err
		}
		if err := conn.Write(ctx, websocket.MessageBinary, reply); err != nil {
			return nil, err
		}
		for _, frame := range session.Pending(sess) {
			if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				return nil, err
			}
		}
		return sess, nil
	default:
		return refuseAccept(ctx, conn)
	}
}

func refuseAccept(ctx context.Context, conn byteConn) (*session.Session, error) {
	recordRefused(ctx)
	_ = conn.Close(websocket.StatusPolicyViolation, "")
	return nil, fmt.Errorf("refused")
}

func recordRefused(ctx context.Context) {
	_, span := startAcceptSpan(ctx)
	status := "refused"
	endAcceptSpan(&span, &status)
}

func startAcceptSpan(ctx context.Context) (context.Context, trace.Span) {
	return otel.Tracer("github.com/Aryagorjipour/surface/surface").Start(ctx, "websocket accept")
}

func endAcceptSpan(span *trace.Span, status *string) {
	if span == nil || *span == nil {
		return
	}
	current := *span
	*span = nil
	value := "refused"
	if status != nil && *status != "" {
		value = *status
	}
	current.SetAttributes(
		attribute.String("endpoint", "websocket accept"),
		attribute.String("status", value),
	)
	current.End()
}

func resumeID(msg []byte) (string, uint64, uint64, []byte, error) {
	return splitResumeLocal(msg)
}

func pushResume(ctx context.Context, conn byteConn, sess *session.Session, now, deadline time.Time, lines []string) error {
	offer, err := session.ResumeOffer(sess)
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, offer); err != nil {
		return err
	}
	_, reply, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if _, err := session.AcceptResume(sess, reply, now, deadline); err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "")
		return err
	}
	for _, frame := range session.Pending(sess) {
		if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
			return err
		}
	}
	for _, line := range lines {
		frame, err := session.Seal(sess, line, session.Highest(sess))
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
			return err
		}
	}
	return nil
}

func splitResumeLocal(msg []byte) (string, uint64, uint64, []byte, error) {
	if len(msg) < 1+2 {
		return "", 0, 0, nil, fmt.Errorf("refused")
	}
	idLen := int(msg[1])<<8 | int(msg[2])
	if len(msg) < 3+idLen {
		return "", 0, 0, nil, fmt.Errorf("refused")
	}
	return string(msg[3 : 3+idLen]), 0, 0, nil, nil
}
