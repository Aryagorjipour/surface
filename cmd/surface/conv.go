package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Aryagorjipour/surface/internal/hostport"
	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

type carrierState interface {
	carrierState()
}

type carrierLive struct {
	conn byteConn
}

func (carrierLive) carrierState() {}

type carrierResumeGap struct {
	until time.Time
	queue []string
}

func (carrierResumeGap) carrierState() {}

type carrierGone struct{}

func (carrierGone) carrierState() {}

type Conversation struct {
	id     ConvID
	peer   string
	inbox  chan convCmd
	ctx    context.Context
	cancel context.CancelFunc
	dialer bool
	addr   hostport.Addr
	window time.Duration
	reg    *Registry

	stateMu  sync.Mutex
	sess     *session.Session
	carrier  carrierState
	lines    []string
	unread   int
	gen      uint64
	quitting bool
	ended    bool
}

type AttachReq struct {
	ID     ConvID
	Peer   string
	Sess   *session.Session
	Conn   byteConn
	Dialer bool
	Addr   hostport.Addr
}

func (c *Conversation) ID() ConvID   { return c.id }
func (c *Conversation) Peer() string { return c.peer }

func (c *Conversation) SendText(line string) error {
	done := make(chan error, 1)
	select {
	case c.inbox <- cmdSend{line: line, done: done}:
		select {
		case err := <-done:
			return err
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *Conversation) Quit() {
	select {
	case c.inbox <- cmdQuit{}:
	case <-c.ctx.Done():
	}
}

func (c *Conversation) DeliverResume(conn byteConn, sess *session.Session) {
	select {
	case c.inbox <- cmdResume{conn: conn, sess: sess}:
	case <-c.ctx.Done():
		if conn != nil {
			_ = conn.CloseNow()
		}
	}
}

func (c *Conversation) Lines() []string {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *Conversation) inResume() bool {
	resume, _ := c.statusBits()
	return resume
}

func (c *Conversation) statusBits() (resume bool, unread int) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	_, resume = c.carrier.(carrierResumeGap)
	return resume, c.unread
}

func (c *Conversation) resumeDeadline() time.Time {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if gap, ok := c.carrier.(carrierResumeGap); ok {
		return gap.until
	}
	return time.Time{}
}

func (c *Conversation) run() {
	defer c.finish()
	var resumeTimer *time.Timer
	stopTimer := func() {
		if resumeTimer == nil {
			return
		}
		resumeTimer.Stop()
		resumeTimer = nil
	}
	c.startRead()
	for {
		var timerC <-chan time.Time
		if resumeTimer != nil {
			timerC = resumeTimer.C
		}
		select {
		case <-c.ctx.Done():
			return
		case cmd := <-c.inbox:
			switch cmd := cmd.(type) {
			case cmdSend:
				if cmd.done != nil {
					cmd.done <- c.doSend(cmd.line)
				} else {
					_ = c.doSend(cmd.line)
				}
			case cmdQuit:
				c.doQuit()
				return
			case cmdResume:
				stopTimer()
				c.doResume(cmd.conn, cmd.sess)
				c.startRead()
			case cmdBreak:
				until, arm := c.doBreak(cmd.from, cmd.gen)
				if !arm {
					c.stateMu.Lock()
					done := c.ended
					c.stateMu.Unlock()
					if done {
						return
					}
					continue
				}
				stopTimer()
				if c.dialer {
					go c.retryDial()
					continue
				}
				d := time.Until(until)
				if d < 0 {
					d = 0
				}
				resumeTimer = time.NewTimer(d)
			case cmdDead:
				if c.doDead(cmd.gen) {
					return
				}
			case cmdExpire:
				if c.expireResume() {
					return
				}
			}
		case <-timerC:
			resumeTimer = nil
			if c.expireResume() {
				return
			}
		}
	}
}

func (c *Conversation) startRead() {
	c.stateMu.Lock()
	live, ok := c.carrier.(carrierLive)
	gen := c.gen
	c.stateMu.Unlock()
	if !ok || live.conn == nil {
		return
	}
	go c.readLoop(live.conn, gen)
}

func (c *Conversation) readLoop(conn byteConn, gen uint64) {
	for {
		_, data, err := conn.Read(c.ctx)
		if err != nil {
			c.stateMu.Lock()
			stop := c.quitting || c.ended
			c.stateMu.Unlock()
			if stop {
				return
			}
			select {
			case c.inbox <- cmdBreak{from: conn, gen: gen}:
			case <-c.ctx.Done():
			}
			return
		}
		c.stateMu.Lock()
		sess := c.sess
		stop := c.quitting || c.ended
		c.stateMu.Unlock()
		if stop {
			return
		}
		text, display, openErr := session.Open(sess, data)
		if openErr != nil {
			c.tellFinished(conn, sess)
			select {
			case c.inbox <- cmdDead{gen: gen}:
			case <-c.ctx.Done():
			}
			return
		}
		if session.Finished(sess) {
			select {
			case c.inbox <- cmdDead{gen: gen}:
			case <-c.ctx.Done():
			}
			return
		}
		if display {
			c.stateMu.Lock()
			c.lines = append(c.lines, "peer: "+text)
			c.stateMu.Unlock()
			c.emit(EvPeerLine, text)
		}
	}
}

func (c *Conversation) doSend(line string) error {
	c.stateMu.Lock()
	switch s := c.carrier.(type) {
	case carrierLive:
		conn, sess := s.conn, c.sess
		c.stateMu.Unlock()
		if sess == nil || conn == nil {
			return errors.New("send failed")
		}
		frame, err := session.Seal(sess, line, session.Highest(sess))
		if err != nil {
			return err
		}
		if err := conn.Write(c.ctx, websocket.MessageBinary, frame); err != nil {
			return err
		}
		c.stateMu.Lock()
		c.lines = append(c.lines, "you: "+line)
		c.stateMu.Unlock()
		c.emit(EvSelfLine, line)
		return nil
	case carrierResumeGap:
		s.queue = append(s.queue, line)
		c.carrier = s
		c.lines = append(c.lines, "you: "+line)
		c.stateMu.Unlock()
		return nil
	default:
		c.stateMu.Unlock()
		return errors.New("conversation ended")
	}
}

func (c *Conversation) doQuit() {
	c.stateMu.Lock()
	if c.quitting || c.ended {
		c.stateMu.Unlock()
		return
	}
	c.quitting = true
	c.ended = true
	sess := c.sess
	var conn byteConn
	if live, ok := c.carrier.(carrierLive); ok {
		conn = live.conn
	}
	c.carrier = carrierGone{}
	c.stateMu.Unlock()
	if sess != nil && conn != nil {
		if frame, err := session.End(sess); err == nil {
			_ = conn.Write(c.ctx, websocket.MessageBinary, frame)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
	c.emit(EvEnded, "quit")
}

func (c *Conversation) doResume(conn byteConn, sess *session.Session) {
	c.stateMu.Lock()
	var queue []string
	if gap, ok := c.carrier.(carrierResumeGap); ok {
		queue = gap.queue
		gap.queue = nil
		c.carrier = gap
	} else if live, ok := c.carrier.(carrierLive); ok && live.conn != nil && live.conn != conn {
		old := live.conn
		c.stateMu.Unlock()
		_ = old.CloseNow()
		c.stateMu.Lock()
	}
	if sess != nil {
		c.sess = sess
	}
	c.gen++
	c.carrier = carrierLive{conn: conn}
	useSess := c.sess
	c.stateMu.Unlock()
	for _, line := range queue {
		if useSess == nil || conn == nil {
			break
		}
		frame, err := session.Seal(useSess, line, session.Highest(useSess))
		if err != nil {
			break
		}
		if err := conn.Write(c.ctx, websocket.MessageBinary, frame); err != nil {
			break
		}
	}
	c.emit(EvResumed, "")
}

func (c *Conversation) doBreak(from byteConn, gen uint64) (time.Time, bool) {
	c.stateMu.Lock()
	if c.quitting || c.ended || gen != c.gen {
		c.stateMu.Unlock()
		return time.Time{}, false
	}
	if session.Finished(c.sess) {
		c.ended = true
		c.carrier = carrierGone{}
		c.stateMu.Unlock()
		c.emit(EvEnded, "")
		return time.Time{}, false
	}
	if live, ok := c.carrier.(carrierLive); ok && live.conn != nil && live.conn != from {
		c.stateMu.Unlock()
		return time.Time{}, false
	}
	until := time.Now().Add(c.window)
	c.carrier = carrierResumeGap{until: until}
	c.stateMu.Unlock()
	c.emit(EvBroke, "")
	return until, true
}

func (c *Conversation) doDead(gen uint64) bool {
	c.stateMu.Lock()
	if gen != c.gen || c.ended {
		c.stateMu.Unlock()
		return c.ended
	}
	c.ended = true
	c.carrier = carrierGone{}
	c.stateMu.Unlock()
	c.emit(EvEnded, "")
	return true
}

func (c *Conversation) expireResume() bool {
	c.stateMu.Lock()
	if c.quitting || c.ended {
		c.stateMu.Unlock()
		return false
	}
	if _, ok := c.carrier.(carrierLive); ok {
		c.stateMu.Unlock()
		return false
	}
	c.ended = true
	c.carrier = carrierGone{}
	c.stateMu.Unlock()
	c.emit(EvEnded, "")
	return true
}

func (c *Conversation) retryDial() {
	for {
		c.stateMu.Lock()
		until := time.Time{}
		if gap, ok := c.carrier.(carrierResumeGap); ok {
			until = gap.until
		}
		sess := c.sess
		addr := c.addr
		stop := c.quitting || c.ended
		c.stateMu.Unlock()
		if stop || sess == nil || addr.String() == "" {
			return
		}
		if !time.Now().Before(until) {
			select {
			case c.inbox <- cmdExpire{}:
			case <-c.ctx.Done():
			}
			return
		}
		time.Sleep(time.Second)
		c.stateMu.Lock()
		var lines []string
		if gap, ok := c.carrier.(carrierResumeGap); ok {
			until = gap.until
			lines = append([]string(nil), gap.queue...)
		} else {
			c.stateMu.Unlock()
			return
		}
		sess = c.sess
		stop = c.quitting || c.ended
		c.stateMu.Unlock()
		if stop || sess == nil {
			return
		}
		if !time.Now().Before(until) {
			select {
			case c.inbox <- cmdExpire{}:
			case <-c.ctx.Done():
			}
			return
		}
		conn, err := dialWebSocket(c.ctx, addr)
		if err != nil {
			continue
		}
		err = pushResume(c.ctx, conn, sess, time.Now(), until, lines)
		if err != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, "")
			continue
		}
		c.stateMu.Lock()
		if gap, ok := c.carrier.(carrierResumeGap); ok {
			if len(gap.queue) >= len(lines) {
				gap.queue = append([]string(nil), gap.queue[len(lines):]...)
			} else {
				gap.queue = nil
			}
			c.carrier = gap
		}
		c.stateMu.Unlock()
		select {
		case c.inbox <- cmdResume{conn: conn, sess: sess}:
		case <-c.ctx.Done():
			_ = conn.CloseNow()
		}
		return
	}
}

func (c *Conversation) tellFinished(conn byteConn, sess *session.Session) {
	if frame := session.FailureNotice(sess); len(frame) > 0 && conn != nil {
		_ = conn.Write(c.ctx, websocket.MessageBinary, frame)
	}
	if conn != nil {
		_ = conn.CloseNow()
	}
}

func (c *Conversation) emit(kind EvKind, text string) {
	if c.reg == nil {
		return
	}
	if kind == EvPeerLine {
		c.reg.noteUnread(c.id)
	}
	ev := ConvEvent{ID: c.id, Peer: c.peer, Kind: kind, Text: text}
	select {
	case c.reg.events <- ev:
	case <-c.ctx.Done():
	}
}

func (c *Conversation) finish() {
	c.cancel()
	c.stateMu.Lock()
	if live, ok := c.carrier.(carrierLive); ok && live.conn != nil {
		_ = live.conn.CloseNow()
		c.carrier = carrierGone{}
	}
	c.stateMu.Unlock()
	if c.reg != nil {
		c.reg.remove(c.id)
	}
}

type convCmd interface{ convCmd() }

type cmdSend struct {
	line string
	done chan error
}

func (cmdSend) convCmd() {}

type cmdQuit struct{}

func (cmdQuit) convCmd() {}

type cmdResume struct {
	conn byteConn
	sess *session.Session
}

func (cmdResume) convCmd() {}

type cmdBreak struct {
	from byteConn
	gen  uint64
}

func (cmdBreak) convCmd() {}

type cmdDead struct {
	gen uint64
}

func (cmdDead) convCmd() {}

type cmdExpire struct{}

func (cmdExpire) convCmd() {}

type EvKind uint8

const (
	EvPeerLine EvKind = iota
	EvSelfLine
	EvBroke
	EvEnded
	EvResumed
)

type ConvEvent struct {
	ID   ConvID
	Peer string
	Kind EvKind
	Text string
}
