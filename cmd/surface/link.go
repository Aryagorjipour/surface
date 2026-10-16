package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const maxCarrierMessage = 2048

// byteConn is one direct-chat carrier. *websocket.Conn already implements it.
type byteConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(status websocket.StatusCode, reason string) error
	CloseNow() error
}

func writeRelayHeader(w io.Writer, requestID string) error {
	_, err := fmt.Fprintf(w, "surface-relay-v1\n%s\n", requestID)
	return err
}

func readRelayHeader(r *bufio.Reader) (string, error) {
	magic, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if strings.TrimRight(magic, "\r\n") != "surface-relay-v1" {
		return "", fmt.Errorf("refused")
	}
	id, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	id = strings.TrimRight(id, "\r\n")
	if id == "" {
		return "", fmt.Errorf("refused")
	}
	return id, nil
}

func writePrefix(w io.Writer, msg []byte) error {
	if len(msg) < 1 || len(msg) > maxCarrierMessage {
		return fmt.Errorf("refused")
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(msg)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

func readPrefix(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n < 1 || n > maxCarrierMessage {
		return nil, fmt.Errorf("refused")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// offerCache remembers the first UDP offer and resends that reply.
// A different offer is refused.
type offerCache struct {
	mu    sync.Mutex
	offer []byte
	reply []byte
}

func (c *offerCache) decide(msg []byte, open func([]byte) ([]byte, error)) (reply []byte, reused bool, refused bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.offer != nil {
		if string(c.offer) == string(msg) {
			return append([]byte(nil), c.reply...), true, false
		}
		return nil, false, true
	}
	out, err := open(msg)
	if err != nil {
		return nil, false, true
	}
	c.offer = append([]byte(nil), msg...)
	c.reply = append([]byte(nil), out...)
	return append([]byte(nil), c.reply...), false, false
}

type udpLink struct {
	conn *net.UDPConn
	peer *net.UDPAddr
}

func (u *udpLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		deadline := time.Now().Add(200 * time.Millisecond)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		if err := u.conn.SetReadDeadline(deadline); err != nil {
			return 0, nil, err
		}
		n, addr, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return 0, nil, err
		}
		if n == 0 || n > maxCarrierMessage {
			continue
		}
		if u.peer == nil && addr != nil {
			u.peer = addr
		}
		out := make([]byte, n)
		copy(out, buf[:n])
		return websocket.MessageBinary, out, nil
	}
}

func (u *udpLink) Write(ctx context.Context, _ websocket.MessageType, p []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p) < 1 || len(p) > maxCarrierMessage || u.peer == nil {
		return fmt.Errorf("refused")
	}
	_, err := u.conn.WriteToUDP(p, u.peer)
	return err
}

func (u *udpLink) Close(websocket.StatusCode, string) error { return u.CloseNow() }

func (u *udpLink) CloseNow() error {
	if u == nil || u.conn == nil {
		return nil
	}
	return u.conn.Close()
}

type relayLink struct {
	conn net.Conn
	r    *bufio.Reader
}

func newRelayLink(conn net.Conn) *relayLink {
	return &relayLink{conn: conn, r: bufio.NewReader(conn)}
}

func (r *relayLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		_ = r.CloseNow()
		return 0, nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = r.CloseNow() })
	defer stop()
	msg, err := readPrefix(r.r)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, ctxErr
		}
		return 0, nil, err
	}
	return websocket.MessageBinary, msg, nil
}

func (r *relayLink) Write(ctx context.Context, _ websocket.MessageType, p []byte) error {
	if err := ctx.Err(); err != nil {
		_ = r.CloseNow()
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = r.CloseNow() })
	defer stop()
	return writePrefix(r.conn, p)
}

func (r *relayLink) Close(websocket.StatusCode, string) error { return r.CloseNow() }

func (r *relayLink) CloseNow() error {
	if r == nil || r.conn == nil {
		return nil
	}
	return r.conn.Close()
}
