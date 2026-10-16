package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
)

func TestRelayFrames(t *testing.T) {
	var header bytes.Buffer
	const id = "11111111-1111-1111-1111-111111111111"
	if err := writeRelayHeader(&header, id); err != nil {
		t.Fatal(err)
	}
	gotID, err := readRelayHeader(bufio.NewReader(bytes.NewReader(header.Bytes())))
	if err != nil || gotID != id {
		t.Fatalf("header %q %v", gotID, err)
	}
	if !strings.HasPrefix(header.String(), "surface-relay-v1\n") {
		t.Fatal(header.String())
	}

	var prefix bytes.Buffer
	if err := writePrefix(&prefix, nil); err == nil {
		t.Fatal("empty frame")
	}
	if err := writePrefix(&prefix, make([]byte, maxCarrierMessage+1)); err == nil {
		t.Fatal("long frame")
	}
	if err := writePrefix(&prefix, []byte{7}); err != nil {
		t.Fatal(err)
	}
	if err := writePrefix(&prefix, make([]byte, maxCarrierMessage)); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrefix(bytes.NewReader([]byte{0, 0, 0x08, 0x01})); err == nil {
		t.Fatal("length 2049")
	}
	if _, err := readPrefix(bytes.NewReader([]byte{0, 0, 0, 0})); err == nil {
		t.Fatal("length 0")
	}

	proofRaw, _, err := session.NewProof()
	if err != nil {
		t.Fatal(err)
	}
	offer, st, err := session.Offer(id, "alice", "bob", proofRaw)
	if err != nil {
		t.Fatal(err)
	}
	reply, listener, err := session.AcceptOffer(offer, id, "alice", "bob", proofRaw, false)
	if err != nil {
		t.Fatal(err)
	}
	dialer, err := session.FinishOffer(reply, st)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := session.Seal(dialer, "surface-round-trip", 0)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(frame, []byte("surface-round-trip")) {
		t.Fatal("sealed frame contains the line")
	}

	addr, copied := startSplicer(t)
	left, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close() })
	right, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = right.Close() })
	if err := writeRelayHeader(left, id); err != nil {
		t.Fatal(err)
	}
	if err := writeRelayHeader(right, id); err != nil {
		t.Fatal(err)
	}
	if err := writePrefix(left, frame); err != nil {
		t.Fatal(err)
	}
	_ = right.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := readPrefix(bufio.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	text, display, err := session.Open(listener, got)
	if err != nil || !display || text != "surface-round-trip" {
		t.Fatalf("open %q %v display %v", text, err, display)
	}
	if bytes.Contains(copied.Bytes(), []byte("surface-round-trip")) {
		t.Fatal("spliced bytes contain surface-round-trip")
	}

	sideA, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sideA.Close() })
	sideB, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sideB.Close() })
	link := &udpLink{conn: sideA, peer: sideB.LocalAddr().(*net.UDPAddr)}
	if _, err := sideB.WriteToUDP(make([]byte, maxCarrierMessage+1), sideA.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	if _, err := sideB.WriteToUDP([]byte("ping"), sideA.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, msg, err := link.Read(ctx)
	if err != nil || string(msg) != "ping" {
		t.Fatalf("udp %q %v", msg, err)
	}
	if err := link.Write(ctx, 0, []byte("pong")); err != nil {
		t.Fatal(err)
	}
	_ = sideB.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	n, _, err := sideB.ReadFromUDP(buf)
	if err != nil || string(buf[:n]) != "pong" {
		t.Fatalf("one datagram %q %v", buf[:n], err)
	}

	var opened int
	var cache offerCache
	replyBytes, reused, refused := cache.decide([]byte("offer-a"), func([]byte) ([]byte, error) {
		opened++
		return []byte("reply-a"), nil
	})
	if reused || refused || string(replyBytes) != "reply-a" || opened != 1 {
		t.Fatalf("first %#v reused %v refused %v opened %d", replyBytes, reused, refused, opened)
	}
	again, reused, refused := cache.decide([]byte("offer-a"), func([]byte) ([]byte, error) {
		t.Fatal("opened the same offer twice")
		return nil, nil
	})
	if !reused || refused || string(again) != "reply-a" {
		t.Fatalf("resend %q reused %v refused %v", again, reused, refused)
	}
	if _, reused, refused = cache.decide([]byte("offer-b"), func([]byte) ([]byte, error) {
		t.Fatal("opened a different offer")
		return nil, nil
	}); !refused || reused {
		t.Fatalf("different offer reused %v refused %v", reused, refused)
	}
}

func TestRelayReadStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server := <-accepted
	t.Cleanup(func() { _ = server.Close() })

	link := newRelayLink(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	errc := make(chan error, 1)
	go func() {
		_, _, err := link.Read(ctx)
		errc <- err
	}()
	time.Sleep(30 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("read succeeded")
		}
		if time.Since(started) > time.Second {
			t.Fatalf("read returned after %s", time.Since(started))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("read still blocked")
	}
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := server.Read(buf); err == nil {
		t.Fatal("relay socket still open")
	}
}

func TestRelayWriteStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server := <-accepted
	t.Cleanup(func() { _ = server.Close() })
	fillTCPWrite(t, client)

	link := newRelayLink(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	errc := make(chan error, 1)
	go func() {
		errc <- link.Write(ctx, 0, []byte{7})
	}()
	time.Sleep(30 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("write succeeded")
		}
		if time.Since(started) > time.Second {
			t.Fatalf("write returned after %s", time.Since(started))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("write still blocked")
	}
	_ = client.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := client.Write([]byte{9}); err == nil || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("relay socket still open: %v", err)
	}
}

func fillTCPWrite(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	junk := bytes.Repeat([]byte{1}, 16*1024)
	blocked := false
	for i := 0; i < 1000 && !blocked; i++ {
		if _, err := conn.Write(junk); err != nil {
			blocked = true
		}
	}
	_ = conn.SetWriteDeadline(time.Time{})
	if !blocked {
		t.Fatal("write buffer did not fill")
	}
}

func startSplicer(t *testing.T) (string, *lockedBuffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	out := &lockedBuffer{}
	go serveSplice(ln, out)
	return ln.Addr().String(), out
}

func serveSplice(ln net.Listener, out *lockedBuffer) {
	type half struct {
		c net.Conn
		r io.Reader
	}
	var mu sync.Mutex
	wait := map[string]half{}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			id, err := readRelayHeader(reader)
			if err != nil {
				_ = conn.Close()
				return
			}
			mu.Lock()
			other, ok := wait[id]
			if !ok {
				wait[id] = half{c: conn, r: reader}
				mu.Unlock()
				return
			}
			delete(wait, id)
			mu.Unlock()
			spliceBoth(reader, conn, other.r, other.c, out)
		}(conn)
	}
}

func spliceBoth(a io.Reader, aConn net.Conn, b io.Reader, bConn net.Conn, out *lockedBuffer) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		copyTrace(a, bConn, out)
	}()
	go func() {
		defer wg.Done()
		copyTrace(b, aConn, out)
	}()
	wg.Wait()
	_ = aConn.Close()
	_ = bConn.Close()
}

func copyTrace(src io.Reader, dst net.Conn, out *lockedBuffer) {
	buf := make([]byte, 1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			_, _ = out.Write(buf[:n])
			_, _ = dst.Write(buf[:n])
		}
		if err != nil {
			return
		}
	}
}

func (l *lockedBuffer) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}
