package connector

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/session"
	"github.com/coder/websocket"
)

const (
	e2eCallerKey  = "e2e-caller"
	e2eCalleeKey  = "e2e-callee"
	e2eCallerAddr = "127.0.0.1:9"
	e2ePayload    = "surface-round-trip"
	e2eBudget     = 8 * time.Second
)

func TestE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eBudget)
	defer cancel()

	store := testStore(t)
	logger := slog.New(slog.DiscardHandler)
	h := newTestHandlers(store, nil, logger)
	mux := http.NewServeMux()
	h.Register(mux)
	connector := httptest.NewServer(mux)
	t.Cleanup(connector.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		failE2E(t, "start", err, e2eCallerKey, e2eCalleeKey, e2eCallerAddr)
	}
	calleeAddr := ln.Addr().String()
	redact := []string{e2eCallerKey, e2eCalleeKey, e2eCallerAddr, calleeAddr}

	payloadCh := make(chan []byte, 1)
	closeCh := make(chan error, 1)
	wsErr := make(chan error, 1)

	var listenKey session.HandshakeKey
	var requestID string
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			sendErr(wsErr, err)
			return
		}
		text, bad := readAdmitted(ctx, c, requestID, e2eCallerKey, e2eCalleeKey, listenKey)
		if bad != nil {
			sendErr(wsErr, bad)
			_ = c.Close(websocket.StatusPolicyViolation, "")
			return
		}
		select {
		case payloadCh <- []byte(text):
		case <-ctx.Done():
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		_, _, readErr := c.Read(ctx)
		closeErr := c.Close(websocket.StatusNormalClosure, "")
		if websocket.CloseStatus(readErr) == websocket.StatusNormalClosure &&
			(closeErr == nil || errors.Is(closeErr, net.ErrClosed) || websocket.CloseStatus(closeErr) == websocket.StatusNormalClosure) {
			sendErr(closeCh, readErr)
			return
		}
		if closeErr != nil && websocket.CloseStatus(readErr) == websocket.StatusNormalClosure {
			sendErr(closeCh, closeErr)
			return
		}
		sendErr(closeCh, readErr)
	})
	wsSrv := &http.Server{Handler: wsMux}
	go func() {
		err := wsSrv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			sendErr(wsErr, err)
		}
	}()
	t.Cleanup(func() {
		shut, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = wsSrv.Shutdown(shut)
	})

	base := connector.URL
	step := func(name string, err error) {
		t.Helper()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			failE2E(t, name, err, redact...)
		}
	}

	step("start", nil)

	callerPriv, callerPub := testIdentity(t)
	calleePriv, calleePub := testIdentity(t)

	var registered struct {
		OK bool `json:"ok"`
	}
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": e2eCalleeKey, "address": calleeAddr, "state": "available", "identity": calleePub,
	}, http.StatusOK, &registered))
	if !registered.OK {
		failE2E(t, "register", errors.New("not registered"), redact...)
	}
	registered.OK = false
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": e2eCallerKey, "address": e2eCallerAddr, "state": "available", "identity": callerPub,
	}, http.StatusOK, &registered))
	if !registered.OK {
		failE2E(t, "register", errors.New("not registered"), redact...)
	}

	var looked struct {
		Address string `json:"address"`
		State   string `json:"state"`
	}
	step("connect", getJSON(ctx, base+"/peers/"+e2eCalleeKey, http.StatusOK, &looked))
	if looked.State != "available" || looked.Address != calleeAddr {
		failE2E(t, "connect", errors.New("address mismatch"), redact...)
	}

	var created struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	step("connect", postJSON(ctx, base+"/connect", map[string]string{
		"from_key": e2eCallerKey, "to_key": e2eCalleeKey, "from_address": e2eCallerAddr,
	}, http.StatusCreated, &created))
	if created.ID == "" || created.Status != "pending" {
		failE2E(t, "connect", errors.New("pending request"), redact...)
	}

	var pending []struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	step("accept", getJSON(ctx, base+"/pending?key="+e2eCalleeKey, http.StatusOK, &pending))
	found := false
	for _, item := range pending {
		if item.ID == created.ID && item.Status == "pending" {
			found = true
			break
		}
	}
	if !found {
		failE2E(t, "accept", errors.New("missing request"), redact...)
	}
	var acceptRaw json.RawMessage
	step("accept", postJSON(ctx, base+"/requests/"+created.ID+"/accept", nil, http.StatusOK, &acceptRaw))
	if bytes.Contains(acceptRaw, []byte("admission_proof")) {
		failE2E(t, "accept", errors.New("status"), redact...)
	}
	var acceptBody struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(acceptRaw, &acceptBody); err != nil || acceptBody.Status != "accepted" {
		failE2E(t, "accept", errors.New("status"), redact...)
	}
	requestID = created.ID
	var acceptedRaw json.RawMessage
	step("accept", getJSON(ctx, base+"/requests/"+created.ID, http.StatusOK, &acceptedRaw))
	if bytes.Contains(acceptedRaw, []byte("admission_proof")) {
		failE2E(t, "accept", errors.New("status"), redact...)
	}
	var accepted struct {
		Status       string `json:"status"`
		FromIdentity string `json:"from_identity"`
		ToIdentity   string `json:"to_identity"`
	}
	if err := json.Unmarshal(acceptedRaw, &accepted); err != nil || accepted.Status != "accepted" || accepted.FromIdentity != callerPub || accepted.ToIdentity != calleePub {
		failE2E(t, "accept", errors.New("status"), redact...)
	}
	dialKey := admitKey(t, callerPriv, accepted.ToIdentity, requestID)
	listenKey = admitKey(t, calleePriv, accepted.FromIdentity, requestID)
	if dialKey != listenKey {
		failE2E(t, "accept", errors.New("handshake"), redact...)
	}

	conn, _, err := websocket.Dial(ctx, "ws://"+looked.Address, nil)
	step("exchange", err)
	step("exchange", writeSealed(ctx, conn, requestID, e2eCallerKey, e2eCalleeKey, dialKey, e2ePayload))

	var got []byte
	select {
	case got = <-payloadCh:
	case err := <-wsErr:
		step("exchange", err)
	case <-ctx.Done():
		step("exchange", ctx.Err())
	}
	if !bytes.Equal(got, []byte(e2ePayload)) {
		failE2E(t, "exchange", errors.New("payload mismatch"), redact...)
	}

	err = conn.Close(websocket.StatusNormalClosure, "")
	if err != nil && !errors.Is(err, net.ErrClosed) && websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		step("close", err)
	}
	select {
	case err := <-closeCh:
		if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
			step("close", err)
		}
	case err := <-wsErr:
		step("close", err)
	case <-ctx.Done():
		step("close", ctx.Err())
	}
}

func TestOpenChatSurvivesExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eBudget)
	defer cancel()

	store := testStore(t)
	logger := slog.New(slog.DiscardHandler)
	h := newTestHandlers(store, nil, logger)
	mux := http.NewServeMux()
	h.Register(mux)
	connector := httptest.NewServer(mux)
	t.Cleanup(connector.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		failE2E(t, "start", err)
	}
	calleeAddr := ln.Addr().String()
	const payload = "surface-e2e-open"

	payloadCh := make(chan []byte, 1)
	wsErr := make(chan error, 1)
	var listenKey session.HandshakeKey
	var requestID string
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			sendErr(wsErr, err)
			return
		}
		text, bad := readAdmitted(ctx, c, requestID, "e2e-caller-open", "e2e-callee-open", listenKey)
		if bad != nil {
			sendErr(wsErr, bad)
			_ = c.Close(websocket.StatusPolicyViolation, "")
			return
		}
		payloadCh <- []byte(text)
		_, _, readErr := c.Read(ctx)
		_ = c.Close(websocket.StatusNormalClosure, "")
		if websocket.CloseStatus(readErr) != websocket.StatusNormalClosure && readErr != nil && !errors.Is(readErr, net.ErrClosed) {
			sendErr(wsErr, readErr)
		}
	})
	wsSrv := &http.Server{Handler: wsMux}
	go func() {
		err := wsSrv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			sendErr(wsErr, err)
		}
	}()
	t.Cleanup(func() {
		shut, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = wsSrv.Shutdown(shut)
	})

	base := connector.URL
	step := func(name string, err error) {
		t.Helper()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			failE2E(t, name, err, calleeAddr)
		}
	}
	step("start", nil)

	var registered struct {
		OK bool `json:"ok"`
	}
	callerPriv, callerPub := testIdentity(t)
	calleePriv, calleePub := testIdentity(t)
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": "e2e-callee-open", "address": calleeAddr, "state": "available", "identity": calleePub,
	}, http.StatusOK, &registered))
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": "e2e-caller-open", "address": e2eCallerAddr, "state": "available", "identity": callerPub,
	}, http.StatusOK, &registered))

	var looked struct {
		Address string `json:"address"`
	}
	step("connect", getJSON(ctx, base+"/peers/e2e-callee-open", http.StatusOK, &looked))
	if looked.Address != calleeAddr {
		failE2E(t, "connect", errors.New("address mismatch"), calleeAddr)
	}
	var created struct {
		ID     string `json:"request_id"`
		Status string `json:"status"`
	}
	step("connect", postJSON(ctx, base+"/connect", map[string]string{
		"from_key": "e2e-caller-open", "to_key": "e2e-callee-open", "from_address": e2eCallerAddr,
	}, http.StatusCreated, &created))
	var acceptRaw json.RawMessage
	step("accept", postJSON(ctx, base+"/requests/"+created.ID+"/accept", nil, http.StatusOK, &acceptRaw))
	if bytes.Contains(acceptRaw, []byte("admission_proof")) {
		failE2E(t, "accept", errors.New("status"))
	}
	var acceptBody struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(acceptRaw, &acceptBody); err != nil || acceptBody.Status != "accepted" {
		failE2E(t, "accept", errors.New("status"))
	}
	requestID = created.ID
	var accepted struct {
		FromIdentity string `json:"from_identity"`
		ToIdentity   string `json:"to_identity"`
	}
	step("accept", getJSON(ctx, base+"/requests/"+created.ID, http.StatusOK, &accepted))
	dialKey := admitKey(t, callerPriv, accepted.ToIdentity, requestID)
	listenKey = admitKey(t, calleePriv, accepted.FromIdentity, requestID)
	if dialKey != listenKey || accepted.FromIdentity != callerPub || accepted.ToIdentity != calleePub {
		failE2E(t, "accept", errors.New("handshake"))
	}

	conn, _, err := websocket.Dial(ctx, "ws://"+looked.Address, nil)
	step("exchange", err)
	h.SetNow(func() time.Time { return time.Now().Add(25 * time.Hour) })
	step("exchange", writeSealed(ctx, conn, requestID, "e2e-caller-open", "e2e-callee-open", dialKey, payload))

	var got []byte
	select {
	case got = <-payloadCh:
	case err := <-wsErr:
		step("exchange", err)
	case <-ctx.Done():
		step("exchange", ctx.Err())
	}
	if !bytes.Equal(got, []byte(payload)) {
		failE2E(t, "exchange", errors.New("payload mismatch"))
	}
	err = conn.Close(websocket.StatusNormalClosure, "")
	if err != nil && !errors.Is(err, net.ErrClosed) && websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		step("close", err)
	}
}

func readAdmitted(ctx context.Context, c *websocket.Conn, requestID, callerKey, targetKey string, key session.HandshakeKey) (string, error) {
	typ, data, err := c.Read(ctx)
	if err != nil {
		return "", err
	}
	if typ != websocket.MessageBinary {
		return "", errors.New("frame type")
	}
	reply, sess, err := session.AcceptOffer(data, requestID, callerKey, targetKey, key, false)
	if err != nil {
		return "", err
	}
	if err := c.Write(ctx, websocket.MessageBinary, reply); err != nil {
		return "", err
	}
	typ, frame, err := c.Read(ctx)
	if err != nil {
		return "", err
	}
	if typ != websocket.MessageBinary {
		return "", errors.New("frame type")
	}
	text, display, err := session.Open(sess, frame)
	if err != nil || !display {
		return "", errors.New("open")
	}
	if bytes.Contains(frame, []byte(text)) {
		return "", errors.New("plaintext visible")
	}
	typ, other, err := c.Read(ctx)
	if err != nil {
		return text, nil
	}
	if typ == websocket.MessageBinary {
		if _, _, openErr := session.Open(sess, other); openErr == nil && bytes.Contains(other, []byte(text)) {
			return "", errors.New("foreign frame displayed")
		}
		if _, _, openErr := session.Open(sess, other); openErr == nil {
			return "", errors.New("foreign frame opened")
		}
	}
	return text, nil
}

func writeSealed(ctx context.Context, conn *websocket.Conn, requestID, callerKey, targetKey string, key session.HandshakeKey, plaintext string) error {
	offer, st, err := session.Offer(requestID, callerKey, targetKey, key)
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
	sess, err := session.FinishOffer(reply, st)
	if err != nil {
		return err
	}
	frame, err := session.Seal(sess, plaintext, 0)
	if err != nil {
		return err
	}
	if bytes.Contains(frame, []byte(plaintext)) {
		return errors.New("plaintext visible")
	}
	if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
		return err
	}
	otherRaw, err := session.NewHandshakeKey()
	if err != nil {
		return err
	}
	otherOffer, otherState, err := session.Offer("other-request", "other-caller", "other-target", otherRaw)
	if err != nil {
		return err
	}
	otherReply, _, err := session.AcceptOffer(otherOffer, "other-request", "other-caller", "other-target", otherRaw, false)
	if err != nil {
		return err
	}
	otherSess, err := session.FinishOffer(otherReply, otherState)
	if err != nil {
		return err
	}
	foreign, err := session.Seal(otherSess, "not-the-line", 0)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, foreign)
}

func postJSON(ctx context.Context, url string, in any, want int, out any) error {
	return doJSON(ctx, http.MethodPost, url, in, want, out)
}

func getJSON(ctx context.Context, url string, want int, out any) error {
	return doJSON(ctx, http.MethodGet, url, nil, want, out)
}

func doJSON(ctx context.Context, method, url string, in any, want int, out any) (err error) {
	var body io.Reader
	if in != nil {
		var raw []byte
		raw, err = json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+testBearer)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != want {
		return errors.New("http status " + http.StatusText(resp.StatusCode))
	}
	if out != nil && len(bytes.TrimSpace(raw)) > 0 {
		if err = json.Unmarshal(raw, out); err != nil {
			return err
		}
	}
	return nil
}

func testIdentity(t *testing.T) (*ecdh.PrivateKey, string) {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv, session.PublicText(priv.PublicKey())
}

func admitKey(t *testing.T, mine *ecdh.PrivateKey, peerText, requestID string) session.HandshakeKey {
	t.Helper()
	pub, err := session.ParsePublic(peerText)
	if err != nil {
		t.Fatal(err)
	}
	key, err := session.DeriveHandshakeKey(mine, pub, requestID)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func failE2E(t *testing.T, step string, err error, redact ...string) {
	t.Helper()
	msg := "e2e " + step
	if err != nil {
		reason := err.Error()
		for _, secret := range redact {
			if secret != "" {
				reason = strings.ReplaceAll(reason, secret, "redacted")
			}
		}
		msg += ": " + reason
	}
	t.Fatal(msg)
}

func sendErr(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}
