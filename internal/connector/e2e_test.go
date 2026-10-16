package connector

import (
	"bytes"
	"context"
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

	"github.com/coder/websocket"
)

const (
	e2eCallerKey  = "e2e-caller"
	e2eCalleeKey  = "e2e-callee"
	e2eCallerAddr = "127.0.0.1:9"
	e2ePayload    = "surface-e2e"
	e2eBudget     = 8 * time.Second
)

func TestE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eBudget)
	defer cancel()

	store := testStore(t)
	logger := slog.New(slog.DiscardHandler)
	h := NewHandlers(store, nil, logger)
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

	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			sendErr(wsErr, err)
			return
		}
		typ, data, err := c.Read(ctx)
		if err != nil {
			sendErr(wsErr, err)
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		if typ != websocket.MessageText {
			sendErr(wsErr, errors.New("frame type"))
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		buf := append([]byte(nil), data...)
		select {
		case payloadCh <- buf:
		case <-ctx.Done():
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		// Read the payload before either side closes. The library discards
		// data frames that arrive during the close handshake.
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

	var registered struct {
		OK bool `json:"ok"`
	}
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": e2eCalleeKey, "address": calleeAddr, "state": "available",
	}, http.StatusOK, &registered))
	if !registered.OK {
		failE2E(t, "register", errors.New("not registered"), redact...)
	}
	registered.OK = false
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": e2eCallerKey, "address": e2eCallerAddr, "state": "available",
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
	var acceptBody struct {
		Status string `json:"status"`
	}
	step("accept", postJSON(ctx, base+"/requests/"+created.ID+"/accept", nil, http.StatusOK, &acceptBody))
	if acceptBody.Status != "accepted" {
		failE2E(t, "accept", errors.New("status"), redact...)
	}
	var accepted struct {
		Status string `json:"status"`
	}
	step("accept", getJSON(ctx, base+"/requests/"+created.ID, http.StatusOK, &accepted))
	if accepted.Status != "accepted" {
		failE2E(t, "accept", errors.New("status"), redact...)
	}

	conn, _, err := websocket.Dial(ctx, "ws://"+looked.Address, nil)
	step("exchange", err)
	step("exchange", conn.Write(ctx, websocket.MessageText, []byte(e2ePayload)))

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
	h := NewHandlers(store, nil, logger)
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
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			sendErr(wsErr, err)
			return
		}
		typ, data, err := c.Read(ctx)
		if err != nil {
			sendErr(wsErr, err)
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		if typ != websocket.MessageText {
			sendErr(wsErr, errors.New("frame type"))
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		payloadCh <- append([]byte(nil), data...)
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
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": "e2e-callee-open", "address": calleeAddr, "state": "available",
	}, http.StatusOK, &registered))
	step("register", postJSON(ctx, base+"/peers", map[string]string{
		"key": "e2e-caller-open", "address": e2eCallerAddr, "state": "available",
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
	var acceptBody struct {
		Status string `json:"status"`
	}
	step("accept", postJSON(ctx, base+"/requests/"+created.ID+"/accept", nil, http.StatusOK, &acceptBody))
	if acceptBody.Status != "accepted" {
		failE2E(t, "accept", errors.New("status"))
	}

	conn, _, err := websocket.Dial(ctx, "ws://"+looked.Address, nil)
	step("exchange", err)
	h.SetNow(func() time.Time { return time.Now().Add(25 * time.Hour) })
	step("exchange", conn.Write(ctx, websocket.MessageText, []byte(payload)))

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
