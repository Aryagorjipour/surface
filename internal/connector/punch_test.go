package connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestPunchObservation locks the process-local punch map.
// Tickets are at most two. A third distinct ticket is 409 traversal closed.
// The source is host:port from the UDP packet, not the HTTP body.
// {"done":true} returns 200 {"ok":true} and removes the entry.
// Request expiry removes the entry. A missing or expired request is 404 request not found.
// A readable request that is not accepted is 409 request not accepted.
// A ticket that is not 32 lowercase hex characters is 400 invalid ticket.
// Before the other source is known the body is {"peer":""}.
// When it is known the body is {"peer":"<host:port>"}.
// After done the body is {"peer":""} and nothing new is stored.
func TestPunchObservation(t *testing.T) {
	spans := installSpanRecorder(t)
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store := testStore(t)
	h := newTestHandlers(store, nil, logger)
	const token = "canary-token"
	h.SetBearer(token)
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		key     = "canary-peer-key"
		regAddr = "203.0.113.10:9001"
		canary  = "203.0.113.50:1"
		srcA    = "198.51.100.50:5050"
		srcB    = "198.51.100.51:5151"
		srcC    = "198.51.100.52:5252"
	)
	if callMuxToken(mux, http.MethodPost, "/peers", peerJSON("alice", "198.51.100.10:1010", "available"), "192.0.2.41:41", token).Code != http.StatusOK {
		t.Fatal("register alice")
	}
	if callMuxToken(mux, http.MethodPost, "/peers", peerJSON(key, regAddr, "available"), "192.0.2.42:42", token).Code != http.StatusOK {
		t.Fatal("register target")
	}
	created := callMuxToken(mux, http.MethodPost, "/connect", connectJSON("alice", key, "198.51.100.10:1010"), "192.0.2.43:43", token)
	if created.Code != http.StatusCreated {
		t.Fatalf("connect %d %s", created.Code, created.Body.String())
	}
	var cr struct {
		ID string `json:"request_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &cr); err != nil || cr.ID == "" {
		t.Fatal(created.Body.String())
	}

	missing := callMuxToken(mux, http.MethodPost, "/requests/missing-request/punch", punchBody(ticketHex(t), canary), "192.0.2.44:44", token)
	requirePhrase(t, missing, http.StatusNotFound, "request not found")
	early := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(ticketHex(t), canary), "192.0.2.45:45", token)
	requirePhrase(t, early, http.StatusConflict, "request not accepted")

	accepted := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/accept", "", "192.0.2.46:46", token)
	proof := requireAccepted(t, accepted)

	rawA, hexA := mustPunchTicket(t)
	rawB, hexB := mustPunchTicket(t)
	rawC, hexC := mustPunchTicket(t)

	before := len(spans.Ended())
	first := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexA, canary), "192.0.2.47:47", token)
	if first.Code != http.StatusOK || strings.TrimSpace(first.Body.String()) != `{"peer":""}` {
		t.Fatalf("before the other source is known: %d %s", first.Code, first.Body.String())
	}
	if len(spans.Ended()) != before+1 {
		t.Fatalf("punch spans %d", len(spans.Ended())-before)
	}
	assertActionSpan(t, spans.Ended()[len(spans.Ended())-1], "POST /requests/{id}/punch", http.StatusOK)

	h.NotePunch(rawA, srcA)
	h.NotePunch([]byte("short"), srcC)
	h.NotePunch(make([]byte, 2049), srcC)
	still := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexA, canary), "192.0.2.48:48", token)
	if strings.TrimSpace(still.Body.String()) != `{"peer":""}` {
		t.Fatalf("duplicate before pair: %s", still.Body.String())
	}

	h.NotePunch(rawB, srcB)
	gotB := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexB, canary), "192.0.2.49:49", token)
	if strings.TrimSpace(gotB.Body.String()) != `{"peer":"`+srcA+`"}` {
		t.Fatalf("peer from udp, not json: %s", gotB.Body.String())
	}
	gotA := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexA, canary), "192.0.2.50:50", token)
	if strings.TrimSpace(gotA.Body.String()) != `{"peer":"`+srcB+`"}` {
		t.Fatalf("resend returns the original other source: %s", gotA.Body.String())
	}

	h.NotePunch(rawC, srcC)
	third := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexC, canary), "192.0.2.51:51", token)
	requirePhrase(t, third, http.StatusConflict, "traversal closed")
	kept := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexA, canary), "192.0.2.52:52", token)
	if strings.TrimSpace(kept.Body.String()) != `{"peer":"`+srcB+`"}` {
		t.Fatalf("third ticket was stored: %s", kept.Body.String())
	}

	for _, bad := range []string{
		"ABCDEF0123456789ABCDEF0123456789",
		"abcd",
		strings.Repeat("ab", 32),
		"",
	} {
		badRec := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(bad, canary), "192.0.2.53:53", token)
		requirePhrase(t, badRec, http.StatusBadRequest, "invalid ticket")
	}

	unauth := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(hexA, canary), "192.0.2.54:54", "")
	requirePhrase(t, unauth, http.StatusUnauthorized, "unauthorized")

	done := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", `{"done":true}`, "192.0.2.55:55", token)
	if done.Code != http.StatusOK || strings.TrimSpace(done.Body.String()) != `{"ok":true}` {
		t.Fatalf("done %d %s", done.Code, done.Body.String())
	}
	h.NotePunch(rawA, srcA)
	rawD, hexD := mustPunchTicket(t)
	h.NotePunch(rawD, "198.51.100.53:5353")
	for _, ticket := range []string{hexA, hexD} {
		after := callMuxToken(mux, http.MethodPost, "/requests/"+cr.ID+"/punch", punchBody(ticket, canary), "192.0.2.56:56", token)
		if strings.TrimSpace(after.Body.String()) != `{"peer":""}` {
			t.Fatalf("after done stored %s", after.Body.String())
		}
	}

	lookup := callMuxToken(mux, http.MethodGet, "/peers/"+key, "", "192.0.2.60:60", token)
	pending := callMuxToken(mux, http.MethodGet, "/pending?key="+key, "", "192.0.2.61:61", token)
	status := callMuxToken(mux, http.MethodGet, "/requests/"+cr.ID, "", "192.0.2.62:62", token)
	for name, body := range map[string]string{
		"lookup": lookup.Body.String(), "pending": pending.Body.String(), "status": status.Body.String(),
	} {
		for _, secret := range []string{hexA, hexB, hexC, srcA, srcB, srcC, "198.51.100.53:5353"} {
			if strings.Contains(body, secret) {
				t.Fatalf("%s contains %s", name, secret)
			}
		}
	}
	var statusBody map[string]any
	if err := json.Unmarshal(status.Body.Bytes(), &statusBody); err != nil {
		t.Fatal(err)
	}
	if statusBody["status"] != "accepted" || statusBody["admission_proof"] != proof {
		t.Fatalf("status changed %#v", statusBody)
	}
	for _, banned := range []string{"ticket", "punch", "peer"} {
		if _, ok := statusBody[banned]; ok {
			t.Fatalf("nil cache document gained %s", banned)
		}
	}

	for _, sp := range spans.Ended() {
		if sp.Name() != "POST /requests/{id}/punch" {
			continue
		}
		text := sp.Name()
		for _, attr := range sp.Attributes() {
			text += " " + string(attr.Key) + "=" + attr.Value.Emit()
		}
		for _, secret := range []string{hexA, hexB, hexC, srcA, srcB, srcC, canary, token} {
			if strings.Contains(text, secret) {
				t.Fatalf("span included %s", secret)
			}
		}
	}
	logText := logs.String()
	for _, secret := range []string{hexA, hexB, hexC, srcA, srcB, srcC, canary} {
		if strings.Contains(logText, secret) {
			t.Fatalf("log included %s", secret)
		}
	}

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	clock := now
	h.SetNow(func() time.Time { return clock })
	again := callMuxToken(mux, http.MethodPost, "/connect", connectJSON("alice", key, "198.51.100.10:1010"), "192.0.2.63:63", token)
	var second struct {
		ID string `json:"request_id"`
	}
	if err := json.Unmarshal(again.Body.Bytes(), &second); err != nil || second.ID == "" {
		t.Fatal(again.Body.String())
	}
	if callMuxToken(mux, http.MethodPost, "/requests/"+second.ID+"/accept", "", "192.0.2.64:64", token).Code != http.StatusOK {
		t.Fatal("second accept")
	}
	rawE, hexE := mustPunchTicket(t)
	h.NotePunch(rawE, srcA)
	if strings.TrimSpace(callMuxToken(mux, http.MethodPost, "/requests/"+second.ID+"/punch", punchBody(hexE, canary), "192.0.2.65:65", token).Body.String()) != `{"peer":""}` {
		t.Fatal("fresh ticket")
	}
	clock = now.Add(11 * time.Minute)
	h.pruneOnce(context.Background())
	expired := callMuxToken(mux, http.MethodPost, "/requests/"+second.ID+"/punch", punchBody(hexE, canary), "192.0.2.66:66", token)
	requirePhrase(t, expired, http.StatusNotFound, "request not found")
}

func punchBody(ticket, canary string) string {
	b, err := json.Marshal(map[string]string{"ticket": ticket, "address": canary, "peer": canary})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func mustPunchTicket(t *testing.T) ([]byte, string) {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return raw, hex.EncodeToString(raw)
}

func ticketHex(t *testing.T) string {
	t.Helper()
	_, text := mustPunchTicket(t)
	return text
}
