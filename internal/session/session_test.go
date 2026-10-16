package session

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"
)

func pair(t *testing.T) (dialer, listener *Session, proof []byte) {
	t.Helper()
	raw, text, err := NewProof()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != ProofBytes || base64.StdEncoding.EncodeToString(raw) != text {
		t.Fatalf("proof %d %q", len(raw), text)
	}
	offer, st, err := Offer("req-1", "caller", "target", raw)
	if err != nil {
		t.Fatal(err)
	}
	reply, listener, err := AcceptOffer(offer, "req-1", "caller", "target", raw, false)
	if err != nil {
		t.Fatal(err)
	}
	dialer, err = FinishOffer(reply, st)
	if err != nil {
		t.Fatal(err)
	}
	return dialer, listener, raw
}

func TestHandshakeRejectsBadProof(t *testing.T) {
	raw, text, err := NewProof()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != ProofBytes || base64.StdEncoding.EncodeToString(raw) != text {
		t.Fatalf("proof %d", len(raw))
	}
	offer, st, err := Offer("req-1", "caller", "target", raw)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(offer, raw) || bytes.Contains(offer, []byte(text)) {
		t.Fatal("offer carries the proof")
	}
	reply, sess, err := AcceptOffer(offer, "req-1", "caller", "target", raw, false)
	if err != nil || sess == nil {
		t.Fatal(err)
	}
	if _, err = FinishOffer(reply, st); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		proof []byte
		id    string
		key   string
		spent bool
	}{
		{name: "first", proof: flip(raw, 0), id: "req-1", key: "caller"},
		{name: "last", proof: flip(raw, len(raw)-1), id: "req-1", key: "caller"},
		{name: "missing", proof: nil, id: "req-1", key: "caller"},
		{name: "id", proof: raw, id: "other", key: "caller"},
		{name: "caller", proof: raw, id: "req-1", key: "other"},
		{name: "spent", proof: raw, id: "req-1", key: "caller", spent: true},
	}
	for _, tc := range cases {
		sum := proofMAC(tc.proof, macInput(typeOffer, tc.id, tc.key, offerEph(t, offer), nil))
		if len(sum) != sha256.Size {
			t.Fatalf("%s mac %d", tc.name, len(sum))
		}
		manual := hmac.New(sha256.New, tc.proof)
		_, _ = manual.Write(macInput(typeOffer, tc.id, tc.key, offerEph(t, offer), nil))
		if !hmac.Equal(sum, manual.Sum(nil)) {
			t.Fatalf("%s mac ignored part of the proof", tc.name)
		}
		gotReply, gotSess, err := AcceptOffer(offer, tc.id, tc.key, "target", tc.proof, tc.spent)
		if err == nil || gotSess != nil || gotReply != nil {
			t.Fatalf("%s accepted", tc.name)
		}
	}
}

func TestSealHidesKnownMessage(t *testing.T) {
	dialer, listener, _ := pair(t)
	const line = "surface-round-trip"
	frame, err := Seal(dialer, line, 0)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(frame, []byte(line)) {
		t.Fatal("ciphertext contains the line")
	}
	got, display, err := Open(listener, frame)
	if err != nil || !display || got != line {
		t.Fatalf("open %q %v %v", got, display, err)
	}
	got, display, err = Open(listener, frame)
	if err != nil || display {
		t.Fatalf("replay %q %v %v", got, display, err)
	}
	if Finished(listener) {
		t.Fatal("replay finished the session")
	}

	otherD, _, _ := pair(t)
	foreign, err := Seal(otherD, line, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = Open(listener, foreign); err == nil {
		t.Fatal("other session opened")
	}

	gapD, gapL, _ := pair(t)
	first, err := Seal(gapD, "one", 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Seal(gapD, "two", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = Open(gapL, second); err == nil {
		t.Fatal("sequence ahead of next opened")
	}
	if _, _, err = Open(gapL, first); err == nil {
		t.Fatal("session continued after a gap")
	}
}

func TestResumeAndQuit(t *testing.T) {
	dialer, listener, _ := pair(t)
	first, err := Seal(dialer, "one", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, display, err := Open(listener, first); err != nil || !display {
		t.Fatal(err)
	}
	held, err := Seal(dialer, "two", Highest(listener))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(held, []byte("two")) {
		t.Fatal("held line is visible")
	}

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Second)
	offer, err := ResumeOffer(dialer)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := AcceptResume(listener, offer, now, deadline)
	if err != nil || len(reply) == 0 {
		t.Fatal(err)
	}
	if back, err := AcceptResume(dialer, reply, now, deadline); err != nil || back != nil {
		t.Fatalf("dialer resume %v %v", back, err)
	}
	again, display, err := Open(listener, first)
	if err != nil || display || again != "one" {
		t.Fatalf("replay after resume %q %v %v", again, display, err)
	}
	var sawHeld bool
	for _, frame := range Pending(dialer) {
		plain, display, err := Open(listener, frame)
		if err != nil {
			t.Fatal(err)
		}
		if plain == "one" && display {
			t.Fatal("displayed line returned again")
		}
		if plain == "two" {
			if !display || sawHeld {
				t.Fatalf("held display %v twice %v", display, sawHeld)
			}
			sawHeld = true
		}
	}
	if !sawHeld {
		t.Fatal("held line was not displayed after resume")
	}
	next, err := Seal(dialer, "three", Highest(listener))
	if err != nil {
		t.Fatal(err)
	}
	plain, display, err := Open(listener, next)
	if err != nil || !display || plain != "three" {
		t.Fatalf("next %q %v %v", plain, display, err)
	}

	late := deadline
	before := NextSend(dialer)
	if _, err := AcceptResume(listener, offer, late, deadline); err == nil {
		t.Fatal("deadline equal to now continued")
	}
	if NextSend(dialer) != before || Finished(dialer) {
		t.Fatal("deadline failure changed the dialer")
	}

	end, err := End(dialer)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(end, []byte("/quit")) {
		t.Fatal("end frame contains /quit")
	}
	plain, display, err = Open(listener, end)
	if err != nil || display || plain != "" {
		t.Fatalf("end %q %v %v", plain, display, err)
	}
	if _, err = ResumeOffer(dialer); err == nil {
		t.Fatal("resume after end")
	}
	if _, err = ResumeOffer(listener); err == nil {
		t.Fatal("listener resume after end")
	}

	badD, _, _ := pair(t)
	if _, _, err = Open(badD, []byte("not-a-frame")); err == nil {
		t.Fatal("bad frame opened")
	}
	if _, err = ResumeOffer(badD); err == nil {
		t.Fatal("resume after failed open")
	}
	notice := FailureNotice(badD)
	if bytes.Contains(notice, []byte("not-a-frame")) || bytes.Contains(notice, []byte("/quit")) {
		t.Fatal("failure notice exposes the frame")
	}
}

func TestFailureNoticeEndsPeer(t *testing.T) {
	dialer, listener, _ := pair(t)
	first, err := Seal(dialer, "one", 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Seal(dialer, "two", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, display, err := Open(listener, second); err == nil || display {
		t.Fatal("sequence gap opened")
	}
	notice := FailureNotice(listener)
	if bytes.Contains(notice, []byte("two")) || bytes.Contains(notice, []byte("/quit")) {
		t.Fatal("notice contains the line")
	}
	plain, display, err := Open(dialer, notice)
	if err != nil || display || plain != "" {
		t.Fatalf("peer end %q %v %v", plain, display, err)
	}
	if !Finished(dialer) || !Finished(listener) {
		t.Fatal("failed open left a side resumable")
	}
	if _, err := ResumeOffer(dialer); err == nil {
		t.Fatal("resume after failure notice")
	}
	if _, display, err := Open(listener, first); err == nil || display {
		t.Fatal("first line displayed after the gap")
	}
}

func offerEph(t *testing.T, offer []byte) []byte {
	t.Helper()
	eph, _, err := splitOffer(offer)
	if err != nil {
		t.Fatal(err)
	}
	return eph
}

func flip(in []byte, i int) []byte {
	out := append([]byte(nil), in...)
	out[i] ^= 0xff
	return out
}
