package connector

import (
	"strings"
	"testing"
	"time"
)

func TestRequestDeadline(t *testing.T) {
	created := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	lifetime := 4 * time.Second
	deadline := RequestDeadline(created, time.Time{}, false, lifetime)
	if !deadline.Equal(created.Add(lifetime)) {
		t.Fatalf("unanswered deadline %s", deadline)
	}
	if !RequestReadable(created.Add(lifetime), deadline) {
		t.Fatal("exact lifetime is still readable")
	}
	if RequestReadable(created.Add(lifetime+time.Nanosecond), deadline) {
		t.Fatal("after lifetime is readable")
	}

	decided := created.Add(time.Second)
	restarted := RequestDeadline(created, decided, true, lifetime)
	if !restarted.Equal(decided.Add(lifetime)) {
		t.Fatalf("decision deadline %s, want %s", restarted, decided.Add(lifetime))
	}
	if restarted.Equal(created.Add(lifetime)) {
		t.Fatal("decision kept the creation deadline")
	}
	if !RequestReadable(decided.Add(lifetime), restarted) {
		t.Fatal("exact restarted lifetime is not readable")
	}
	if RequestReadable(decided.Add(lifetime+time.Nanosecond), restarted) {
		t.Fatal("past the restarted lifetime is readable")
	}
}

func TestPeerStaleCutoff(t *testing.T) {
	now := time.Date(2026, 10, 16, 15, 0, 0, 0, time.UTC)
	window := 10 * time.Second
	cutoff := now.Add(-window).Unix()
	if !PeerStale(cutoff-1, now, window) {
		t.Fatal("last_seen < now.Add(-window).Unix() is fresh")
	}
	if PeerStale(cutoff, now, window) {
		t.Fatal("equal Unix second is stale")
	}
	if PeerStale(now.Add(time.Hour).Unix(), now, window) {
		t.Fatal("future last_seen is stale")
	}
}

func TestParseExpiryConfig(t *testing.T) {
	def, err := ParseExpiryConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if def.RequestLifetime != 10*time.Minute || def.SilenceWindow != 24*time.Hour {
		t.Fatalf("nil lookup %+v", def)
	}
	missing, err := ParseExpiryConfig(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if missing != def {
		t.Fatalf("missing %+v", missing)
	}

	got, err := ParseExpiryConfig(func(k string) (string, bool) {
		switch k {
		case "SURFACE_REQUEST_LIFETIME":
			return "2s", true
		case "SURFACE_SILENCE_WINDOW":
			return "3s", true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestLifetime != 2*time.Second || got.SilenceWindow != 3*time.Second {
		t.Fatalf("parsed %+v", got)
	}

	empty, err := ParseExpiryConfig(func(k string) (string, bool) {
		if k == "SURFACE_REQUEST_LIFETIME" || k == "SURFACE_SILENCE_WINDOW" {
			return "", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if empty != def {
		t.Fatalf("empty %+v", empty)
	}

	cases := []struct {
		name string
		raw  string
	}{
		{"SURFACE_REQUEST_LIFETIME", "0s"},
		{"SURFACE_REQUEST_LIFETIME", "-1s"},
		{"SURFACE_REQUEST_LIFETIME", "nope"},
		{"SURFACE_SILENCE_WINDOW", "0s"},
		{"SURFACE_SILENCE_WINDOW", "-1s"},
		{"SURFACE_SILENCE_WINDOW", "nope"},
	}
	for _, tc := range cases {
		_, err := ParseExpiryConfig(func(k string) (string, bool) {
			if k == tc.name {
				return tc.raw, true
			}
			return "", false
		})
		if err == nil {
			t.Fatalf("%s %q accepted", tc.name, tc.raw)
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.name) || !strings.Contains(msg, "not a positive duration") {
			t.Fatalf("%s %q: %s", tc.name, tc.raw, msg)
		}
		if strings.Contains(msg, tc.raw) {
			t.Fatalf("%s error contains raw %q: %s", tc.name, tc.raw, msg)
		}
	}
}
