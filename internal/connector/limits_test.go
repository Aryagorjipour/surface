package connector

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestValidateBounds(t *testing.T) {
	cfg := DefaultConfig()
	longKey := strings.Repeat("k", 129)
	maxKey := strings.Repeat("k", 128)
	longAddr := strings.Repeat("a", 257)
	maxAddr := strings.Repeat("a", 256)
	runeKey := strings.Repeat("é", 128)
	runeKeyLong := strings.Repeat("é", 129)

	f, ok := ValidateRegister(cfg, longKey, maxAddr, "available")
	if ok || f.Status != http.StatusBadRequest || f.Phrase != "key too long" {
		t.Fatalf("long key: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateRegister(cfg, maxKey, longAddr, "nope")
	if ok || f.Phrase != "address too long" {
		t.Fatalf("long address before state: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateRegister(cfg, maxKey, maxAddr, "Available")
	if ok || f.Phrase != "invalid state" {
		t.Fatalf("presence: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateRegister(cfg, "", maxAddr, "available")
	if ok || f.Phrase != "key, address, state required" {
		t.Fatalf("empty key: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateRegister(cfg, "  a", " z ", "available")
	if !ok {
		t.Fatalf("untrimmed values refused: %+v", f)
	}
	if _, ok = ValidateRegister(cfg, maxKey, maxAddr, "available"); !ok {
		t.Fatal("exact ceilings refused")
	}
	if _, ok = ValidateRegister(cfg, runeKey, "127.0.0.1:1", "busy"); !ok {
		t.Fatal("128 code points refused")
	}
	f, ok = ValidateRegister(cfg, runeKeyLong, "127.0.0.1:1", "offline")
	if ok || f.Phrase != "key too long" {
		t.Fatalf("129 code points: ok=%v fault=%+v", ok, f)
	}

	f, ok = ValidateConnect(cfg, longKey, longKey, longAddr)
	if ok || f.Phrase != "key too long" {
		t.Fatalf("from_key first: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateConnect(cfg, "from", longKey, longAddr)
	if ok || f.Phrase != "key too long" {
		t.Fatalf("to_key next: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateConnect(cfg, "from", "to", longAddr)
	if ok || f.Phrase != "address too long" {
		t.Fatalf("address last: ok=%v fault=%+v", ok, f)
	}
	f, ok = ValidateConnect(cfg, "", "to", "addr")
	if ok || f.Phrase != "from_key, to_key, from_address required" {
		t.Fatalf("missing connect: ok=%v fault=%+v", ok, f)
	}

	if _, ok = ValidateKey(cfg, maxKey); !ok {
		t.Fatal("128 key refused")
	}
	f, ok = ValidateKey(cfg, longKey)
	if ok || f.Phrase != "key too long" {
		t.Fatalf("key ceiling: ok=%v fault=%+v", ok, f)
	}
	if _, ok = ValidateID(cfg, strings.Repeat("i", 64)); !ok {
		t.Fatal("64 id refused")
	}
	f, ok = ValidateID(cfg, strings.Repeat("i", 65))
	if ok || f.Phrase != "identifier too long" {
		t.Fatalf("id ceiling: ok=%v fault=%+v", ok, f)
	}
}

func TestSourceWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceAllowance = 2
	lim := NewLimits(cfg)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	lim.SetNow(func() time.Time { return now })

	if SourceHost("192.0.2.1:1234") != "192.0.2.1" {
		t.Fatalf("ipv4 host: %s", SourceHost("192.0.2.1:1234"))
	}
	if SourceHost("[::1]:8080") != "::1" {
		t.Fatalf("ipv6 host: %s", SourceHost("[::1]:8080"))
	}
	if SourceHost("") != "unknown" {
		t.Fatalf("empty host: %q", SourceHost(""))
	}
	if SourceHost("192.0.2.1:1234") == SourceHost("[::ffff:192.0.2.1]:9") {
		t.Fatal("spellings must stay distinct")
	}

	if !lim.AllowSource("198.51.100.1") {
		t.Fatal("first source request refused")
	}
	if !lim.AllowSource("198.51.100.1") {
		t.Fatal("second source request refused")
	}
	if lim.AllowSource("198.51.100.1") {
		t.Fatal("third source request allowed")
	}
	if !lim.AllowSource("198.51.100.2") {
		t.Fatal("other source refused")
	}

	now = now.Add(60 * time.Second)
	if lim.AllowSource("198.51.100.1") {
		t.Fatal("time exactly 60s later still inside the window")
	}
	now = now.Add(time.Nanosecond)
	if !lim.AllowSource("198.51.100.1") {
		t.Fatal("after the window the source is not allowed again")
	}
	if !lim.AllowSource("198.51.100.1") {
		t.Fatal("second request after the window was refused")
	}
	unknown := NewLimits(cfg)
	unknown.SetNow(func() time.Time { return now })
	if !unknown.AllowSource("") || !unknown.AllowSource("unknown") {
		t.Fatal("empty address did not share the unknown source")
	}
	if unknown.AllowSource("") {
		t.Fatal("shared unknown source exceeded without refusal")
	}
}

func TestParseLimitConfig(t *testing.T) {
	cfg, err := ParseLimitConfig(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	def := DefaultConfig()
	if cfg != def {
		t.Fatalf("unset: %+v", cfg)
	}
	cfg, err = ParseLimitConfig(func(k string) (string, bool) {
		if k == "SURFACE_SOURCE_ALLOWANCE" {
			return "", true
		}
		return "", false
	})
	if err != nil || cfg != def {
		t.Fatalf("empty value: %+v %v", cfg, err)
	}

	env := map[string]string{
		"SURFACE_SOURCE_ALLOWANCE":  "2",
		"SURFACE_KEY_ALLOWANCE":     "5",
		"SURFACE_MAX_KEY_CHARS":     "10",
		"SURFACE_MAX_ADDRESS_CHARS": "20",
		"SURFACE_MAX_ID_CHARS":      "8",
		"SURFACE_MAX_BODY_BYTES":    "100",
	}
	cfg, err = ParseLimitConfig(func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SourceAllowance != 2 || cfg.KeyAllowance != 5 || cfg.MaxKeyChars != 10 ||
		cfg.MaxAddressChars != 20 || cfg.MaxIDChars != 8 || cfg.MaxBodyBytes != 100 {
		t.Fatalf("parsed: %+v", cfg)
	}

	cases := []struct {
		key    string
		val    string
		reason string
	}{
		{"SURFACE_SOURCE_ALLOWANCE", "0", "not a positive integer"},
		{"SURFACE_KEY_ALLOWANCE", "-3", "not a positive integer"},
		{"SURFACE_MAX_KEY_CHARS", "nope", "not a positive integer"},
		{"SURFACE_MAX_KEY_CHARS", "129", "above default"},
		{"SURFACE_MAX_ADDRESS_CHARS", "257", "above default"},
		{"SURFACE_MAX_ID_CHARS", "65", "above default"},
		{"SURFACE_MAX_BODY_BYTES", "8193", "above default"},
	}
	for _, tc := range cases {
		_, err = ParseLimitConfig(func(k string) (string, bool) {
			if k == tc.key {
				return tc.val, true
			}
			return "", false
		})
		if err == nil {
			t.Fatalf("%s=%s accepted", tc.key, tc.val)
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.key) || !strings.Contains(msg, tc.reason) || strings.Contains(msg, tc.val) {
			t.Fatalf("%s=%s error %q", tc.key, tc.val, msg)
		}
	}
}

func TestScrubForTrace(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/peers/secret-key?key=secret-key", nil)
	req.SetPathValue("key", "secret-key")
	req.Header.Set("X-Forwarded-For", "203.0.113.5")
	req.Header.Set("Forwarded", "for=203.0.113.5")
	req.Header.Set("Authorization", "Bearer secret-token")
	req.RemoteAddr = "198.51.100.10:9999"

	got := ScrubForTrace(req, "GET /peers/{key}")
	if got.URL.Path != "/peers/{key}" {
		t.Fatalf("path %s", got.URL.Path)
	}
	if got.URL.RawQuery != "key=secret-key" {
		t.Fatalf("query %s", got.URL.RawQuery)
	}
	if got.PathValue("key") != "secret-key" {
		t.Fatalf("path value %s", got.PathValue("key"))
	}
	if got.Header.Get("X-Forwarded-For") != "" || got.Header.Get("Forwarded") != "" || got.Header.Get("Authorization") != "" {
		t.Fatal("forwarded headers kept")
	}
	if req.Header.Get("Authorization") != "Bearer secret-token" {
		t.Fatal("original authorization header mutated")
	}
	if got.RemoteAddr != "" {
		t.Fatalf("remote %s", got.RemoteAddr)
	}
	if req.URL.Path != "/peers/secret-key" || req.RemoteAddr != "198.51.100.10:9999" {
		t.Fatal("original request mutated")
	}

	idReq := httptest.NewRequest(http.MethodGet, "/requests/secret-id", nil)
	idReq.SetPathValue("id", "secret-id")
	idGot := ScrubForTrace(idReq, "GET /requests/{id}")
	if idGot.URL.Path != "/requests/{id}" {
		t.Fatalf("id path %s", idGot.URL.Path)
	}
	if idGot.PathValue("id") != "secret-id" {
		t.Fatalf("id value %s", idGot.PathValue("id"))
	}
}

func TestKeyWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeyAllowance = 2
	lim := NewLimits(cfg)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	lim.SetNow(func() time.Time { return now })

	one := DefaultConfig()
	one.KeyAllowance = 1
	lim = NewLimits(one)
	lim.SetNow(func() time.Time { return now })
	if !lim.AllowKey("alice", "bob") {
		t.Fatal("first connect of two keys was refused")
	}
	if lim.AllowKey("alice", "carol") {
		t.Fatal("exhausted key was allowed")
	}
	if !lim.AllowKey("carol") {
		t.Fatal("a key named on a refused request was charged")
	}
	if !lim.AllowKey("same", "same") {
		t.Fatal("one request charged the same key twice")
	}
	if lim.AllowKey("same") {
		t.Fatal("same key ignored the single charge")
	}
	now = now.Add(60 * time.Second)
	if lim.AllowKey("same") {
		t.Fatal("key time exactly 60s later still counts")
	}
	now = now.Add(time.Nanosecond)
	if !lim.AllowKey("same") {
		t.Fatal("key window did not drop a time older than 60s")
	}

	bad := NewLimits(one)
	bad.SetNow(func() time.Time { return now })
	long := strings.Repeat("k", 129)
	for range 3 {
		if !bad.AllowKey(long) {
			t.Fatal("oversized key became a rate-limit identity")
		}
	}
}
