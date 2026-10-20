package hostport

import (
	"errors"
	"testing"
)

func TestParse(t *testing.T) {
	if (Addr{}).String() != "" {
		t.Fatal("zero Addr String is not empty")
	}
	accepted := []string{
		"127.0.0.1:9001",
		"127.0.0.1:0",
		"127.0.0.1:1",
		"127.0.0.1:01",
		"[::1]:9001",
		"localhost:9001",
		":9001",
		"[::]:9001",
	}
	for _, raw := range accepted {
		addr, err := Parse(raw)
		if err != nil {
			t.Fatalf("Parse(%q): %v", raw, err)
		}
		if addr.String() != raw {
			t.Fatalf("String() = %q, input %q", addr.String(), raw)
		}
	}
	rejected := []string{
		"::1:9001",
		"[127.0.0.1]:9001",
		"not-a-host",
		"127.0.0.1:65536",
		"[::1]:",
		"[fe80::1%eth0]:9001",
		"[fe80::1%25eth0]:9001",
	}
	for _, raw := range rejected {
		addr, err := Parse(raw)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("Parse(%q) err = %v", raw, err)
		}
		if addr.String() != "" {
			t.Fatalf("Parse(%q) addr = %q", raw, addr.String())
		}
	}
}
