package connector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorPrecedence(t *testing.T) {
	def, err := ParseOperator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if def.Listen != ":8080" || def.RequestLifetime != 10*time.Minute || def.Heartbeat != 30*time.Second || def.Maintenance != time.Second {
		t.Fatalf("defaults %+v", def)
	}
	if def.Limits.SourceAllowance != 60 || def.Limits.KeyAllowance != 60 || def.Limits.MaxKeyChars != 128 ||
		def.Limits.MaxAddressChars != 256 || def.Limits.MaxIDChars != 64 || def.Limits.MaxBodyBytes != 8192 {
		t.Fatalf("limit defaults %+v", def.Limits)
	}
	if def.CacheSet || def.SecretStoreSet || def.TraceSet {
		t.Fatalf("unset addresses were set: %+v", def)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "connector.env")
	if err := os.WriteFile(path, []byte("\nSURFACE_LISTEN_ADDR=:9000\nSURFACE_CONFIG=/do-not-recurse\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := ParseOperator([]string{"--config", path}, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if fromFile.Listen != ":9000" {
		t.Fatalf("file listen %s", fromFile.Listen)
	}
	fromEnv, err := ParseOperator([]string{"--config", path}, func(k string) (string, bool) {
		if k == "SURFACE_LISTEN_ADDR" {
			return ":9001", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if fromEnv.Listen != ":9001" {
		t.Fatalf("env listen %s", fromEnv.Listen)
	}
	fromFlag, err := ParseOperator([]string{"--config", path, "--listen", ":9002"}, func(k string) (string, bool) {
		if k == "SURFACE_LISTEN_ADDR" {
			return ":9001", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if fromFlag.Listen != ":9002" {
		t.Fatalf("flag listen %s", fromFlag.Listen)
	}

	_, err = ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_MAX_KEY_CHARS" {
			return "999", true
		}
		return "", false
	})
	if err == nil || !strings.Contains(err.Error(), "SURFACE_MAX_KEY_CHARS") || strings.Contains(err.Error(), "999") {
		t.Fatalf("ceiling: %v", err)
	}

	_, err = ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_SILENCE_WINDOW" {
			return "24h", true
		}
		return "", false
	})
	if err == nil || !strings.Contains(err.Error(), "SURFACE_SILENCE_WINDOW") || !strings.Contains(err.Error(), "retired") || strings.Contains(err.Error(), "24h") {
		t.Fatalf("retired: %v", err)
	}

	const sentinel = "SENTINEL-SECRET-VALUE"
	for _, name := range []string{
		"connector_token", "VAULT_TOKEN", "TURSO_TOKEN", "turso_token", "ENCRYPTION_KEY", "encryption_key",
		"REDIS_PASSWORD", "redis_password", "TURSO_URL", "turso_url", "SURFACE_CONNECTOR_TOKEN", "CONNECTOR_TOKEN",
	} {
		secretPath := filepath.Join(dir, name+".env")
		if err := os.WriteFile(secretPath, []byte(name+"="+sentinel+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := ParseOperator([]string{"--config", secretPath}, func(string) (string, bool) { return "", false })
		if err == nil || strings.Contains(err.Error(), sentinel) {
			t.Fatalf("%s: %v", name, err)
		}
	}

	unknown := filepath.Join(dir, "unknown.env")
	if err := os.WriteFile(unknown, []byte("NOT_A_SETTING=nope-value\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = ParseOperator([]string{"--config", unknown}, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "NOT_A_SETTING") || strings.Contains(err.Error(), "nope-value") {
		t.Fatalf("unknown: %v", err)
	}

	fastOK, err := ParseOperator([]string{"--heartbeat-interval", "1s", "--key-allowance", "60"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fastOK.Heartbeat != time.Second || fastOK.Limits.KeyAllowance != 60 {
		t.Fatalf("fast %+v", fastOK)
	}
	_, err = ParseOperator([]string{"--heartbeat-interval", "500ms", "--key-allowance", "60"}, nil)
	if err == nil || strings.Contains(err.Error(), "500ms") {
		t.Fatalf("too fast: %v", err)
	}

	emptyCache, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_CACHE_ADDR" {
			return "", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if !emptyCache.CacheSet || emptyCache.CacheAddr != "" {
		t.Fatalf("empty cache %+v", emptyCache)
	}
}

func TestMailboxDuration(t *testing.T) {
	def, err := ParseOperator(nil, nil)
	if err != nil || def.Mailbox != 0 {
		t.Fatalf("unset %+v %v", def.Mailbox, err)
	}
	if !strings.Contains(ConnectorHelp(), "--mailbox") || !strings.Contains(ConnectorHelp(), "default unset") {
		t.Fatal(ConnectorHelp())
	}
	_, err = ParseOperator([]string{"--mailbox", "0s"}, nil)
	if err == nil || !strings.Contains(err.Error(), "SURFACE_MAILBOX") || strings.Contains(err.Error(), "0s") {
		t.Fatalf("zero: %v", err)
	}
	got, err := ParseOperator([]string{"--mailbox", "24h"}, nil)
	if err != nil || got.Mailbox != 24*time.Hour {
		t.Fatalf("flag %+v %v", got.Mailbox, err)
	}
	blank, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return " \t ", true
		}
		return "", false
	})
	if err != nil || blank.Mailbox != 0 {
		t.Fatalf("blank %+v %v", blank.Mailbox, err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "mailbox.env")
	if err := os.WriteFile(path, []byte("SURFACE_MAILBOX=1h\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := ParseOperator([]string{"--config", path}, func(string) (string, bool) { return "", false })
	if err != nil || fromFile.Mailbox != time.Hour {
		t.Fatalf("file %+v %v", fromFile.Mailbox, err)
	}
	fromEnv, err := ParseOperator([]string{"--config", path}, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return "2h", true
		}
		return "", false
	})
	if err != nil || fromEnv.Mailbox != 2*time.Hour {
		t.Fatalf("env %+v %v", fromEnv.Mailbox, err)
	}
	fromFlag, err := ParseOperator([]string{"--config", path, "--mailbox", "3h"}, func(k string) (string, bool) {
		if k == "SURFACE_MAILBOX" {
			return "2h", true
		}
		return "", false
	})
	if err != nil || fromFlag.Mailbox != 3*time.Hour {
		t.Fatalf("flag wins %+v %v", fromFlag.Mailbox, err)
	}
}

func TestCensusSwitch(t *testing.T) {
	def, err := ParseOperator(nil, nil)
	if err != nil || def.Census {
		t.Fatalf("unset %+v %v", def.Census, err)
	}
	if !strings.Contains(ConnectorHelp(), "--census") || !strings.Contains(ConnectorHelp(), "default off") {
		t.Fatal(ConnectorHelp())
	}

	on, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return "TRUE", true
		}
		return "", false
	})
	if err != nil || !on.Census {
		t.Fatalf("true %+v %v", on.Census, err)
	}
	off, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return " 0 ", true
		}
		return "", false
	})
	if err != nil || off.Census {
		t.Fatalf("zero %+v %v", off.Census, err)
	}
	blank, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return " \t ", true
		}
		return "", false
	})
	if err != nil || blank.Census {
		t.Fatalf("blank %+v %v", blank.Census, err)
	}
	_, err = ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return "yes-not-a-bool", true
		}
		return "", false
	})
	if err == nil || !strings.Contains(err.Error(), "SURFACE_CENSUS") || strings.Contains(err.Error(), "yes-not-a-bool") {
		t.Fatalf("bad: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "census.env")
	if err := os.WriteFile(path, []byte("SURFACE_CENSUS=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := ParseOperator([]string{"--config", path}, func(string) (string, bool) { return "", false })
	if err != nil || !fromFile.Census {
		t.Fatalf("file %+v %v", fromFile.Census, err)
	}
	fromEnv, err := ParseOperator([]string{"--config", path}, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return "false", true
		}
		return "", false
	})
	if err != nil || fromEnv.Census {
		t.Fatalf("env %+v %v", fromEnv.Census, err)
	}
	fromFlag, err := ParseOperator([]string{"--config", path, "--census"}, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return "false", true
		}
		return "", false
	})
	if err != nil || !fromFlag.Census {
		t.Fatalf("flag wins %+v %v", fromFlag.Census, err)
	}
	forcedOff, err := ParseOperator([]string{"--census=false"}, func(k string) (string, bool) {
		if k == "SURFACE_CENSUS" {
			return "1", true
		}
		return "", false
	})
	if err != nil || forcedOff.Census {
		t.Fatalf("flag off %+v %v", forcedOff.Census, err)
	}
}

func TestPunchFlag(t *testing.T) {
	def, err := ParseOperator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if def.Punch != "127.0.0.1:8081" || def.PunchSet {
		t.Fatalf("default punch %+v", def.Punch)
	}
	if !strings.Contains(ConnectorHelp(), "--punch") {
		t.Fatal(ConnectorHelp())
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "punch.env")
	if err := os.WriteFile(path, []byte("SURFACE_PUNCH_ADDR=127.0.0.1:7001\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := ParseOperator([]string{"--config", path}, func(string) (string, bool) { return "", false })
	if err != nil || fromFile.Punch != "127.0.0.1:7001" || !fromFile.PunchSet {
		t.Fatalf("file %+v %v", fromFile.Punch, err)
	}
	fromEnv, err := ParseOperator([]string{"--config", path}, func(k string) (string, bool) {
		if k == "SURFACE_PUNCH_ADDR" {
			return "127.0.0.1:7002", true
		}
		return "", false
	})
	if err != nil || fromEnv.Punch != "127.0.0.1:7002" {
		t.Fatalf("env %+v %v", fromEnv.Punch, err)
	}
	fromFlag, err := ParseOperator([]string{"--config", path, "--punch", "127.0.0.1:7003"}, func(k string) (string, bool) {
		if k == "SURFACE_PUNCH_ADDR" {
			return "127.0.0.1:7002", true
		}
		return "", false
	})
	if err != nil || fromFlag.Punch != "127.0.0.1:7003" {
		t.Fatalf("flag %+v %v", fromFlag.Punch, err)
	}

	disabled, err := ParseOperator([]string{"--punch", "  "}, nil)
	if err != nil || disabled.Punch != "" || !disabled.PunchSet {
		t.Fatalf("flag empty %+v %v", disabled, err)
	}
	envEmpty, err := ParseOperator(nil, func(k string) (string, bool) {
		if k == "SURFACE_PUNCH_ADDR" {
			return " \t ", true
		}
		return "", false
	})
	if err != nil || envEmpty.Punch != "" {
		t.Fatalf("env empty %+v %v", envEmpty.Punch, err)
	}
	blank := filepath.Join(dir, "blank.env")
	if err := os.WriteFile(blank, []byte("SURFACE_PUNCH_ADDR=\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fileEmpty, err := ParseOperator([]string{"--config", blank}, func(string) (string, bool) { return "", false })
	if err != nil || fileEmpty.Punch != "" {
		t.Fatalf("file empty %+v %v", fileEmpty.Punch, err)
	}
}
