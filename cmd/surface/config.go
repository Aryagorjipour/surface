package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Aryagorjipour/surface/internal/shared"
)

var errHelp = errors.New("help")

type surfaceConfig struct {
	heartbeat time.Duration
	resume    time.Duration
	pending   time.Duration
	relay     string
	punch     string
	maxChats  int
	mailbox   time.Duration
	direct    shared.CertPair
	directCA  string
	rest      []string
}

func surfaceHelp() string {
	return `usage: surface <connector-url> <my-key> [listen-addr]
  --heartbeat-interval duration
        heartbeat interval (default 30s)
  --resume-window duration
        resume window (default 60s)
  --pending-poll duration
        pending poll interval (default 2s)
  --mailbox duration
        mailbox TTL (default unset)
  --relay address
        relay address (default unset)
  --punch address
        punch address (default the connector host port 8081)
  --max-chats int
        max concurrent chats (default 4)
  --direct-cert path
        direct tls certificate (default unset)
  --direct-key path
        direct tls private key (default unset)
  --direct-ca path
        direct tls root (default the system pool)
  --config path
        config file (default unset)
  --help
        show help
`
}

var surfaceFileKeys = map[string]struct{}{
	"SURFACE_HEARTBEAT_INTERVAL": {},
	"SURFACE_RESUME_WINDOW":      {},
	"SURFACE_PENDING_POLL":       {},
	"SURFACE_MAILBOX":            {},
	"SURFACE_RELAY":              {},
	"SURFACE_PUNCH":              {},
	"SURFACE_MAX_CHATS":          {},
	"SURFACE_CONFIG":             {},
	"SURFACE_DIRECT_CERT":        {},
	"SURFACE_DIRECT_KEY":         {},
	"SURFACE_DIRECT_CA":          {},
}

var surfaceSecretNames = map[string]struct{}{
	"TURSO_URL":               {},
	"turso_url":               {},
	"TURSO_TOKEN":             {},
	"turso_token":             {},
	"ENCRYPTION_KEY":          {},
	"encryption_key":          {},
	"REDIS_PASSWORD":          {},
	"redis_password":          {},
	"CONNECTOR_TOKEN":         {},
	"connector_token":         {},
	"VAULT_TOKEN":             {},
	"SURFACE_CONNECTOR_TOKEN": {},
	"SURFACE_IDENTITY_SEED":   {},
}

func parseSurface(args []string, lookup func(string) (string, bool)) (surfaceConfig, error) {
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	fs := flag.NewFlagSet("surface", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	help := fs.Bool("help", false, "show help")
	heartbeat := fs.String("heartbeat-interval", "30s", "heartbeat interval")
	resume := fs.String("resume-window", "60s", "resume window")
	pending := fs.String("pending-poll", "2s", "pending poll interval")
	mailboxFlag := fs.String("mailbox", "", "mailbox TTL")
	relayFlag := fs.String("relay", "", "relay address")
	punchFlag := fs.String("punch", "", "punch address")
	maxChatsFlag := fs.String("max-chats", "4", "max concurrent chats")
	directCert := fs.String("direct-cert", "", "direct tls certificate")
	directKey := fs.String("direct-key", "", "direct tls private key")
	directCA := fs.String("direct-ca", "", "direct tls root")
	configPath := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return surfaceConfig{}, err
	}
	if *help {
		return surfaceConfig{}, errHelp
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	path := ""
	if set["config"] {
		path = *configPath
	} else if v, ok := lookup("SURFACE_CONFIG"); ok && v != "" {
		path = v
	}
	file := map[string]string{}
	if path != "" {
		parsed, err := parseSurfaceFile(path)
		if err != nil {
			return surfaceConfig{}, err
		}
		file = parsed
	}
	if _, ok := lookup("SURFACE_SILENCE_WINDOW"); ok {
		return surfaceConfig{}, fmt.Errorf("SURFACE_SILENCE_WINDOW is retired")
	}
	if _, ok := file["SURFACE_SILENCE_WINDOW"]; ok {
		return surfaceConfig{}, fmt.Errorf("SURFACE_SILENCE_WINDOW is retired")
	}

	merged := func(name string) (string, bool) {
		switch name {
		case "SURFACE_HEARTBEAT_INTERVAL":
			if set["heartbeat-interval"] && *heartbeat != "" {
				return *heartbeat, true
			}
		case "SURFACE_RESUME_WINDOW":
			if set["resume-window"] && *resume != "" {
				return *resume, true
			}
		case "SURFACE_PENDING_POLL":
			if set["pending-poll"] && *pending != "" {
				return *pending, true
			}
		case "SURFACE_MAX_CHATS":
			if set["max-chats"] && *maxChatsFlag != "" {
				return *maxChatsFlag, true
			}
		}
		if raw, ok := lookup(name); ok && raw != "" {
			return raw, true
		}
		if raw, ok := file[name]; ok && raw != "" {
			return raw, true
		}
		return "", false
	}
	hb, err := positiveDuration(merged, "SURFACE_HEARTBEAT_INTERVAL", 30*time.Second)
	if err != nil {
		return surfaceConfig{}, err
	}
	window, err := positiveDuration(merged, "SURFACE_RESUME_WINDOW", 60*time.Second)
	if err != nil {
		return surfaceConfig{}, err
	}
	poll, err := positiveDuration(merged, "SURFACE_PENDING_POLL", 2*time.Second)
	if err != nil {
		return surfaceConfig{}, err
	}
	relay, _ := pickSurface(set["relay"], *relayFlag, lookup, file, "SURFACE_RELAY")
	punch, punchSet := pickSurface(set["punch"], *punchFlag, lookup, file, "SURFACE_PUNCH")
	if !punchSet && len(fs.Args()) > 0 {
		punch = defaultPunch(fs.Args()[0])
	}
	maxChats, err := positiveInt(merged, "SURFACE_MAX_CHATS", 4)
	if err != nil {
		return surfaceConfig{}, err
	}
	mailboxTTL, err := mailboxDuration(set["mailbox"], *mailboxFlag, lookup, file)
	if err != nil {
		return surfaceConfig{}, err
	}
	certPath, _ := pickSurface(set["direct-cert"], *directCert, lookup, file, "SURFACE_DIRECT_CERT")
	keyPath, _ := pickSurface(set["direct-key"], *directKey, lookup, file, "SURFACE_DIRECT_KEY")
	caPath, _ := pickSurface(set["direct-ca"], *directCA, lookup, file, "SURFACE_DIRECT_CA")
	pair, err := shared.ResolveCertPair(certPath, keyPath)
	if err != nil {
		return surfaceConfig{}, err
	}
	return surfaceConfig{
		heartbeat: hb,
		resume:    window,
		pending:   poll,
		relay:     relay,
		punch:     punch,
		maxChats:  maxChats,
		mailbox:   mailboxTTL,
		direct:    pair,
		directCA:  caPath,
		rest:      fs.Args(),
	}, nil
}

func mailboxDuration(flagSet bool, flagVal string, lookup func(string) (string, bool), file map[string]string) (time.Duration, error) {
	raw := ""
	set := false
	switch {
	case flagSet:
		raw, set = flagVal, true
	default:
		if v, ok := lookup("SURFACE_MAILBOX"); ok {
			raw, set = v, true
		} else if v, ok := file["SURFACE_MAILBOX"]; ok {
			raw, set = v, true
		}
	}
	if !set {
		return 0, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("SURFACE_MAILBOX not a positive duration")
	}
	return d, nil
}

func pickSurface(flagSet bool, flagVal string, lookup func(string) (string, bool), file map[string]string, name string) (string, bool) {
	if flagSet {
		return strings.TrimSpace(flagVal), true
	}
	if raw, ok := lookup(name); ok {
		return strings.TrimSpace(raw), true
	}
	if raw, ok := file[name]; ok {
		return strings.TrimSpace(raw), true
	}
	return "", false
}

func defaultPunch(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := parsed.Hostname()
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, "8081")
}

func positiveInt(lookup func(string) (string, bool), name string, fallback int) (int, error) {
	raw, ok := lookup(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s not a positive integer", name)
	}
	return n, nil
}

func positiveDuration(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s not a positive duration", name)
	}
	return d, nil
}

func parseSurfaceFile(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("SURFACE_CONFIG")
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("unknown setting %s", strings.TrimSpace(line))
		}
		name = strings.TrimSpace(name)
		if name == "" || name == "SURFACE_CONFIG" {
			if name == "SURFACE_CONFIG" {
				continue
			}
			return nil, fmt.Errorf("unknown setting")
		}
		if _, secret := surfaceSecretNames[name]; secret {
			return nil, fmt.Errorf("%s is not allowed in the config file", name)
		}
		if name == "SURFACE_SILENCE_WINDOW" {
			return nil, fmt.Errorf("SURFACE_SILENCE_WINDOW is retired")
		}
		if _, known := surfaceFileKeys[name]; !known {
			return nil, fmt.Errorf("unknown setting %s", name)
		}
		out[name] = strings.TrimSpace(value)
	}
	return out, nil
}
