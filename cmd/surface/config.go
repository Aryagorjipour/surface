package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

// errHelp asks main to print help and exit success.
var errHelp = errors.New("help")

type surfaceConfig struct {
	heartbeat time.Duration
	resume    time.Duration
	pending   time.Duration
	relay     string
	punch     string
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
  --relay address
        relay address (default unset)
  --punch address
        punch address (default the connector host port 8081)
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
	"SURFACE_RELAY":              {},
	"SURFACE_PUNCH":              {},
	"SURFACE_CONFIG":             {},
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
	relayFlag := fs.String("relay", "", "relay address")
	punchFlag := fs.String("punch", "", "punch address")
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
	return surfaceConfig{heartbeat: hb, resume: window, pending: poll, relay: relay, punch: punch, rest: fs.Args()}, nil
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
