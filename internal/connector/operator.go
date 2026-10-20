package connector

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Aryagorjipour/surface/internal/shared"
)

// ErrHelp means the process should print help and exit success.
var ErrHelp = errors.New("help")

// Operator is the Connector configuration after flags, environment, and file.
type Operator struct {
	Listen          string
	RequestLifetime time.Duration
	Heartbeat       time.Duration
	Maintenance     time.Duration
	Limits          Config
	CacheAddr       string
	CacheSet        bool
	SecretStoreAddr string
	SecretStoreSet  bool
	TraceEndpoint   string
	TraceSet        bool
	Punch           string
	PunchSet        bool
	Mailbox         time.Duration
	Census          bool
	Ready           bool
	TLS             shared.CertPair
}

func ConnectorHelp() string {
	return `usage: connector [flags]
  --listen address
        listen address (default :8080)
  --request-lifetime duration
        request lifetime (default 10m)
  --heartbeat-interval duration
        heartbeat interval (default 30s)
  --maintenance-interval duration
        maintenance interval (default 1s)
  --source-allowance n
        source allowance (default 60)
  --key-allowance n
        key allowance (default 60)
  --max-key-chars n
        max key characters (default 128)
  --max-address-chars n
        max address characters (default 256)
  --max-id-chars n
        max id characters (default 64)
  --max-body-bytes n
        max body bytes (default 8192)
  --cache-addr address
        cache address (default unset)
  --secret-store-addr address
        secret store address (default the Vault client address)
  --trace-endpoint endpoint
        trace endpoint (default unset)
  --punch address
        punch listen address (default 127.0.0.1:8081)
  --mailbox duration
        mailbox TTL (default unset)
  --census
        operator peer and pending counts (default off)
  --tls-cert path
        tls certificate (default unset)
  --tls-key path
        tls private key (default unset)
  --ready
        probe http://127.0.0.1:8080/health and exit
  --config path
        config file (default unset)
  --help
        show help
`
}

var connectorFileKeys = map[string]struct{}{
	"SURFACE_LISTEN_ADDR":          {},
	"SURFACE_REQUEST_LIFETIME":     {},
	"SURFACE_HEARTBEAT_INTERVAL":   {},
	"SURFACE_MAINTENANCE_INTERVAL": {},
	"SURFACE_SOURCE_ALLOWANCE":     {},
	"SURFACE_KEY_ALLOWANCE":        {},
	"SURFACE_MAX_KEY_CHARS":        {},
	"SURFACE_MAX_ADDRESS_CHARS":    {},
	"SURFACE_MAX_ID_CHARS":         {},
	"SURFACE_MAX_BODY_BYTES":       {},
	"SURFACE_CACHE_ADDR":           {},
	"VAULT_ADDR":                   {},
	"OTEL_EXPORTER_OTLP_ENDPOINT":  {},
	"SURFACE_PUNCH_ADDR":           {},
	"SURFACE_MAILBOX":              {},
	"SURFACE_CENSUS":               {},
	"SURFACE_CONFIG":               {},
	"SURFACE_TLS_CERT":             {},
	"SURFACE_TLS_KEY":              {},
}

var secretFileNames = map[string]struct{}{
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

// ParseOperator applies defaults, then the file, then the environment, then set flags.
// lookup is the process environment. A nil lookup is an empty environment.
func ParseOperator(args []string, lookup func(string) (string, bool)) (Operator, error) {
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	fs := flag.NewFlagSet("connector", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	help := fs.Bool("help", false, "show help")
	listen := fs.String("listen", ":8080", "listen address")
	life := fs.String("request-lifetime", "10m", "request lifetime")
	heartbeat := fs.String("heartbeat-interval", "30s", "heartbeat interval")
	maintenance := fs.String("maintenance-interval", "1s", "maintenance interval")
	source := fs.String("source-allowance", "60", "source allowance")
	keyAllow := fs.String("key-allowance", "60", "key allowance")
	maxKey := fs.String("max-key-chars", "128", "max key characters")
	maxAddr := fs.String("max-address-chars", "256", "max address characters")
	maxID := fs.String("max-id-chars", "64", "max id characters")
	maxBody := fs.String("max-body-bytes", "8192", "max body bytes")
	cacheAddr := fs.String("cache-addr", "", "cache address")
	secretAddr := fs.String("secret-store-addr", "", "secret store address")
	trace := fs.String("trace-endpoint", "", "trace endpoint")
	punch := fs.String("punch", "127.0.0.1:8081", "punch listen address")
	mailboxFlag := fs.String("mailbox", "", "mailbox TTL")
	censusFlag := fs.Bool("census", false, "operator census")
	tlsCert := fs.String("tls-cert", "", "tls certificate")
	tlsKey := fs.String("tls-key", "", "tls private key")
	ready := fs.Bool("ready", false, "probe health")
	configPath := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return Operator{}, err
	}
	if fs.NArg() > 0 {
		return Operator{}, fmt.Errorf("unexpected argument")
	}
	if *help {
		return Operator{}, ErrHelp
	}

	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = true
	})

	path := ""
	if set["config"] {
		path = *configPath
	} else if v, ok := lookup("SURFACE_CONFIG"); ok && v != "" {
		path = v
	}
	file := map[string]string{}
	if path != "" {
		parsed, err := parseNameFile(path)
		if err != nil {
			return Operator{}, err
		}
		if err := checkFileNames(parsed); err != nil {
			return Operator{}, err
		}
		file = parsed
	}
	if _, ok := lookup("SURFACE_SILENCE_WINDOW"); ok {
		return Operator{}, fmt.Errorf("SURFACE_SILENCE_WINDOW is retired")
	}

	merged := func(name string) (string, bool) {
		return mergedValue(name, set, flagValue(name, listen, life, heartbeat, maintenance, source, keyAllow, maxKey, maxAddr, maxID, maxBody, cacheAddr, secretAddr, trace, configPath), lookup, file)
	}
	limits, err := ParseLimitConfig(merged)
	if err != nil {
		return Operator{}, err
	}
	reqLife, err := positiveDuration(merged, "SURFACE_REQUEST_LIFETIME", 10*time.Minute)
	if err != nil {
		return Operator{}, err
	}
	hb, err := positiveDuration(merged, "SURFACE_HEARTBEAT_INTERVAL", 30*time.Second)
	if err != nil {
		return Operator{}, err
	}
	maint, err := positiveDuration(merged, "SURFACE_MAINTENANCE_INTERVAL", time.Second)
	if err != nil {
		return Operator{}, err
	}
	if refreshesPerMinute(hb) > limits.KeyAllowance {
		return Operator{}, fmt.Errorf("SURFACE_HEARTBEAT_INTERVAL exceeds SURFACE_KEY_ALLOWANCE")
	}

	listenAddr, _ := pick(set["listen"], *listen, lookup, file, "SURFACE_LISTEN_ADDR", ":8080", true)
	cache, cacheSet := pick(set["cache-addr"], *cacheAddr, lookup, file, "SURFACE_CACHE_ADDR", "", false)
	vaultAddr, vaultSet := pick(set["secret-store-addr"], *secretAddr, lookup, file, "VAULT_ADDR", "", true)
	traceEndpoint, traceSet := pick(set["trace-endpoint"], *trace, lookup, file, "OTEL_EXPORTER_OTLP_ENDPOINT", "", true)
	punchAddr, punchSet := resolvePunch(set["punch"], *punch, lookup, file)
	mailboxTTL, err := mailboxDuration(set["mailbox"], *mailboxFlag, lookup, file)
	if err != nil {
		return Operator{}, err
	}
	census, err := resolveCensus(set["census"], *censusFlag, lookup, file)
	if err != nil {
		return Operator{}, err
	}
	certPath, _ := pick(set["tls-cert"], *tlsCert, lookup, file, "SURFACE_TLS_CERT", "", true)
	keyPath, _ := pick(set["tls-key"], *tlsKey, lookup, file, "SURFACE_TLS_KEY", "", true)
	pair, err := shared.ResolveCertPair(certPath, keyPath)
	if err != nil {
		return Operator{}, err
	}

	return Operator{
		Listen:          listenAddr,
		RequestLifetime: reqLife,
		Heartbeat:       hb,
		Maintenance:     maint,
		Limits:          limits,
		CacheAddr:       cache,
		CacheSet:        cacheSet,
		SecretStoreAddr: vaultAddr,
		SecretStoreSet:  vaultSet,
		TraceEndpoint:   traceEndpoint,
		TraceSet:        traceSet,
		Punch:           punchAddr,
		PunchSet:        punchSet,
		Mailbox:         mailboxTTL,
		Census:          census,
		Ready:           *ready,
		TLS:             pair,
	}, nil
}

func resolveCensus(flagSet, flagVal bool, lookup func(string) (string, bool), file map[string]string) (bool, error) {
	if flagSet {
		return flagVal, nil
	}
	if raw, ok := lookup("SURFACE_CENSUS"); ok {
		return parseCensus(raw)
	}
	if raw, ok := file["SURFACE_CENSUS"]; ok {
		return parseCensus(raw)
	}
	return false, nil
}

func parseCensus(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "0", "false", "f":
		return false, nil
	case "1", "true", "t":
		return true, nil
	default:
		return false, fmt.Errorf("SURFACE_CENSUS is not a boolean")
	}
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

// resolvePunch applies flag, then environment, then file. An empty value after trim disables punch.
// An unset value keeps the loopback default.
func resolvePunch(flagSet bool, flagVal string, lookup func(string) (string, bool), file map[string]string) (string, bool) {
	if flagSet {
		return strings.TrimSpace(flagVal), true
	}
	if raw, ok := lookup("SURFACE_PUNCH_ADDR"); ok {
		return strings.TrimSpace(raw), true
	}
	if raw, ok := file["SURFACE_PUNCH_ADDR"]; ok {
		return strings.TrimSpace(raw), true
	}
	return "127.0.0.1:8081", false
}

func flagValue(name string, listen, life, heartbeat, maintenance, source, keyAllow, maxKey, maxAddr, maxID, maxBody, cacheAddr, secretAddr, trace, configPath *string) string {
	switch name {
	case "SURFACE_LISTEN_ADDR":
		return *listen
	case "SURFACE_REQUEST_LIFETIME":
		return *life
	case "SURFACE_HEARTBEAT_INTERVAL":
		return *heartbeat
	case "SURFACE_MAINTENANCE_INTERVAL":
		return *maintenance
	case "SURFACE_SOURCE_ALLOWANCE":
		return *source
	case "SURFACE_KEY_ALLOWANCE":
		return *keyAllow
	case "SURFACE_MAX_KEY_CHARS":
		return *maxKey
	case "SURFACE_MAX_ADDRESS_CHARS":
		return *maxAddr
	case "SURFACE_MAX_ID_CHARS":
		return *maxID
	case "SURFACE_MAX_BODY_BYTES":
		return *maxBody
	case "SURFACE_CACHE_ADDR":
		return *cacheAddr
	case "VAULT_ADDR":
		return *secretAddr
	case "OTEL_EXPORTER_OTLP_ENDPOINT":
		return *trace
	case "SURFACE_CONFIG":
		return *configPath
	default:
		return ""
	}
}

func envNameForFlag(flagName string) string {
	switch flagName {
	case "listen":
		return "SURFACE_LISTEN_ADDR"
	case "request-lifetime":
		return "SURFACE_REQUEST_LIFETIME"
	case "heartbeat-interval":
		return "SURFACE_HEARTBEAT_INTERVAL"
	case "maintenance-interval":
		return "SURFACE_MAINTENANCE_INTERVAL"
	case "source-allowance":
		return "SURFACE_SOURCE_ALLOWANCE"
	case "key-allowance":
		return "SURFACE_KEY_ALLOWANCE"
	case "max-key-chars":
		return "SURFACE_MAX_KEY_CHARS"
	case "max-address-chars":
		return "SURFACE_MAX_ADDRESS_CHARS"
	case "max-id-chars":
		return "SURFACE_MAX_ID_CHARS"
	case "max-body-bytes":
		return "SURFACE_MAX_BODY_BYTES"
	case "cache-addr":
		return "SURFACE_CACHE_ADDR"
	case "secret-store-addr":
		return "VAULT_ADDR"
	case "trace-endpoint":
		return "OTEL_EXPORTER_OTLP_ENDPOINT"
	case "config":
		return "SURFACE_CONFIG"
	default:
		return ""
	}
}

func mergedValue(name string, set map[string]bool, flagVal string, lookup func(string) (string, bool), file map[string]string) (string, bool) {
	flagName := ""
	for _, candidate := range []string{
		"listen", "request-lifetime", "heartbeat-interval", "maintenance-interval",
		"source-allowance", "key-allowance", "max-key-chars", "max-address-chars",
		"max-id-chars", "max-body-bytes", "cache-addr", "secret-store-addr",
		"trace-endpoint", "config",
	} {
		if envNameForFlag(candidate) == name {
			flagName = candidate
			break
		}
	}
	emptyUnset := name != "SURFACE_CACHE_ADDR"
	if flagName != "" && set[flagName] && (!emptyUnset || flagVal != "") {
		return flagVal, true
	}
	if raw, ok := lookup(name); ok && (!emptyUnset || raw != "") {
		return raw, true
	}
	if raw, ok := file[name]; ok && (!emptyUnset || raw != "") {
		return raw, true
	}
	return "", false
}

// pick resolves one string setting. emptyUnset treats "" as missing so a file value can show through.
// Cache address keeps an explicit empty string, which disables Redis.
func pick(flagSet bool, flagVal string, lookup func(string) (string, bool), file map[string]string, envName, fallback string, emptyUnset bool) (string, bool) {
	if flagSet && (!emptyUnset || flagVal != "") {
		return flagVal, true
	}
	if raw, ok := lookup(envName); ok && (!emptyUnset || raw != "") {
		return raw, true
	}
	if raw, ok := file[envName]; ok && (!emptyUnset || raw != "") {
		return raw, true
	}
	if fallback != "" || !emptyUnset {
		return fallback, false
	}
	return "", false
}

func refreshesPerMinute(d time.Duration) int {
	ms := d.Milliseconds()
	if ms <= 0 {
		return int(^uint(0) >> 1)
	}
	return int((60000 + ms - 1) / ms)
}

func parseNameFile(path string) (map[string]string, error) {
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
			return nil, fmt.Errorf("unknown setting %s", strings.TrimSpace(name))
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("unknown setting")
		}
		if name == "SURFACE_CONFIG" {
			continue
		}
		out[name] = strings.TrimSpace(value)
	}
	return out, nil
}

func checkFileNames(file map[string]string) error {
	for name := range file {
		if _, secret := secretFileNames[name]; secret {
			return fmt.Errorf("%s is not allowed in the config file", name)
		}
		if name == "SURFACE_SILENCE_WINDOW" {
			return fmt.Errorf("SURFACE_SILENCE_WINDOW is retired")
		}
		if _, known := connectorFileKeys[name]; !known {
			return fmt.Errorf("unknown setting %s", name)
		}
	}
	return nil
}
