package connector

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Aryagorjipour/surface/internal/hostport"
)

const window = 60 * time.Second

// Config is the operator's ceilings and allowances for one Connector process.
type Config struct {
	SourceAllowance int
	KeyAllowance    int
	MaxKeyChars     int
	MaxAddressChars int
	MaxIDChars      int
	MaxBodyBytes    int
}

// Fault is one caller-visible refusal.
type Fault struct {
	Status int
	Phrase string
}

// DefaultConfig returns the spec defaults: 60, 60, 128, 256, 64, and 8192.
func DefaultConfig() Config {
	return Config{
		SourceAllowance: 60,
		KeyAllowance:    60,
		MaxKeyChars:     128,
		MaxAddressChars: 256,
		MaxIDChars:      64,
		MaxBodyBytes:    8192,
	}
}

// Limits counts requests for one Connector process.
type Limits struct {
	cfg     Config
	mu      sync.Mutex
	now     func() time.Time
	sources map[string][]time.Time
	keys    map[string][]time.Time
}

// NewLimits returns a limiter with the given config.
func NewLimits(cfg Config) *Limits {
	return &Limits{
		cfg:     cfg,
		now:     time.Now,
		sources: make(map[string][]time.Time),
		keys:    make(map[string][]time.Time),
	}
}

// SetNow replaces the clock used for window checks.
func (l *Limits) SetNow(now func() time.Time) {
	if l == nil || now == nil {
		return
	}
	l.mu.Lock()
	l.now = now
	l.mu.Unlock()
}

func (l *Limits) clock() time.Time {
	if l.now == nil {
		return time.Now()
	}
	return l.now()
}

// SourceHost is the connection host without the port.
// An empty address shares the source named unknown.
func SourceHost(remoteAddr string) string {
	if remoteAddr == "" {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	if host == "" {
		return "unknown"
	}
	return host
}

// AllowSource reports whether this source may proceed.
// A refused call does not occupy a slot.
func (l *Limits) AllowSource(host string) bool {
	if host == "" {
		host = "unknown"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	kept := sweep(l.sources[host], now)
	if len(kept) >= l.cfg.SourceAllowance {
		storeStamps(l.sources, host, kept)
		return false
	}
	l.sources[host] = append(kept, now)
	return true
}

// AllowKey reports whether every distinct, already-valid key may proceed.
// If any named key would exceed its allowance, no key on this call is charged.
func (l *Limits) AllowKey(keys ...string) bool {
	distinct := distinctKeys(l.cfg, keys)
	if len(distinct) == 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	kept := make([][]time.Time, len(distinct))
	for i, key := range distinct {
		kept[i] = sweep(l.keys[key], now)
		if len(kept[i]) >= l.cfg.KeyAllowance {
			for j := 0; j <= i; j++ {
				storeStamps(l.keys, distinct[j], kept[j])
			}
			return false
		}
	}
	for i, key := range distinct {
		l.keys[key] = append(kept[i], now)
	}
	return true
}

func distinctKeys(cfg Config, keys []string) []string {
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := ValidateKey(cfg, key); !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

// A stamp exactly window old still counts. Older stamps are dropped.
func sweep(stamps []time.Time, now time.Time) []time.Time {
	cutoff := now.Add(-window)
	kept := make([]time.Time, 0, len(stamps))
	for _, stamp := range stamps {
		if !stamp.Before(cutoff) {
			kept = append(kept, stamp)
		}
	}
	return kept
}

func storeStamps(m map[string][]time.Time, id string, stamps []time.Time) {
	if len(stamps) == 0 {
		delete(m, id)
		return
	}
	m[id] = stamps
}

func bad(phrase string) (Fault, bool) {
	return Fault{Status: http.StatusBadRequest, Phrase: phrase}, false
}

func over(s string, max int) bool {
	return utf8.RuneCountInString(s) > max
}

// ValidateRegister checks a peer write. Empty fields stay a missing-field fault.
func ValidateRegister(cfg Config, key, address, state string) (Fault, bool) {
	if key == "" || address == "" || state == "" {
		return bad("key, address, state required")
	}
	if over(key, cfg.MaxKeyChars) {
		return bad("key too long")
	}
	if over(address, cfg.MaxAddressChars) {
		return bad("address too long")
	}
	if state != "available" && state != "busy" && state != "offline" {
		return bad("invalid state")
	}
	if _, err := hostport.Parse(address); err != nil {
		return bad("invalid address")
	}
	return Fault{}, true
}

// ValidateConnect checks a connect body. Key length is from_key, then to_key.
func ValidateConnect(cfg Config, fromKey, toKey, fromAddress string) (Fault, bool) {
	if fromKey == "" || toKey == "" || fromAddress == "" {
		return bad("from_key, to_key, from_address required")
	}
	if over(fromKey, cfg.MaxKeyChars) || over(toKey, cfg.MaxKeyChars) {
		return bad("key too long")
	}
	if over(fromAddress, cfg.MaxAddressChars) {
		return bad("address too long")
	}
	if _, err := hostport.Parse(fromAddress); err != nil {
		return bad("invalid address")
	}
	return Fault{}, true
}

// ValidateKey checks one peer key. An empty key is not a length fault.
func ValidateKey(cfg Config, key string) (Fault, bool) {
	if over(key, cfg.MaxKeyChars) {
		return bad("key too long")
	}
	return Fault{}, true
}

// ValidateID checks a request identifier. An empty id is not a length fault.
func ValidateID(cfg Config, id string) (Fault, bool) {
	if over(id, cfg.MaxIDChars) {
		return bad("identifier too long")
	}
	return Fault{}, true
}

// ParseLimitConfig reads operator settings through lookup.
// It does not read the process environment itself.
func ParseLimitConfig(lookup func(string) (string, bool)) (Config, error) {
	cfg := DefaultConfig()
	ceilings := map[string]int{
		"SURFACE_MAX_KEY_CHARS":     cfg.MaxKeyChars,
		"SURFACE_MAX_ADDRESS_CHARS": cfg.MaxAddressChars,
		"SURFACE_MAX_ID_CHARS":      cfg.MaxIDChars,
		"SURFACE_MAX_BODY_BYTES":    cfg.MaxBodyBytes,
	}
	type item struct {
		name    string
		ceiling bool
		apply   func(int)
	}
	items := []item{
		{"SURFACE_SOURCE_ALLOWANCE", false, func(n int) { cfg.SourceAllowance = n }},
		{"SURFACE_KEY_ALLOWANCE", false, func(n int) { cfg.KeyAllowance = n }},
		{"SURFACE_MAX_KEY_CHARS", true, func(n int) { cfg.MaxKeyChars = n }},
		{"SURFACE_MAX_ADDRESS_CHARS", true, func(n int) { cfg.MaxAddressChars = n }},
		{"SURFACE_MAX_ID_CHARS", true, func(n int) { cfg.MaxIDChars = n }},
		{"SURFACE_MAX_BODY_BYTES", true, func(n int) { cfg.MaxBodyBytes = n }},
	}
	if lookup == nil {
		return cfg, nil
	}
	for _, it := range items {
		raw, ok := lookup(it.name)
		if !ok || raw == "" {
			continue
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("%s not a positive integer", it.name)
		}
		if it.ceiling && n > ceilings[it.name] {
			return Config{}, fmt.Errorf("%s above default", it.name)
		}
		it.apply(n)
	}
	return cfg, nil
}
