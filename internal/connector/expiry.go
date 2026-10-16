package connector

import (
	"fmt"
	"time"
)

// ExpiryConfig is the two durations for one Connector process.
type ExpiryConfig struct {
	RequestLifetime time.Duration
	SilenceWindow   time.Duration
}

// DefaultExpiryConfig returns a 10-minute request lifetime and a 24-hour silence window.
func DefaultExpiryConfig() ExpiryConfig {
	return ExpiryConfig{
		RequestLifetime: 10 * time.Minute,
		SilenceWindow:   24 * time.Hour,
	}
}

// ParseExpiryConfig reads the two durations through lookup.
// It does not read the process environment itself.
// A missing or empty value keeps the default. A zero, negative, or unreadable value is refused.
// The error names the variable and does not include the raw text.
func ParseExpiryConfig(lookup func(string) (string, bool)) (ExpiryConfig, error) {
	cfg := DefaultExpiryConfig()
	if lookup == nil {
		return cfg, nil
	}
	var err error
	cfg.RequestLifetime, err = positiveDuration(lookup, "SURFACE_REQUEST_LIFETIME", cfg.RequestLifetime)
	if err != nil {
		return ExpiryConfig{}, err
	}
	cfg.SilenceWindow, err = positiveDuration(lookup, "SURFACE_SILENCE_WINDOW", cfg.SilenceWindow)
	if err != nil {
		return ExpiryConfig{}, err
	}
	return cfg, nil
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

// RequestDeadline is created+lifetime until a decision, then decided+lifetime.
// A status read is not an input.
func RequestDeadline(created, decided time.Time, decidedSet bool, lifetime time.Duration) time.Time {
	if decidedSet {
		return decided.Add(lifetime)
	}
	return created.Add(lifetime)
}

// RequestReadable reports whether now is still at or before deadline.
// A zero deadline is already expired for any later now.
func RequestReadable(now, deadline time.Time) bool {
	return !now.After(deadline)
}

// PeerStale reports whether lastSeenUnix is strictly older than now-window.
// An equal Unix second is fresh. A future timestamp is fresh.
func PeerStale(lastSeenUnix int64, now time.Time, window time.Duration) bool {
	return lastSeenUnix < now.Add(-window).Unix()
}
