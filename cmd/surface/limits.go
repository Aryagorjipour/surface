package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	limitsDirName  = "surface-limits"
	limitsFileName = "config.json"
	limitsLockName = "config.lock"
	auditSetting   = "chat_limit"
	auditKeep      = 100
	reasonMax      = 200
	limitMin       = 1
	limitMax       = 32
	defaultLimit   = 4
)

var (
	errLimitRange    = errors.New("chat limit must be a whole number from 1 to 32")
	errReasonLong    = errors.New("reason is too long")
	errAlreadySaved  = errors.New("chat limit is already saved")
	errLimitConflict = errors.New("saved limit changed in another session; latest values loaded")
	errAuditMissing  = errors.New("that saved change is no longer available")
	errSaveFailed    = errors.New("the save did not happen")
	errAuditCount    = errors.New("show the most recent 1 to 100 entries")
	errNoAudit       = errors.New("no saved changes")

	renameLimits = os.Rename
)

type auditEntry struct {
	Setting  string `json:"setting"`
	Previous int    `json:"previous"`
	New      int    `json:"new"`
	At       string `json:"at"`
	Version  int    `json:"version"`
	Reason   string `json:"reason,omitempty"`
}

type limitsDocument struct {
	Version int          `json:"version"`
	Limit   int          `json:"limit"`
	Audit   []auditEntry `json:"audit"`
}

type loadedLimits struct {
	present bool
	version int
	limit   int
	audit   []auditEntry
}

func limitsPaths(dir string) (doc, lock string) {
	base := filepath.Join(dir, limitsDirName)
	return filepath.Join(base, limitsFileName), filepath.Join(base, limitsLockName)
}

func readLimits(dir string) (loadedLimits, error) {
	path, lockPath := limitsPaths(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return loadedLimits{}, err
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return loadedLimits{}, err
	}
	defer func() { _ = lock.Close() }()
	if err := lockFile(lock); err != nil {
		return loadedLimits{}, err
	}
	defer func() { _ = unlockFile(lock) }()
	return readLimitsUnlocked(path)
}

func readLimitsUnlocked(path string) (loadedLimits, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return loadedLimits{}, nil
		}
		return loadedLimits{}, err
	}
	var doc limitsDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return loadedLimits{}, err
	}
	return loadedLimits{present: true, version: doc.Version, limit: doc.Limit, audit: append([]auditEntry(nil), doc.Audit...)}, nil
}

func saveLimits(dir string, loadedVersion, sessionLimit int, reason string) (loadedLimits, error) {
	reason = strings.TrimSpace(reason)
	if len([]rune(reason)) > reasonMax {
		return loadedLimits{}, errReasonLong
	}
	if sessionLimit < limitMin || sessionLimit > limitMax {
		return loadedLimits{}, errLimitRange
	}
	return rewriteLimits(dir, loadedVersion, func(cur loadedLimits) (limitsDocument, error) {
		stored := defaultLimit
		if cur.present {
			stored = cur.limit
		}
		if sessionLimit == stored {
			return limitsDocument{}, errAlreadySaved
		}
		next := cur.version + 1
		if !cur.present {
			next = 1
		}
		row := auditEntry{
			Setting:  auditSetting,
			Previous: stored,
			New:      sessionLimit,
			At:       limitStamp(time.Now()),
			Version:  next,
			Reason:   reason,
		}
		doc := limitsDocument{Version: next, Limit: sessionLimit, Audit: append(append([]auditEntry(nil), cur.audit...), row)}
		doc.Audit = trimAudit(doc.Audit)
		return doc, nil
	})
}

func applyLimits(dir string, loadedVersion, rowVersion int) (loadedLimits, error) {
	return rewriteLimits(dir, loadedVersion, func(cur loadedLimits) (limitsDocument, error) {
		if !cur.present {
			return limitsDocument{}, errAuditMissing
		}
		var found *auditEntry
		for i := range cur.audit {
			if cur.audit[i].Version == rowVersion {
				found = &cur.audit[i]
				break
			}
		}
		if found == nil {
			return limitsDocument{}, errAuditMissing
		}
		next := cur.version + 1
		row := auditEntry{
			Setting:  auditSetting,
			Previous: cur.limit,
			New:      found.New,
			At:       limitStamp(time.Now()),
			Version:  next,
		}
		doc := limitsDocument{Version: next, Limit: found.New, Audit: append(append([]auditEntry(nil), cur.audit...), row)}
		doc.Audit = trimAudit(doc.Audit)
		return doc, nil
	})
}

func rewriteLimits(dir string, loadedVersion int, build func(loadedLimits) (limitsDocument, error)) (loadedLimits, error) {
	path, lockPath := limitsPaths(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return loadedLimits{}, err
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return loadedLimits{}, err
	}
	defer func() { _ = lock.Close() }()
	if err := lockFile(lock); err != nil {
		return loadedLimits{}, errSaveFailed
	}
	defer func() { _ = unlockFile(lock) }()
	cur, err := readLimitsUnlocked(path)
	if err != nil {
		return loadedLimits{}, err
	}
	if cur.present && cur.version != loadedVersion || !cur.present && loadedVersion != 0 {
		return cur, errLimitConflict
	}
	doc, err := build(cur)
	if err != nil {
		return cur, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return cur, err
	}
	raw = append(raw, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), "config-*.json")
	if err != nil {
		return cur, errSaveFailed
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return cur, errSaveFailed
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return cur, errSaveFailed
	}
	if err := tmp.Close(); err != nil {
		return cur, errSaveFailed
	}
	if err := renameLimits(tmpName, path); err != nil {
		return cur, errSaveFailed
	}
	ok = true
	return loadedLimits{present: true, version: doc.Version, limit: doc.Limit, audit: doc.Audit}, nil
}

func trimAudit(rows []auditEntry) []auditEntry {
	if len(rows) <= auditKeep {
		return rows
	}
	return append([]auditEntry(nil), rows[len(rows)-auditKeep:]...)
}

func limitStamp(now time.Time) string {
	now = now.Truncate(time.Second)
	return now.Format(time.RFC3339)
}

func (a *app) limitsDir() string {
	if a != nil && a.limitsRoot != "" {
		return a.limitsRoot
	}
	return "."
}

func (a *app) loadSavedLimit() {
	if a == nil {
		return
	}
	if a.auditShow == 0 {
		a.auditShow = 10
	}
	loaded, err := readLimits(a.limitsDir())
	if err != nil || !loaded.present {
		return
	}
	a.savedPresent = true
	a.savedLimit = loaded.limit
	a.savedVersion = loaded.version
	a.auditRows = loaded.audit
	if !a.maxNamed {
		a.maxChats = loaded.limit
		if a.registry != nil {
			a.registry.SetMax(loaded.limit)
		}
	}
}

func (a *app) rememberLimits(loaded loadedLimits) {
	a.savedPresent = loaded.present
	if !loaded.present {
		return
	}
	a.savedLimit = loaded.limit
	a.savedVersion = loaded.version
	a.auditRows = loaded.audit
}

func (a *app) setSessionLimit(n int) error {
	if n < limitMin || n > limitMax {
		return errLimitRange
	}
	a.maxChats = n
	if a.registry != nil {
		a.registry.SetMax(n)
	}
	a.refreshLimitWarning()
	return nil
}

func (a *app) refreshLimitWarning() {
	open := 0
	max := a.maxChats
	if max < 1 {
		max = defaultLimit
	}
	if a.registry != nil {
		open = a.registry.Len()
		if a.registry.Max() > 0 {
			max = a.registry.Max()
		}
	}
	text, level, notify := limitDecision(open, max)
	changed := text != a.limitWarn
	a.limitWarn = text
	a.limitLevel = level
	if !notify || !changed || text == "" {
		return
	}
	a.limitToast = text
	a.limitToasts = append(a.limitToasts, text)
	a.note(note{kind: noteToast, level: level, text: text})
}

func limitDecision(open, max int) (string, toastLevel, bool) {
	if max > 1 && open == max-1 {
		return "approaching chat limit", toastWarn, false
	}
	if open == max {
		return "chat limit reached", toastError, true
	}
	if open > max {
		return "open chats stay open; new chats are blocked", toastError, true
	}
	return "", toastInfo, false
}

func (a *app) printLimit() {
	a.println("session limit", a.maxChats)
	if a.savedPresent && a.savedLimit != a.maxChats {
		a.println("saved limit", a.savedLimit)
	}
}

func (a *app) saveSessionLimit(reason string) {
	loaded, err := saveLimits(a.limitsDir(), a.savedVersion, a.maxChats, reason)
	if err != nil {
		if errors.Is(err, errLimitConflict) {
			a.rememberLimits(loaded)
			if loaded.present {
				_ = a.setSessionLimit(loaded.limit)
			}
			a.println(errLimitConflict.Error())
			return
		}
		a.println(err.Error())
		return
	}
	a.rememberLimits(loaded)
	a.refreshLimitWarning()
}

func (a *app) applyAudit(version int) {
	loaded, err := applyLimits(a.limitsDir(), a.savedVersion, version)
	if err != nil {
		if errors.Is(err, errLimitConflict) {
			a.rememberLimits(loaded)
			if loaded.present {
				_ = a.setSessionLimit(loaded.limit)
			}
			a.println(errLimitConflict.Error())
			return
		}
		a.println(err.Error())
		return
	}
	a.rememberLimits(loaded)
	_ = a.setSessionLimit(loaded.limit)
}

func (a *app) showAudit(count int) {
	if count < 1 || count > auditKeep {
		a.println(errAuditCount.Error())
		return
	}
	a.auditShow = count
	if !a.savedPresent || len(a.auditRows) == 0 {
		a.println(errNoAudit.Error())
		return
	}
	rows := newestAudit(a.auditRows, count)
	for _, row := range rows {
		if row.Reason == "" {
			a.println(row.Version, row.Previous, row.New, row.At)
			continue
		}
		a.println(row.Version, row.Previous, row.New, row.At, row.Reason)
	}
}

func newestAudit(rows []auditEntry, count int) []auditEntry {
	if count > len(rows) {
		count = len(rows)
	}
	if count < 0 {
		count = 0
	}
	out := append([]auditEntry(nil), rows[len(rows)-count:]...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
