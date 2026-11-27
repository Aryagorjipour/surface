package main

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLimitsFile(t *testing.T) {
	dir := t.TempDir()
	other := t.TempDir()
	loaded, err := readLimits(dir)
	if err != nil || loaded.present {
		t.Fatalf("absent %+v %v", loaded, err)
	}
	if _, err := saveLimits(dir, 0, 4, ""); !errors.Is(err, errAlreadySaved) {
		t.Fatalf("save 4: %v", err)
	}
	docPath, _ := limitsPaths(dir)
	if _, err := os.Stat(docPath); !os.IsNotExist(err) {
		t.Fatal("already-saved created a file")
	}
	got, err := saveLimits(dir, 0, 6, "   ")
	if err != nil || !got.present || got.version != 1 || got.limit != 6 || len(got.audit) != 1 {
		t.Fatalf("first %+v %v", got, err)
	}
	row := got.audit[0]
	if row.Setting != "chat_limit" || row.Previous != 4 || row.New != 6 || row.Version != 1 || row.Reason != "" {
		t.Fatalf("row %+v", row)
	}
	if _, err := time.Parse(time.RFC3339, row.At); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(docPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", info.Mode().Perm())
	}
	long := strings.Repeat("x", 201)
	if _, err := saveLimits(dir, 1, 7, long); !errors.Is(err, errReasonLong) {
		t.Fatalf("reason %v", err)
	}
	same, err := readLimits(dir)
	if err != nil || same.version != 1 || len(same.audit) != 1 {
		t.Fatalf("unchanged %+v %v", same, err)
	}
	if _, err := saveLimits(dir, 1, 6, ""); !errors.Is(err, errAlreadySaved) {
		t.Fatalf("already %v", err)
	}
	again, err := readLimits(dir)
	if err != nil || again.version != 1 {
		t.Fatalf("version %+v %v", again, err)
	}
	applied, err := applyLimits(dir, 1, 1)
	if err != nil || applied.version != 2 || applied.limit != 6 || len(applied.audit) != 2 {
		t.Fatalf("apply %+v %v", applied, err)
	}
	last := applied.audit[1]
	if last.Previous != 6 || last.New != 6 || last.Version != 2 {
		t.Fatalf("reapply %+v", last)
	}
	if _, err := saveLimits(dir, 1, 8, "stale"); !errors.Is(err, errLimitConflict) {
		t.Fatalf("conflict %v", err)
	}
	kept, err := readLimits(dir)
	if err != nil || kept.version != 2 || kept.limit != 6 {
		t.Fatalf("kept %+v %v", kept, err)
	}
	for n := 0; n < 99; n++ {
		cur, err := readLimits(dir)
		if err != nil {
			t.Fatal(err)
		}
		next := 5
		if cur.limit == 5 {
			next = 6
		}
		if _, err := saveLimits(dir, cur.version, next, ""); err != nil {
			t.Fatalf("save %d: %v", n, err)
		}
	}
	full, err := readLimits(dir)
	if err != nil || len(full.audit) != 100 {
		t.Fatalf("cap %d %v", len(full.audit), err)
	}
	if full.audit[0].Version != 2 {
		t.Fatalf("renumbered %+v", full.audit[0])
	}
	if _, err := applyLimits(dir, full.version, 1); !errors.Is(err, errAuditMissing) {
		t.Fatalf("missing %v", err)
	}
	if _, err := saveLimits(other, 0, 8, "elsewhere"); err != nil {
		t.Fatal(err)
	}
	left, err := readLimits(dir)
	if err != nil || left.version != full.version {
		t.Fatalf("other dir changed this one %+v", left)
	}
	prev := renameLimits
	renameLimits = func(string, string) error { return errors.New("rename") }
	t.Cleanup(func() { renameLimits = prev })
	before, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	cur, err := readLimits(dir)
	if err != nil {
		t.Fatal(err)
	}
	next := 5
	if cur.limit == 5 {
		next = 6
	}
	if _, err := saveLimits(dir, cur.version, next, "nope"); !errors.Is(err, errSaveFailed) {
		t.Fatalf("rename %v", err)
	}
	after, err := os.ReadFile(docPath)
	if err != nil || string(after) != string(before) {
		t.Fatal("rename changed the file")
	}
	raw := string(before)
	if strings.Contains(raw, "hello chat") || strings.Contains(raw, "127.0.0.1") || strings.Contains(raw, "peer-key") {
		t.Fatalf("file %s", raw)
	}
}
