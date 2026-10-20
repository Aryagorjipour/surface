package connector

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Aryagorjipour/surface/internal/mailbox"
)

func TestMailboxUnset(t *testing.T) {
	store := testStore(t)
	names := mailboxTables(t, store.db)
	if len(names) != 1 || names[0] != "peers" {
		t.Fatalf("migrate tables %v", names)
	}
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux)
	id := strings.Repeat("ab", 16)
	rec := callMux(mux, http.MethodPut, "/mailbox/"+id, `{"slot":"`+strings.Repeat("cd", 32)+`","body":"aaaa"}`, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unset put %d", rec.Code)
	}

	ctx := context.Background()
	if err := mailbox.Migrate(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	mux2 := http.NewServeMux()
	h.Register(mux2)
	wait, err := h.MountMailbox(ctx, mux2, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wait != nil {
		t.Fatal("zero ttl started expiry")
	}
	names = mailboxTables(t, store.db)
	if len(names) != 1 || names[0] != "peers" {
		t.Fatalf("removed tables %v", names)
	}
	rec = callMux(mux2, http.MethodPut, "/mailbox/"+id, `{"slot":"`+strings.Repeat("cd", 32)+`","body":"aaaa"}`, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("zero ttl put %d", rec.Code)
	}
}

func TestMailboxRoundTrip(t *testing.T) {
	store, srv, cleanup := mountMailboxServer(t, time.Hour)
	defer cleanup()
	ctx := context.Background()
	root := t.TempDir()
	alice := openTestBox(t, filepath.Join(root, "alice"), "alice", srv.URL)
	bob := openTestBox(t, filepath.Join(root, "bob"), "bob", srv.URL)
	if err := alice.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if err := bob.Hold("alice", alice.Public().String()); err != nil {
		t.Fatal(err)
	}
	id, err := alice.Leave(ctx, "bob", "meet at the dock")
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	if err := store.db.QueryRowContext(ctx, `SELECT body FROM mailbox WHERE id = ?`, id.String()).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("meet at the dock")) {
		t.Fatal("plaintext on the shelf")
	}
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/mailbox/"+mailbox.SlotOf(bob.Public()).String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testBearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pull %d %s", resp.StatusCode, wire)
	}
	if bytes.Contains(wire, []byte("meet at the dock")) {
		t.Fatalf("pull leaked plaintext %s", wire)
	}
	var rows []map[string]any
	if err := json.Unmarshal(wire, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != id.String() || rows[0]["body"] == "" {
		t.Fatalf("pull rows %+v", rows)
	}
	if _, ok := rows[0]["from"]; ok || len(rows[0]) != 2 {
		t.Fatalf("pull fields %+v", rows[0])
	}

	var got []mailbox.Delivered
	show := func(d mailbox.Delivered) error {
		got = append(got, d)
		return nil
	}
	if err := bob.Collect(ctx, show); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].From.String() != "alice" || got[0].Text != "meet at the dock" {
		t.Fatalf("collect %+v", got)
	}
	got = nil
	if err := bob.Collect(ctx, show); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("second collect %+v", got)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO mailbox (id, slot, body, expires_at) VALUES (?, ?, ?, ?)`,
		id.String(), mailbox.SlotOf(bob.Public()).String(), body, time.Now().Add(time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := bob.Collect(ctx, show); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("resurrected %+v", got)
	}
	var left int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE id = ?`, id.String()).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("resurrected row %d", left)
	}
}

func TestMailboxTamperAndUnknown(t *testing.T) {
	store, srv, cleanup := mountMailboxServer(t, time.Hour)
	defer cleanup()
	ctx := context.Background()
	root := t.TempDir()
	alice := openTestBox(t, filepath.Join(root, "alice"), "alice", srv.URL)
	bob := openTestBox(t, filepath.Join(root, "bob"), "bob", srv.URL)
	if err := alice.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if err := bob.Hold("alice", alice.Public().String()); err != nil {
		t.Fatal(err)
	}
	id, err := alice.Leave(ctx, "bob", "meet at the dock")
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	if err := store.db.QueryRowContext(ctx, `SELECT body FROM mailbox WHERE id = ?`, id.String()).Scan(&body); err != nil {
		t.Fatal(err)
	}
	body[len(body)-1] ^= 0xff
	if _, err := store.db.ExecContext(ctx, `UPDATE mailbox SET body = ? WHERE id = ?`, body, id.String()); err != nil {
		t.Fatal(err)
	}
	var shown int
	if err := bob.Collect(ctx, func(mailbox.Delivered) error {
		shown++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if shown != 0 {
		t.Fatalf("tamper shown %d", shown)
	}
	var n int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE id = ?`, id.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("tamper row %d", n)
	}

	stranger := openTestBox(t, filepath.Join(root, "stranger"), "stranger", srv.URL)
	if err := alice.Hold("stranger", stranger.Public().String()); err != nil {
		t.Fatal(err)
	}
	sid, err := alice.Leave(ctx, "stranger", "hidden line")
	if err != nil {
		t.Fatal(err)
	}
	shown = 0
	if err := stranger.Collect(ctx, func(mailbox.Delivered) error {
		shown++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if shown != 0 {
		t.Fatalf("unknown sender shown %d", shown)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE id = ?`, sid.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("unknown sender row %d", n)
	}
}

func TestMailboxLeaveRefusals(t *testing.T) {
	store, srv, cleanup := mountMailboxServer(t, time.Hour)
	defer cleanup()
	ctx := context.Background()
	root := t.TempDir()
	alice := openTestBox(t, filepath.Join(root, "alice"), "alice", srv.URL)
	bob := openTestBox(t, filepath.Join(root, "bob"), "bob", srv.URL)
	if _, err := alice.Leave(ctx, "bob", "nope"); !errors.Is(err, mailbox.ErrNoKey) {
		t.Fatalf("no key: %v", err)
	}
	if rows := mailboxCount(t, store); rows != 0 || outboxFiles(t, filepath.Join(root, "alice")) != 0 {
		t.Fatalf("no key rows %d", rows)
	}
	busy := openTestBoxBusy(t, filepath.Join(root, "busy"), "alice", srv.URL)
	if err := busy.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Leave(ctx, "bob", "nope"); !errors.Is(err, mailbox.ErrBusy) {
		t.Fatalf("busy: %v", err)
	}
	if rows := mailboxCount(t, store); rows != 0 || outboxFiles(t, filepath.Join(root, "busy")) != 0 {
		t.Fatalf("busy rows %d", rows)
	}
	if err := alice.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Leave(ctx, "bob", strings.Repeat("x", mailbox.MaxPlain+1)); !errors.Is(err, mailbox.ErrTooBig) {
		t.Fatalf("long: %v", err)
	}
	if rows := mailboxCount(t, store); rows != 0 || outboxFiles(t, filepath.Join(root, "alice")) != 0 {
		t.Fatalf("long rows %d", rows)
	}
}

func TestMailboxStageReplayAndDelete(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	h.SetNow(func() time.Time { return now })
	mux := http.NewServeMux()
	h.Register(mux)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait, err := h.MountMailbox(ctx, mux, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if wait != nil {
			wait()
		}
	}()
	bare := callMuxToken(mux, http.MethodPut, "/mailbox/"+strings.Repeat("ab", 16), `{}`, "", "")
	if bare.Code != http.StatusUnauthorized {
		t.Fatalf("bearer %d", bare.Code)
	}
	bad := callMux(mux, http.MethodPut, "/mailbox/"+strings.Repeat("ab", 16), `{"slot":"nope","body":"@@"}`, "")
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("bad body %d", bad.Code)
	}
	id := strings.Repeat("cd", 16)
	slot := strings.Repeat("ef", 32)
	raw := bytes.Repeat([]byte{7}, 80)
	payload := `{"slot":"` + slot + `","body":"` + base64.StdEncoding.EncodeToString(raw) + `"}`
	first := callMux(mux, http.MethodPut, "/mailbox/"+id, payload, "")
	if first.Code != http.StatusOK {
		t.Fatalf("stage %d %s", first.Code, first.Body.String())
	}
	var exp int64
	if err := store.db.QueryRowContext(ctx, `SELECT expires_at FROM mailbox WHERE id = ?`, id).Scan(&exp); err != nil {
		t.Fatal(err)
	}
	if exp != now.Add(time.Hour).Unix() {
		t.Fatalf("expires %d", exp)
	}
	now = now.Add(5 * time.Hour)
	second := callMux(mux, http.MethodPut, "/mailbox/"+id, payload, "")
	if second.Code != http.StatusOK {
		t.Fatalf("replay %d", second.Code)
	}
	var exp2 int64
	var n int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(expires_at) FROM mailbox WHERE id = ?`, id).Scan(&n, &exp2); err != nil {
		t.Fatal(err)
	}
	if n != 1 || exp2 != exp {
		t.Fatalf("replay rows %d expires %d", n, exp2)
	}
	other := append([]byte(nil), raw...)
	other[0] ^= 0xff
	changed := `{"slot":"` + slot + `","body":"` + base64.StdEncoding.EncodeToString(other) + `"}`
	conflict := callMux(mux, http.MethodPut, "/mailbox/"+id, changed, "")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict %d", conflict.Code)
	}
	missing := callMux(mux, http.MethodDelete, "/mailbox/"+strings.Repeat("12", 16), "", "")
	if missing.Code != http.StatusOK {
		t.Fatalf("delete missing %d", missing.Code)
	}
	gone := callMux(mux, http.MethodDelete, "/mailbox/"+id, "", "")
	if gone.Code != http.StatusOK {
		t.Fatalf("delete %d", gone.Code)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rows left %d", n)
	}
}

func TestMailboxExpireBeforeCollect(t *testing.T) {
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	h.SetNow(func() time.Time { return now })
	mux := http.NewServeMux()
	h.Register(mux)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ttl := time.Hour
	wait, err := h.MountMailbox(ctx, mux, ttl, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if wait != nil {
			wait()
		}
	}()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	alice := openTestBox(t, filepath.Join(t.TempDir(), "alice"), "alice", srv.URL)
	bob := openTestBox(t, filepath.Join(t.TempDir(), "bob"), "bob", srv.URL)
	if err := alice.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if err := bob.Hold("alice", alice.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Leave(ctx, "bob", "late"); err != nil {
		t.Fatal(err)
	}
	if mailboxCount(t, store) != 1 {
		t.Fatal("missing staged row")
	}
	now = now.Add(ttl)
	shelf, err := mailbox.NewShelf(store.db, ttl, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.Expire(ctx, now); err != nil {
		t.Fatal(err)
	}
	if mailboxCount(t, store) != 0 {
		t.Fatal("expire left the row")
	}
	var shown int
	if err := bob.Collect(ctx, func(mailbox.Delivered) error {
		shown++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if shown != 0 {
		t.Fatalf("expired line shown %d", shown)
	}
}

func mountMailboxServer(t *testing.T, ttl time.Duration) (*Store, *httptest.Server, func()) {
	t.Helper()
	store := testStore(t)
	h := newTestHandlers(store, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux)
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := h.MountMailbox(ctx, mux, ttl, time.Hour)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := httptest.NewServer(mux)
	return store, srv, func() {
		srv.Close()
		cancel()
		if wait != nil {
			wait()
		}
	}
}

func openTestBox(t *testing.T, dir, me, endpoint string) *mailbox.Box {
	t.Helper()
	return openTestBoxBusyFunc(t, dir, me, endpoint, func(string) bool { return false })
}

func openTestBoxBusy(t *testing.T, dir, me, endpoint string) *mailbox.Box {
	t.Helper()
	return openTestBoxBusyFunc(t, dir, me, endpoint, func(string) bool { return true })
}

func openTestBoxBusyFunc(t *testing.T, dir, me, endpoint string, busy func(string) bool) *mailbox.Box {
	t.Helper()
	box, err := mailbox.Open(mailbox.Config{
		Dir:      dir,
		Me:       me,
		TTL:      time.Hour,
		Endpoint: endpoint,
		Bearer:   testBearer,
		Busy:     busy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func mailboxTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return names
}

func mailboxCount(t *testing.T, store *Store) int {
	t.Helper()
	var n int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM mailbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func outboxFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			n++
		}
	}
	return n
}
