package mailbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "m.db")
	db, err := sql.Open("libsql", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	return db
}

func mustPeer(t *testing.T, name string) PeerName {
	t.Helper()
	n, err := ParsePeer(name)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustLine(t *testing.T, text string) Line {
	t.Helper()
	ln, err := ParseLine(text)
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func mustID(t *testing.T) DropID {
	t.Helper()
	id, err := NewDropID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSealRoundTrip(t *testing.T) {
	alice, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	carol, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	from := mustPeer(t, "alice")
	to := Held{name: mustPeer(t, "bob"), key: bob.Public()}
	id := mustID(t)
	ct, err := seal(alice, from, to, id, mustLine(t, "meet at the dock"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ct.raw, []byte("meet at the dock")) || bytes.Contains(ct.raw, []byte("alice")) {
		t.Fatal("plaintext in body")
	}
	opened, err := unseal(bob, Held{name: from, key: alice.Public()}, id, ct)
	if err != nil {
		t.Fatal(err)
	}
	if opened.From.String() != "alice" || opened.Text != "meet at the dock" || opened.ID != id {
		t.Fatalf("%+v", opened)
	}
	if _, err := unseal(carol, Held{name: from, key: alice.Public()}, id, ct); !errors.Is(err, ErrUnauth) {
		t.Fatalf("other identity: %v", err)
	}
	other := mustID(t)
	if _, err := unseal(bob, Held{name: from, key: alice.Public()}, other, ct); !errors.Is(err, ErrUnauth) {
		t.Fatalf("other id: %v", err)
	}
	flipped := Ciphertext{raw: append([]byte(nil), ct.raw...)}
	flipped.raw[len(flipped.raw)-1] ^= 0xff
	if _, err := unseal(bob, Held{name: from, key: alice.Public()}, id, flipped); !errors.Is(err, ErrUnauth) {
		t.Fatalf("flipped: %v", err)
	}
	if _, err := unseal(bob, Held{name: mustPeer(t, "ally"), key: alice.Public()}, id, ct); !errors.Is(err, ErrUnauth) {
		t.Fatalf("name: %v", err)
	}
}

func TestSealMaxPlain(t *testing.T) {
	name := strings.Repeat("😀", 128)
	if len(name) != maxNameBytes || len([]rune(name)) != maxNameRunes {
		t.Fatalf("name bytes %d runes %d", len(name), len([]rune(name)))
	}
	from, err := ParsePeer(name)
	if err != nil {
		t.Fatal(err)
	}
	alice, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	line, err := ParseLine(strings.Repeat("x", MaxPlain))
	if err != nil {
		t.Fatal(err)
	}
	ct, err := seal(alice, from, Held{name: mustPeer(t, "bob"), key: bob.Public()}, mustID(t), line)
	if err != nil {
		t.Fatal(err)
	}
	if len(ct.raw) != MaxBytes {
		t.Fatalf("sealed %d MaxBytes %d", len(ct.raw), MaxBytes)
	}
	if _, err := ParseLine(strings.Repeat("x", MaxPlain+1)); !errors.Is(err, ErrTooBig) {
		t.Fatalf("extra byte: %v", err)
	}
	proof := "cHJvb2YtcHJvb2YtcHJvb2YtcHJvb2YtcHJvb2YtcHJvZg=="
	if _, err := ParseRecipientKey(proof); !errors.Is(err, ErrInvalid) {
		t.Fatalf("proof parsed: %v", err)
	}
}

func TestShelfIdempotentExpireDelete(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if err := Remove(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	names := tableNames(t, db)
	if len(names) != 1 || names[0] != "mailbox" {
		t.Fatalf("tables %v", names)
	}
	var indexes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'mailbox_slot'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatalf("mailbox_slot indexes %d", indexes)
	}
	now := time.Unix(1_700_000_000, 0)
	shelf, err := NewShelf(db, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewShelf(nil, time.Hour, time.Now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil db: %v", err)
	}
	if _, err := NewShelf(db, 0, time.Now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero ttl: %v", err)
	}
	id := mustID(t)
	var slot Slot
	slot.b[0] = 9
	body := Ciphertext{raw: bytes.Repeat([]byte{2}, headerLen)}
	if err := shelf.Stage(ctx, id, slot, body); err != nil {
		t.Fatal(err)
	}
	var exp int64
	if err := db.QueryRowContext(ctx, `SELECT expires_at FROM mailbox WHERE id = ?`, id.String()).Scan(&exp); err != nil {
		t.Fatal(err)
	}
	want := now.Add(time.Hour).Unix()
	if exp != want {
		t.Fatalf("expires %d want %d", exp, want)
	}
	now = now.Add(3 * time.Hour)
	if err := shelf.Stage(ctx, id, slot, body); err != nil {
		t.Fatal(err)
	}
	var exp2 int64
	var got []byte
	if err := db.QueryRowContext(ctx, `SELECT body, expires_at FROM mailbox WHERE id = ?`, id.String()).Scan(&got, &exp2); err != nil {
		t.Fatal(err)
	}
	if exp2 != want || !bytes.Equal(got, body.raw) {
		t.Fatalf("replay expires %d body changed %v", exp2, !bytes.Equal(got, body.raw))
	}
	n, err := shelf.count(ctx)
	if err != nil || n != 1 {
		t.Fatalf("count %d %v", n, err)
	}
	other := Ciphertext{raw: append([]byte(nil), body.raw...)}
	other.raw[0] ^= 0xff
	if err := shelf.Stage(ctx, id, slot, other); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT body, expires_at FROM mailbox WHERE id = ?`, id.String()).Scan(&got, &exp2); err != nil {
		t.Fatal(err)
	}
	if exp2 != want || !bytes.Equal(got, body.raw) {
		t.Fatal("conflict changed the row")
	}
	shortBody := Ciphertext{raw: []byte{1, 2, 3}}
	if err := shelf.Stage(ctx, mustID(t), slot, shortBody); !errors.Is(err, ErrTooBig) {
		t.Fatalf("short: %v", err)
	}
	huge := Ciphertext{raw: bytes.Repeat([]byte{3}, MaxBytes+1)}
	if err := shelf.Stage(ctx, mustID(t), slot, huge); !errors.Is(err, ErrTooBig) {
		t.Fatalf("huge: %v", err)
	}
	missing := mustID(t)
	if err := shelf.Delete(ctx, missing); err != nil {
		t.Fatal(err)
	}
	if n, err = shelf.count(ctx); err != nil || n != 1 {
		t.Fatalf("after missing delete %d %v", n, err)
	}
	if err := shelf.Expire(ctx, time.Unix(want, 0)); err != nil {
		t.Fatal(err)
	}
	if n, err = shelf.count(ctx); err != nil || n != 0 {
		t.Fatalf("after expire %d %v", n, err)
	}
	pulled, err := shelf.Pull(ctx, slot, time.Unix(want, 0))
	if err != nil || len(pulled) != 0 {
		t.Fatalf("pull after expire %d %v", len(pulled), err)
	}
	if err := Remove(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := Remove(ctx, db); err != nil {
		t.Fatal(err)
	}
	names = tableNames(t, db)
	if len(names) != 0 {
		t.Fatalf("after remove %v", names)
	}
}

func TestPullLimit(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	shelf, err := NewShelf(db, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var slot Slot
	slot.b[1] = 4
	body := Ciphertext{raw: bytes.Repeat([]byte{1}, headerLen)}
	ids := make([]DropID, 33)
	for i := range ids {
		ids[i] = mustID(t)
		if err := shelf.Stage(ctx, ids[i], slot, body); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	got, err := shelf.Pull(ctx, slot, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pullLimit {
		t.Fatalf("pull %d", len(got))
	}
	for i := range got {
		if got[i].ID != ids[i] {
			t.Fatalf("order %d got %s want %s", i, got[i].ID, ids[i])
		}
	}
	again, err := shelf.Pull(ctx, slot, now)
	if err != nil || len(again) != len(got) {
		t.Fatalf("second pull %d %v", len(again), err)
	}
}

func TestOpenHoldLeaveLocal(t *testing.T) {
	root := t.TempDir()
	off := filepath.Join(root, "off")
	if _, err := Open(Config{Dir: off, Me: "alice", TTL: 0, Busy: func(string) bool { return false }}); !errors.Is(err, ErrOff) {
		t.Fatalf("off: %v", err)
	}
	if _, err := os.Stat(off); !os.IsNotExist(err) {
		t.Fatal("ttl zero created a directory")
	}
	none := filepath.Join(root, "none")
	if _, err := Open(Config{Dir: none, Me: "alice", TTL: time.Hour}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil busy: %v", err)
	}
	if _, err := os.Stat(none); !os.IsNotExist(err) {
		t.Fatal("nil busy created a directory")
	}

	var puts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dir := filepath.Join(root, "alice")
	box, err := Open(Config{
		Dir: dir, Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, identityFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode %o", fi.Mode().Perm())
	}
	again, err := Open(Config{
		Dir: dir, Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.Public().String() != box.Public().String() {
		t.Fatal("identity was replaced")
	}
	bob, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	bookPath := filepath.Join(dir, bookFile)
	before, err := os.ReadFile(bookPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(bookPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("same hold rewrote the book")
	}
	carol, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", carol.Public().String()); !errors.Is(err, ErrHeld) {
		t.Fatalf("different key: %v", err)
	}
	if err := box.Hold("carol", bob.Public().String()); !errors.Is(err, ErrHeld) {
		t.Fatalf("same key: %v", err)
	}
	mid, err := os.ReadFile(bookPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, mid) {
		t.Fatal("refused hold wrote the book")
	}
	if puts != 0 {
		t.Fatalf("hold used the network %d", puts)
	}

	_, err = box.Leave(context.Background(), "carol", "hi")
	if !errors.Is(err, ErrNoKey) {
		t.Fatalf("no key: %v", err)
	}
	if n := outboxCount(t, dir); n != 0 || puts != 0 {
		t.Fatalf("no key outbox %d puts %d", n, puts)
	}

	busy, err := Open(Config{
		Dir: filepath.Join(root, "busy"), Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return true },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := busy.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Leave(context.Background(), "bob", "hi"); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy: %v", err)
	}
	if n := outboxCount(t, filepath.Join(root, "busy")); n != 0 || puts != 0 {
		t.Fatalf("busy outbox %d puts %d", n, puts)
	}

	longBox, err := Open(Config{
		Dir: filepath.Join(root, "long"), Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := longBox.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := longBox.Leave(context.Background(), "bob", strings.Repeat("x", MaxPlain+1)); !errors.Is(err, ErrTooBig) {
		t.Fatalf("long: %v", err)
	}
	if n := outboxCount(t, filepath.Join(root, "long")); n != 0 || puts != 0 {
		t.Fatalf("long outbox %d puts %d", n, puts)
	}
}

func TestLeaveRetriesSameBytes(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen[r.URL.Path] = append(seen[r.URL.Path], append([]byte(nil), body...))
		n := len(seen[r.URL.Path])
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	bob, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	box, err := Open(Config{
		Dir: dir, Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := box.Leave(context.Background(), "bob", "one"); err == nil || Class(err) != "unavailable" {
		t.Fatalf("first: %v", err)
	}
	if outboxCount(t, dir) != 1 {
		t.Fatal("failed put removed the outbox")
	}
	if _, err := box.Leave(context.Background(), "bob", "two"); err == nil || Class(err) != "unavailable" {
		t.Fatalf("second: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	var retried bool
	for _, bodies := range seen {
		if len(bodies) >= 2 && bytes.Equal(bodies[0], bodies[1]) {
			retried = true
		}
	}
	if !retried {
		t.Fatal("retry did not send the same bytes")
	}
}

func TestLeaveConflictKeepsFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	bob, err := newIdentity()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	box, err := Open(Config{
		Dir: dir, Me: "alice", TTL: time.Hour, Endpoint: srv.URL, Bearer: "tok",
		Busy: func(string) bool { return false },
		HTTP: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Hold("bob", bob.Public().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := box.Leave(context.Background(), "bob", "one"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if outboxCount(t, dir) != 1 {
		t.Fatal("conflict removed the outbox")
	}
}

func tableNames(t *testing.T, db *sql.DB) []string {
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

func outboxCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, outboxDir))
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
