package connector

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

type Store struct {
	db     *sql.DB
	encKey []byte
	now    func() time.Time
	window time.Duration
}

// SetNow replaces the clock used for last_seen and staleness.
func (s *Store) SetNow(now func() time.Time) {
	if s == nil || now == nil {
		return
	}
	s.now = now
}

// SetSilenceWindow installs the operator window. Open does not choose it.
func (s *Store) SetSilenceWindow(window time.Duration) {
	if s == nil {
		return
	}
	s.window = window
}

func (s *Store) clock() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func (s *Store) silenceWindow() time.Duration {
	if s.window > 0 {
		return s.window
	}
	return DefaultExpiryConfig().SilenceWindow
}

func Open(ctx context.Context, url, token string, encKey []byte) (*Store, error) {
	if len(encKey) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes")
	}

	dsn := url
	isFile := strings.HasPrefix(url, "file:")

	if isFile {
		path := strings.TrimPrefix(url, "file:")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("create data dir: %w", err)
		}
	} else if token != "" {
		dsn = fmt.Sprintf("%s?authToken=%s", url, token)
	}

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open turso: %w", err)
	}
	// A file database is one SQLite file. A second connection returns SQLITE_BUSY.
	if isFile {
		db.SetMaxOpenConns(1)
	}

	if err := db.PingContext(ctx); err != nil {
		if cerr := db.Close(); cerr != nil {
			return nil, fmt.Errorf("ping turso: %w; close: %v", err, cerr)
		}
		return nil, fmt.Errorf("ping turso: %w", err)
	}

	s := &Store{db: db, encKey: encKey}
	if err := s.migrate(ctx); err != nil {
		if cerr := db.Close(); cerr != nil {
			return nil, fmt.Errorf("migrate: %w; close: %v", err, cerr)
		}
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
	CREATE TABLE IF NOT EXISTS peers (
		key TEXT PRIMARY KEY,
		address_enc BLOB NOT NULL,
		address_nonce BLOB NOT NULL,
		state TEXT NOT NULL,
		last_seen INTEGER NOT NULL
	);`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) UpsertPeer(ctx context.Context, key, address, state string) error {
	if key == "" || address == "" || state == "" {
		return fmt.Errorf("key, address and state are required")
	}

	ct, nonce, err := encrypt(s.encKey, address)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	const q = `
	INSERT INTO peers (key, address_enc, address_nonce, state, last_seen)
	VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(key) DO UPDATE SET
		address_enc = excluded.address_enc,
		address_nonce = excluded.address_nonce,
		state = excluded.state,
		last_seen = excluded.last_seen;`

	_, err = s.db.ExecContext(ctx, q, key, ct, nonce, state, s.clock().Unix())
	return err
}

// Prune deletes peers whose last_seen is strictly older than now-window.
// It returns the deleted keys. It does not decrypt addresses and it does not log keys.
func (s *Store) Prune(ctx context.Context, now time.Time, window time.Duration) ([]string, error) {
	cutoff := now.Add(-window).Unix()
	rows, err := s.db.QueryContext(ctx, `SELECT key FROM peers WHERE last_seen < ?`, cutoff)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM peers WHERE last_seen < ?`, cutoff); err != nil {
		return nil, err
	}
	return keys, nil
}

// CountLivePeers counts rows whose last_seen is still inside the silence window.
// The query does not select keys and does not decrypt addresses.
func (s *Store) CountLivePeers(ctx context.Context, now time.Time, window time.Duration) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("no store")
	}
	cutoff := now.Add(-window).Unix()
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peers WHERE last_seen >= ?`, cutoff).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) GetPeer(ctx context.Context, key string) (address, state string, lastSeen time.Time, err error) {
	const q = `SELECT address_enc, address_nonce, state, last_seen FROM peers WHERE key = ?`
	var ct, nonce []byte
	var ts int64
	err = s.db.QueryRowContext(ctx, q, key).Scan(&ct, &nonce, &state, &ts)
	if err == sql.ErrNoRows {
		return "", "", time.Time{}, fmt.Errorf("peer not found")
	}
	if err != nil {
		return "", "", time.Time{}, err
	}
	if PeerStale(ts, s.clock(), s.silenceWindow()) {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM peers WHERE key = ?`, key); err != nil {
			return "", "", time.Time{}, err
		}
		return "", "", time.Time{}, fmt.Errorf("peer not found")
	}

	address, err = decrypt(s.encKey, ct, nonce)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("decrypt: %w", err)
	}
	lastSeen = time.Unix(ts, 0)
	return address, state, lastSeen, nil
}

func encrypt(key []byte, plaintext string) (ciphertext, nonce []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	ciphertext = gcm.Seal(nil, nonce, []byte(plaintext), nil)
	return ciphertext, nonce, nil
}

func decrypt(key, ciphertext, nonce []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	pt, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
