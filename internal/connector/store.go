package connector

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite" // required for local file: DSNs in tests
)

type Store struct {
	db     *sql.DB
	encKey []byte
}

func Open(ctx context.Context, url, token string, encKey []byte) (*Store, error) {
	if len(encKey) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes")
	}

	dsn := url
	if token != "" {
		dsn = fmt.Sprintf("%s?authToken=%s", url, token)
	}

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open turso: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping turso: %w", err)
	}

	s := &Store{db: db, encKey: encKey}
	if err := s.migrate(ctx); err != nil {
		db.Close()
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

	_, err = s.db.ExecContext(ctx, q, key, ct, nonce, state, time.Now().Unix())
	return err
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
