package mailbox

import (
	"bytes"
	"context"
	"database/sql"
	"time"
)

type Shelf struct {
	db  *sql.DB
	ttl time.Duration
	now func() time.Time
}

func NewShelf(db *sql.DB, ttl time.Duration, now func() time.Time) (*Shelf, error) {
	if db == nil || ttl <= 0 {
		return nil, ErrInvalid
	}
	if now == nil {
		now = time.Now
	}
	return &Shelf{db: db, ttl: ttl, now: now}, nil
}

func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS mailbox (
			id TEXT PRIMARY KEY,
			slot TEXT NOT NULL,
			body BLOB NOT NULL,
			expires_at INTEGER NOT NULL
		)`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS mailbox_slot ON mailbox (slot, expires_at)`)
	return err
}

func Remove(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS mailbox`)
	return err
}

// Stage inserts one body for id with expires_at = now+ttl.
// The same id and the same bytes leave expires_at unchanged.
// The same id and different bytes return ErrConflict.
func (s *Shelf) Stage(ctx context.Context, id DropID, slot Slot, body Ciphertext) error {
	if s == nil || s.db == nil {
		return ErrInvalid
	}
	n := len(body.raw)
	if n < headerLen || n > MaxBytes {
		return ErrTooBig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	exp := s.now().Add(s.ttl).Unix()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO mailbox (id, slot, body, expires_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING`, id.String(), slot.String(), body.raw, exp); err != nil {
		return err
	}
	var existing []byte
	if err := tx.QueryRowContext(ctx, `SELECT body FROM mailbox WHERE id = ?`, id.String()).Scan(&existing); err != nil {
		return err
	}
	if !bytes.Equal(existing, body.raw) {
		return ErrConflict
	}
	return tx.Commit()
}

// Pull returns at most 32 unexpired rows for slot, oldest expiry first.
// It does not delete.
func (s *Shelf) Pull(ctx context.Context, slot Slot, now time.Time) ([]Stored, error) {
	if s == nil || s.db == nil {
		return nil, ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, body FROM mailbox
		WHERE slot = ? AND expires_at > ?
		ORDER BY expires_at ASC, id ASC
		LIMIT ?`, slot.String(), now.Unix(), pullLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Stored{}
	for rows.Next() {
		var idText string
		var blob []byte
		if err := rows.Scan(&idText, &blob); err != nil {
			return nil, err
		}
		id, err := ParseDropID(idText)
		if err != nil {
			return nil, ErrInvalid
		}
		out = append(out, Stored{ID: id, Body: Ciphertext{raw: append([]byte(nil), blob...)}})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Shelf) Delete(ctx context.Context, id DropID) error {
	if s == nil || s.db == nil {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM mailbox WHERE id = ?`, id.String())
	return err
}

func (s *Shelf) Expire(ctx context.Context, now time.Time) error {
	if s == nil || s.db == nil {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM mailbox WHERE expires_at <= ?`, now.Unix())
	return err
}

// RunExpire deletes due rows on each tick until ctx ends.
// The returned function waits until that loop has stopped.
func (s *Shelf) RunExpire(ctx context.Context, every time.Duration) func() {
	if every <= 0 {
		every = time.Second
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s != nil {
					_ = s.Expire(context.Background(), s.now())
				}
			}
		}
	}()
	return func() { <-done }
}

func (s *Shelf) count(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox`).Scan(&n)
	return n, err
}
