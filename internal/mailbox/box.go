package mailbox

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	identityFile = "identity"
	bookFile     = "book"
	bookMagic    = "mb1"
	outboxDir    = "outbox"
	inboxDir     = "inbox"
)

type Config struct {
	Dir      string
	Me       string
	TTL      time.Duration
	Endpoint string
	Bearer   string
	Busy     func(peer string) bool
	HTTP     *http.Client
}

type Box struct {
	mu   sync.Mutex
	dir  string
	me   PeerName
	id   Identity
	busy func(string) bool
	http *http.Client
	base string
	auth string
	held []Held
}

// Open loads or creates the identity file and the book.
// A non-positive TTL returns ErrOff and does not create the directory.
// A nil Busy is refused.
func Open(cfg Config) (*Box, error) {
	if cfg.TTL <= 0 {
		return nil, ErrOff
	}
	if cfg.Busy == nil {
		return nil, ErrInvalid
	}
	me, err := ParsePeer(cfg.Me)
	if err != nil {
		return nil, err
	}
	if cfg.Dir == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, outboxDir), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, inboxDir), 0o700); err != nil {
		return nil, err
	}
	id, err := loadOrCreateIdentity(filepath.Join(cfg.Dir, identityFile))
	if err != nil {
		return nil, err
	}
	held, err := loadBook(filepath.Join(cfg.Dir, bookFile))
	if err != nil {
		return nil, err
	}
	client := cfg.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	return &Box{
		dir:  cfg.Dir,
		me:   me,
		id:   id,
		busy: cfg.Busy,
		http: client,
		base: strings.TrimRight(cfg.Endpoint, "/"),
		auth: cfg.Bearer,
		held: held,
	}, nil
}

func (b *Box) Public() RecipientKey {
	if b == nil {
		return RecipientKey{}
	}
	return b.id.Public()
}

// Hold records peer to key in the local book.
// The same pair is success.
// A different key for that name, or the same key for another name, is ErrHeld.
// Hold does not use the network.
func (b *Box) Hold(peer, pub string) error {
	if b == nil {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	name, err := ParsePeer(peer)
	if err != nil {
		return err
	}
	key, err := ParseRecipientKey(pub)
	if err != nil {
		return err
	}
	for _, e := range b.held {
		if e.name.s == name.s && e.key.equal(key) {
			return nil
		}
		if e.name.s == name.s || e.key.equal(key) {
			return ErrHeld
		}
	}
	b.held = append(b.held, Held{name: name, key: key})
	return writeBook(filepath.Join(b.dir, bookFile), b.held)
}

// Leave stages one line for peer.
// ErrBusy and ErrNoKey return before that line is sealed.
func (b *Box) Leave(ctx context.Context, peer, line string) (DropID, error) {
	if b == nil {
		return DropID{}, ErrOff
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.flush(ctx); err != nil {
		return DropID{}, err
	}
	if b.busy == nil {
		return DropID{}, ErrInvalid
	}
	if b.busy(peer) {
		return DropID{}, ErrBusy
	}
	name, err := ParsePeer(peer)
	if err != nil {
		return DropID{}, err
	}
	held, ok := b.heldByName(name)
	if !ok {
		return DropID{}, ErrNoKey
	}
	ln, err := ParseLine(line)
	if err != nil {
		return DropID{}, err
	}
	id, err := NewDropID()
	if err != nil {
		return DropID{}, err
	}
	blob, err := seal(b.id, b.me, held, id, ln)
	if err != nil {
		return DropID{}, err
	}
	payload, err := EncodeStage(SlotOf(held.key), blob)
	if err != nil {
		return DropID{}, err
	}
	path := filepath.Join(b.dir, outboxDir, id.String())
	if err := writeFileAtomic(path, payload, 0o600); err != nil {
		return DropID{}, err
	}
	if err := b.put(ctx, id, payload); err != nil {
		return DropID{}, err
	}
	if err := os.Remove(path); err != nil {
		return DropID{}, err
	}
	return id, nil
}

// Collect flushes the outbox, pulls this identity's slot, and opens each body.
// A fingerprint with no book entry stays on the shelf.
// ErrUnauth deletes that row and does not show it.
// An inbox file that already exists deletes a resurrected row and does not call show.
func (b *Box) Collect(ctx context.Context, show func(Delivered) error) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.flush(ctx); err != nil {
		return err
	}
	if err := b.recover(ctx, show); err != nil {
		return err
	}
	rows, err := b.pull(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := b.take(ctx, row, show); err != nil {
			return err
		}
	}
	return nil
}

func (b *Box) heldByName(name PeerName) (Held, bool) {
	for _, e := range b.held {
		if e.name.s == name.s {
			return e, true
		}
	}
	return Held{}, false
}

func (b *Box) heldByFingerprint(fp [fpLen]byte) (Held, bool) {
	for _, e := range b.held {
		if e.key.pub == nil {
			continue
		}
		sum := sha256.Sum256(e.key.pub.Bytes())
		if subtle.ConstantTimeCompare(sum[:], fp[:]) == 1 {
			return e, true
		}
	}
	return Held{}, false
}

func (b *Box) flush(ctx context.Context) error {
	dir := filepath.Join(b.dir, outboxDir)
	names, err := idFiles(dir, "")
	if err != nil {
		return err
	}
	for _, name := range names {
		id, err := ParseDropID(name)
		if err != nil {
			continue
		}
		path := filepath.Join(dir, name)
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := b.put(ctx, id, payload); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func (b *Box) recover(ctx context.Context, show func(Delivered) error) error {
	dir := filepath.Join(b.dir, inboxDir)
	names, err := idFiles(dir, ".new")
	if err != nil {
		return err
	}
	for _, name := range names {
		idText := strings.TrimSuffix(name, ".new")
		id, err := ParseDropID(idText)
		if err != nil {
			continue
		}
		final := filepath.Join(dir, idText)
		fresh := filepath.Join(dir, name)
		if _, err := os.Stat(final); err == nil {
			if err := os.Remove(fresh); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if err := b.finishLocal(ctx, id, fresh, final, show); err != nil {
			return err
		}
	}
	return nil
}

func (b *Box) take(ctx context.Context, row Stored, show func(Delivered) error) error {
	final := filepath.Join(b.dir, inboxDir, row.ID.String())
	if _, err := os.Stat(final); err == nil {
		return b.remove(ctx, row.ID)
	} else if !os.IsNotExist(err) {
		return err
	}
	fresh := final + ".new"
	if _, err := os.Stat(fresh); err == nil {
		return b.finishLocal(ctx, row.ID, fresh, final, show)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	fp, ok := bodyFingerprint(row.Body)
	if !ok {
		return nil
	}
	held, ok := b.heldByFingerprint(fp)
	if !ok {
		return nil
	}
	delivered, err := unseal(b.id, held, row.ID, row.Body)
	if errors.Is(err, ErrUnauth) {
		return b.remove(ctx, row.ID)
	}
	if err != nil {
		return err
	}
	if err := writePlain(fresh, delivered); err != nil {
		return err
	}
	if err := b.remove(ctx, row.ID); err != nil {
		return err
	}
	if show != nil {
		if err := show(delivered); err != nil {
			return err
		}
	}
	return os.Rename(fresh, final)
}

func (b *Box) finishLocal(ctx context.Context, id DropID, fresh, final string, show func(Delivered) error) error {
	delivered, err := readPlain(fresh, id)
	if err != nil {
		return err
	}
	if err := b.remove(ctx, id); err != nil {
		return err
	}
	if show != nil {
		if err := show(delivered); err != nil {
			return err
		}
	}
	return os.Rename(fresh, final)
}

func (b *Box) put(ctx context.Context, id DropID, payload []byte) error {
	code, _, err := b.exchange(ctx, http.MethodPut, b.base+"/mailbox/"+id.String(), payload)
	if err != nil {
		return err
	}
	switch code {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return ErrConflict
	case http.StatusBadRequest:
		return ErrInvalid
	default:
		return errors.New("mailbox unavailable")
	}
}

func (b *Box) pull(ctx context.Context) ([]Stored, error) {
	code, body, err := b.exchange(ctx, http.MethodGet, b.base+"/mailbox/"+SlotOf(b.id.Public()).String(), nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, errors.New("mailbox unavailable")
	}
	return DecodePull(body)
}

func (b *Box) remove(ctx context.Context, id DropID) error {
	code, _, err := b.exchange(ctx, http.MethodDelete, b.base+"/mailbox/"+id.String(), nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return errors.New("mailbox unavailable")
	}
	return nil
}

func (b *Box) exchange(ctx context.Context, method, url string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if b.auth != "" {
		req.Header.Set("Authorization", "Bearer "+b.auth)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	slurp, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, slurp, nil
}

func loadOrCreateIdentity(path string) (Identity, error) {
	raw, err := os.ReadFile(path)
	if err == nil {
		return identityFromSeed(raw)
	}
	if !os.IsNotExist(err) {
		return Identity{}, err
	}
	id, err := newIdentity()
	if err != nil {
		return Identity{}, err
	}
	if err := writeFileAtomic(path, id.priv.Bytes(), 0o600); err != nil {
		return Identity{}, err
	}
	return id, nil
}

func loadBook(path string) ([]Held, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	return decodeBook(raw)
}

func writeBook(path string, held []Held) error {
	raw, err := encodeBook(held)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, raw, 0o600)
}

func encodeBook(held []Held) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(bookMagic)
	var nbuf [2]byte
	for _, e := range held {
		if e.key.pub == nil {
			return nil, ErrInvalid
		}
		name := []byte(e.name.s)
		if len(name) == 0 || len(name) > maxNameBytes {
			return nil, ErrInvalid
		}
		binary.BigEndian.PutUint16(nbuf[:], uint16(len(name)))
		buf.Write(nbuf[:])
		buf.Write(name)
		raw := e.key.pub.Bytes()
		if len(raw) != pubLen {
			return nil, ErrInvalid
		}
		buf.Write(raw)
	}
	return buf.Bytes(), nil
}

func decodeBook(raw []byte) ([]Held, error) {
	if len(raw) < len(bookMagic) || string(raw[:len(bookMagic)]) != bookMagic {
		return nil, ErrInvalid
	}
	rest := raw[len(bookMagic):]
	var out []Held
	for len(rest) > 0 {
		if len(rest) < 2 {
			return nil, ErrInvalid
		}
		n := int(binary.BigEndian.Uint16(rest[:2]))
		rest = rest[2:]
		if n < 1 || n > maxNameBytes || len(rest) < n+pubLen {
			return nil, ErrInvalid
		}
		name, err := ParsePeer(string(rest[:n]))
		if err != nil {
			return nil, err
		}
		rest = rest[n:]
		pub, err := ecdh.X25519().NewPublicKey(rest[:pubLen])
		if err != nil {
			return nil, ErrInvalid
		}
		rest = rest[pubLen:]
		out = append(out, Held{name: name, key: RecipientKey{pub: pub}})
	}
	return out, nil
}

func writePlain(path string, d Delivered) error {
	name := []byte(d.From.s)
	if len(name) == 0 || len(name) > maxNameBytes {
		return ErrInvalid
	}
	buf := make([]byte, 2+len(name)+len(d.Text))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(name)))
	copy(buf[2:], name)
	copy(buf[2+len(name):], d.Text)
	return writeFileAtomic(path, buf, 0o600)
}

func readPlain(path string, id DropID) (Delivered, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Delivered{}, err
	}
	if len(raw) < 2 {
		return Delivered{}, ErrInvalid
	}
	n := int(binary.BigEndian.Uint16(raw[:2]))
	if n < 1 || n > maxNameBytes || len(raw) < 2+n {
		return Delivered{}, ErrInvalid
	}
	name, err := ParsePeer(string(raw[2 : 2+n]))
	if err != nil {
		return Delivered{}, err
	}
	return Delivered{ID: id, From: name, Text: string(raw[2+n:])}, nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func idFiles(dir, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}
		if strings.HasPrefix(name, ".") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
