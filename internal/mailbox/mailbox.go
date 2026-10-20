// Package mailbox is the offline drop.
// The shelf stores ciphertext. It does not store a peer name or a public key.
package mailbox

import (
	"crypto/ecdh"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	versionByte  = 1
	idLen        = 16
	fpLen        = 32
	pubLen       = 32
	nonceLen     = 12
	tagLen       = 16
	headerLen    = 1 + fpLen + pubLen + nonceLen
	maxNameRunes = 128
	maxNameBytes = 512
	// MaxPlain is the longest line in bytes after UTF-8 validation.
	MaxPlain  = 1024
	MaxBytes  = headerLen + 2 + maxNameBytes + MaxPlain + tagLen
	pullLimit = 32
	keyPrefix = "x25519:"
	sealInfo  = "surface-mailbox-v1"
)

var (
	ErrOff      = errors.New("mailbox off")
	ErrNoKey    = errors.New("recipient key not held")
	ErrBusy     = errors.New("conversation owns peer")
	ErrHeld     = errors.New("held key differs")
	ErrTooBig   = errors.New("line too long")
	ErrInvalid  = errors.New("invalid drop")
	ErrConflict = errors.New("drop id already stored")
	ErrUnauth   = errors.New("drop unreadable")
)

// Class is the log word for err.
// The word is not a peer name and not a payload.
func Class(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrOff):
		return "off"
	case errors.Is(err, ErrNoKey):
		return "no-key"
	case errors.Is(err, ErrBusy):
		return "busy"
	case errors.Is(err, ErrHeld):
		return "differs"
	case errors.Is(err, ErrTooBig):
		return "too-big"
	case errors.Is(err, ErrInvalid):
		return "invalid"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrUnauth):
		return "unauth"
	default:
		return "unavailable"
	}
}

// Status maps ErrConflict to 409, a local refusal to 400, and any other error to 500.
func Status(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrOff),
		errors.Is(err, ErrNoKey),
		errors.Is(err, ErrBusy),
		errors.Is(err, ErrHeld),
		errors.Is(err, ErrTooBig),
		errors.Is(err, ErrInvalid),
		errors.Is(err, ErrUnauth):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

type PeerName struct{ s string }

// ParsePeer refuses an empty name or a name over 128 runes.
func ParsePeer(s string) (PeerName, error) {
	if s == "" || utf8.RuneCountInString(s) > maxNameRunes || len(s) > maxNameBytes {
		return PeerName{}, ErrInvalid
	}
	return PeerName{s: s}, nil
}

func (n PeerName) String() string { return n.s }

// RecipientKey is an X25519 public key the sender already holds.
// The text form is x25519: and standard base64, so an admission proof does not parse.
type RecipientKey struct {
	pub *ecdh.PublicKey
}

func ParseRecipientKey(text string) (RecipientKey, error) {
	rest, ok := strings.CutPrefix(text, keyPrefix)
	if !ok || rest == "" {
		return RecipientKey{}, ErrInvalid
	}
	raw, err := base64.StdEncoding.DecodeString(rest)
	if err != nil || len(raw) != pubLen {
		return RecipientKey{}, ErrInvalid
	}
	pub, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return RecipientKey{}, ErrInvalid
	}
	return RecipientKey{pub: pub}, nil
}

func (k RecipientKey) String() string {
	if k.pub == nil {
		return ""
	}
	return keyPrefix + base64.StdEncoding.EncodeToString(k.pub.Bytes())
}

func (k RecipientKey) equal(o RecipientKey) bool {
	if k.pub == nil || o.pub == nil {
		return false
	}
	a := k.pub.Bytes()
	b := o.pub.Bytes()
	return subtle.ConstantTimeCompare(a, b) == 1
}

type Held struct {
	name PeerName
	key  RecipientKey
}

type Line struct{ text string }

func ParseLine(s string) (Line, error) {
	if !utf8.ValidString(s) {
		return Line{}, ErrInvalid
	}
	if len(s) > MaxPlain {
		return Line{}, ErrTooBig
	}
	if s == "" {
		return Line{}, ErrInvalid
	}
	return Line{text: s}, nil
}

func (l Line) String() string { return l.text }

// DropID is the owner of one staged line.
// Sixteen random bytes, 32 hex characters on the wire.
type DropID struct{ b [idLen]byte }

func NewDropID() (DropID, error) {
	var id DropID
	if _, err := randRead(id.b[:]); err != nil {
		return DropID{}, err
	}
	return id, nil
}

func ParseDropID(text string) (DropID, error) {
	if len(text) != idLen*2 {
		return DropID{}, ErrInvalid
	}
	raw, err := hex.DecodeString(text)
	if err != nil || len(raw) != idLen {
		return DropID{}, ErrInvalid
	}
	var id DropID
	copy(id.b[:], raw)
	return id, nil
}

func (id DropID) String() string { return hex.EncodeToString(id.b[:]) }

// Slot is SHA-256 of the recipient public key bytes.
type Slot struct{ b [fpLen]byte }

func SlotOf(k RecipientKey) Slot {
	if k.pub == nil {
		return Slot{}
	}
	return Slot{b: sha256.Sum256(k.pub.Bytes())}
}

func (s Slot) String() string { return hex.EncodeToString(s.b[:]) }

func ParseSlot(text string) (Slot, error) {
	if len(text) != fpLen*2 {
		return Slot{}, ErrInvalid
	}
	raw, err := hex.DecodeString(text)
	if err != nil || len(raw) != fpLen {
		return Slot{}, ErrInvalid
	}
	var slot Slot
	copy(slot.b[:], raw)
	return slot, nil
}

type Ciphertext struct{ raw []byte }

type Identity struct {
	priv *ecdh.PrivateKey
}

func (id Identity) Public() RecipientKey {
	if id.priv == nil {
		return RecipientKey{}
	}
	return RecipientKey{pub: id.priv.PublicKey()}
}

type Delivered struct {
	ID   DropID
	From PeerName
	Text string
}

type Stored struct {
	ID   DropID
	Body Ciphertext
}
