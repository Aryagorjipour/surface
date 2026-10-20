package mailbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"io"
)

func randRead(b []byte) (int, error) {
	return io.ReadFull(rand.Reader, b)
}

func newIdentity() (Identity, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Identity{}, err
	}
	return Identity{priv: priv}, nil
}

func identityFromSeed(seed []byte) (Identity, error) {
	if len(seed) != pubLen {
		return Identity{}, ErrInvalid
	}
	priv, err := ecdh.X25519().NewPrivateKey(seed)
	if err != nil {
		return Identity{}, ErrInvalid
	}
	return Identity{priv: priv}, nil
}

func seal(me Identity, from PeerName, to Held, id DropID, line Line) (Ciphertext, error) {
	if me.priv == nil || to.key.pub == nil || from.s == "" || line.text == "" {
		return Ciphertext{}, ErrInvalid
	}
	name := []byte(from.s)
	if headerLen+2+len(name)+len(line.text)+tagLen > MaxBytes {
		return Ciphertext{}, ErrTooBig
	}
	plain := make([]byte, 2+len(name)+len(line.text))
	binary.BigEndian.PutUint16(plain[:2], uint16(len(name)))
	copy(plain[2:], name)
	copy(plain[2+len(name):], line.text)

	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Ciphertext{}, err
	}
	ephPub := append([]byte(nil), eph.PublicKey().Bytes()...)
	shared, err := eph.ECDH(to.key.pub)
	if err != nil {
		return Ciphertext{}, ErrInvalid
	}
	defer clear(shared)

	fp := sha256.Sum256(me.Public().pub.Bytes())
	out, err := sealBox(shared, ephPub, to.key.pub.Bytes(), fp, id, plain)
	if err != nil {
		return Ciphertext{}, err
	}
	if len(out) < headerLen || len(out) > MaxBytes {
		return Ciphertext{}, ErrTooBig
	}
	return Ciphertext{raw: out}, nil
}

func sealBox(shared, ephPub, recipPub []byte, fp [fpLen]byte, id DropID, plain []byte) ([]byte, error) {
	salt := make([]byte, 0, len(ephPub)+len(recipPub))
	salt = append(salt, ephPub...)
	salt = append(salt, recipPub...)
	key, err := hkdf.Key(sha256.New, shared, salt, sealInfo, 32)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if gcm.NonceSize() != nonceLen {
		return nil, ErrInvalid
	}
	nonce := make([]byte, nonceLen)
	if _, err := randRead(nonce); err != nil {
		return nil, err
	}
	aad := append(append([]byte{}, id.b[:]...), fp[:]...)
	ct := gcm.Seal(nil, nonce, plain, aad)
	out := make([]byte, 0, headerLen+len(ct))
	out = append(out, versionByte)
	out = append(out, fp[:]...)
	out = append(out, ephPub...)
	out = append(out, nonce...)
	out = append(out, ct...)
	return out, nil
}

func unseal(me Identity, from Held, id DropID, body Ciphertext) (Delivered, error) {
	raw := body.raw
	if me.priv == nil || from.key.pub == nil || len(raw) < headerLen+tagLen || len(raw) > MaxBytes || raw[0] != versionByte {
		return Delivered{}, ErrUnauth
	}
	var fp [fpLen]byte
	copy(fp[:], raw[1:1+fpLen])
	want := sha256.Sum256(from.key.pub.Bytes())
	if subtle.ConstantTimeCompare(fp[:], want[:]) != 1 {
		return Delivered{}, ErrUnauth
	}
	ephPub := raw[1+fpLen : 1+fpLen+pubLen]
	nonce := raw[headerLen-nonceLen : headerLen]
	ct := raw[headerLen:]
	eph, err := ecdh.X25519().NewPublicKey(ephPub)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	shared, err := me.priv.ECDH(eph)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	defer clear(shared)
	recip := me.priv.PublicKey().Bytes()
	salt := make([]byte, 0, len(ephPub)+len(recip))
	salt = append(salt, ephPub...)
	salt = append(salt, recip...)
	key, err := hkdf.Key(sha256.New, shared, salt, sealInfo, 32)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	aad := append(append([]byte{}, id.b[:]...), fp[:]...)
	plain, err := gcm.Open(nil, nonce, ct, aad)
	if err != nil {
		return Delivered{}, ErrUnauth
	}
	if len(plain) < 2 {
		return Delivered{}, ErrUnauth
	}
	n := int(binary.BigEndian.Uint16(plain[:2]))
	if n < 1 || n > maxNameBytes || len(plain) < 2+n {
		return Delivered{}, ErrUnauth
	}
	name := string(plain[2 : 2+n])
	if name != from.name.s {
		return Delivered{}, ErrUnauth
	}
	text := string(plain[2+n:])
	if text == "" || len(text) > MaxPlain {
		return Delivered{}, ErrUnauth
	}
	return Delivered{ID: id, From: from.name, Text: text}, nil
}

func bodyFingerprint(body Ciphertext) ([fpLen]byte, bool) {
	var fp [fpLen]byte
	if len(body.raw) < headerLen || body.raw[0] != versionByte {
		return fp, false
	}
	copy(fp[:], body.raw[1:1+fpLen])
	return fp, true
}
