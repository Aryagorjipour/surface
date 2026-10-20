// Package session is the direct-chat codec. The Connector never sees these frames.
package session

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	// ProofBytes is the raw admission proof length.
	ProofBytes = 32

	typeOffer   = 0x01
	typeReply   = 0x02
	typeChat    = 0x03
	typeEnd     = 0x04
	typeResume  = 0x05
	typeResume2 = 0x06

	dirDialer   = 0x01
	dirListener = 0x02

	chainBytes   = 32
	chainMessage = 0x01
	chainNext    = 0x02
)

var (
	errRefused  = errors.New("refused")
	errFinished = errors.New("finished")
)

// NewProof returns 32 random bytes and their standard base64 text.
func NewProof() (raw []byte, text string, err error) {
	raw = make([]byte, ProofBytes)
	if _, err = rand.Read(raw); err != nil {
		return nil, "", err
	}
	return raw, base64.StdEncoding.EncodeToString(raw), nil
}

// DecodeProof decodes a standard-base64 admission proof.
func DecodeProof(text string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(text)
	if err != nil || len(raw) != ProofBytes {
		return nil, errRefused
	}
	return raw, nil
}

type offerState struct {
	requestID string
	callerKey string
	targetKey string
	proof     []byte
	priv      *ecdh.PrivateKey
	eph       []byte
}

type replay struct {
	frame []byte
	plain string
}

func (r replay) match(frame []byte) (string, bool) {
	if !bytes.Equal(r.frame, frame) {
		return "", false
	}
	return r.plain, true
}

// Session is one admitted direct chat. Its chain keys stay in this process.
type Session struct {
	mu       sync.Mutex
	request  string
	caller   string
	target   string
	sendDir  byte
	send     []byte
	recv     []byte
	resume   []byte
	nextSend uint64
	nextRecv uint64
	highest  uint64
	peerAck  uint64
	sent     map[uint64][]byte
	opened   map[uint64]replay
	finished bool
	notice   []byte
}

// Offer builds a type 0x01 handshake. The proof is the HMAC key and is not written.
func Offer(requestID, callerKey, targetKey string, proof []byte) ([]byte, *offerState, error) {
	priv, eph, err := newEph()
	if err != nil {
		return nil, nil, err
	}
	msg := handshake(typeOffer, requestID, callerKey, eph, nil, proof)
	st := &offerState{
		requestID: requestID,
		callerKey: callerKey,
		targetKey: targetKey,
		proof:     append([]byte(nil), proof...),
		priv:      priv,
		eph:       eph,
	}
	return msg, st, nil
}

// AcceptOffer checks a type 0x01 message. alreadyAdmitted refuses without a session.
func AcceptOffer(msg []byte, requestID, callerKey, targetKey string, proof []byte, alreadyAdmitted bool) ([]byte, *Session, error) {
	eph, mac, err := splitOffer(msg)
	input := macInput(typeOffer, requestID, callerKey, eph, nil)
	sum := proofMAC(proof, input)
	if err != nil || len(proof) != ProofBytes || !hmac.Equal(sum, mac) {
		return nil, nil, errRefused
	}
	if alreadyAdmitted {
		return nil, nil, errRefused
	}
	priv, mine, err := newEph()
	if err != nil {
		return nil, nil, err
	}
	shared, err := sharedSecret(priv, eph)
	if err != nil {
		return nil, nil, errRefused
	}
	sess, err := newSession(dirListener, requestID, callerKey, targetKey, shared, proof)
	if err != nil {
		return nil, nil, err
	}
	reply := handshake(typeReply, requestID, callerKey, eph, mine, proof)
	return reply, sess, nil
}

// FinishOffer checks a type 0x02 reply and returns the dialer session.
func FinishOffer(reply []byte, state *offerState) (*Session, error) {
	if state == nil {
		return nil, errRefused
	}
	mine, mac, err := splitReply(reply)
	input := macInput(typeReply, state.requestID, state.callerKey, state.eph, mine)
	sum := proofMAC(state.proof, input)
	if err != nil || len(state.proof) != ProofBytes || !hmac.Equal(sum, mac) {
		return nil, errRefused
	}
	shared, err := sharedSecret(state.priv, mine)
	if err != nil {
		return nil, errRefused
	}
	return newSession(dirDialer, state.requestID, state.callerKey, state.targetKey, shared, state.proof)
}

// Seal encrypts one chat line as type 0x03. ack 0 means no acknowledgement.
func Seal(sess *Session, plaintext string, ack uint64) ([]byte, error) {
	if sess == nil {
		return nil, errRefused
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.sealLocked(typeChat, plaintext, ack)
}

// End seals an empty type 0x04 frame and finishes this side.
func End(sess *Session) ([]byte, error) {
	if sess == nil {
		return nil, errRefused
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	frame, err := sess.sealLocked(typeEnd, "", 0)
	if err != nil {
		return nil, err
	}
	sess.finished = true
	return frame, nil
}

// Open decrypts a chat or end frame.
// An already displayed sequence returns display false and a nil error.
func Open(sess *Session, frame []byte) (plaintext string, display bool, err error) {
	if sess == nil {
		return "", false, errRefused
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.finished {
		return "", false, errFinished
	}
	if len(sess.send) != chainBytes || len(sess.recv) != chainBytes {
		return "", false, sess.failLocked()
	}
	typ, seq, ack, ct, perr := parseChat(frame)
	if perr != nil {
		return "", false, sess.failLocked()
	}
	sess.applyAck(ack)
	if typ != typeChat && typ != typeEnd {
		return "", false, sess.failLocked()
	}
	if seq == 0 || seq > sess.nextRecv {
		return "", false, sess.failLocked()
	}
	if seq < sess.nextRecv {
		return sess.replayLocked(seq, frame)
	}
	mk, next := chainStep(sess.recv)
	defer clear(mk)
	plain, oerr := openGCM(mk, otherDir(sess.sendDir), seq, ct)
	if oerr != nil {
		clear(next)
		return "", false, sess.failLocked()
	}
	if typ == typeEnd {
		if plain != "" {
			clear(next)
			return "", false, sess.failLocked()
		}
		clear(sess.recv)
		sess.recv = next
		sess.finished = true
		return "", false, nil
	}
	sess.opened[seq] = replay{frame: append([]byte(nil), frame...), plain: plain}
	clear(sess.recv)
	sess.recv = next
	sess.nextRecv = seq + 1
	if seq > sess.highest {
		sess.highest = seq
	}
	return plain, true, nil
}

func (s *Session) replayLocked(seq uint64, frame []byte) (string, bool, error) {
	held, ok := s.opened[seq]
	if !ok {
		return "", false, s.failLocked()
	}
	plain, match := held.match(frame)
	if !match {
		return "", false, s.failLocked()
	}
	return plain, false, nil
}

// failLocked seals one empty end frame for the peer, then marks this side finished.
// End cannot seal after Open has already set finished, so the notice is taken here.
func (s *Session) failLocked() error {
	if !s.finished && len(s.notice) == 0 {
		if frame, err := s.sealLocked(typeEnd, "", 0); err == nil {
			s.notice = frame
		}
	}
	s.finished = true
	return errRefused
}

// FailureNotice returns the end frame from a failed Open, once.
func FailureNotice(sess *Session) []byte {
	if sess == nil {
		return nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.notice) == 0 {
		return nil
	}
	frame := append([]byte(nil), sess.notice...)
	sess.notice = nil
	return frame
}

// ResumeOffer is the dialer's type 0x05 message. It is authenticated with the resume key.
func ResumeOffer(sess *Session) ([]byte, error) {
	if sess == nil {
		return nil, errRefused
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.finished || sess.sendDir != dirDialer {
		return nil, errRefused
	}
	return sess.resumeMsg(typeResume), nil
}

// AcceptResume checks a resume message. now equal to deadline refuses and does not continue.
// A listener answers type 0x05 with type 0x06. A dialer consumes type 0x06 and returns a nil reply.
func AcceptResume(sess *Session, msg []byte, now, deadline time.Time) ([]byte, error) {
	if sess == nil {
		return nil, errRefused
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.finished || !now.Before(deadline) {
		return nil, errRefused
	}
	id, next, highest, mac, err := splitResume(msg)
	if err != nil || id != sess.request || len(msg) < 1 {
		return nil, errRefused
	}
	sum := proofMAC(sess.resume, resumeInput(msg[0], id, next, highest))
	if !hmac.Equal(sum, mac) {
		return nil, errRefused
	}
	want := byte(typeResume)
	var reply []byte
	if sess.sendDir == dirDialer {
		want = typeResume2
	}
	if msg[0] != want {
		return nil, errRefused
	}
	sess.applyAck(highest)
	if sess.sendDir == dirListener {
		reply = sess.resumeMsg(typeResume2)
	}
	return reply, nil
}

// Pending returns sealed frames the peer has not displayed, in order.
func Pending(sess *Session) [][]byte {
	if sess == nil {
		return nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	out := make([][]byte, 0)
	for seq := sess.peerAck + 1; seq < sess.nextSend; seq++ {
		if frame, ok := sess.sent[seq]; ok {
			out = append(out, frame)
		}
	}
	return out
}

// Highest is the highest sequence this side has displayed. Zero means none.
func Highest(sess *Session) uint64 {
	if sess == nil {
		return 0
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.highest
}

// Finished reports an end frame, a failed open, or a local End.
func Finished(sess *Session) bool {
	if sess == nil {
		return true
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.finished
}

// NextSend is the next sequence this side will seal.
func NextSend(sess *Session) uint64 {
	if sess == nil {
		return 0
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.nextSend
}

func (s *Session) sealLocked(typ byte, plaintext string, ack uint64) ([]byte, error) {
	if s.finished {
		return nil, errFinished
	}
	if len(s.send) != chainBytes {
		return nil, errRefused
	}
	seq := s.nextSend
	mk, next := chainStep(s.send)
	defer clear(mk)
	ct, err := sealGCM(mk, s.sendDir, seq, []byte(plaintext))
	if err != nil {
		clear(next)
		return nil, err
	}
	frame := make([]byte, 21+len(ct))
	frame[0] = typ
	binary.BigEndian.PutUint64(frame[1:9], seq)
	binary.BigEndian.PutUint64(frame[9:17], ack)
	binary.BigEndian.PutUint32(frame[17:21], uint32(len(ct)))
	copy(frame[21:], ct)
	if s.sent == nil {
		s.sent = make(map[uint64][]byte)
	}
	s.sent[seq] = append([]byte(nil), frame...)
	clear(s.send)
	s.send = next
	s.nextSend = seq + 1
	return frame, nil
}

func (s *Session) applyAck(ack uint64) {
	if ack == 0 || s.nextSend == 0 {
		return
	}
	last := s.nextSend - 1
	if ack > last || ack <= s.peerAck {
		return
	}
	s.peerAck = ack
}

func (s *Session) resumeMsg(typ byte) []byte {
	body := resumeHead(typ, s.request, s.nextSend, s.highest)
	mac := proofMAC(s.resume, resumeInput(typ, s.request, s.nextSend, s.highest))
	return append(body, mac...)
}

func newSession(sendDir byte, requestID, callerKey, targetKey string, shared, proof []byte) (*Session, error) {
	defer clear(shared)
	sendLabel, recvLabel := "surface-chain-dialer-v1", "surface-chain-listener-v1"
	if sendDir == dirListener {
		sendLabel, recvLabel = recvLabel, sendLabel
	}
	send, err := derive(shared, proof, callerKey, targetKey, sendLabel)
	if err != nil {
		return nil, err
	}
	recv, err := derive(shared, proof, callerKey, targetKey, recvLabel)
	if err != nil {
		clear(send)
		return nil, err
	}
	resume, err := derive(shared, proof, callerKey, targetKey, "surface-resume-v1")
	if err != nil {
		clear(send)
		clear(recv)
		return nil, err
	}
	return &Session{
		request:  requestID,
		caller:   callerKey,
		target:   targetKey,
		sendDir:  sendDir,
		send:     send,
		recv:     recv,
		resume:   resume,
		nextSend: 1,
		nextRecv: 1,
		sent:     make(map[uint64][]byte),
		opened:   make(map[uint64]replay),
	}, nil
}

func chainStep(chain []byte) (messageKey, next []byte) {
	messageKey = proofMAC(chain, []byte{chainMessage})
	next = proofMAC(chain, []byte{chainNext})
	return messageKey, next
}

func derive(shared, proof []byte, caller, target, label string) ([]byte, error) {
	return hkdf.Key(sha256.New, shared, proof, label+caller+target, chainBytes)
}

func newEph() (*ecdh.PrivateKey, []byte, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	eph := priv.PublicKey().Bytes()
	if len(eph) != 32 {
		return nil, nil, errRefused
	}
	return priv, eph, nil
}

func sharedSecret(priv *ecdh.PrivateKey, raw []byte) ([]byte, error) {
	if priv == nil {
		return nil, errRefused
	}
	remote, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return nil, errRefused
	}
	return priv.ECDH(remote)
}

func handshake(typ byte, requestID, callerKey string, dialerEph, listenerEph, proof []byte) []byte {
	var body []byte
	if typ == typeOffer {
		body = append([]byte{typ}, u16(requestID)...)
		body = append(body, u16(callerKey)...)
		body = append(body, dialerEph...)
	} else {
		body = append([]byte{typ}, listenerEph...)
	}
	mac := proofMAC(proof, macInput(typ, requestID, callerKey, dialerEph, listenerEph))
	return append(body, mac...)
}

func macInput(typ byte, requestID, callerKey string, dialerEph, listenerEph []byte) []byte {
	out := []byte{typ}
	out = append(out, requestID...)
	out = append(out, callerKey...)
	out = append(out, dialerEph...)
	out = append(out, listenerEph...)
	return out
}

func proofMAC(key, input []byte) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(input)
	return m.Sum(nil)
}

func u16(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b[:2], uint16(len(s)))
	copy(b[2:], s)
	return b
}

// OfferNames returns the request id and caller key carried in a type 0x01 message.
func OfferNames(msg []byte) (requestID, callerKey string, err error) {
	if len(msg) < 1+2 || msg[0] != typeOffer {
		return "", "", errRefused
	}
	idLen := int(binary.BigEndian.Uint16(msg[1:3]))
	if len(msg) < 3+idLen+2 {
		return "", "", errRefused
	}
	keyLen := int(binary.BigEndian.Uint16(msg[3+idLen : 5+idLen]))
	if len(msg) < 5+idLen+keyLen {
		return "", "", errRefused
	}
	return string(msg[3 : 3+idLen]), string(msg[5+idLen : 5+idLen+keyLen]), nil
}

func splitOffer(msg []byte) (eph, mac []byte, err error) {
	if len(msg) < 1+2+2+32+32 || msg[0] != typeOffer {
		return nil, nil, errRefused
	}
	idLen := int(binary.BigEndian.Uint16(msg[1:3]))
	if len(msg) < 3+idLen+2 {
		return nil, nil, errRefused
	}
	keyLen := int(binary.BigEndian.Uint16(msg[3+idLen : 5+idLen]))
	rest := 5 + idLen + keyLen
	if len(msg) != rest+32+32 {
		return nil, nil, errRefused
	}
	return msg[rest : rest+32], msg[rest+32:], nil
}

func splitReply(msg []byte) (eph, mac []byte, err error) {
	if len(msg) != 1+32+32 || msg[0] != typeReply {
		return nil, nil, errRefused
	}
	return msg[1:33], msg[33:], nil
}

func parseChat(frame []byte) (typ byte, seq, ack uint64, ct []byte, err error) {
	if len(frame) < 21 {
		return 0, 0, 0, nil, errRefused
	}
	n := binary.BigEndian.Uint32(frame[17:21])
	if uint32(len(frame)-21) != n {
		return 0, 0, 0, nil, errRefused
	}
	return frame[0], binary.BigEndian.Uint64(frame[1:9]), binary.BigEndian.Uint64(frame[9:17]), frame[21:], nil
}

func resumeHead(typ byte, requestID string, next, highest uint64) []byte {
	body := append([]byte{typ}, u16(requestID)...)
	tail := make([]byte, 16)
	binary.BigEndian.PutUint64(tail[:8], next)
	binary.BigEndian.PutUint64(tail[8:], highest)
	return append(body, tail...)
}

func resumeInput(typ byte, requestID string, next, highest uint64) []byte {
	out := []byte{typ}
	out = append(out, requestID...)
	tail := make([]byte, 16)
	binary.BigEndian.PutUint64(tail[:8], next)
	binary.BigEndian.PutUint64(tail[8:], highest)
	return append(out, tail...)
}

func splitResume(msg []byte) (id string, next, highest uint64, mac []byte, err error) {
	if len(msg) < 1+2+16+32 {
		return "", 0, 0, nil, errRefused
	}
	idLen := int(binary.BigEndian.Uint16(msg[1:3]))
	if len(msg) != 3+idLen+16+32 {
		return "", 0, 0, nil, errRefused
	}
	id = string(msg[3 : 3+idLen])
	next = binary.BigEndian.Uint64(msg[3+idLen : 11+idLen])
	highest = binary.BigEndian.Uint64(msg[11+idLen : 19+idLen])
	mac = msg[19+idLen:]
	return id, next, highest, mac, nil
}

func sealGCM(key []byte, dir byte, seq uint64, plain []byte) ([]byte, error) {
	gcm, err := gcmOf(key)
	if err != nil {
		return nil, err
	}
	return gcm.Seal(nil, nonce(dir, seq), plain, nil), nil
}

func openGCM(key []byte, dir byte, seq uint64, ct []byte) (string, error) {
	gcm, err := gcmOf(key)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce(dir, seq), ct, nil)
	if err != nil {
		return "", errRefused
	}
	return string(plain), nil
}

func gcmOf(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func nonce(dir byte, seq uint64) []byte {
	n := make([]byte, 12)
	n[0] = dir
	binary.BigEndian.PutUint64(n[4:], seq)
	return n
}

func otherDir(dir byte) byte {
	if dir == dirDialer {
		return dirListener
	}
	return dirDialer
}
