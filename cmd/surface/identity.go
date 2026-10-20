package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func loadIdentity(lookup func(string) (string, bool), myKey string) (*ecdh.PrivateKey, error) {
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	if raw, ok := lookup("SURFACE_IDENTITY_SEED"); ok && strings.TrimSpace(raw) != "" {
		seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
		if err != nil || len(seed) != 32 {
			return nil, errors.New("SURFACE_IDENTITY_SEED")
		}
		return ecdh.X25519().NewPrivateKey(seed)
	}
	if myKey == "" || myKey != filepath.Base(myKey) {
		return nil, errors.New("SURFACE_IDENTITY_SEED")
	}
	dir := "surface-identity"
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, myKey)
	seed, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		seed = make([]byte, 32)
		if _, err := rand.Read(seed); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, seed, 0o600); err != nil {
			seed, err = os.ReadFile(path)
			if err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	if len(seed) != 32 {
		return nil, errors.New("SURFACE_IDENTITY_SEED")
	}
	return ecdh.X25519().NewPrivateKey(seed)
}

func loadDirectRoots(path string) (*x509.CertPool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("SURFACE_DIRECT_CA")
	}
	return pool, nil
}
