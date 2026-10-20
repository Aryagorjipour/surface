package shared

import (
	"errors"
	"net/http"
	"strings"
)

type CertPair struct {
	Cert string
	Key  string
}

func ResolveCertPair(cert, key string) (CertPair, error) {
	cert = strings.TrimSpace(cert)
	key = strings.TrimSpace(key)
	if cert == "" && key == "" {
		return CertPair{}, nil
	}
	if cert == "" || key == "" {
		return CertPair{}, errors.New("certificate and key must both be set")
	}
	return CertPair{Cert: cert, Key: key}, nil
}

func (p CertPair) Serve(srv *http.Server) error {
	if p.Cert == "" {
		return srv.ListenAndServe()
	}
	return srv.ListenAndServeTLS(p.Cert, p.Key)
}
