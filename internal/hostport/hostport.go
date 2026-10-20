package hostport

import (
	"errors"
	"net"
	"net/url"
	"strconv"
)

var ErrInvalid = errors.New("invalid address")

type Addr struct {
	raw string
}

func (a Addr) String() string {
	return a.raw
}

func Parse(raw string) (Addr, error) {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return Addr{}, ErrInvalid
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return Addr{}, ErrInvalid
	}
	if net.JoinHostPort(host, port) != raw {
		return Addr{}, ErrInvalid
	}
	u, err := url.Parse("http://" + raw)
	if err != nil || u == nil || u.Host != raw {
		return Addr{}, ErrInvalid
	}
	return Addr{raw: raw}, nil
}
