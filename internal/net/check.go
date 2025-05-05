package net

import (
	"net"
	"net/url"

	"github.com/pkg/errors"
)

// ConnCheck checks if a connection can be established to the given host and port.
func ConnCheck(address string) error {
	// when address is in url format, it will be parsed to get host and port
	u, err := url.Parse(address)
	if err != nil {
		return errors.WithStack(err)
	}

	var host string
	var port string

	if u.Host != "" {
		host = u.Hostname()
		port = u.Port()
		if port == "" {
			if u.Scheme == "http" {
				port = "80" // default port for http
			} else if u.Scheme == "https" {
				port = "443" // default port for https
			} else {
				return errors.New("unsupported scheme")
			}
		}
	} else {
		// when address is in host:port format, it will be split to get host and port
		host, port, err = net.SplitHostPort(address)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// do a connection check
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return errors.WithStack(err)
	}
	return conn.Close()
}
