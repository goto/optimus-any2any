package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/pkg/errors"
)

func NewTLSConfig(tlsCert, tlsKey, tlsCACert string) (*tls.Config, error) {
	// load the certificate and key
	cert, err := tls.X509KeyPair([]byte(tlsCert), []byte(tlsKey))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// load the CA certificate
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(tlsCACert)); !ok {
		return nil, errors.WithStack(fmt.Errorf("failed to append CA certificate"))
	}
	// create a new tls.Config
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
