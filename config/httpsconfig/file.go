// Package httpsconfig defines a configuration provider named "file"
// which can be used to configure HTTPS certificates. The package is
// usually imported for its registration side effects.
package httpsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/grailbio/reflow/config"
)

func init() {
	config.Register(config.HTTPS, "file", "authoritycert,cert,key", "configure HTTPS from provided files",
		func(cfg config.Config, arg string) (config.Config, error) {
			parts := strings.Split(arg, ",")
			if n := len(parts); n != 3 {
				return nil, fmt.Errorf("https: file: expected 3 arguments, got %d", n)
			}
			return &https{cfg, parts[0], parts[1], parts[2]}, nil
		},
	)
}

type https struct {
	config.Config
	authorityCertPath, certPath, keyPath string
}

// HTTPS returns TLS configs using the supplied on-disk HTTPS certificates.
// TODO(marius): serialize these certificates in Keys.
func (h *https) HTTPS() (client, server *tls.Config, err error) {
	authorityCert, err := ioutil.ReadFile(h.authorityCertPath)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(authorityCert) {
		return nil, nil, errors.New("failed to add authority certificate")
	}
	cert, err := tls.LoadX509KeyPair(h.certPath, h.keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("load x509 key pair: %v", err)
	}
	clientConfig := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
	serverConfig := &tls.Config{
		ClientCAs:    pool,
		Certificates: []tls.Certificate{cert},
	}
	return clientConfig, serverConfig, nil
}
