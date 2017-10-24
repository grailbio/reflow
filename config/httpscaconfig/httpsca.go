// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package httpscaconfig defines a configuration provider named
// "httpsca" which can be used to configure HTTPS certificates via an
// on-disk certificate authority.
package httpscaconfig

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/grailbio/reflow/config"
)

const (
	// TODO(marius): make these configurable under config keys
	driftMargin  = time.Minute
	certDuration = 27 * 7 * 24 * time.Hour

	httpsca = "httpsca"
)

func init() {
	config.Register(config.HTTPS, "httpsca", "pem", "configure a HTTPS CA from the provided PEM-encoded signing certificate",
		func(cfg config.Config, arg string) (config.Config, error) {
			if v := cfg.Value(httpsca); v != nil {
				// In this case we're dealing with a marshaled certificate.
				s, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("expected key %s to be a string, not %T", v)
				}
				cert, err := newCert([]byte(s))
				if err != nil {
					return nil, err
				}
				cert.Config = cfg
				return cert, nil
			}
			if arg == "" {
				return nil, errors.New("httpsca: missing PEM file")
			}
			ca, err := newCA(arg)
			if err != nil {
				return nil, err
			}
			ca.Config = cfg
			return ca, nil
		},
	)
}

type ca struct {
	config.Config

	key  *rsa.PrivateKey
	cert *x509.Certificate

	// The CA certificate and key are stored in PEM-encoded bytes
	// as most of the Go APIs operate directly on these.
	certPEM, keyPEM []byte
}

// newCA creates a new certificate authority config, reading the
// PEM-encoded certificate and private key from the provided path. If
// the path does not exist, newCA instead creates a new certificate
// authority and stores it at the provided path.
func newCA(path string) (*ca, error) {
	// As an extra precaution, we always exercise the read path, so if
	// the CA PEM is missing, we generate it, and then read it back.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		template := x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				CommonName: "reflow", // make this configurable?
			},
			NotBefore: time.Now(),
			NotAfter:  time.Now().Add(certDuration),

			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA: true,
		}
		cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
		if err != nil {
			return nil, err
		}
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		// Save it also.
		if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: cert}); err != nil {
			f.Close()
			return nil, err
		}
		if err := pem.Encode(f, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	pemBlock, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var certBlock, keyBlock []byte
	for {
		var derBlock *pem.Block
		derBlock, pemBlock = pem.Decode(pemBlock)
		if derBlock == nil {
			break
		}
		switch derBlock.Type {
		case "CERTIFICATE":
			certBlock = derBlock.Bytes
		case "RSA PRIVATE KEY":
			keyBlock = derBlock.Bytes
		}
	}

	if certBlock == nil || keyBlock == nil {
		return nil, errors.New("httpsca: incomplete certificate")
	}
	ca := new(ca)
	ca.cert, err = x509.ParseCertificate(certBlock)
	if err != nil {
		return nil, err
	}
	ca.key, err = x509.ParsePKCS1PrivateKey(keyBlock)
	if err != nil {
		return nil, err
	}
	ca.certPEM, err = encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: certBlock})
	if err != nil {
		return nil, err
	}
	ca.keyPEM, err = encodePEM(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(ca.key)})
	if err != nil {
		return nil, err
	}
	return ca, nil
}

// Issue issues a new certificate out of this CA with the provided common name, ttl, ips, and DNSes.
func (c *ca) issue(cn string, ttl time.Duration, ips []net.IP, dnss []string) ([]byte, *rsa.PrivateKey, error) {
	maxSerial := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, maxSerial)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().Add(-driftMargin)
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             now,
		NotAfter:              now.Add(driftMargin + ttl),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	template.IPAddresses = append(template.IPAddresses, ips...)
	template.DNSNames = append(template.DNSNames, dnss...)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.CreateCertificate(rand.Reader, &template, c.cert, &key.PublicKey, c.key)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// Marshal issues a new certificate and private key and adds it,
// along with the authority certificate, to the marshal keys.
func (c *ca) Marshal(keys config.Keys) error {
	if err := c.Config.Marshal(keys); err != nil {
		return err
	}
	cert, key, err := c.issue("reflow", certDuration, nil, nil)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	if err := pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: cert}); err != nil {
		return err
	}
	if err := pem.Encode(&b, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		return err
	}
	if err := pem.Encode(&b, &pem.Block{Type: "AUTHORITY CERTIFICATE", Bytes: c.cert.Raw}); err != nil {
		return err
	}
	keys[httpsca] = b.String()
	return nil
}

// HTTPS returns a tls configs based on newly issued TLS certificates from this CA.
func (c *ca) HTTPS() (client, server *tls.Config, err error) {
	cert, key, err := c.issue("reflow", certDuration, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.certPEM)

	// Load the newly created certificate.
	certPEM, err := encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := encodePEM(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err != nil {
		return nil, nil, err
	}
	tlscert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}
	clientConfig := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{tlscert},
	}
	serverConfig := &tls.Config{
		ClientCAs:    pool,
		Certificates: []tls.Certificate{tlscert},
	}
	return clientConfig, serverConfig, nil
}

// newCert creates a new cert config, serving HTTPS configurations from a
// single certificate and private key, as provided by (*ca).Marshal.
func newCert(pemBlock []byte) (*cert, error) {
	cert := new(cert)
	for {
		var derBlock *pem.Block
		derBlock, pemBlock = pem.Decode(pemBlock)
		if derBlock == nil {
			break
		}
		var err error
		switch derBlock.Type {
		case "CERTIFICATE":
			cert.certPEM, err = encodePEM(derBlock)
		case "RSA PRIVATE KEY":
			cert.keyPEM, err = encodePEM(derBlock)
		case "AUTHORITY CERTIFICATE":
			derBlock.Type = "CERTIFICATE"
			cert.authorityCertPEM, err = encodePEM(derBlock)
		}
		if err != nil {
			return nil, err
		}
	}
	if cert.authorityCertPEM == nil || cert.certPEM == nil || cert.keyPEM == nil {
		return nil, errors.New("https: incomplete marshaled certificate")
	}
	return cert, nil
}

type cert struct {
	config.Config

	authorityCertPEM, certPEM, keyPEM []byte
}

// HTTPS creates a new client and server TLS configs based on this
// cert's certificate, private key, and authority certificate.
func (c *cert) HTTPS() (client, server *tls.Config, err error) {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.authorityCertPEM)
	tlscert, err := tls.X509KeyPair(c.certPEM, c.keyPEM)
	if err != nil {
		return nil, nil, err
	}
	clientConfig := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{tlscert},
	}
	serverConfig := &tls.Config{
		ClientCAs:    pool,
		Certificates: []tls.Certificate{tlscert},
	}
	return clientConfig, serverConfig, nil
}

func encodePEM(block *pem.Block) ([]byte, error) {
	var w bytes.Buffer
	if err := pem.Encode(&w, block); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
