package httpscaconfig

import (
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/reflow/config"
)

func TestHTTPSCA(t *testing.T) {
	name, err := tempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(name)
	ca, err := newCA(name)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test.grail.com"}
	certBytes, priv, err := ca.issue("test", 10*time.Minute, ips, dnses)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatal(err)
	}
	opts := x509.VerifyOptions{}
	opts.Roots = x509.NewCertPool()
	opts.Roots.AddCert(ca.cert)
	if _, err := cert.Verify(opts); err != nil {
		t.Fatal(err)
	}
	if err := priv.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, want := priv.Public(), cert.PublicKey; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.Subject.CommonName, "test"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := cert.NotBefore, now.Add(-driftMargin); want.Before(got) {
		t.Errorf("wanted %s <= %s", got, want)
	}
	if got, want := cert.NotAfter.Sub(cert.NotBefore), 10*time.Minute+driftMargin; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if cert.IsCA {
		t.Error("cert is CA")
	}
	if got, want := cert.IPAddresses, ips; !ipsEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.DNSNames, dnses; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMarshal(t *testing.T) {
	name, err := tempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(name)
	ca, err := newCA(name)
	if err != nil {
		t.Fatal(err)
	}
	ca.Config = make(config.Base)
	keys := make(config.Keys)
	if err := ca.Marshal(keys); err != nil {
		t.Fatal(err)
	}
	s, ok := keys[httpsca].(string)
	if !ok {
		t.Fatal("invalid value for key httpsca")
	}
	cert, err := newCert([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = cert.HTTPS()
	if err != nil {
		t.Fatal(err)
	}
}

func tempFile() (string, error) {
	// Test that the CA generates valid certs.
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		return "", err
	}
	return name, os.Remove(name)
}

func ipsEqual(x, y []net.IP) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !x[i].Equal(y[i]) {
			return false
		}
	}
	return true
}
