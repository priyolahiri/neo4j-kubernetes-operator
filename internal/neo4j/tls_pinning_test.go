/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package neo4j

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// issueCert mints a certificate for cn, valid for "localhost"/127.0.0.1 so it
// can serve a real handshake. A nil parent self-signs; otherwise the cert is
// signed by parent, which is how the "same CA, different leaf" case is built.
func issueCert(t *testing.T, cn string, parent *tls.Certificate) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		// BasicConstraints so the same helper can mint a signing CA.
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	signerCert, signerKey := tmpl, any(key)
	chain := [][]byte(nil)
	if parent != nil {
		signerCert, signerKey = parent.Leaf, parent.PrivateKey
		chain = parent.Certificate
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return tls.Certificate{
			Certificate: append([][]byte{der}, chain...),
			PrivateKey:  key,
			Leaf:        parsed,
		},
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// selfSignedCert is issueCert with no parent.
func selfSignedCert(t *testing.T, cn string) (tls.Certificate, []byte) {
	t.Helper()
	return issueCert(t, cn, nil)
}

// pinnedConfigFor builds the client config buildTLSConfig produces for a Secret
// that has tls.crt but NO ca.crt — the strictPeerValidation:false shape.
func pinnedConfigFor(t *testing.T, certPEM []byte) *tls.Config {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "srv-tls-secret", Namespace: "ns"},
		Data:       map[string][]byte{"tls.crt": certPEM, "tls.key": []byte("unused")},
	}).Build()

	cfg, degraded := buildTLSConfig(context.Background(), c, "ns", "srv",
		&neo4jv1beta1.TLSSpec{Mode: "cert-manager"})
	if cfg == nil {
		t.Fatal("expected a TLS config when tls.crt is present")
	}
	// Pinning IS verification, so this must not report a degraded connection.
	if degraded {
		t.Error("pinned connection reported as verification-disabled; it is verified")
	}
	if cfg.RootCAs == nil {
		t.Fatal("expected the pinned certificate to be expressed as RootCAs")
	}
	// The pin must NOT depend on InsecureSkipVerify: the Bolt driver overwrites
	// that field from the URI scheme, so any pin that needs it is silently
	// inert in production. See the asDriverWouldUse comment below.
	if cfg.InsecureSkipVerify {
		t.Error("pinned config must not set InsecureSkipVerify — the driver overwrites it")
	}
	return cfg
}

// asDriverWouldUse applies the two mutations the Neo4j Bolt driver makes to any
// caller-supplied TlsConfig before dialing, mirroring
// neo4j/internal/connector/connector.go tlsConfig() and documented on
// config.Config.TlsConfig: ServerName comes from the URI host and
// InsecureSkipVerify from the URI scheme (false for neo4j+s://).
//
// Every handshake in this file goes through here on purpose. The first attempt
// at pinning used InsecureSkipVerify + VerifyPeerCertificate, passed tests that
// dialed with the config verbatim, and then failed on a live cluster with
// "certificate signed by unknown authority" because the driver had reset the
// flag and Go's default chain check ran first. Emulating the driver is what
// makes these tests able to catch that.
func asDriverWouldUse(cfg *tls.Config, serverName string) *tls.Config {
	out := cfg.Clone()
	out.ServerName = serverName
	out.InsecureSkipVerify = false
	return out
}

// handshake performs a REAL TLS handshake against a server using srvCert, with
// the given client config. This is the point of the test: assert cryptographic
// behaviour, not struct fields.
func handshake(t *testing.T, srvCert tls.Certificate, clientCfg *tls.Config) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		// Drive the server side of the handshake, then close.
		if tc, ok := conn.(*tls.Conn); ok {
			_ = tc.HandshakeContext(context.Background())
		}
		_ = conn.Close()
	}()

	dialer := &tls.Dialer{Config: asDriverWouldUse(clientCfg, "localhost")}
	netConn, err := dialer.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		return err
	}
	conn, ok := netConn.(*tls.Conn)
	if !ok {
		t.Fatalf("dialer returned %T, want *tls.Conn", netConn)
	}
	defer func() { _ = conn.Close() }()
	return conn.HandshakeContext(context.Background())
}

// The certificate in the Secret is the one the server presents: handshake must
// succeed, with no CA anywhere in the picture.
func TestPinning_AcceptsTheCertificateFromTheSecret(t *testing.T) {
	srvCert, certPEM := selfSignedCert(t, "neo4j-server")
	cfg := pinnedConfigFor(t, certPEM)

	if err := handshake(t, srvCert, cfg); err != nil {
		t.Fatalf("handshake against the pinned certificate should succeed, got: %v", err)
	}
}

// The MITM case: a server presenting a DIFFERENT certificate must be refused.
// Before pinning this connection was accepted, because verification was off.
func TestPinning_RejectsADifferentCertificate(t *testing.T) {
	_, pinnedPEM := selfSignedCert(t, "neo4j-server")
	attackerCert, _ := selfSignedCert(t, "neo4j-server") // same CN, different key
	cfg := pinnedConfigFor(t, pinnedPEM)

	err := handshake(t, attackerCert, cfg)
	if err == nil {
		t.Fatal("handshake against a DIFFERENT certificate must fail — pinning is not enforcing anything")
	}
	t.Logf("correctly refused impostor: %v", err)
}

// The property that makes this a PIN rather than CA verification: another
// certificate issued by the very CA that signed the pinned one is still
// refused, because the pool holds the leaf, not its issuer. Anyone who can get
// a certificate out of the shared issuer does not thereby get to impersonate
// this server.
func TestPinning_RejectsAnotherLeafFromTheSameCA(t *testing.T) {
	ca, _ := selfSignedCert(t, "shared-ca")
	_, pinnedPEM := issueCert(t, "neo4j-server", &ca)
	sibling, _ := issueCert(t, "neo4j-server", &ca)

	cfg := pinnedConfigFor(t, pinnedPEM)

	err := handshake(t, sibling, cfg)
	if err == nil {
		t.Fatal("a different leaf from the same CA must be refused; the pin is behaving like CA verification")
	}
	t.Logf("correctly refused a sibling of the pinned certificate: %v", err)
}

// A CA-verified Secret must keep using the CA, not the pinned leaf.
func TestPinning_CAPathStillPreferred(t *testing.T) {
	_, certPEM := selfSignedCert(t, "ca-and-leaf")
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "srv-tls-secret", Namespace: "ns"},
		Data:       map[string][]byte{"ca.crt": certPEM, "tls.crt": certPEM},
	}).Build()

	cfg, degraded := buildTLSConfig(context.Background(), c, "ns", "srv",
		&neo4jv1beta1.TLSSpec{Mode: "cert-manager"})
	if cfg == nil {
		t.Fatal("expected a config")
	}
	if degraded {
		t.Error("CA path must not report degraded")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be used when ca.crt is present")
	}
	if cfg.InsecureSkipVerify {
		t.Error("CA path must not set InsecureSkipVerify")
	}
}

// Neither key: fail closed by returning nil so the driver verifies against
// system roots, and report the connection as unverified.
func TestPinning_NoMaterialFailsClosed(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "srv-tls-secret", Namespace: "ns"},
		Data:       map[string][]byte{"tls.key": []byte("only-a-key")},
	}).Build()

	cfg, degraded := buildTLSConfig(context.Background(), c, "ns", "srv",
		&neo4jv1beta1.TLSSpec{Mode: "cert-manager"})
	if cfg != nil {
		t.Errorf("expected nil config (fail closed via system roots), got %+v", cfg)
	}
	if !degraded {
		t.Error("expected the connection to be reported as unverified")
	}
}
