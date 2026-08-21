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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// generateSelfSignedCAPEM creates a self-signed CA certificate PEM for testing.
func generateSelfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

func TestBuildTLSConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = neo4jv1beta1.AddToScheme(scheme)

	caPEM := generateSelfSignedCAPEM(t)

	tests := []struct {
		name             string
		tlsSpec          *neo4jv1beta1.TLSSpec
		resourceName     string
		namespace        string
		secrets          []corev1.Secret
		wantNil          bool
		wantInsecureSkip bool
		wantPinned       bool
		wantRootCAs      bool
	}{
		{
			name:    "nil TLS spec returns nil",
			tlsSpec: nil,
			wantNil: true,
		},
		{
			name:    "disabled mode returns nil",
			tlsSpec: &neo4jv1beta1.TLSSpec{Mode: "disabled"},
			wantNil: true,
		},
		{
			name:             "cert-manager with no secrets fails closed (nil config)",
			tlsSpec:          &neo4jv1beta1.TLSSpec{Mode: "cert-manager"},
			resourceName:     "my-cluster",
			namespace:        "default",
			secrets:          []corev1.Secret{},
			wantInsecureSkip: true,
		},
		{
			name:         "auto-discovers CA from cert-manager secret",
			tlsSpec:      &neo4jv1beta1.TLSSpec{Mode: "cert-manager"},
			resourceName: "my-cluster",
			namespace:    "default",
			secrets: []corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "my-cluster-tls-secret", Namespace: "default"},
					Data:       map[string][]byte{"ca.crt": caPEM, "tls.crt": []byte("cert"), "tls.key": []byte("key")},
				},
			},
			wantRootCAs: true,
		},
		{
			name: "TrustedCASecret takes priority over auto-discover",
			tlsSpec: &neo4jv1beta1.TLSSpec{
				Mode:            "cert-manager",
				TrustedCASecret: "custom-ca",
			},
			resourceName: "my-cluster",
			namespace:    "default",
			secrets: []corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "custom-ca", Namespace: "default"},
					Data:       map[string][]byte{"ca.crt": caPEM},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "my-cluster-tls-secret", Namespace: "default"},
					Data:       map[string][]byte{"ca.crt": caPEM},
				},
			},
			wantRootCAs: true,
		},
		{
			name: "TrustedCASecret missing and no tls.crt fails closed",
			tlsSpec: &neo4jv1beta1.TLSSpec{
				Mode:            "cert-manager",
				TrustedCASecret: "nonexistent",
			},
			resourceName:     "my-cluster",
			namespace:        "default",
			secrets:          []corev1.Secret{},
			wantInsecureSkip: true,
		},
		{
			// tls.crt here is the literal string "cert" — NOT valid PEM. So this
			// asserts that unparseable material is NOT pinned and instead fails
			// closed. Pinning with real certificates, including refusing an
			// impostor mid-handshake, is covered in tls_pinning_test.go.
			name:         "unparseable tls.crt is not pinned and fails closed",
			tlsSpec:      &neo4jv1beta1.TLSSpec{Mode: "cert-manager"},
			resourceName: "my-cluster",
			namespace:    "default",
			secrets: []corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "my-cluster-tls-secret", Namespace: "default"},
					Data:       map[string][]byte{"tls.crt": []byte("cert"), "tls.key": []byte("key")},
				},
			},
			wantInsecureSkip: true,
		},
		{
			// No ca.crt, valid tls.crt: the strictPeerValidation:false shape.
			// Verification is achieved by pinning, so wantInsecureSkip is false.
			name:         "no ca.crt but valid tls.crt pins that certificate",
			tlsSpec:      &neo4jv1beta1.TLSSpec{Mode: "cert-manager"},
			resourceName: "my-cluster",
			namespace:    "default",
			secrets: []corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "my-cluster-tls-secret", Namespace: "default"},
					Data:       map[string][]byte{"tls.crt": caPEM, "tls.key": []byte("key")},
				},
			},
			wantPinned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for i := range tt.secrets {
				builder = builder.WithObjects(&tt.secrets[i])
			}
			k8sClient := builder.Build()

			result, insecure := buildTLSConfig(context.Background(), k8sClient, tt.namespace, tt.resourceName, tt.tlsSpec)

			// wantInsecureSkip means "verification was NOT achieved", which after
			// the pinning change is only the no-material case: pinning verifies
			// the peer, so it reports false. See tls_pinning_test.go for the
			// handshake proof that the pinned config really does verify.
			if insecure != tt.wantInsecureSkip {
				t.Errorf("verificationDisabled flag = %v, want %v", insecure, tt.wantInsecureSkip)
			}

			// No CA and no tls.crt: fail closed with a nil config so the driver
			// verifies against system roots.
			if tt.wantInsecureSkip {
				if result != nil {
					t.Errorf("expected nil config when no verification material exists, got %+v", result)
				}
				return
			}

			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil TLS config, got %+v", result)
				}
				if insecure {
					t.Error("verificationDisabled must be false when TLS is not configured at all")
				}
				return
			}

			if result == nil {
				t.Fatal("expected non-nil TLS config, got nil")
			}

			if tt.wantPinned {
				// The pin is a one-certificate trust store, not a callback: the
				// Bolt driver overwrites InsecureSkipVerify from the URI scheme,
				// so a VerifyPeerCertificate-based pin never runs in production.
				if result.RootCAs == nil {
					t.Error("expected the pinned certificate as RootCAs when ca.crt is absent but tls.crt is present")
				}
				if result.InsecureSkipVerify {
					t.Error("pinned config must not rely on InsecureSkipVerify — the driver resets it")
				}
				if result.MinVersion != tls.VersionTLS12 {
					t.Errorf("pinned config must pin MinVersion TLS1.2, got %x", result.MinVersion)
				}
			}

			if tt.wantRootCAs {
				if result.RootCAs == nil {
					t.Error("expected RootCAs to be set")
				}
				if result.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify=false when RootCAs set")
				}
			}
		})
	}
}
