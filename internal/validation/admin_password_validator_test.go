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

package validation

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func pwScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

func pwSecret(name, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte(password)},
	}
}

// A leading "-" makes neo4j-admin's parser read the password as an option, so the
// container dies with "Missing required parameter: '<password>'" and crash-loops.
// This is the exact failure that took down the shared RBAC cluster in CI.
func TestValidateAdminSecretPassword_RejectsLeadingDash(t *testing.T) {
	cases := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{"plain", "supersecret123", false},
		{"contains a dash", "super-secret-123", false},
		{"contains an underscore", "super_secret_123", false},
		{"base64url-ish, safe first char", "pAbC-dEf_gHi", false},
		{"LEADING dash", "-AbC123xyz", true},
		{"leading dash after whitespace", "  -AbC123xyz", true},
		{"leading double dash", "--password", true},
		{"just a dash", "-", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(pwScheme(t)).
				WithObjects(pwSecret("admin", tc.password)).Build()
			errs := ValidateAdminSecretPassword(context.Background(), c, "ns", "admin",
				field.NewPath("spec", "auth", "adminSecret"))
			if tc.wantErr && len(errs) == 0 {
				t.Fatalf("password %q must be rejected: it crash-loops the pod", tc.password)
			}
			if !tc.wantErr && len(errs) != 0 {
				t.Fatalf("password %q must be accepted, got %v", tc.password, errs)
			}
			if tc.wantErr {
				msg := errs[0].Error()
				// The message must explain the cause, since the runtime symptom
				// ("Missing required parameter") never mentions the password.
				if !strings.Contains(msg, "set-initial-password") {
					t.Errorf("error should name the failing command, got: %s", msg)
				}
				// It must never echo the password itself. Only meaningful for a
				// password long enough not to be a trivial substring of the
				// explanatory text (which necessarily contains "-" and "--").
				if len(strings.TrimSpace(tc.password)) > 3 && strings.Contains(msg, tc.password) {
					t.Errorf("error must not leak the password, got: %s", msg)
				}
			}
		})
	}
}

// A Secret that does not exist yet is NOT an error: it may legitimately be
// created after the CR, and Kubernetes already blocks container creation with a
// clear message because the env var is projected with Optional:false.
func TestValidateAdminSecretPassword_ToleratesAbsentSecretOrKey(t *testing.T) {
	t.Run("secret missing entirely", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(pwScheme(t)).Build()
		if errs := ValidateAdminSecretPassword(context.Background(), c, "ns", "nope",
			field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("absent Secret must not fail validation, got %v", errs)
		}
	})

	t.Run("password key missing", func(t *testing.T) {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "admin", Namespace: "ns"},
			Data:       map[string][]byte{"username": []byte("neo4j")},
		}
		c := fake.NewClientBuilder().WithScheme(pwScheme(t)).WithObjects(sec).Build()
		if errs := ValidateAdminSecretPassword(context.Background(), c, "ns", "admin",
			field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("absent password key must not fail validation, got %v", errs)
		}
	})

	t.Run("empty password value", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(pwScheme(t)).
			WithObjects(pwSecret("admin", "")).Build()
		if errs := ValidateAdminSecretPassword(context.Background(), c, "ns", "admin",
			field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("empty password is a different problem; not this check's job, got %v", errs)
		}
	})

	t.Run("nil client / empty name are no-ops", func(t *testing.T) {
		if errs := ValidateAdminSecretPassword(context.Background(), nil, "ns", "admin",
			field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("nil client must be a no-op, got %v", errs)
		}
		var c client.Client = fake.NewClientBuilder().WithScheme(pwScheme(t)).Build()
		if errs := ValidateAdminSecretPassword(context.Background(), c, "ns", "",
			field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("empty secret name must be a no-op, got %v", errs)
		}
	})
}

// The validator must judge the SAME Secret the StatefulSet mounts — the explicit
// spec.auth.adminSecret when set, else the builder's default.
func TestResolveAdminSecretName(t *testing.T) {
	if got := resolveAdminSecretName(nil); got != "neo4j-admin-secret" {
		t.Errorf("nil auth -> %q, want the operator default", got)
	}
	if got := resolveAdminSecretName(&neo4jv1beta1.AuthSpec{AdminSecret: "chosen"}); got != "chosen" {
		t.Errorf("explicit adminSecret -> %q, want chosen", got)
	}
}
