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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// validStandalone returns a minimal standalone that should pass all validations.
func validStandalone() *neo4jv1beta1.Neo4jEnterpriseStandalone {
	return &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "test-standalone", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
}

// ---------------------------------------------------------------------------
// TestStandaloneValidator_ValidateCreate
// ---------------------------------------------------------------------------

func TestStandaloneValidator_ValidateCreate(t *testing.T) {
	v := NewStandaloneValidator()

	cases := []struct {
		name     string
		mutate   func(*neo4jv1beta1.Neo4jEnterpriseStandalone)
		wantErrs int
		errField string
	}{
		{
			name:     "valid standalone - no errors",
			mutate:   func(_ *neo4jv1beta1.Neo4jEnterpriseStandalone) {},
			wantErrs: 0,
		},
		{
			name:     "missing image.repo",
			mutate:   func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Image.Repo = "" },
			wantErrs: 1,
			errField: "spec.image.repo",
		},
		{
			// When tag is empty, two errors are raised: Required + version-invalid
			name:     "missing image.tag",
			mutate:   func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Image.Tag = "" },
			wantErrs: 2,
			errField: "spec.image.tag",
		},
		{
			name:     "unsupported Neo4j 4.x version",
			mutate:   func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Image.Tag = "4.4.0-enterprise" },
			wantErrs: 1,
			errField: "spec.image.tag",
		},
		{
			// Empty className is valid: the PVC inherits the cluster's default
			// StorageClass. A named class's existence is checked by the reconciler
			// at apply time, not here.
			name:     "empty storage.className uses cluster default",
			mutate:   func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Storage.ClassName = "" },
			wantErrs: 0,
		},
		{
			name:     "missing storage.size",
			mutate:   func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Storage.Size = "" },
			wantErrs: 1,
			errField: "spec.storage.size",
		},
		{
			name: "TLS mode mutual-tls is invalid",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "mutual-tls"}
			},
			wantErrs: 1,
			errField: "spec.tls.mode",
		},
		{
			name: "TLS mode cert-manager without issuerRef",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager"}
			},
			wantErrs: 1,
			errField: "spec.tls.issuerRef",
		},
		{
			name: "TLS mode cert-manager without issuerRef.name",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.TLS = &neo4jv1beta1.TLSSpec{
					Mode:      "cert-manager",
					IssuerRef: &neo4jv1beta1.IssuerRef{Name: ""},
				}
			},
			wantErrs: 1,
			errField: "spec.tls.issuerRef.name",
		},
		{
			name: "clustering key in spec.config is rejected",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.Config = map[string]string{
					"dbms.cluster.discovery.version": "V2_ONLY",
				}
			},
			wantErrs: 1,
		},
		{
			name: "dbms.mode in spec.config is rejected",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.Config = map[string]string{"dbms.mode": "SINGLE"}
			},
			wantErrs: 1,
		},
		{
			name: "db.format=block in spec.config is rejected (operator-managed)",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.Config = map[string]string{"db.format": "block"}
			},
			wantErrs: 1,
			errField: "spec.config.db.format",
		},
		{
			name: "db.format=high_limit in spec.config is rejected",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.Config = map[string]string{"db.format": "high_limit"}
			},
			wantErrs: 1,
			errField: "spec.config.db.format",
		},
		{
			name: "invalid auth provider",
			mutate: func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				s.Spec.Auth = &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"bogus-auth"}}
			},
			wantErrs: 1, // invalid provider name
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := validStandalone()
			tc.mutate(s)

			errs := v.ValidateCreate(s)
			if len(errs) != tc.wantErrs {
				t.Errorf("expected %d errors, got %d: %v", tc.wantErrs, len(errs), errs)
				return
			}

			if tc.errField != "" && len(errs) > 0 {
				found := false
				for _, err := range errs {
					if err.Field == tc.errField {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error on field %q, got: %v", tc.errField, errs)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestStandaloneValidator_ValidateUpdate
// ---------------------------------------------------------------------------

func TestStandaloneValidator_ValidateUpdate(t *testing.T) {
	v := NewStandaloneValidator()

	t.Run("storage class change is rejected", func(t *testing.T) {
		old := validStandalone()
		updated := validStandalone()
		updated.Spec.Storage.ClassName = "premium"

		errs := v.ValidateUpdate(old, updated)
		if len(errs) == 0 {
			t.Error("expected error when storage class changes, got none")
		}
		found := false
		for _, err := range errs {
			if err.Field == "spec.storage.className" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected error on spec.storage.className, got: %v", errs)
		}
	})

	t.Run("image tag change is accepted", func(t *testing.T) {
		old := validStandalone()
		updated := validStandalone()
		updated.Spec.Image.Tag = "2025.01.0-enterprise"

		errs := v.ValidateUpdate(old, updated)
		if len(errs) != 0 {
			t.Errorf("expected no errors for image tag change, got: %v", errs)
		}
	})
}

// The two Kinds must agree about Neo4j's own rules. Each validator used to
// carry its own list, so the SAME manifest was rejected as a cluster and
// accepted as a standalone — in both directions. The v1.15.0 release-verify
// journey watched a standalone that validated clean crash-loop on Neo4j's own
// "Invalid memory configuration - exceeds physical memory".
func TestStandaloneAndClusterAgreeOnNeo4jRules(t *testing.T) {
	config := map[string]string{
		"server.memory.heap.max_size":  "2G",
		"server.memory.pagecache.size": "1G",
	}
	resources := &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceCPU:    resource.MustParse("2"),
		},
	}

	t.Run("heap plus pagecache over the container limit", func(t *testing.T) {
		standalone := validStandalone()
		standalone.Spec.Config = config
		standalone.Spec.Resources = resources
		errs := NewStandaloneValidator().ValidateCreate(standalone)
		requireFieldError(t, errs, "spec.config")
	})

	t.Run("dbms.mode is rejected here too", func(t *testing.T) {
		standalone := validStandalone()
		standalone.Spec.Config = map[string]string{"dbms.mode": "SINGLE"}
		errs := NewStandaloneValidator().ValidateCreate(standalone)
		requireFieldError(t, errs, "spec.config.dbms.mode")
	})

	t.Run("a 4.x family is rejected by prefix", func(t *testing.T) {
		standalone := validStandalone()
		standalone.Spec.Config = map[string]string{"causal_clustering.minimum_core_cluster_size_at_formation": "3"}
		errs := NewStandaloneValidator().ValidateCreate(standalone)
		require.NotEmpty(t, errs, "the causal_clustering.* family was removed in Neo4j 5.x")
	})
}

// An absent memory limit is not a limit of zero: reporting `Invalid value:
// "0b"` named a field the user never set and a value they never wrote.
func TestMissingMemoryLimitSaysWhatIsRequired(t *testing.T) {
	standalone := validStandalone()
	standalone.Spec.Config = nil
	standalone.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}

	errs := NewStandaloneValidator().ValidateCreate(standalone)

	var found bool
	for _, e := range errs {
		if e.Field == "spec.resources.limits.memory" {
			found = true
			assert.NotContains(t, e.Error(), "0b", "must not quote a value the user never wrote")
			assert.Contains(t, e.Error(), "set a memory limit")
		}
	}
	require.True(t, found, "an absent limit must be reported on its own field, got: %v", errs)
}

// requireFieldError asserts that some error targets the given field path.
func requireFieldError(t *testing.T, errs field.ErrorList, path string) {
	t.Helper()
	for _, e := range errs {
		if e.Field == path {
			return
		}
	}
	t.Fatalf("expected an error on field %q, got: %v", path, errs)
}
