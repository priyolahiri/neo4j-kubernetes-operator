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

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// spec.auth.externalSecrets builds an ExternalSecret just as spec.tls does, and
// the builder dereferences secretStoreRef. Only the TLS block was checked, so
// an auth block without a store reached the builder and panicked every pass.
func TestAuthValidator_ChecksTheAuthExternalSecretsLikeTLS(t *testing.T) {
	enabledWithoutStore := &neo4jv1beta1.AuthSpec{
		ExternalSecrets: &neo4jv1beta1.ExternalSecretsConfig{Enabled: true},
	}

	t.Run("a cluster's auth block without a store is refused", func(t *testing.T) {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{Auth: enabledWithoutStore}}
		errs := NewAuthValidator().Validate(cluster)
		require.NotEmpty(t, errs)
		var paths []string
		for _, e := range errs {
			paths = append(paths, e.Field)
		}
		assert.Contains(t, paths, "spec.auth.externalSecrets.secretStoreRef")
		assert.Contains(t, paths, "spec.auth.externalSecrets.data")
	})

	t.Run("a complete auth block is accepted", func(t *testing.T) {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{Auth: &neo4jv1beta1.AuthSpec{
			ExternalSecrets: &neo4jv1beta1.ExternalSecretsConfig{
				Enabled:        true,
				SecretStoreRef: &neo4jv1beta1.SecretStoreRef{Name: "vault", Kind: "ClusterSecretStore"},
				Data:           []neo4jv1beta1.ExternalSecretData{{SecretKey: "password", RemoteRef: &neo4jv1beta1.ExternalSecretRemoteRef{Key: "neo4j/admin"}}},
			},
		}}}
		assert.Empty(t, NewAuthValidator().Validate(cluster))
	})

	// The standalone never builds an ExternalSecret from spec.auth, so the
	// same block on a standalone stays accepted, as before.
	t.Run("a standalone's auth block is not judged", func(t *testing.T) {
		assert.Empty(t, NewAuthValidator().ValidateAuthSpec(enabledWithoutStore, nil))
	})
}
