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

package resources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// An enabled ExternalSecrets block without secretStoreRef is refused by
// validation, but the builders must not panic if one reaches them: the auth
// block was never validated, and every pass of such a cluster panicked.
func TestBuildExternalSecret_WithoutAStoreBuildsNothing(t *testing.T) {
	noStore := &neo4jv1beta1.ExternalSecretsConfig{Enabled: true}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			TLS:  &neo4jv1beta1.TLSSpec{Mode: "cert-manager", ExternalSecrets: noStore},
			Auth: &neo4jv1beta1.AuthSpec{ExternalSecrets: noStore},
		},
	}
	assert.NotPanics(t, func() { assert.Nil(t, BuildExternalSecretForAuth(cluster)) })
	assert.NotPanics(t, func() { assert.Nil(t, BuildExternalSecretForTLS(cluster)) })
}
