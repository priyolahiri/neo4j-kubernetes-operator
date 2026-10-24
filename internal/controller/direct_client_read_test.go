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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Several writers read an object again into one that already carries fields:
// setDeploymentAnnotation into a copy of its argument, EnsureSeedCredsProjected
// into the caller's target, reconcileMutableResource into a copy of the
// desired object, updateServerStatefulSet's conflict retry into the object the
// failed attempt read and changed. Decoding JSON into a non-empty Go object
// keeps what the JSON leaves out, so in the operator's direct-client mode
// (--cache-strategy=none) and through an APIReader those reads could have
// kept fields the server no longer has (#540). They do not: controller-runtime
// zeroes the target before it decodes a typed object
// (pkg/client/apiutil/apimachinery.go, targetZeroingDecoder). This pins that,
// against a real API server and the client the direct-client mode builds: if a
// controller-runtime upgrade drops the zeroing, every one of those writers
// needs a fresh object to read into.
func TestDirectClientReadReplacesTheObject_APIServer(t *testing.T) {
	c, _ := startStoredSpecEnv(t)
	ctx := context.Background()
	key := applyManifest(t, c, "Neo4jEnterpriseCluster", "reread", map[string]any{
		"topology": map[string]any{"servers": int64(3)},
	})

	stale := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, key, stale))
	stale.Annotations[AuraFleetDeploymentAnnotation] = "not-stored" // a map key the server lacks
	stale.Spec.ExtraEnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: "not-stored"}}}} // an omitted list
	stale.Spec.Service = &neo4jv1beta1.ServiceSpec{Type: "LoadBalancer"} // an omitted pointer

	require.NoError(t, c.Get(ctx, key, stale))
	assert.NotContains(t, stale.Annotations, AuraFleetDeploymentAnnotation)
	assert.Empty(t, stale.Spec.ExtraEnvFrom)
	assert.Nil(t, stale.Spec.Service)
}
