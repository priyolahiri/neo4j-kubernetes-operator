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

package resources_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// The data volume's size is user input. The StatefulSet builder used to hand it
// to resource.MustParse, which panics on a malformed value — and a panic in a
// reconciler takes the whole manager down. The inline validator is the front line
// (and refuses malformed sizes), but the builder must not be what dies if a value
// ever reaches it: a malformed size yields a ZERO request, which the apiserver
// refuses with "must be greater than zero" instead of the process crashing.
func TestBuildServerStatefulSet_MalformedStorageSizeDoesNotPanic(t *testing.T) {
	for _, size := range []string{"fifty", "10 Gi", "5K", "", "1e"} {
		t.Run(size, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.Storage.Size = size

			var request = func() (zero bool) {
				sts := resources.BuildServerStatefulSetForEnterprise(cluster)
				require.NotNil(t, sts)
				require.NotEmpty(t, sts.Spec.VolumeClaimTemplates)
				q := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
				return q.IsZero()
			}
			require.NotPanics(t, func() { assert.True(t, request(), "a malformed size must produce a zero request") })
		})
	}
}

func TestBuildServerStatefulSet_ValidStorageSizeIsRequested(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage.Size = "20Gi"
	sts := resources.BuildServerStatefulSetForEnterprise(cluster)
	q := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "20Gi", q.String())
}
