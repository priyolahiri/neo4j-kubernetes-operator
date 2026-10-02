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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Invariant 5 (docs/knowledge/invariants.md, INV-5): the cluster is ONE
// `{cluster}-server` StatefulSet with `replicas: N`, whose pods are
// `{cluster}-server-0…N-1`. This pins the PRODUCTION builder,
// BuildServerStatefulSetForEnterprise.
//
// It exists because the older tests that talk about "the server StatefulSet"
// drive the deprecated plural builder (BuildServerStatefulSetsForEnterprise,
// one single-replica StatefulSet per server), so they would stay green if the
// production path regressed to N StatefulSets — or to primary-*/secondary-*
// naming — and nothing else asserted the shape.
func TestServerStatefulSet_IsOneStatefulSetWithReplicasEqualToServers(t *testing.T) {
	for _, servers := range []int32{2, 3, 5} {
		t.Run(fmt.Sprintf("%d servers", servers), func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.Topology.Servers = servers

			sts := resources.BuildServerStatefulSetForEnterprise(cluster)
			require.NotNil(t, sts)

			assert.Equal(t, cluster.Name+"-server", sts.Name,
				"the single StatefulSet must be named {cluster}-server, not {cluster}-server-0")
			require.NotNil(t, sts.Spec.Replicas)
			assert.Equal(t, servers, *sts.Spec.Replicas,
				"replicas must equal spec.topology.servers; pods are {cluster}-server-0…N-1")

			lower := strings.ToLower(sts.Name)
			assert.NotContains(t, lower, "primary", "primary-* naming was removed (invariant 5)")
			assert.NotContains(t, lower, "secondary", "secondary-* naming was removed (invariant 5)")
		})
	}
}
