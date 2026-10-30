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

// The drain set (neo4j.com/scale-down-draining-servers) holds server ids
// only; pods held before their servers are named are in an annotation of
// their own (operations.md id 213). The walk of the #619 fixes: while a stuck
// cluster was lowered to 3, the drain set read "c3-server-3,c3-server-4" — the
// pod names the drain holds the replicas by before it asks Neo4j, read back
// on the next pass as server ids that matched nothing.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// drainSetWrites records every value a patch gives the cluster's drain set.
func drainSetWrites(t *testing.T, r *Neo4jEnterpriseClusterReconciler, fc client.Client) *[]string {
	t.Helper()
	ww, ok := fc.(client.WithWatch)
	require.True(t, ok)
	var seen []string
	r.Client = interceptor.NewClient(ww, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if cl, isCluster := obj.(*neo4jv1beta1.Neo4jEnterpriseCluster); isCluster {
				seen = append(seen, cl.Annotations[ScaleDownDrainingServersAnnotation])
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	return &seen
}

// noPodNames asserts no value the drain set took names a pod.
func noPodNames(t *testing.T, seen []string, cluster string) {
	t.Helper()
	for _, v := range seen {
		assert.NotContains(t, v, cluster+"-server-", "the drain set holds server ids only: %q", v)
	}
}

func TestScaleDownDrain_TheDrainSetHoldsServerIDsOnly(t *testing.T) {
	t.Run("before Neo4j is asked, the pods are held by name", func(t *testing.T) {
		neo := newDrainNeo4j("held", 5)
		r, fc, c := scalingDown(t, "held", neo, nil)
		seen := drainSetWrites(t, r, fc)
		reachable := false
		r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) {
			if !reachable {
				return nil, errors.New("connection refused")
			}
			return neo, nil
		}

		drainPass(t, r, fc, c)
		stored := latestCluster(t, fc, c)
		assert.Equal(t, "held-server-3,held-server-4", stored.Annotations[ScaleDownHeldPodsAnnotation])
		assert.Empty(t, drainServerIDsFromAnnotation(stored))
		assert.True(t, scaleDownDrainInProgress(stored), "the replicas are held")

		reachable = true
		drainPass(t, r, fc, c)
		stored = latestCluster(t, fc, c)
		assert.Equal(t, []string{"uuid-3", "uuid-4"}, drainServerIDsFromAnnotation(stored))
		assert.Empty(t, stored.Annotations[ScaleDownHeldPodsAnnotation], "the drain set names the servers now")
		noPodNames(t, *seen, "held")
	})

	t.Run("a stuck cluster lowered: its dropped servers are not drained", func(t *testing.T) {
		// The walk's case: server-3 and server-4 run as servers Neo4j dropped,
		// and the spec goes back to 3.
		neo := newDrainNeo4j("stuck", 5)
		neo.servers["uuid-3"].State = "Dropped"
		neo.servers["uuid-4"].State = "Dropped"
		r, fc, c := scalingDown(t, "stuck", neo, nil)
		seen := drainSetWrites(t, r, fc)

		drainPass(t, r, fc, c)
		stored := latestCluster(t, fc, c)
		assert.False(t, scaleDownDrainInProgress(stored), "nothing to drain: the hold is released")
		assert.Empty(t, stored.Annotations[ScaleDownHeldPodsAnnotation])
		noPodNames(t, *seen, "stuck")
	})

	t.Run("while it cannot tell which servers go, the pods are held by name", func(t *testing.T) {
		// Server-1, which the scale-down keeps, and server-3 are down, and no
		// address is known for either (operations.md id 213).
		neo := newDrainNeo4j("unsure", 5, 1, 3)
		r, fc, c := scalingDown(t, "unsure", neo, nil)
		seen := drainSetWrites(t, r, fc)

		for range 6 {
			drainPass(t, r, fc, c)
		}
		stored := latestCluster(t, fc, c)
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State, "the server it can name is drained")
		assert.True(t, scaleDownDrainInProgress(stored), "and the replicas stay held")
		assert.Equal(t, "unsure-server-3,unsure-server-4", stored.Annotations[ScaleDownHeldPodsAnnotation])
		noPodNames(t, *seen, "unsure")
	})

	t.Run("set back before Neo4j was asked, the held pods go", func(t *testing.T) {
		neo := newDrainNeo4j("early", 5)
		r, fc, c := scalingDown(t, "early", neo, nil)
		asked := 0
		r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) {
			asked++
			return nil, errors.New("connection refused")
		}
		drainPass(t, r, fc, c)
		require.True(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
		asked = 0

		setServers(t, fc, c, 5)
		drainPass(t, r, fc, c)
		stored := latestCluster(t, fc, c)
		assert.False(t, scaleDownDrainInProgress(stored), "nothing was named or cordoned: the hold goes")
		assert.Zero(t, asked, "and Neo4j is not asked")
	})
}

func TestScaleDownDrainInProgress_EitherRecordHolds(t *testing.T) {
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	assert.False(t, scaleDownDrainInProgress(c))
	c.Annotations = map[string]string{ScaleDownHeldPodsAnnotation: "c-server-3"}
	assert.True(t, scaleDownDrainInProgress(c))
	c.Annotations = map[string]string{ScaleDownDrainingServersAnnotation: "uuid-3"}
	assert.True(t, scaleDownDrainInProgress(c))
	// A drain set an operator before #619 left with pod names still holds,
	// and the next pass replaces it with the servers' ids.
	c.Annotations = map[string]string{ScaleDownDrainingServersAnnotation: strings.Join([]string{"c-server-3", "c-server-4"}, ",")}
	assert.True(t, scaleDownDrainInProgress(c))
}
