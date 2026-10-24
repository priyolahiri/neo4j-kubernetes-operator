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

// A change of spec.topology.servers waits for an upgrade (upgrade_topology.go,
// operations.md id 163). The envtest spec "Neo4jEnterpriseCluster whose
// upgrade holds its servers" drives the normal pass; these pin the pieces.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestHoldTopologyWhileUpgradeHolds(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		servers    int32
		mutate     func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		wantPinned int32
		wantCond   metav1.ConditionStatus // "" for absent
	}{
		{"Paused: a scale-up waits", 4, nil, 3, metav1.ConditionTrue},
		{"Paused: a scale-down waits", 2, nil, 3, metav1.ConditionTrue},
		{"Failed: a scale-up waits", 5, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseFailed
		}, 3, metav1.ConditionTrue},
		{"no change", 3, nil, 3, ""},
		{"a change reverted clears the condition", 3, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.Conditions = []metav1.Condition{topologyDeferral(c, 4, 3)}
		}, 3, metav1.ConditionFalse},
		{"the upgrade completed: applied", 4, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseCompleted
			c.Status.Conditions = []metav1.Condition{topologyDeferral(c, 4, 3)}
		}, 4, metav1.ConditionFalse},
		{"a drain already in progress finishes", 2, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Annotations = map[string]string{ScaleDownDrainingServersAnnotation: "id-a"}
		}, 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := heldCluster(upgradePhasePaused, true)
			c.Spec.Topology.Servers = tc.servers
			if tc.mutate != nil {
				tc.mutate(c)
			}
			r := gateTestReconciler(t, c.DeepCopy(), heldSTS())

			r.holdTopologyWhileUpgradeHolds(ctx, c)

			assert.Equal(t, tc.wantPinned, c.Spec.Topology.Servers, "the server count the rest of the pass works on")
			stored := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(c), stored))
			cond := findCondition(stored.Status.Conditions, ConditionTypeTopologyChangeDeferred)
			if tc.wantCond == "" {
				assert.Nil(t, cond)
				return
			}
			require.NotNil(t, cond)
			assert.Equal(t, tc.wantCond, cond.Status)
			if tc.wantCond == metav1.ConditionTrue {
				assert.Equal(t, ConditionReasonWaitsForUpgrade, cond.Reason)
				assert.Contains(t, cond.Message, "the server StatefulSet keeps 3 servers until the upgrade to "+holdTarget)
			}
		})
	}
}

// While the machine rolls, a change in either direction is reported; it was
// only a scale-down, through ServersPendingDrain.
func TestStepUpgradeRolling_ReportsADeferredTopologyChange(t *testing.T) {
	ctx := context.Background()
	c := heldCluster(upgradePhaseRolling, false)
	c.Spec.Topology.Servers = 4
	r := gateTestReconciler(t, c.DeepCopy(), heldSTS())
	r.UpgradeGates = &upgradeGates{
		serverAvailable: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, string, error) {
			return false, "", nil
		},
	}
	_, err := r.stepUpgradeRolling(ctx, c)
	require.NoError(t, err)
	stored := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(c), stored))
	cond := findCondition(stored.Status.Conditions, ConditionTypeTopologyChangeDeferred)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, "spec.topology.servers is 4; the server StatefulSet keeps 3 servers")
}
