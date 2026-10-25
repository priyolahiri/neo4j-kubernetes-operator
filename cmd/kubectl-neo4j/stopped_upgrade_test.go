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

package main

// A cluster whose upgrade stopped stays Ready (#565): `status --problems` and
// `diagnose` read its upgrade record, or neither would say that the image
// change did not land.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func stoppedUpgradeCluster(name, upgradePhase string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase:   "Ready",
			Message: "Neo4j cluster is fully formed and ready; its upgrade to 2026.09.0-enterprise failed (status.upgradeStatus)",
			UpgradeStatus: &neo4jv1beta1.UpgradeStatus{
				Phase: upgradePhase, TargetVersion: "2026.09.0-enterprise",
				LastError: "timed out waiting for server ordinal 2 to roll and rejoin the cluster",
			},
		},
	}
}

func TestStatusProblems_ReportsAClusterWhoseUpgradeStopped(t *testing.T) {
	c := testClient(t,
		stoppedUpgradeCluster("failed", "Failed"),
		stoppedUpgradeCluster("paused", "Paused"),
		stoppedUpgradeCluster("rolling", "Rolling"),
		stoppedUpgradeCluster("done", "Completed"),
	)
	rows, err := collectStatus(context.Background(), c, "neo4j")
	require.NoError(t, err)
	byName := map[string]resourceStatus{}
	for _, r := range rows {
		byName[r.name] = r
	}
	assert.False(t, byName["failed"].healthy(), "a Failed upgrade: the image change did not land")
	assert.False(t, byName["paused"].healthy(), "a Paused upgrade waits for the user")
	assert.True(t, byName["rolling"].healthy(), "an upgrade in progress is not a problem")
	assert.True(t, byName["done"].healthy())

	out := renderTo(t, rows, false, true, "neo4j")
	assert.Contains(t, out, "Ready (upgrade Failed)")
	assert.Contains(t, out, "Ready (upgrade Paused)")
	assert.NotContains(t, out, "rolling")
	assert.Contains(t, out, "✗ Neo4jEnterpriseCluster/failed: Neo4j cluster is fully formed and ready; its upgrade to 2026.09.0-enterprise failed")
	assert.Contains(t, out, "last error: timed out waiting for server ordinal 2")
}

func TestDiagnose_AClusterWhoseUpgradeStoppedHasAProblem(t *testing.T) {
	for upgrade, action := range map[string]string{"Failed": "The operator retries it", "Paused": "It waits for you"} {
		c := testClient(t, stoppedUpgradeCluster("prod", upgrade))
		results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
		require.NoError(t, err)
		out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
		assert.Contains(t, out, markProblem+" upgrade to 2026.09.0-enterprise is "+upgrade)
		assert.Contains(t, out, "timed out waiting for server ordinal 2")
		assert.Contains(t, out, action)
		require.Len(t, results, 1)
		assert.True(t, results[0].problems(), "%s: diagnose exits with problems", upgrade)
	}

	c := testClient(t, stoppedUpgradeCluster("prod", "Rolling"))
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	assert.NotContains(t, joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod")), "upgrade to")
}
