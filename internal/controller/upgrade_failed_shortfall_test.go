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

// A Failed upgrade on a cluster short of a server (operations.md ids 105,
// 149). The walk of be1e0a6f: an upgrade with autoPauseOnFailure off Failed
// on server-2, which could not pull the target. #565 keeps the cluster's
// phase Ready, and the retry of a Failed upgrade started on every pass, so the
// normal pass, which records a server short, never ran: for over ten minutes
// the Degraded condition said "All 3 servers are available", SHOW SERVERS and
// ServersHealthy=False said server-2 was not, and the phase never turned
// Degraded. A Paused upgrade's normal pass recorded it.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// failedOnAServer is the walk's cluster: Ready with every server available
// when its upgrade Failed with server-2 restarted onto the target, and the
// server StatefulSet with server-2's pod not Ready.
func failedOnAServer(t *testing.T) (*Neo4jEnterpriseClusterReconciler, *record.FakeRecorder) {
	t.Helper()
	c := heldCluster(upgradePhaseFailed, false)
	c.Status.UpgradeStatus.LastError = "timed out waiting for server ordinal 2 to roll and rejoin the cluster"
	c.Status.Conditions = []metav1.Condition{
		cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady),
		cond(ConditionTypeClusterFormed, metav1.ConditionTrue, ConditionReasonFormationComplete),
		{Type: ConditionTypeDegraded, Status: metav1.ConditionFalse, Reason: ConditionReasonAllServersAvailable,
			Message: "All 3 servers are available", LastTransitionTime: metav1.Now()},
	}
	syncStalledCondition(c)
	sts := heldSTS()
	sts.Status.ReadyReplicas = 2 // server-2: ImagePullBackOff
	r := gateTestReconciler(t, sts, c)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec
	return r, rec
}

func TestFailedUpgrade_AServerShortIsRecordedThenDegraded(t *testing.T) {
	ctx := context.Background()
	r, rec := failedOnAServer(t)

	// The retry waits for server-2: the normal pass runs.
	require.False(t, r.isUpgradeRequired(ctx, refetch(t, r)),
		"a Failed upgrade is not retried while a server pod is not Ready; the normal pass records the shortfall")

	// The normal pass: Ready, with the Degraded condition naming server-2.
	oneDown := formationCheck{message: "Cluster formation in progress: 2/3 servers available", servers: availServers(3, 2)}
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	c := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase)
	degraded := findCondition(c.Status.Conditions, ConditionTypeDegraded)
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Contains(t, degraded.Message, "c-server-2 (Unavailable)")
	assert.Contains(t, c.Status.Message, "2 of 3 servers available")
	assert.Equal(t, metav1.ConditionTrue, findCondition(c.Status.Conditions, ConditionTypeStalled).Status, "the stopped upgrade stays Stalled")
	assert.False(t, r.isUpgradeRequired(ctx, c), "and is not retried while it is short")

	// Past the grace period: phase Degraded, announced once; still no retry.
	backdateDegraded(t, r, 6*time.Minute)
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	c = refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseDegraded, c.Status.Phase)
	assert.Equal(t, metav1.ConditionTrue, findCondition(c.Status.Conditions, ConditionTypeStalled).Status)
	var degradedEvents int
	for _, e := range drainEvents(rec) {
		if strings.HasPrefix(e, "Warning "+EventReasonClusterDegraded+" ") {
			degradedEvents++
		}
	}
	assert.Equal(t, 1, degradedEvents)
	assert.False(t, r.isUpgradeRequired(ctx, c), "a Degraded cluster's Failed upgrade waits for every server")

	// Every server back: Ready, and the retry follows.
	sts := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server"}, sts))
	sts.Status.ReadyReplicas = 3
	require.NoError(t, r.Status().Update(ctx, sts))
	r.recordClusterReady(ctx, refetch(t, r), availServers(3, 3), clusterFormedCondition(), allServersAvailableCondition(3))
	c = refetch(t, r)
	require.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase)
	assert.True(t, r.isUpgradeRequired(ctx, c), "retried once every server is available")
}

// A retry the pre-upgrade check refused (refuseUpgradeStart, which records the
// attempt's start) is not tried again for upgradeStartRetry, so the normal
// pass runs in between and records a server that Neo4j, not Kubernetes,
// reports unavailable.
func TestFailedUpgrade_ARefusedRetryWaitsForTheInterval(t *testing.T) {
	ctx := context.Background()
	r, _ := failedOnAServer(t)
	sts := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server"}, sts))
	sts.Status.ReadyReplicas = 3
	require.NoError(t, r.Status().Update(ctx, sts))

	c := refetch(t, r)
	require.True(t, r.isUpgradeRequired(ctx, c))
	refused := metav1.NewTime(time.Now().Add(-10 * time.Second))
	c.Status.UpgradeStatus.StepStartTime = &refused
	assert.False(t, r.isUpgradeRequired(ctx, c), "refused ten seconds ago")
	refused = metav1.NewTime(time.Now().Add(-upgradeStartRetry - time.Second))
	c.Status.UpgradeStatus.StepStartTime = &refused
	assert.True(t, r.isUpgradeRequired(ctx, c), "due again")
}
