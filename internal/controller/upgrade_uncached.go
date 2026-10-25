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

// What the upgrade steps decide from (operations.md id 203).
//
// The informer cache can hold an object from before a write: the pass after a
// write is started by that write's watch event, but a pass started by
// anything else (a pod event, a requeue) can read the cache in between, and a
// lost watch keeps it stale for longer. A stale copy is consistently old — its
// spec and its status are from the same moment — so it looks like a state the
// cluster was really in. Read where a step only waits, that costs a pass. Read
// where a step moves the partition or declares a server done, it can act on a
// state that has moved on:
//
//   - the server StatefulSet from before Staging wrote a new target: its status
//     still counts the servers on the old target as updated, every one ready, so
//     the Rolling step found the step settled, checked the server at the
//     partition (available, on the old target) and lowered the partition onto
//     the new template, a server restarted with no check;
//   - the upgrade record from before a write that moved it on — a retarget
//     recorded, a stop, a partition lowered: the step acted on the record that
//     write replaced, and its own write put the old phase back over the new one
//     (a Verifying pass completing an upgrade the user had just retargeted ended
//     the hold, and the normal pass then rolled every server, ungated).
//
// So Staging (since #562), and now Rolling, Stabilizing and Verifying, read
// the record as stored and wait while it differs from the cached copy in phase,
// target or partition; Rolling reads the StatefulSet from the API server; and
// the pods a step stages or replaces by are read from the API server too
// (serverPodsUncached). Where a stale read only delays — the start checks of
// isUpgradeRequired and startRollingUpgrade, which Staging judges again from
// the API server, the replica count, the cluster's own Ready checks — the
// cache stays.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// upgradeRecordMovedOn says why the cluster's upgrade record as stored is not
// the one this pass's step would act on: another phase, target or partition
// than the cached copy's, or a record that cannot be read. "" when it is the
// same.
func (r *Neo4jEnterpriseClusterReconciler) upgradeRecordMovedOn(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	stored := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(cluster), stored); err != nil {
		return "the stored upgrade record cannot be read: " + err.Error()
	}
	have, want := cluster.Status.UpgradeStatus, stored.Status.UpgradeStatus
	switch {
	case want == nil:
		return "the stored cluster has no upgrade record"
	case have == nil:
		return fmt.Sprintf("the stored upgrade record is %s towards %s", want.Phase, want.TargetVersion)
	case want.Phase != have.Phase || want.TargetVersion != have.TargetVersion:
		return fmt.Sprintf("the stored upgrade record is %s towards %s, the cached one %s towards %s",
			want.Phase, want.TargetVersion, have.Phase, have.TargetVersion)
	case !samePartition(want.CurrentPartition, have.CurrentPartition):
		return fmt.Sprintf("the stored upgrade record is at partition %s, the cached one at %s",
			partitionString(want.CurrentPartition), partitionString(have.CurrentPartition))
	}
	return ""
}

func samePartition(a, b *int32) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

func partitionString(p *int32) string {
	if p == nil {
		return "none"
	}
	return fmt.Sprintf("%d", *p)
}

// serverStatefulSetUncached reads the cluster's server StatefulSet from the
// API server.
func (r *Neo4jEnterpriseClusterReconciler) serverStatefulSetUncached(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, sts); err != nil {
		return nil, err
	}
	return sts, nil
}
