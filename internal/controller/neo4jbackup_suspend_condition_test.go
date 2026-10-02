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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A suspended Neo4jBackup is paused on purpose. Through the real status write
// (updateBackupStatus), its Ready condition must stay False — it is not Ready to
// run — while the phase stays Suspended and the reason says why, instead of the
// ReconciliationFailed it shared with a genuine failure.
func TestUpdateBackupStatus_SuspendedReportsASuspendedReason(t *testing.T) {
	const ns = "default"
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "ec",
			AllDatabases: true,
			Suspend:      true,
		},
	}
	r := newShardedTestReconciler(t, backup)
	ctx := context.Background()

	r.updateBackupStatus(ctx, backup, neo4jv1beta1.PhaseSuspended, "Backup is suspended")

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "nightly", Namespace: ns}, got))
	assert.Equal(t, neo4jv1beta1.PhaseSuspended, got.Status.Phase, "the phase is unchanged")

	ready := findCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status, "a suspended backup is not Ready")
	assert.Equal(t, "Suspended", ready.Reason)
	assert.NotEqual(t, "ReconciliationFailed", ready.Reason)
}

// A genuinely failed backup keeps the failure reason — the new reason must not
// leak onto Failed or Invalid.
func TestUpdateBackupStatus_FailedAndInvalidKeepTheFailureReason(t *testing.T) {
	const ns = "default"
	for _, phase := range []string{neo4jv1beta1.PhaseFailed, neo4jv1beta1.PhaseInvalid} {
		t.Run(phase, func(t *testing.T) {
			backup := &neo4jv1beta1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns},
				Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "ec", AllDatabases: true},
			}
			r := newShardedTestReconciler(t, backup)
			ctx := context.Background()

			r.updateBackupStatus(ctx, backup, phase, "boom")

			got := &neo4jv1beta1.Neo4jBackup{}
			require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "nightly", Namespace: ns}, got))
			ready := findCondition(got.Status.Conditions, ConditionTypeReady)
			require.NotNil(t, ready)
			assert.Equal(t, metav1.ConditionFalse, ready.Status)
			assert.Equal(t, "ReconciliationFailed", ready.Reason)
		})
	}
}
