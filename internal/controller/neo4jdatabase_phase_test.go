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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// #565: the Neo4jDatabase status writer mapped a reason it had no arm for, and
// a condition status other than True or False, to phase Unknown — which no
// caller reached and the API reference listed as "any other blocked state".
// A Neo4jDatabase reports only the phases its API reference lists; a reason
// without an arm of its own is a failure.
func TestUpdateDatabaseStatus_WritesOnlyTheDocumentedPhases(t *testing.T) {
	documented := map[string]bool{
		neo4jv1beta1.PhaseReady: true, neo4jv1beta1.PhasePending: true, neo4jv1beta1.PhaseFailed: true,
		neo4jv1beta1.PhaseValidationFailed: true, neo4jv1beta1.PhaseError: true,
	}
	reasons := []string{
		EventReasonDatabaseReady, EventReasonValidationFailed, EventReasonClusterNotFound, EventReasonClusterNotReady,
		ConditionReasonClusterUnreachable, EventReasonSeedCredsMissing, EventReasonSeedCredsAutoInherited,
		EventReasonConnectionFailed, EventReasonCreationFailed, EventReasonDataImportFailed, EventReasonDeletionFailed,
		"AReasonWithoutAnArm",
	}
	ctx := context.Background()
	for _, status := range []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown} {
		for _, reason := range reasons {
			db := &neo4jv1beta1.Neo4jDatabase{
				ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", Generation: 1},
				Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "c", Name: "orders"},
			}
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(db).
				WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}).Build()
			r := &Neo4jDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

			r.updateDatabaseStatus(ctx, db, status, reason, "message")
			got := &neo4jv1beta1.Neo4jDatabase{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(db), got))
			assert.True(t, documented[got.Status.Phase], "%s/%s wrote phase %q", status, reason, got.Status.Phase)
		}
	}

	assert.Equal(t, neo4jv1beta1.PhaseFailed, databasePhaseFor(metav1.ConditionFalse, "AReasonWithoutAnArm"))
	assert.Equal(t, neo4jv1beta1.PhasePending, databasePhaseFor(metav1.ConditionFalse, ConditionReasonClusterUnreachable),
		"a host that does not answer is a wait")
	assert.Equal(t, neo4jv1beta1.PhaseError, databasePhaseFor(metav1.ConditionFalse, EventReasonDeletionFailed))
}
