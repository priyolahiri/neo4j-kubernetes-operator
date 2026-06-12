/*
Copyright 2025.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
)

// Pins the #227 shared-SA conflict detection: new keys and identical values
// are NOT conflicts; only overwriting a different existing value is.
func TestServiceAccountAnnotationConflicts(t *testing.T) {
	existing := map[string]string{
		"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/backup-a",
		"unrelated.io/by-user":       "keep",
	}
	desired := map[string]string{
		"eks.amazonaws.com/role-arn":     "arn:aws:iam::1:role/backup-b", // conflict
		"unrelated.io/by-user":           "keep",                         // identical — no conflict
		"iam.gke.io/gcp-service-account": "new@p.iam",                    // new key — no conflict
	}
	conflicts := serviceAccountAnnotationConflicts(existing, desired)
	require.Len(t, conflicts, 1)
	assert.Contains(t, conflicts[0], "eks.amazonaws.com/role-arn")
	assert.Contains(t, conflicts[0], "backup-a")
	assert.Contains(t, conflicts[0], "backup-b")

	assert.Empty(t, serviceAccountAnnotationConflicts(nil, desired))
	assert.Empty(t, serviceAccountAnnotationConflicts(existing, nil))
}

// End-to-end on the restore side: a second CR declaring a DIFFERENT identity
// on the shared SA must win the write (documented last-writer-wins) AND emit
// the ServiceAccountAnnotationConflict warning so the fight is visible.
func TestEnsureRestoreServiceAccount_ConflictEmitsWarning(t *testing.T) {
	existingSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreServiceAccountName,
			Namespace: "default",
			Annotations: map[string]string{
				"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/other-restore",
			},
		},
	}
	restore := restoreWithBackupRef("r1", "default", "nightly")
	restore.Spec.Source.Type = "storage"
	restore.Spec.Source.Storage = &neo4jv1beta1.StorageLocation{
		Type: "s3",
		Cloud: &neo4jv1beta1.CloudBlock{
			Provider: "aws",
			Identity: &neo4jv1beta1.CloudIdentity{
				AutoCreate: &neo4jv1beta1.AutoCreateSpec{
					Annotations: map[string]string{
						"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/this-restore",
					},
				},
			},
		},
	}

	r := newResolvedSourceReconciler(t, restore, existingSA)
	require.NoError(t, r.ensureRestoreServiceAccount(context.Background(), restore))

	rec := r.Recorder.(*record.FakeRecorder)
	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, EventReasonServiceAccountAnnotationConflict)
		assert.Contains(t, ev, "other-restore")
		assert.Contains(t, ev, "this-restore")
		assert.True(t, strings.HasPrefix(ev, corev1.EventTypeWarning), "must be a Warning event: %s", ev)
	default:
		t.Fatal("expected a ServiceAccountAnnotationConflict warning event")
	}

	// Same identity re-applied: no conflict, no event. (The previous ensure
	// already wrote this-restore's value onto the SA.)
	require.NoError(t, r.ensureRestoreServiceAccount(context.Background(), restore))
	select {
	case ev := <-rec.Events:
		t.Fatalf("no event expected for an identical re-apply, got: %s", ev)
	default:
	}
}
