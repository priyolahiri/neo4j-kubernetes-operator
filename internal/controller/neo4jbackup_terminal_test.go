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
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A one-time backup is terminal once Completed or Failed (#116), whatever its
// target is doing. Live, a completed backup read Waiting while its standalone
// restarted, and once its Job had been garbage-collected the backup ran again.
func TestReconcile_TerminalOneTimeBackupIgnoresItsTarget(t *testing.T) {
	for _, phase := range []string{neo4jv1beta1.PhaseCompleted, neo4jv1beta1.PhaseFailed} {
		for _, target := range []string{"missing", "not ready"} {
			t.Run(phase+", target "+target, func(t *testing.T) {
				backup := &neo4jv1beta1.Neo4jBackup{
					ObjectMeta: metav1.ObjectMeta{Name: "first-backup", Namespace: "ns", Finalizers: []string{BackupFinalizer}},
					Spec: neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "sa", Database: "neo4j",
						Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}}},
					Status: neo4jv1beta1.Neo4jBackupStatus{Phase: phase, Message: "done"},
				}
				var r *Neo4jBackupReconciler
				if target == "missing" {
					r = newBackupTestReconcilerWithStatus(t, backup)
				} else {
					sa := minimalStandaloneForRestore("sa", "ns") // Status.Ready is false
					r = newBackupTestReconcilerWithStatus(t, backup, sa)
				}

				_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "first-backup", Namespace: "ns"}})
				require.NoError(t, err)

				got := &neo4jv1beta1.Neo4jBackup{}
				require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "first-backup", Namespace: "ns"}, got))
				assert.Equal(t, phase, got.Status.Phase, "the terminal phase is not overwritten")
				err = r.Get(context.Background(), types.NamespacedName{Name: "first-backup-backup", Namespace: "ns"}, &batchv1.Job{})
				assert.True(t, apierrors.IsNotFound(err), "no backup Job is created again")
			})
		}
	}
}
