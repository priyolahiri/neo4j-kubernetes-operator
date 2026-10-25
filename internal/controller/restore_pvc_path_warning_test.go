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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A PVC backupPath that starts with the /backup mount path is announced as an
// advisory ValidationWarning, once, on the restore's first pass — before the
// restore waits for its target or fails on the path (2026-10-25 walk:
// backupPath /backup/bk1 became /backup//backup/bk1).
func TestRestoreReconcile_APVCBackupPathUnderTheMountPathIsAnnounced(t *testing.T) {
	restore := minimalRestore("r", "ns", "sa")
	restore.Finalizers = []string{RestoreFinalizer}
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type: "storage", BackupPath: "/backup/bk1",
		Storage: &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
	}
	r := statusRestoreReconciler(t, restore)
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)

	for range 3 {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)})
		require.NoError(t, err)
	}
	warnings := eventsWithReason(drainEvents(rec), EventReasonValidationWarning)
	require.Len(t, warnings, 1, "advisory: once, not on every pass: %q", warnings)
	assert.Contains(t, warnings[0], `spec.source.backupPath "/backup/bk1"`)
	assert.Contains(t, warnings[0], `write "bk1"`)
}
