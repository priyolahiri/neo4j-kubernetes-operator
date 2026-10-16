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

// A cloud restore whose target's server pods lack the seed credentials or
// endpoint waits, and carries on once the target has them (knowledge
// backup-restore.md Rule 138). It used to go Failed, and the previously-failed
// guard kept it there after the user set the annotation its message offered.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// seedConfigWaitFixture runs the real Reconcile against a fake client holding
// a 5.26 cluster without the seed credentials and a restore from S3 whose
// source is already pinned.
type seedConfigWaitFixture struct {
	t        *testing.T
	ctx      context.Context
	r        *Neo4jRestoreReconciler
	recorder *record.FakeRecorder
	req      ctrl.Request
}

func newSeedConfigWaitFixture(t *testing.T, allDatabases bool) *seedConfigWaitFixture {
	t.Helper()
	cluster := minimalClusterForRestore("c", "ns")
	restore := minimalRestore("r", "ns", "c")
	restore.Finalizers = []string{RestoreFinalizer}
	restore.Spec.Source = neo4jv1beta1.RestoreSource{Type: SourceTypeBackup, BackupRef: "b"}
	snap := &neo4jv1beta1.ResolvedRestoreSource{
		BackupRef:  "b",
		BackupPath: "nightly",
		Storage: &neo4jv1beta1.StorageLocation{
			Type: "s3", Bucket: "neo4j-backups", Path: "prod",
			Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "creds"},
		},
		ArtifactFilename: "neo4j-2026-10-14T08-00-00.backup",
		ArtifactType:     "FULL",
	}
	if allDatabases {
		restore.Spec.AllDatabases = true
		restore.Spec.Database = ""
		snap.ArtifactFilename, snap.ArtifactType = "", ""
		snap.DatabaseArtifacts = []neo4jv1beta1.DatabaseArtifact{
			{Database: "system", Filename: "system-2026-10-14T08-00-00.backup", Type: "FULL"},
			{Database: "orders", Filename: "orders-2026-10-14T08-00-00.backup", Type: "FULL"},
		}
	}
	restore.Status.ResolvedSource = snap
	backup := &neo4jv1beta1.Neo4jBackup{}
	backup.Name, backup.Namespace = "b", "ns"

	r := statusRestoreReconciler(t, cluster, restore, backup)
	r.RequeueAfter = 30 * time.Second
	recorder := record.NewFakeRecorder(64)
	r.Recorder = recorder
	return &seedConfigWaitFixture{
		t: t, ctx: context.Background(), r: r, recorder: recorder,
		req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "r", Namespace: "ns"}},
	}
}

func (f *seedConfigWaitFixture) reconcile() ctrl.Result {
	f.t.Helper()
	res, err := f.r.Reconcile(f.ctx, f.req)
	require.NoError(f.t, err)
	return res
}

func (f *seedConfigWaitFixture) restore() *neo4jv1beta1.Neo4jRestore {
	f.t.Helper()
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(f.t, f.r.Get(f.ctx, f.req.NamespacedName, got))
	return got
}

func (f *seedConfigWaitFixture) cluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	f.t.Helper()
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(f.t, f.r.Get(f.ctx, types.NamespacedName{Name: "c", Namespace: "ns"}, got))
	return got
}

// seedConfigWarnings drains the recorder and counts SeedEndpointNotProjected Warnings.
func (f *seedConfigWaitFixture) seedConfigWarnings() int {
	n := 0
	for {
		select {
		case e := <-f.recorder.Events:
			if strings.HasPrefix(e, "Warning "+EventReasonSeedEndpointNotProjected) {
				n++
			}
		default:
			return n
		}
	}
}

func (f *seedConfigWaitFixture) optIn() {
	f.t.Helper()
	c := f.cluster()
	c.Annotations = map[string]string{AutoInheritSeedCredsAnnotation: "true"}
	require.NoError(f.t, f.r.Update(f.ctx, c))
}

func TestRestore_WaitsForSeedConfigAndCarriesOn(t *testing.T) {
	for _, tc := range []struct {
		name         string
		allDatabases bool
	}{
		{"one database", false},
		{"all databases", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSeedConfigWaitFixture(t, tc.allDatabases)

			res := f.reconcile()
			got := f.restore()
			assert.Equal(t, StatusPending, got.Status.Phase, "nothing has touched a database yet; the restore waits, never Failed: %s", got.Status.Message)
			assert.True(t, strings.HasPrefix(got.Status.Message, `cluster "c"'s server pods can't reach the seed source: missing credentials Secret "creds"`), got.Status.Message)
			assert.Contains(t, got.Status.Message, AutoInheritSeedCredsAnnotation+`="true"`)
			assert.Contains(t, got.Status.Message, "carries on by itself")
			assert.Equal(t, 30*time.Second, res.RequeueAfter, "the restore must look again on its own")
			assert.Equal(t, 1, f.seedConfigWarnings())

			// Looking again while nothing changed: still waiting, and no second Warning.
			_ = f.reconcile()
			assert.Equal(t, StatusPending, f.restore().Status.Phase)
			assert.Equal(t, 0, f.seedConfigWarnings(), "the Warning is raised when the restore starts waiting, not on every check")

			// The user does what the message says: the next pass projects the
			// credentials onto the cluster (one rolling restart) and waits for it.
			f.optIn()
			res = f.reconcile()
			assert.True(t, hasSecretEnvFrom(f.cluster().Spec.ExtraEnvFrom, "creds"), "the opted-in cluster must get the credentials Secret")
			got = f.restore()
			assert.Equal(t, StatusPending, got.Status.Phase)
			assert.Contains(t, got.Status.Message, "Projected seed configuration")
			assert.Equal(t, 30*time.Second, res.RequeueAfter)
		})
	}
}

// A restore that already failed for another reason stays Failed: the
// previously-failed guard is unchanged.
func TestRestore_GenuineFailureStaysTerminal(t *testing.T) {
	f := newSeedConfigWaitFixture(t, false)
	got := f.restore()
	got.Status.Phase = StatusFailed
	got.Status.Message = "Database check failed: boom"
	got.Status.ObservedGeneration = got.Generation
	require.NoError(t, f.r.Status().Update(f.ctx, got))
	f.optIn()

	res := f.reconcile()
	assert.Equal(t, ctrl.Result{}, res)
	assert.Equal(t, StatusFailed, f.restore().Status.Phase)
	assert.False(t, hasSecretEnvFrom(f.cluster().Spec.ExtraEnvFrom, "creds"), "a Failed restore does nothing to its target")
}
