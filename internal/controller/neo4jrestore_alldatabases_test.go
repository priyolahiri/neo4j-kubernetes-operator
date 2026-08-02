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
	"errors"
	"strings"
	"testing"

	"context"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUserDatabasesFromArtifacts_ExcludesSystem(t *testing.T) {
	arts := []neo4jv1beta1.DatabaseArtifact{
		{Database: "neo4j", Filename: "neo4j-t.backup"},
		{Database: "system", Filename: "system-t.backup"},
		{Database: "customers", Filename: "customers-t.backup"},
	}
	got := userDatabasesFromArtifacts(arts)
	want := []string{"neo4j", "customers"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestFilenameForDB(t *testing.T) {
	arts := []neo4jv1beta1.DatabaseArtifact{
		{Database: "neo4j", Filename: "neo4j-t.backup"},
		{Database: "customers", Filename: "customers-t.backup"},
	}
	if got := filenameForDB(arts, "customers"); got != "customers-t.backup" {
		t.Errorf("filenameForDB(customers) = %q, want customers-t.backup", got)
	}
	if got := filenameForDB(arts, "missing"); got != "" {
		t.Errorf("filenameForDB(missing) = %q, want empty", got)
	}
}

func TestEnsureDatabaseResults_SeedsAndIsIdempotent(t *testing.T) {
	r := &Neo4jRestoreReconciler{}
	restore := &neo4jv1beta1.Neo4jRestore{}

	r.ensureDatabaseResults(restore, []string{"neo4j", "customers"})
	if len(restore.Status.DatabaseResults) != 2 {
		t.Fatalf("expected 2 results, got %d", len(restore.Status.DatabaseResults))
	}
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Phase != StatusPending {
			t.Errorf("result %d phase = %q, want Pending", i, restore.Status.DatabaseResults[i].Phase)
		}
	}

	// Mark one done, then re-run: existing results must be preserved (no reset,
	// no duplicates) and a newly-discovered DB appended.
	restore.Status.DatabaseResults[0].Phase = StatusCompleted
	r.ensureDatabaseResults(restore, []string{"neo4j", "customers", "orders"})
	if len(restore.Status.DatabaseResults) != 3 {
		t.Fatalf("expected 3 results after re-run, got %d", len(restore.Status.DatabaseResults))
	}
	if restore.Status.DatabaseResults[0].Phase != StatusCompleted {
		t.Errorf("existing Completed result was reset to %q", restore.Status.DatabaseResults[0].Phase)
	}
}

// buildAllDatabasesRestoreCommand drives the STANDALONE all-databases offline
// restore (#288): one `neo4j-admin database restore` per user database, from its
// exact .backup file in the resolved artifact map, system excluded, temp dir
// reset per database, and --overwrite-destination gated on spec.force.
func TestBuildAllDatabasesRestoreCommand_PVC(t *testing.T) {
	r := &Neo4jRestoreReconciler{}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
		},
	}
	storage := &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "sa-backup-store"}}
	restore := &neo4jv1beta1.Neo4jRestore{
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef:  "sa",
			AllDatabases: true,
			Options:      &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true},
			Source:       neo4jv1beta1.RestoreSource{Type: "storage", Storage: storage, BackupPath: "sa-all-backup"},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{
			ResolvedSource: &neo4jv1beta1.ResolvedRestoreSource{
				Storage:    storage,
				BackupPath: "sa-all-backup",
				DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
					{Database: "system", Filename: "system-t.backup"},
					{Database: "neo4j", Filename: "neo4j-t.backup"},
					{Database: "customers", Filename: "customers-t.backup"},
					{Database: "inventory", Filename: "inventory-t.backup"},
				},
			},
		},
	}

	cmd, err := r.buildAllDatabasesRestoreCommand(restore, cluster)
	if err != nil {
		t.Fatalf("buildAllDatabasesRestoreCommand: %v", err)
	}

	// system must never appear; every user DB must reference its EXACT file under
	// the chain-root directory (no `ls` glob — we have the precise filename).
	if strings.Contains(cmd, "system-t.backup") || strings.Contains(cmd, "'system'") {
		t.Errorf("system database must be excluded; got:\n%s", cmd)
	}
	for _, db := range []string{"neo4j", "customers", "inventory"} {
		want := "--from-path='/backup/sa-all-backup/" + db + "-t.backup' '" + db + "'"
		if !strings.Contains(cmd, want) {
			t.Errorf("command missing exact restore for %q\nwant substring: %s\ngot:\n%s", db, want, cmd)
		}
	}
	// force → overwrite confirmed; per-database temp dir reset + explicit temp path.
	if !strings.Contains(cmd, "--overwrite-destination=true") {
		t.Errorf("spec.force should add --overwrite-destination=true; got:\n%s", cmd)
	}
	if strings.Count(cmd, "rm -rf /tmp/restore-tmp") != 3 {
		t.Errorf("expected one temp-dir reset per user database (3); got:\n%s", cmd)
	}
	if !strings.Contains(cmd, "--temp-path=/tmp/restore-tmp") {
		t.Errorf("expected explicit --temp-path; got:\n%s", cmd)
	}
}

// Without spec.force (and without options.replaceExisting) the command must NOT
// pass --overwrite-destination — a non-confirming all-databases restore against
// existing databases fails at neo4j-admin rather than silently clobbering.
func TestBuildAllDatabasesRestoreCommand_NoForceNoOverwrite(t *testing.T) {
	r := &Neo4jRestoreReconciler{}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{AcceptLicenseAgreement: "eval", Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"}},
	}
	storage := &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "store"}}
	restore := &neo4jv1beta1.Neo4jRestore{
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			AllDatabases: true,
			Source:       neo4jv1beta1.RestoreSource{Type: "storage", Storage: storage, BackupPath: "b"},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{
			ResolvedSource: &neo4jv1beta1.ResolvedRestoreSource{
				Storage:           storage,
				BackupPath:        "b",
				DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{{Database: "neo4j", Filename: "neo4j-t.backup"}},
			},
		},
	}
	cmd, err := r.buildAllDatabasesRestoreCommand(restore, cluster)
	if err != nil {
		t.Fatalf("buildAllDatabasesRestoreCommand: %v", err)
	}
	if strings.Contains(cmd, "--overwrite-destination") {
		t.Errorf("no force/replaceExisting must not add --overwrite-destination; got:\n%s", cmd)
	}
}

// A resolved source carrying no per-database artifacts (e.g. the backup wasn't an
// all-databases backup) must be a clear error, not an empty/no-op command.
func TestBuildAllDatabasesRestoreCommand_NoArtifactsErrors(t *testing.T) {
	r := &Neo4jRestoreReconciler{}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{AcceptLicenseAgreement: "eval", Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"}},
	}
	storage := &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "store"}}
	restore := &neo4jv1beta1.Neo4jRestore{
		Spec: neo4jv1beta1.Neo4jRestoreSpec{AllDatabases: true, Source: neo4jv1beta1.RestoreSource{Type: "storage", Storage: storage, BackupPath: "b"}},
		Status: neo4jv1beta1.Neo4jRestoreStatus{
			ResolvedSource: &neo4jv1beta1.ResolvedRestoreSource{Storage: storage, BackupPath: "b"},
		},
	}
	if _, err := r.buildAllDatabasesRestoreCommand(restore, cluster); err == nil {
		t.Fatalf("expected an error when no per-database artifacts are recorded")
	}
}

// A stalled seed must say WHY. The polling branch used to discard both the error
// and the allocation counts, so a database that never came online — or a SHOW
// DATABASE that failed on every poll — left the restore in Running for its whole
// budget with an empty message, no event and no log line. That is exactly how the
// 2026-07-31 extended run failed, and it made the failure undiagnosable after the
// fact.
func TestSeedingProgressMessageDistinguishesStallFromProgress(t *testing.T) {
	failed := seedingProgressMessage(0, 0, errors.New("ConnectivityError: timeout"))
	if !strings.Contains(failed, "FAILED") || !strings.Contains(failed, "timeout") {
		t.Errorf("a failing poll must be reported as such, got %q", failed)
	}

	recreating := seedingProgressMessage(0, 0, nil)
	if strings.Contains(recreating, "FAILED") {
		t.Errorf("no-allocations-yet is not a failure, got %q", recreating)
	}
	if !strings.Contains(recreating, "no allocations yet") {
		t.Errorf("mid-recreate must be distinguishable from a stall, got %q", recreating)
	}

	progressing := seedingProgressMessage(1, 2, nil)
	if !strings.Contains(progressing, "1/2") {
		t.Errorf("partial progress must report the counts, got %q", progressing)
	}

	// The three must be mutually distinguishable — that is the whole point.
	if failed == recreating || recreating == progressing || failed == progressing {
		t.Error("the three states must produce different messages")
	}
}

// The all-databases restore issues a DESTRUCTIVE, non-idempotent recreate. Its
// only guard used to be the per-database phase read through the informer cache
// — which lags writes, so a second reconcile could still see Pending and issue
// a second concurrent seed.
//
// That is not theoretical. Reproduced 2026-08-02: two RestoreStarted events in
// the same second (consecutive resourceVersions), both seeds racing on
// /data/databases/neo4j/temp-copy/neo4j/database_lock, and Neo4j parked the
// database offline with "Unable to obtain lock on file" — permanently, with
// requestedStatus=online and currentStatus=offline. No timeout can rescue that.
func TestIssueGuardRefusesASecondRecreate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	newRestore := func(phase string) *neo4jv1beta1.Neo4jRestore {
		return &neo4jv1beta1.Neo4jRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"},
			Status: neo4jv1beta1.Neo4jRestoreStatus{
				DatabaseResults: []neo4jv1beta1.DatabaseRestoreResult{{Database: "neo4j", Phase: phase}},
			},
		}
	}

	t.Run("server says Running -> refuse, even though the caller's cached copy said Pending", func(t *testing.T) {
		// The caller's (stale, cached) view.
		stale := newRestore(StatusPending)
		// What the API server actually holds.
		fresh := newRestore(StatusRunning)
		api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fresh).Build()
		r := &Neo4jRestoreReconciler{APIReader: api}
		if r.issueGuardAllowsRecreate(context.Background(), stale, "neo4j") {
			t.Error("issued a SECOND recreate: the server already had this database at Running. " +
				"Two concurrent seeds brick the database on a temp-copy lock.")
		}
	})

	t.Run("server still says Pending -> allow", func(t *testing.T) {
		rest := newRestore(StatusPending)
		api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rest).Build()
		r := &Neo4jRestoreReconciler{APIReader: api}
		if !r.issueGuardAllowsRecreate(context.Background(), rest, "neo4j") {
			t.Error("refused the FIRST recreate; the restore would never start")
		}
	})

	t.Run("no entry on the server yet -> allow (genuinely the first pass)", func(t *testing.T) {
		rest := newRestore(StatusPending)
		bare := &neo4jv1beta1.Neo4jRestore{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"}}
		api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bare).Build()
		r := &Neo4jRestoreReconciler{APIReader: api}
		if !r.issueGuardAllowsRecreate(context.Background(), rest, "neo4j") {
			t.Error("refused although the server has no record of this database at all")
		}
	})

	t.Run("read failure -> refuse, never guess", func(t *testing.T) {
		rest := newRestore(StatusPending)
		// Empty client: the object does not exist, so Get returns NotFound.
		api := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &Neo4jRestoreReconciler{APIReader: api}
		if r.issueGuardAllowsRecreate(context.Background(), rest, "neo4j") {
			t.Error("issued a destructive recreate without being able to confirm it had not already been issued")
		}
	})

	t.Run("no APIReader wired -> fail open (the cached check already ran)", func(t *testing.T) {
		r := &Neo4jRestoreReconciler{}
		if !r.issueGuardAllowsRecreate(context.Background(), newRestore(StatusPending), "neo4j") {
			t.Error("must not block when no APIReader is configured")
		}
	})
}
