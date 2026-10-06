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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// fakeNeo4jAdmin stands in for neo4j-admin, with the two behaviours the merge
// script relies on, as observed on 5.26 and 2026.09: `backup inspect
// --latest-chain --format=JSON` lists the chain's files as file:// URIs, and
// the aggregate command reports its result on stdout and exits 0 whatever
// happened. Its "merged artifact" holds the names of the files it was given,
// so a test can see which files the script copied.
const fakeNeo4jAdmin = `#!/bin/bash
if [ "$1" = backup ] && [ "$2" = inspect ]; then
  printf '['; first=1
  for f in $FAKE_CHAIN; do [ $first = 1 ] || printf ','; first=0; printf '{"uri":"file://%s/%s"}' "${3%/}" "$f"; done
  printf ']\n'; exit 0
fi
for a in "$@"; do case $a in --from-path=*) from=${a#--from-path=};; esac; done
d=$(dirname "$from")
case "${FAKE_MODE:-merge}" in
  merge) out="$d/neo4j-2099-01-01T00-00-00.backup"; ls "$d" > "$out.tmp"; mv "$out.tmp" "$out"
         echo "Successfully aggregated backup chain of database 'neo4j', new artifact: '$out'. " ;;
  full)  echo "No aggregation needed for database 'neo4j'. Found existing full backup: '$from'. " ;;
  fail)  echo "Failed to aggregate backup chain of database 'neo4j' reason: 'boom'" ;;
esac
exit 0
`

type mergeRun struct {
	backup, scratch, termLog string
	out                      []byte
	err                      error
}

// runSeedMergeScript runs the real script against a backup directory laid out
// from files (name → content) and a fake neo4j-admin.
func runSeedMergeScript(t *testing.T, files map[string]string, env []string, args ...string) mergeRun {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	root := t.TempDir()
	run := mergeRun{backup: filepath.Join(root, "backup"), scratch: filepath.Join(root, "scratch"), termLog: filepath.Join(root, "termination-log")}
	for name, content := range files {
		p := filepath.Join(run.backup, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
	admin := filepath.Join(root, "neo4j-admin")
	require.NoError(t, os.WriteFile(admin, []byte(fakeNeo4jAdmin), 0o755))

	cmd := exec.CommandContext(t.Context(), "bash", append([]string{"-c", seedMergeScript, seedMergeContainerName}, args...)...)
	cmd.Env = append(os.Environ(),
		"SEED_MERGE_COMMAND="+admin+" backup aggregate",
		"SEED_BACKUP_ROOT="+run.backup,
		"SEED_SCRATCH_ROOT="+run.scratch,
		"SEED_TERMINATION_LOG="+run.termLog,
	)
	cmd.Env = append(cmd.Env, env...)
	run.out, run.err = cmd.CombinedOutput()
	return run
}

func (m mergeRun) served(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(m.scratch, "serve", rel))
	require.NoError(t, err, "script output:\n%s", m.out)
	return string(b)
}

// A chain in a backup directory, with neighbours that are not part of it.
var seedChainFiles = map[string]string{
	"nightly/neo4j-2026-01-01T00-00-00.backup":   "full",
	"nightly/neo4j-2026-01-01T01-00-00.backup":   "diff1",
	"nightly/neo4j-2026-01-01T01-00-00.backup.1": "diff1 part",
	"nightly/neo4j-2026-01-01T02-00-00.backup":   "diff2",
	"nightly/neo4j2-2026-01-01T00-00-00.backup":  "another database",
	"nightly/other-2026-01-01T00-00-00.backup":   "another database",
	"nightly/.chain.lock":                        "",
}

func TestSeedMergeScript_MergesTheLatestChain(t *testing.T) {
	run := runSeedMergeScript(t, seedChainFiles,
		[]string{"FAKE_CHAIN=neo4j-2026-01-01T02-00-00.backup neo4j-2026-01-01T01-00-00.backup neo4j-2026-01-01T00-00-00.backup"},
		"nightly/", "neo4j-2026-01-01T02-00-00.backup", "neo4j", "merge")
	require.NoError(t, run.err, "%s", run.out)

	merged := run.served(t, "nightly/neo4j-2026-01-01T02-00-00.backup")
	for _, f := range []string{"neo4j-2026-01-01T00-00-00.backup", "neo4j-2026-01-01T01-00-00.backup", "neo4j-2026-01-01T01-00-00.backup.1", "neo4j-2026-01-01T02-00-00.backup"} {
		assert.Contains(t, merged, f, "the chain and each artifact's parts are copied")
	}
	assert.NotContains(t, merged, "neo4j2-", "a database whose name merely starts the same is not part of the chain")
	assert.NotContains(t, merged, "other-")
	_, err := os.Stat(filepath.Join(run.scratch, "work", "1"))
	assert.True(t, os.IsNotExist(err), "the working copy is removed once merged")
}

// A run that is not the newest in its directory: `--latest-chain` would name
// the wrong chain, so everything of the database up to the target is copied
// and neo4j-admin merges the chain ending at the target.
func TestSeedMergeScript_AnEarlierRunCopiesUpToIt(t *testing.T) {
	files := map[string]string{}
	for k, v := range seedChainFiles {
		files[k] = v
	}
	files["nightly/my-db-2026-01-01T00-00-00.backup"] = "another database" // sorts before my.db-
	files["nightly/my.db-2026-01-01T00-00-00.backup"] = "dotted full"
	files["nightly/my.db-2026-01-01T03-00-00.backup"] = "dotted later"

	run := runSeedMergeScript(t, files, []string{"FAKE_CHAIN=unused"},
		"nightly", "neo4j-2026-01-01T01-00-00.backup", "neo4j", "merge")
	require.NoError(t, run.err, "%s", run.out)
	merged := run.served(t, "nightly/neo4j-2026-01-01T01-00-00.backup")
	assert.Contains(t, merged, "neo4j-2026-01-01T00-00-00.backup")
	assert.Contains(t, merged, "neo4j-2026-01-01T01-00-00.backup.1")
	assert.NotContains(t, merged, "neo4j-2026-01-01T02-00-00.backup", "nothing newer than the target")

	run = runSeedMergeScript(t, files, []string{"FAKE_CHAIN=unused"},
		"nightly", "my.db-2026-01-01T00-00-00.backup", "my.db", "merge")
	require.NoError(t, run.err, "%s", run.out)
	merged = run.served(t, "nightly/my.db-2026-01-01T00-00-00.backup")
	assert.Contains(t, merged, "my.db-2026-01-01T00-00-00.backup")
	assert.NotContains(t, merged, "my-db-", "a dot in the database name is literal")
}

func TestSeedMergeScript_AFullIsServedAsItIs(t *testing.T) {
	run := runSeedMergeScript(t, seedChainFiles, []string{"FAKE_CHAIN=neo4j-2026-01-01T00-00-00.backup", "FAKE_MODE=full"},
		"nightly", "neo4j-2026-01-01T00-00-00.backup", "neo4j", "merge")
	require.NoError(t, run.err, "%s", run.out)
	assert.Equal(t, "full", run.served(t, "nightly/neo4j-2026-01-01T00-00-00.backup"))

	run = runSeedMergeScript(t, seedChainFiles, nil,
		"nightly", "neo4j-2026-01-01T01-00-00.backup", "neo4j", "link")
	require.NoError(t, run.err, "%s", run.out)
	link := filepath.Join(run.scratch, "serve", "nightly", "neo4j-2026-01-01T01-00-00.backup")
	target, err := os.Readlink(link)
	require.NoError(t, err, "a known full backup is linked, not copied")
	assert.Equal(t, filepath.Join(run.backup, "nightly", "neo4j-2026-01-01T01-00-00.backup"), target)
	_, err = os.Readlink(link + ".1")
	assert.NoError(t, err, "its parts are linked too")
}

// neo4j-admin exits 0 when it fails; the script must not.
func TestSeedMergeScript_FailsWithNeo4jAdminsReason(t *testing.T) {
	run := runSeedMergeScript(t, seedChainFiles, []string{"FAKE_CHAIN=neo4j-2026-01-01T02-00-00.backup", "FAKE_MODE=fail"},
		"nightly", "neo4j-2026-01-01T02-00-00.backup", "neo4j", "merge")
	require.Error(t, run.err)
	msg, err := os.ReadFile(run.termLog)
	require.NoError(t, err)
	assert.Contains(t, string(msg), "could not merge the backup chain ending at nightly/neo4j-2026-01-01T02-00-00.backup")
	assert.Contains(t, string(msg), "reason: 'boom'")

	run = runSeedMergeScript(t, seedChainFiles, nil, "nightly", "neo4j-2026-09-09T00-00-00.backup", "neo4j", "merge")
	require.Error(t, run.err)
	msg, _ = os.ReadFile(run.termLog)
	assert.Contains(t, string(msg), "is not on the backup PVC")
}

func TestNewSeedMergePlan(t *testing.T) {
	full := newSeedArtifact("nightly/", "neo4j-2026-01-01T00-00-00.backup", "FULL", "neo4j")
	diff := newSeedArtifact("nightly/", "neo4j-2026-01-01T01-00-00.backup", "DIFF", "neo4j")
	untyped := newSeedArtifact("nightly/", "neo4j-2026-01-01T02-00-00.backup", "", "neo4j")
	assert.False(t, full.Merge)
	assert.True(t, diff.Merge)
	assert.True(t, untyped.Merge, "a run that did not record its type is merged; for a full that is a no-op")
	assert.Equal(t, "neo4j", diff.Database)
	assert.Equal(t, "my.db", newSeedArtifact("d", "my.db-2026-01-01T00-00-00.backup", "", "copy").Database, "the chain belongs to the database in the file, not the restore's target name")
	assert.Equal(t, "fallback", newSeedArtifact("d", "odd-name.backup", "", "fallback").Database)

	lts := neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise", PullPolicy: "IfNotPresent", PullSecrets: []string{"regcred"}}
	assert.Nil(t, newSeedMergePlan(lts, nil, []seedArtifact{full}), "only full backups: the proxy serves the PVC as it is")

	plan := newSeedMergePlan(lts, nil, []seedArtifact{full, diff})
	require.NotNil(t, plan)
	assert.Equal(t, "neo4j:5.26-enterprise", plan.Image)
	assert.Equal(t, "neo4j-admin database aggregate-backup", plan.Command)
	assert.Equal(t, corev1.PullIfNotPresent, plan.ImagePullPolicy)
	assert.Equal(t, "regcred", plan.ImagePullSecrets[0].Name)
	assert.Equal(t, resolveRestoreJobResources(nil), plan.Resources)
	assert.Nil(t, plan.TempStorage)

	calver := neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.09.0-enterprise"}
	opts := &neo4jv1beta1.RestoreOptionsSpec{TempStorage: &neo4jv1beta1.TempStorageSpec{Size: "50Gi"}}
	plan = newSeedMergePlan(calver, opts, []seedArtifact{diff})
	assert.Equal(t, "neo4j-admin backup aggregate", plan.Command)
	assert.Equal(t, "50Gi", plan.TempStorage.Size)
}

func TestBuildPVCSeedProxyDeployment_Merge(t *testing.T) {
	owner := minimalRestore("r", "ns", "c")
	plain := buildPVCSeedProxyDeployment(owner, "r", "backups", nil)
	assert.Empty(t, plain.Spec.Template.Spec.InitContainers)
	assert.Contains(t, plain.Spec.Template.Spec.Containers[0].Command[2], "-h /backup")

	plan := newSeedMergePlan(neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.09.0-enterprise"}, nil, []seedArtifact{
		newSeedArtifact("all/", "other-2026-01-01T00-00-00.backup", "FULL", "other"),
		newSeedArtifact("all/", "neo4j-2026-01-01T01-00-00.backup", "DIFF", "neo4j"),
	})
	pod := buildPVCSeedProxyDeployment(owner, "r", "backups", plan).Spec.Template.Spec

	require.Len(t, pod.InitContainers, 1)
	init := pod.InitContainers[0]
	assert.Equal(t, "neo4j:2026.09.0-enterprise", init.Image)
	assert.Equal(t, seedMergeScript, init.Command[2])
	assert.Equal(t, []string{"all", "other-2026-01-01T00-00-00.backup", "other", "link", "all", "neo4j-2026-01-01T01-00-00.backup", "neo4j", "merge"}, init.Command[4:])
	assert.Contains(t, init.Env, corev1.EnvVar{Name: "SEED_MERGE_COMMAND", Value: "neo4j-admin backup aggregate"})
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, init.TerminationMessagePolicy)
	assert.Contains(t, init.VolumeMounts, corev1.VolumeMount{Name: "backup", MountPath: "/backup", ReadOnly: true})
	require.NotNil(t, pod.SecurityContext)
	assert.Equal(t, int64(7474), *pod.SecurityContext.FSGroup, "the scratch volume is writable by the Neo4j UID")

	httpd := pod.Containers[0]
	assert.Contains(t, httpd.Command[2], "-h "+seedMergeServeRoot)
	assert.Contains(t, httpd.VolumeMounts, corev1.VolumeMount{Name: seedMergeScratchVolume, MountPath: seedMergeScratchPath, ReadOnly: true})

	var scratch *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == seedMergeScratchVolume {
			scratch = &pod.Volumes[i]
		}
	}
	require.NotNil(t, scratch)
	assert.NotNil(t, scratch.EmptyDir, "no tempStorage: an emptyDir")

	plan.TempStorage = &neo4jv1beta1.TempStorageSpec{Size: "50Gi", StorageClassName: "fast"}
	pod = buildPVCSeedProxyDeployment(owner, "r", "backups", plan).Spec.Template.Spec
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == seedMergeScratchVolume {
			scratch = &pod.Volumes[i]
		}
	}
	require.NotNil(t, scratch.Ephemeral, "tempStorage: a PVC that lives as long as the proxy")
	claim := scratch.Ephemeral.VolumeClaimTemplate.Spec
	assert.Equal(t, "50Gi", claim.Resources.Requests.Storage().String())
	assert.Equal(t, "fast", *claim.StorageClassName)
}

func TestPVCSeedProxyMergeFailure(t *testing.T) {
	pod := func(name string, status corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{
				"app.kubernetes.io/name": "backup-seed-proxy", "app.kubernetes.io/instance": "r"}},
			Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{status}},
		}
	}
	failed := pod("p", corev1.ContainerStatus{Name: seedMergeContainerName, LastTerminationState: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "could not merge the backup chain ending at x: boom\n"}}})
	r := newRestoreTestReconciler(t, failed)
	assert.Equal(t, "could not merge the backup chain ending at x: boom", pvcSeedProxyMergeFailure(context.Background(), r.Client, "ns", "r"))

	running := pod("p", corev1.ContainerStatus{Name: seedMergeContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	r = newRestoreTestReconciler(t, running)
	assert.Empty(t, pvcSeedProxyMergeFailure(context.Background(), r.Client, "ns", "r"), "still merging")

	done := pod("p", corev1.ContainerStatus{Name: seedMergeContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}})
	r = newRestoreTestReconciler(t, done)
	assert.Empty(t, pvcSeedProxyMergeFailure(context.Background(), r.Client, "ns", "r"))
}

func TestSeedProxyBudget(t *testing.T) {
	r := minimalRestore("r", "ns", "c")
	assert.Equal(t, seedProxyWaitTimeout, seedProxyBudget(r, false))
	assert.Equal(t, seedProxyMergeWaitTimeout, seedProxyBudget(r, true), "merging copies and recovers the chain: a longer default")
	r.Spec.Timeout = "2h"
	assert.Equal(t, 2*time.Hour, seedProxyBudget(r, true), "spec.timeout wins")
	assert.True(t, strings.HasPrefix(seedProxyMergeWaitTimeout.String(), "30m"))
}

// An all-databases restore merges each database the backup did not record as
// a full backup, and links the rest; system is never restored.
func TestAllDatabasesRestore_MergesPVCDiffsPerDatabase(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := pinnedBackupRestore("pvc", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "everything/",
		DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
			{Database: "system", Filename: "system-2026-10-06T07-30-23.backup", Type: "FULL"},
			{Database: "neo4j", Filename: "neo4j-2026-10-06T07-30-23.backup", Type: "DIFF"},
			{Database: "other", Filename: "other-2026-10-06T07-30-21.backup", Type: "FULL"},
		},
	})
	restore.Spec.AllDatabases = true
	restore.Status.ResolvedSource.Storage.PVC = &neo4jv1beta1.PVCSpec{Name: "backups"}
	r := statusRestoreReconciler(t, sa, restore)

	_, _ = r.startAllDatabasesRestore(context.Background(), restore, standaloneAsCluster(sa))

	proxy := &appsv1.Deployment{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Name: pvcSeedProxyName("r"), Namespace: "ns"}, proxy))
	require.Len(t, proxy.Spec.Template.Spec.InitContainers, 1)
	assert.Equal(t, []string{
		"everything", "neo4j-2026-10-06T07-30-23.backup", "neo4j", "merge",
		"everything", "other-2026-10-06T07-30-21.backup", "other", "link",
	}, proxy.Spec.Template.Spec.InitContainers[0].Command[4:])
}
