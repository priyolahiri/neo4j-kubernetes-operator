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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// The seed proxy serves a PVC backup to Neo4j over HTTP, and Neo4j's URL seed
// provider fetches exactly one file: a differential, which needs the rest of
// its chain, is refused ("not part of a valid backup chain"). So before
// serving, the proxy merges each chain into one full artifact with
// neo4j-admin's aggregate command (rule 109), in an init container running the
// target's own Neo4j image.
//
// Three properties of that command, established on 5.26 and 2026.09, shape
// the script:
//   - it writes the merged artifact beside its input — symlinks are resolved
//     first — so the chain is COPIED to scratch space; the backup PVC stays
//     read-only;
//   - it exits 0 when it fails, so the result is read from its output: "new
//     artifact: '<path>'", or "Found existing full backup: '<path>'" when the
//     file is already a full;
//   - pointed at a differential, it merges the chain ENDING at that file.
//
// And one of `backup inspect`: it leaves out EMPTY backups — a run that found
// no new transactions, routine for a quiet database in an all-databases
// backup — unless asked with --empty. Without it the chain it names can stop
// short of the very file being restored.
//
// The merged artifact is published at the same relative path the proxy served
// the original under, so the seed URL does not change.
const (
	seedMergeContainerName = "merge-chain"
	seedMergeScratchVolume = "scratch"
	seedMergeScratchPath   = "/scratch"
	// seedMergeServeRoot is what httpd serves when the proxy merges: the
	// merged artifacts, plus links to those served as they are.
	seedMergeServeRoot = seedMergeScratchPath + "/serve"
)

// seedArtifact is one file a restore seeds from, relative to the backup PVC.
type seedArtifact struct {
	Dir      string // the backup's directory on the PVC (its backupsPath)
	Filename string
	Database string // the database the file holds; chooses its chain
	Merge    bool   // false: a known full backup, served as it is
}

// newSeedArtifact describes a file to seed from. recordedType is what the
// backup recorded (FULL, DIFF, or "" when it did not); anything but FULL is
// merged. The database is read from the filename — the chain belongs to the
// database the file holds, whatever name the restore gives it — falling back
// to fallbackDB for a name that does not parse.
func newSeedArtifact(dir, filename, recordedType, fallbackDB string) seedArtifact {
	db := artifactDatabase(filename)
	if db == "" {
		db = fallbackDB
	}
	return seedArtifact{Dir: dir, Filename: filename, Database: db, Merge: recordedType != backupArtifactFull}
}

// seedMergePlan is what the seed proxy needs to merge chains before serving.
// nil means serve the PVC as it is.
type seedMergePlan struct {
	Image            string
	ImagePullPolicy  corev1.PullPolicy
	ImagePullSecrets []corev1.LocalObjectReference
	// Command is neo4j-admin's aggregate command for the image's version.
	Command     string
	Resources   corev1.ResourceRequirements
	TempStorage *neo4jv1beta1.TempStorageSpec
	Artifacts   []seedArtifact
}

// seedMergeCommand is neo4j-admin's aggregate command for a Neo4j version:
// `database aggregate-backup` on 5.x, `backup aggregate` on CalVer, where the
// old name is deprecated.
func seedMergeCommand(v *neo4j.Version) string {
	if v != nil && v.IsCalver {
		return "neo4j-admin backup aggregate"
	}
	return "neo4j-admin database aggregate-backup"
}

// newSeedMergePlan returns the plan for seeding artifacts into a target whose
// pods run image, or nil when every artifact is a known full backup — then the
// proxy serves the PVC as it is, as it always has. An artifact whose type was
// not recorded is merged too: for a full backup that is a no-op.
func newSeedMergePlan(image neo4jv1beta1.ImageSpec, options *neo4jv1beta1.RestoreOptionsSpec, artifacts []seedArtifact) *seedMergePlan {
	merge := false
	for _, a := range artifacts {
		merge = merge || a.Merge
	}
	if !merge {
		return nil
	}
	ref := image.Repo + ":" + image.Tag
	version, err := neo4j.GetImageVersion(ref)
	if err != nil {
		version = nil // a 5.x name is the safe default: CalVer still accepts it
	}
	plan := &seedMergePlan{
		Image:            ref,
		ImagePullPolicy:  corev1.PullPolicy(image.PullPolicy),
		ImagePullSecrets: resources.ImagePullSecretsFromNames(image.PullSecrets),
		Command:          seedMergeCommand(version),
		Resources:        resolveRestoreJobResources(options),
		Artifacts:        artifacts,
	}
	if options != nil {
		plan.TempStorage = options.TempStorage
	}
	return plan
}

// seedMergeScript copies each chain to scratch space, merges it, and
// publishes the result under seedMergeServeRoot at the artifact's own relative
// path. Arguments come in groups of four: DIR FILE DATABASE MODE, MODE being
// "merge" or "link". SEED_MERGE_COMMAND is the aggregate command; its first
// word (neo4j-admin) also runs `backup inspect`. The SEED_*_ROOT and
// SEED_TERMINATION_LOG overrides exist for the unit test, which runs the
// script against a fake neo4j-admin.
const seedMergeScript = `set -uo pipefail
backup=${SEED_BACKUP_ROOT:-/backup}; scratch=${SEED_SCRATCH_ROOT:-/scratch}
fail() { printf '%s\n' "$1" | tee "${SEED_TERMINATION_LOG:-/dev/termination-log}" >&2; exit 1; }
read -r -a merge <<< "$SEED_MERGE_COMMAND"
mkdir -p "$scratch/serve" "$scratch/work" "$scratch/tmp" || fail "cannot write to the scratch volume"
n=0
while [ "$#" -ge 4 ]; do
  dir=$1; file=$2; db=$3; mode=$4; shift 4; n=$((n+1))
  src="$backup/$dir"; out="$scratch/serve/$dir"
  mkdir -p "$out"
  [ -f "$src/$file" ] || fail "seed artifact $dir/$file is not on the backup PVC"
  if [ "$mode" = link ]; then
    for f in "$src/$file" "$src/$file".*; do [ -e "$f" ] && ln -sf "$f" "$out/"; done
    continue
  fi
  work="$scratch/work/$n"; tmp="$scratch/tmp/$n"; mkdir -p "$work" "$tmp"
  pattern="^$(printf '%s' "$db" | sed 's/\./\\./g')-[0-9]{4}-[0-9]{2}-[0-9]{2}T[^/]*\.backup$"
  latest=$(ls -1 "$src" | grep -E "$pattern" | sort | tail -n 1)
  chain=""
  if [ "$latest" = "$file" ]; then
    chain=$("${merge[0]}" backup inspect "$src" --database="$db" --latest-chain --empty --format=JSON 2>/dev/null \
      | grep -o '"uri":"file://[^"]*"' | sed -e 's#^"uri":"file://##' -e 's#"$##')
    printf '%s\n' $chain | grep -qxF "$src/$file" || chain=""
  fi
  if [ -z "$chain" ]; then
    chain=$(ls -1 "$src" | grep -E "$pattern" | sort | awk -v t="$file" '$0 <= t' | sed "s#^#$src/#")
  fi
  echo "merging the chain ending at $dir/$file:" $chain
  for f in $chain; do
    cp "$f" "$work/" || fail "cannot copy $f to the scratch volume (is it large enough? see spec.options.tempStorage)"
    for p in "$f".*; do
      [ -e "$p" ] || continue
      cp "$p" "$work/" || fail "cannot copy $p to the scratch volume (is it large enough? see spec.options.tempStorage)"
    done
  done
  log=$("${merge[@]}" --from-path="$work/$file" --keep-old-backup=true --temp-path="$tmp" 2>&1)
  printf '%s\n' "$log"
  result=$(printf '%s\n' "$log" | sed -n -e "s/.*new artifact: '\([^']*\)'.*/\1/p" \
    -e "s/.*Found existing full backup: '\([^']*\)'.*/\1/p" | tail -n 1)
  if [ -z "$result" ] || [ ! -f "$result" ]; then
    fail "could not merge the backup chain ending at $dir/$file: $(printf '%s\n' "$log" | grep -v -e JAVA_TOOL -e incubator -e '^-*$' | tail -n 3 | tr '\n' ' ')"
  fi
  mv "$result" "$out/$file"
  for p in "$result".*; do [ -e "$p" ] && mv "$p" "$out/$file.${p##*.}"; done
  rm -rf "$work" "$tmp"
done
echo "seed artifacts ready"
`

// seedMergeArgs flattens the plan's artifacts into the script's arguments.
func (p *seedMergePlan) seedMergeArgs() []string {
	var args []string
	for _, a := range p.Artifacts {
		mode := "link"
		if a.Merge {
			mode = "merge"
		}
		args = append(args, strings.Trim(a.Dir, "/"), a.Filename, a.Database, mode)
	}
	return args
}

// applySeedMerge adds the merge init container and its scratch volume to the
// proxy's pod, and points httpd at the merged artifacts. The pod runs as the
// Neo4j UID (fsGroup included) so the scratch volume is writable whether it
// is an emptyDir or a PVC.
func applySeedMerge(pod *corev1.PodSpec, plan *seedMergePlan) {
	scratch := corev1.Volume{Name: seedMergeScratchVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	if ts := plan.TempStorage; ts != nil && ts.Size != "" {
		claim := corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(ts.Size)},
			},
		}
		if ts.StorageClassName != "" {
			class := ts.StorageClassName
			claim.StorageClassName = &class
		}
		scratch.VolumeSource = corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{
			VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: claim},
		}}
	}
	pod.Volumes = append(pod.Volumes, scratch)
	pod.SecurityContext = resources.DefaultNeo4jPodSecurityContext()
	pod.ImagePullSecrets = plan.ImagePullSecrets
	pod.InitContainers = []corev1.Container{{
		Name:            seedMergeContainerName,
		Image:           plan.Image,
		ImagePullPolicy: plan.ImagePullPolicy,
		Command:         append([]string{"/bin/bash", "-c", seedMergeScript, seedMergeContainerName}, plan.seedMergeArgs()...),
		Env:             []corev1.EnvVar{{Name: "SEED_MERGE_COMMAND", Value: plan.Command}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "backup", MountPath: "/backup", ReadOnly: true},
			{Name: seedMergeScratchVolume, MountPath: seedMergeScratchPath},
		},
		Resources:                plan.Resources,
		SecurityContext:          resources.DefaultNeo4jContainerSecurityContext(),
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}}
	for i := range pod.Containers {
		if pod.Containers[i].Name != pvcSeedProxyContainerName {
			continue
		}
		pod.Containers[i].Command = []string{"sh", "-c", fmt.Sprintf("httpd -f -v -p %d -h %s", pvcSeedProxyPort, seedMergeServeRoot)}
		pod.Containers[i].VolumeMounts = append(pod.Containers[i].VolumeMounts,
			corev1.VolumeMount{Name: seedMergeScratchVolume, MountPath: seedMergeScratchPath, ReadOnly: true})
	}
}

// pvcSeedProxyMergeFailure returns why the proxy's merge init container
// failed, or "" if it has not. A failed merge does not heal by retrying — the
// chain is broken, or the scratch volume too small — so the restore fails at
// once with the reason instead of waiting out its budget.
func pvcSeedProxyMergeFailure(ctx context.Context, c client.Client, namespace, ownerName string) string {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/name":     "backup-seed-proxy",
		"app.kubernetes.io/instance": ownerName,
	}); err != nil {
		return ""
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.InitContainerStatuses {
			if cs.Name != seedMergeContainerName {
				continue
			}
			for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
				if t != nil && t.ExitCode != 0 {
					msg := strings.TrimSpace(t.Message)
					if msg == "" {
						msg = fmt.Sprintf("exit code %d (%s)", t.ExitCode, t.Reason)
					}
					return msg
				}
			}
		}
	}
	return ""
}
