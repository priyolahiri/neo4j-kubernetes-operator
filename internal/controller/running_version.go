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

// status.version is the version a deployment's servers run, and the version
// rule compares spec.image.tag with it (operations.md ids 139 and 144). This
// file reads that version from the StatefulSet the servers run under.

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// rolledImageTag returns the image tag every replica of sts runs: the tag of
// its pod template's first container (the Neo4j container), once the
// StatefulSet controller has seen the current template and every replica is
// on that revision and ready. "" when it cannot say: a rollout in flight, a
// status the controller has not written for the current template yet, no
// replicas, or an image without a tag.
//
// The template and its status come from the same read, so a cache that has
// not seen a template change yet still describes the pods consistently: the
// old template, rolled.
func rolledImageTag(sts *appsv1.StatefulSet) string {
	if sts == nil || len(sts.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	if sts.Status.ObservedGeneration < sts.Generation {
		return ""
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas < 1 {
		return ""
	}
	if sts.Status.UpdateRevision != "" && sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return ""
	}
	if sts.Status.UpdatedReplicas < replicas || sts.Status.ReadyReplicas < replicas {
		return ""
	}
	return imageTag(sts.Spec.Template.Spec.Containers[0].Image)
}

// imageTag returns the tag of a repo:tag image reference, as spec.image.tag
// spells it: the operator builds the image as "<repo>:<tag>", so a tag that
// carries a digest ("2026.08.1-enterprise@sha256:…") comes back whole. "" for
// an image without a tag (one pinned by digest alone). A registry port
// ("registry:5000/neo4j") is not a tag separator.
func imageTag(image string) string {
	_, tag, _ := splitImage(image)
	return tag
}

// splitImage splits a "<repo>:<tag>" image reference, the way the operator
// builds one from spec.image, back into the repo and the tag: the tag keeps a
// digest it carries ("2026.08.1-enterprise@sha256:…"), and a registry port
// ("registry:5000/neo4j") is part of the repo. ok is false for an image
// without a tag. The colon of the digest is not the tag's: splitting on the
// last colon read "neo4j:2026.08.1-enterprise@sha256" as the repo and the
// digest's hex as the tag (#531).
func splitImage(image string) (repo, tag string, ok bool) {
	name, digest := image, ""
	if at := strings.Index(image, "@"); at >= 0 {
		name, digest = image[:at], image[at:]
	}
	i := strings.LastIndex(name, ":")
	if i <= 0 || strings.Contains(name[i+1:], "/") {
		return "", "", false
	}
	return name[:i], name[i+1:] + digest, true
}

// clusterVersionToRecord decides what a pass that finds the cluster Ready
// writes to status.version, given the tag every server runs (rolledImageTag);
// "" leaves status.version as it is.
//
// The tag the servers run is recorded when it differs from status.version: on
// the first Ready, and after a new image reached the servers without the
// upgrade machine, which happens when the cluster has no ready server
// (holdImageDriftUntilReady lets the image through so a wedged cluster can
// recover). Left as it was, status.version named the older version, and moving
// the tag back to it — a downgrade of what the servers run — passed the
// version rule.
//
// Not when the upgrade machine has a Failed or Paused record for that same
// tag: the machine re-enters a Failed upgrade (isUpgradeRequired) and records
// the version itself once it has verified every server; Paused waits for the
// user. Until then status.version stays at the version the servers ran before
// the upgrade, as operations.md id 139 says.
func clusterVersionToRecord(status *neo4jv1beta1.Neo4jEnterpriseClusterStatus, running string) string {
	if running == "" || running == status.Version {
		return ""
	}
	if us := status.UpgradeStatus; status.Version != "" && us != nil && us.TargetVersion == running &&
		(us.Phase == upgradePhaseFailed || us.Phase == upgradePhasePaused) {
		return ""
	}
	return running
}
