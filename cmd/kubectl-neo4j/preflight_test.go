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

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func joinChecks(cs []symptom) string {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.mark + " " + c.subject + " " + c.what + " " + c.detail + " " + c.action + "\n")
	}
	return b.String()
}

func boolPtr(b bool) *bool { return &b }

const clusterManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata: {name: prod, namespace: neo4j}
spec:
  image: {repo: neo4j, tag: 5.26.0-enterprise}
  topology: {servers: 3}
  storage: {className: fast-ssd, size: 10Gi}
  resources:
    requests: {memory: 2Gi}
`

// The check that closes the loop the operator can only close after apply: a
// StorageClass that does not exist means no StatefulSet is ever built.
func TestPreflight_MissingStorageClassIsAProblem(t *testing.T) {
	c := testClient(t)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, "storageclass fast-ssd")
	assert.Contains(t, out, "does not exist")
	assert.Contains(t, out, "StorageClassNotFound")
}

// The quiet one: expansion costs nothing today and is only discovered during
// the resize a full disk made urgent (#284).
func TestPreflight_StorageClassWithoutExpansionWarnsButDoesNotFail(t *testing.T) {
	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(false),
		},
		// Nodes big enough for the pod, so this test isolates the storage check.
		node("n1", "8Gi", true), node("n2", "8Gi", true), node("n3", "8Gi", true),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))

	out := joinChecks(res.checks)
	assert.Contains(t, out, "does not allow volume expansion")
	assert.Contains(t, out, "allowVolumeExpansion")
	assert.False(t, res.problems(), "a warning must not fail the run")
}

func TestPreflight_NoNodeLargeEnoughIsAProblem(t *testing.T) {
	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(true),
		},
		node("n1", "1Gi", true),
		node("n2", "1Gi", true),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, "no Ready node can fit a 2Gi memory request")
	assert.Contains(t, out, "1.5Gi", "the floor must be named so the user does not chase it down")
}

// The scheduler places a pod on ONE node, so capacity must never be summed.
func TestPreflight_CapacityIsPerNodeNotClusterTotal(t *testing.T) {
	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(true),
		},
		node("n1", "1Gi", true), node("n2", "1Gi", true), node("n3", "1Gi", true),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))
	assert.True(t, res.problems(), "3Gi of total capacity must not satisfy a 2Gi pod")
}

// A NotReady node has no capacity to offer.
func TestPreflight_NotReadyNodesDoNotCount(t *testing.T) {
	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(true),
		},
		node("big-but-down", "64Gi", false),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))
	assert.True(t, res.problems())
}

func TestPreflight_FewerFittingNodesThanServersWarns(t *testing.T) {
	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(true),
		},
		node("n1", "8Gi", true),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(clusterManifest))

	out := joinChecks(res.checks)
	assert.False(t, res.problems())
	assert.Contains(t, out, "1 Ready node(s) can fit")
	assert.Contains(t, out, "antiAffinity", "the warning must say WHEN it actually bites")
}

func TestPreflight_MissingImagePullSecretIsAProblem(t *testing.T) {
	manifest := strings.Replace(clusterManifest,
		"image: {repo: neo4j, tag: 5.26.0-enterprise}",
		"image: {repo: neo4j, tag: 5.26.0-enterprise, pullSecrets: [regcred]}", 1)

	c := testClient(t,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: "fast-ssd"},
			AllowVolumeExpansion: boolPtr(true),
		},
		node("n1", "8Gi", true), node("n2", "8Gi", true), node("n3", "8Gi", true),
	)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(manifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, "imagePullSecret regcred")
	assert.Contains(t, out, "ImagePullBackOff")
}

const backupManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jBackup
metadata: {name: nightly, namespace: neo4j}
spec:
  instanceRef: prod
  storage:
    type: s3
    bucket: my-backups
    cloud:
      provider: aws
      credentialsSecretRef: cloud-creds
`

// This is the check that replaces `kubectl run backup-auth-check`: a Secret
// missing a key the Job mounts never starts the pod, and the kubelet's
// CreateContainerConfigError does not mention backups.
func TestPreflight_CredentialsSecretMissingKeysIsAProblem(t *testing.T) {
	c := testClient(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: "neo4j"},
		Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("x")},
	})
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(backupManifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, "AWS_SECRET_ACCESS_KEY")
	assert.Contains(t, out, "AWS_REGION")
	assert.Contains(t, out, "CreateContainerConfigError")
}

// The keys checked must be the operator's, not a list restated in the CLI.
func TestPreflight_ChecksExactlyTheKeysTheOperatorDeclares(t *testing.T) {
	data := map[string][]byte{}
	for _, k := range resources.CloudCredentialKeys("aws") {
		data[k] = []byte("x")
	}
	c := testClient(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: "neo4j"},
		Data:       data,
	})
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(backupManifest))

	assert.False(t, res.problems())
	out := joinChecks(res.checks)
	assert.Contains(t, out, "carries every key the Job mounts")
	assert.Contains(t, out, "only known at run time", "the boundary must be stated where it applies")
}

func TestPreflight_MissingCredentialsSecretIsAProblem(t *testing.T) {
	c := testClient(t)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(backupManifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, "secret cloud-creds")
	assert.Contains(t, out, "does not exist")
}

const ambientBackupManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jBackup
metadata: {name: nightly, namespace: neo4j}
spec:
  instanceRef: prod
  storage:
    type: s3
    bucket: my-backups
    cloud:
      provider: aws
`

// No Secret means the Job leans on an ambient identity. A ServiceAccount with
// no IRSA annotation means it leans on nothing.
func TestPreflight_AmbientIdentityWithoutAnnotationIsAProblem(t *testing.T) {
	c := testClient(t, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: resources.BackupServiceAccountName, Namespace: "neo4j"},
	})
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(ambientBackupManifest))

	out := joinChecks(res.checks)
	assert.True(t, res.problems())
	assert.Contains(t, out, resources.CloudIdentityAnnotation("aws"))
	assert.Contains(t, out, "autoCreate.annotations")
}

func TestPreflight_AmbientIdentityWithAnnotationPasses(t *testing.T) {
	c := testClient(t, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: resources.BackupServiceAccountName, Namespace: "neo4j",
			Annotations: map[string]string{
				resources.CloudIdentityAnnotation("aws"): "arn:aws:iam::1:role/neo4j-backup",
			},
		},
	})
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(ambientBackupManifest))

	assert.False(t, res.problems())
	assert.Contains(t, joinChecks(res.checks), "is bound to a aws identity")
}

// Before the first backup the ServiceAccount does not exist yet, which is
// expected — reporting it as a failure would cry wolf on every new install.
func TestPreflight_AbsentServiceAccountBeforeFirstBackupIsNotAFailure(t *testing.T) {
	c := testClient(t)
	res := preflightObject(context.Background(), c, "neo4j", "f.yaml", []byte(ambientBackupManifest))

	assert.False(t, res.problems())
	out := joinChecks(res.checks)
	assert.Contains(t, out, "does not exist yet")
	assert.Contains(t, out, "creates it on the first backup")
}

// A kind with no cluster-side preconditions must say so. A silent pass would
// imply a check that was never made.
func TestPreflight_UncheckedKindIsReportedAsSkipped(t *testing.T) {
	manifest := `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata: {name: analytics, namespace: neo4j}
spec: {clusterRef: prod, name: analytics}
`
	res := preflightObject(context.Background(), testClient(t), "neo4j", "f.yaml", []byte(manifest))
	assert.NotEmpty(t, res.skipped)
	assert.False(t, res.problems())
}

func TestReadManifests_SplitsMultiDocumentFiles(t *testing.T) {
	p := writeManifest(t, clusterManifest+"\n---\n"+backupManifest+"\n---\n# just a comment\n")
	docs, err := readManifests(p)
	require.NoError(t, err)
	assert.Len(t, docs, 2, "the comment-only document must not count as an object")
}

func TestRenderPreflight_ExitCodeAndBoundaryLine(t *testing.T) {
	ok := preflightResult{kind: "Neo4jEnterpriseCluster", name: "prod", source: "cluster"}
	bad := preflightResult{kind: "Neo4jBackup", name: "nightly", source: "b.yaml", checks: []symptom{
		{mark: markProblem, subject: "secret cloud-creds", what: "does not exist in neo4j"},
	}}

	out, code := renderPreflightTo(t, []preflightResult{ok}, "neo4j")
	assert.Equal(t, exitOK, code)
	assert.Contains(t, out, "every cluster-side precondition passed")
	assert.Contains(t, out, "Shape only: no bucket, registry or endpoint was contacted.")

	out, code = renderPreflightTo(t, []preflightResult{ok, bad}, "neo4j")
	assert.Equal(t, exitProblems, code)
	assert.Contains(t, out, "Neo4jBackup/nightly (b.yaml)")
	assert.Contains(t, out, "1 of 2 resource(s) would fail on a precondition")
}

func renderPreflightTo(t *testing.T, results []preflightResult, ns string) (string, int) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	code := renderPreflight(results, f, ns)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b), code
}

func node(name, memory string, ready bool) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{corev1.ResourceMemory: mustQuantity(memory)},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
		},
	}
}

var _ = neo4jv1beta1.GroupVersion

func mustQuantity(s string) apiresource.Quantity {
	q, err := apiresource.ParseQuantity(s)
	if err != nil {
		panic(err)
	}
	return q
}

// The precondition that costs most to discover the hard way: a backup-mode
// replica reads its chain FROM THE SERVER, so the downstream cluster's own
// environment must carry the object-store settings. Without them the failure
// surfaces inside the AWS SDK as "Unable to load region from any of the
// providers", mentioning neither replicas nor buckets.
func TestPreflightReplica(t *testing.T) {
	replica := func(mode, pullURI, credsRef string) *neo4jv1beta1.Neo4jReplicaDatabase {
		return &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "foo-replica", Namespace: "dr"},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef: "dr-cluster",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode: mode, PullURI: pullURI, CredentialsSecretRef: credsRef,
				},
			},
		}
	}
	clusterWith := func(env ...string) *neo4jv1beta1.Neo4jEnterpriseCluster {
		var vars []corev1.EnvVar
		for _, n := range env {
			vars = append(vars, corev1.EnvVar{Name: n, Value: "x"})
		}
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "dr-cluster", Namespace: "dr"},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{Env: vars},
		}
	}

	t.Run("s3 source, cluster without AWS_REGION", func(t *testing.T) {
		c := testClient(t, clusterWith())
		got := preflightReplica(context.Background(), c, "dr", replica("backup", "s3://b/chain/", ""))
		require.NotEmpty(t, got)
		assert.Contains(t, got[0].what, "AWS_REGION")
		assert.Contains(t, got[0].action, "ON THE SERVER")
	})

	t.Run("s3 source, cluster with AWS_REGION is clean", func(t *testing.T) {
		c := testClient(t, clusterWith("AWS_REGION"))
		assert.Empty(t, preflightReplica(context.Background(), c, "dr", replica("backup", "s3://b/chain/", "")))
	})

	// Network mode reads over the wire; there is no bucket to authenticate to,
	// and saying so is more useful than saying nothing.
	t.Run("network mode needs no credentials", func(t *testing.T) {
		c := testClient(t, clusterWith())
		got := preflightReplica(context.Background(), c, "dr", replica("network", "", ""))
		require.Len(t, got, 1)
		assert.Equal(t, markWaiting, got[0].mark)
	})

	t.Run("a named Secret that does not exist is reported", func(t *testing.T) {
		c := testClient(t, clusterWith("AWS_REGION"))
		got := preflightReplica(context.Background(), c, "dr", replica("backup", "s3://b/chain/", "absent-creds"))
		require.NotEmpty(t, got)
		assert.Contains(t, got[0].subject, "absent-creds")
		// The Secret is now projected onto the servers, so its absence blocks
		// the seed rather than being a bookkeeping detail. The action said the
		// opposite until 2026-09-14.
		assert.Contains(t, got[0].action, "projects its")
	})

	// AWS_DEFAULT_REGION is the AWS SDK's own equally-valid spelling.
	// Accepting only AWS_REGION reported a problem on a cluster that had
	// already set the region.
	t.Run("AWS_DEFAULT_REGION satisfies the region check", func(t *testing.T) {
		c := testClient(t, clusterWith("AWS_DEFAULT_REGION"))
		got := preflightReplica(context.Background(), c, "dr", replica("backup", "s3://b/chain/", ""))
		assert.Empty(t, got)
	})

	// Cloud workload identity injects credentials into the POD at admission,
	// where neither the CLI nor the operator can see them. The operator skips
	// its own check in that case, so preflight must not report a problem the
	// operator ignores — but silence would read as "checked and fine" for
	// something nobody checked, so it says which it is.
	t.Run("a cloud role is reported as unverifiable, not as missing credentials", func(t *testing.T) {
		cluster := clusterWith()
		cluster.Spec.PodServiceAccountAnnotations = map[string]string{
			"eks.amazonaws.com/role-arn": "arn:aws:iam::1234:role/dr",
		}
		c := testClient(t, cluster)
		got := preflightReplica(context.Background(), c, "dr", replica("backup", "s3://b/chain/", ""))
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
		assert.Contains(t, got[0].what, "cloud role")
	})
}
