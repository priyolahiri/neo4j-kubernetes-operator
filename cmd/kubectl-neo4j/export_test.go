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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

func replicationBackup(name string, runs ...neo4jv1beta1.BackupRun) *neo4jv1beta1.Neo4jBackup {
	return &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "prod",
			Storage: neo4jv1beta1.StorageLocation{
				Type: "s3", Bucket: "prod-backups",
				Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "cloud-creds"},
			},
		},
		Status: neo4jv1beta1.Neo4jBackupStatus{
			ReplicationPullURI: "s3://prod-backups/nightly-chain/",
			History:            runs,
		},
	}
}

// The whole point: the pull URI comes out of the upstream's status, not out of
// the user's head. The CRD says assembling it by hand is the single most likely
// thing to get wrong.
func TestExport_ReplicaFromBackupTakesPullURIFromStatus(t *testing.T) {
	c := testClient(t, replicationBackup("nightly"))

	replica, notes, err := replicaFromBackup(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "nightly", "dr", "neo4j", false)
	require.NoError(t, err)

	assert.Equal(t, "s3://prod-backups/nightly-chain/", replica.Spec.Source.PullURI)
	assert.Equal(t, "backup", replica.Spec.Source.Mode)
	assert.Equal(t, "dr", replica.Spec.ClusterRef)
	assert.Equal(t, "neo4j", replica.Spec.UpstreamDatabase)
	assert.Empty(t, replica.Spec.Source.SeedURI, "seedURI is opt-in")

	joined := strings.Join(notes, "\n")
	assert.Contains(t, joined, "status.replicationPullURI")
	// Credentials cannot cross a cluster boundary, and silently emitting a
	// manifest that references a Secret which does not exist there would be a
	// trap.
	assert.Contains(t, joined, "cloud-creds")
	assert.Contains(t, joined, "DOWNSTREAM")
}

// A backup that is not a replication source has nothing to publish, and the
// error has to say why rather than emitting an empty pullURI.
func TestExport_BackupWithoutPullURIIsAClearError(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "prod"},
	})

	_, _, err := replicaFromBackup(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "nightly", "dr", "neo4j", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication-source")
}

func TestExport_SeedFromLatestPicksNewestSuccessfulRun(t *testing.T) {
	older := neo4jv1beta1.BackupRun{
		RunID: "1", Status: "Completed", ArtifactFilename: "neo4j-2026-08-01T01-00-00.backup",
		StartTime: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
	}
	newer := neo4jv1beta1.BackupRun{
		RunID: "2", Status: "Completed", ArtifactFilename: "neo4j-2026-08-02T01-00-00.backup",
		StartTime: metav1.NewTime(time.Now().Add(-24 * time.Hour)),
	}
	failed := neo4jv1beta1.BackupRun{
		RunID: "3", Status: "Failed", ArtifactFilename: "neo4j-2026-08-03T01-00-00.backup",
		StartTime: metav1.NewTime(time.Now()),
	}

	c := testClient(t, replicationBackup("nightly", older, newer, failed))
	replica, notes, err := replicaFromBackup(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "nightly", "dr", "neo4j", true)
	require.NoError(t, err)

	assert.Equal(t, "s3://prod-backups/nightly-chain/neo4j-2026-08-02T01-00-00.backup",
		replica.Spec.Source.SeedURI, "the newest SUCCESSFUL run wins, and the URI is joined once")
	assert.Contains(t, strings.Join(notes, "\n"), "newest successful run")
}

// The operator parses the artifact name out of a pod log, so it can be missing
// on a run that genuinely succeeded. Reconstructing it would be a guess.
func TestExport_SeedFromLatestRefusesToGuessAMissingArtifact(t *testing.T) {
	run := neo4jv1beta1.BackupRun{
		RunID: "1", Status: "Completed", StartTime: metav1.NewTime(time.Now()),
	}
	c := testClient(t, replicationBackup("nightly", run))

	_, _, err := replicaFromBackup(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "nightly", "dr", "neo4j", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "artifactFilename")
	assert.Contains(t, err.Error(), "by hand")
}

func TestExport_ReplicaFromClusterPrefersCrossClusterAddresses(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			InternalAddresses: []string{"prod-server-0.prod-headless.neo4j.svc.cluster.local:6000"},
			CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationStatus{
				Ready:     true,
				Addresses: []string{"ccr.example.com:6000"},
			},
		},
	})

	replica, notes, err := replicaFromCluster(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "prod", "dr", "neo4j")
	require.NoError(t, err)

	assert.Equal(t, "network", replica.Spec.Source.Mode)
	assert.Equal(t, []string{"ccr.example.com:6000"}, replica.Spec.Source.Addresses)
	assert.Contains(t, strings.Join(notes, "\n"), "crossClusterReplication.addresses")
}

// The trap this command must not set: in-cluster DNS names handed to someone
// building a genuinely separate cluster.
func TestExport_InternalAddressesCarryAnUnmissableWarning(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			InternalAddresses: []string{"prod-server-0.prod-headless.neo4j.svc.cluster.local:6000"},
		},
	})

	replica, notes, err := replicaFromCluster(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "prod", "dr", "neo4j")
	require.NoError(t, err)

	assert.Len(t, replica.Spec.Source.Addresses, 1)
	joined := strings.Join(notes, "\n")
	assert.Contains(t, joined, "IN-CLUSTER addresses")
	assert.Contains(t, joined, "NOT from a separate one")
	// And it must point at the better same-cluster answer rather than leaving
	// the user with a literal that goes stale.
	assert.Contains(t, joined, "upstreamClusterRef")
}

func TestExport_ClusterWithNoAddressesIsAClearError(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
	})
	_, _, err := replicaFromCluster(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "prod", "dr", "neo4j")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crossClusterReplication.enabled")
}

// The property that makes generation safe: what it emits is what the operator
// accepts. Asserted by running the operator's own validator over the result.
func TestExport_OutputPassesTheOperatorsOwnValidator(t *testing.T) {
	c := testClient(t, replicationBackup("nightly"))

	replica, _, err := replicaFromBackup(context.Background(), c, "neo4j", "neo4j",
		"dr-copy", "nightly", "dr", "neo4j", false)
	require.NoError(t, err)

	res := validation.NewReplicaValidator(c).Validate(context.Background(), replica)
	assert.Empty(t, res.Errors, "generated manifests must pass kubectl neo4j validate")
}

// Cross-namespace targeting: the manifest is for another cluster, so its
// namespace need not match the source's.
func TestExport_DownstreamNamespaceIsIndependentOfTheSource(t *testing.T) {
	c := testClient(t, replicationBackup("nightly"))
	replica, _, err := replicaFromBackup(context.Background(), c, "neo4j", "dr-namespace",
		"dr-copy", "nightly", "dr", "neo4j", false)
	require.NoError(t, err)
	assert.Equal(t, "dr-namespace", replica.Namespace)
}

// TypeMeta must be set: a manifest without apiVersion/kind cannot be applied,
// and objects read back from a typed client do not carry it.
func TestExport_ManifestCarriesAPIVersionAndKind(t *testing.T) {
	r := newReplicaSkeleton("dr-copy", "neo4j", "dr", "neo4j")
	assert.Equal(t, "Neo4jReplicaDatabase", r.Kind)
	assert.Equal(t, neo4jv1beta1.GroupVersion.String(), r.APIVersion)
}
