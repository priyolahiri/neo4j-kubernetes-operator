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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func newTestReplicaDatabase(name, ns string) *neo4jv1beta1.Neo4jReplicaDatabase {
	return &neo4jv1beta1.Neo4jReplicaDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
			ClusterRef:       "dr-cluster",
			UpstreamDatabase: "foo",
			Source: neo4jv1beta1.ReplicaSourceSpec{
				Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
				UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{Name: "prod-cluster", Namespace: "prod"},
			},
		},
	}
}

// TestResolveUpstreamClusterAddresses_Ready confirms a same-Kubernetes-cluster
// upstream with status.internalAddresses already populated resolves cleanly.
func TestResolveUpstreamClusterAddresses_Ready(t *testing.T) {
	replica := newTestReplicaDatabase("foo-replica", "dr")
	upstream := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-cluster", Namespace: "prod"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			InternalAddresses: []string{
				"prod-cluster-server-0.prod-cluster-headless.prod.svc.cluster.local:6000",
				"prod-cluster-server-1.prod-cluster-headless.prod.svc.cluster.local:6000",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, upstream).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	addrs, ok, err := r.resolveUpstreamClusterAddresses(context.Background(), replica, replica.Spec.Source.UpstreamClusterRef)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, upstream.Status.InternalAddresses, addrs)
}

// TestResolveUpstreamClusterAddresses_NotFound confirms a missing upstream is
// an ordinary Pending/requeue condition, not a terminal error — the upstream
// may simply not have been created yet.
func TestResolveUpstreamClusterAddresses_NotFound(t *testing.T) {
	replica := newTestReplicaDatabase("foo-replica", "dr")
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	addrs, ok, err := r.resolveUpstreamClusterAddresses(context.Background(), replica, replica.Spec.Source.UpstreamClusterRef)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, addrs)
}

// TestResolveUpstreamClusterAddresses_NotReady confirms an upstream that
// exists but hasn't published status.internalAddresses yet (e.g. its own
// first reconcile hasn't landed) is Pending/requeue, not a failure.
func TestResolveUpstreamClusterAddresses_NotReady(t *testing.T) {
	replica := newTestReplicaDatabase("foo-replica", "dr")
	upstream := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-cluster", Namespace: "prod"},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, upstream).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	addrs, ok, err := r.resolveUpstreamClusterAddresses(context.Background(), replica, replica.Spec.Source.UpstreamClusterRef)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, addrs)
}

// TestResolveUpstreamClusterAddresses_DefaultsNamespace confirms an omitted
// UpstreamClusterRef.Namespace defaults to the Neo4jReplicaDatabase's own
// namespace.
func TestResolveUpstreamClusterAddresses_DefaultsNamespace(t *testing.T) {
	replica := newTestReplicaDatabase("foo-replica", "dr")
	upstream := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-cluster", Namespace: "dr"}, // same namespace as the replica
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			InternalAddresses: []string{"prod-cluster-server-0.prod-cluster-headless.dr.svc.cluster.local:6000"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, upstream).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	ref := &neo4jv1beta1.UpstreamClusterRef{Name: "prod-cluster"} // Namespace omitted
	addrs, ok, err := r.resolveUpstreamClusterAddresses(context.Background(), replica, ref)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, upstream.Status.InternalAddresses, addrs)
}

func newTestReplicaDatabaseBackup(name, ns string) *neo4jv1beta1.Neo4jReplicaDatabase {
	return &neo4jv1beta1.Neo4jReplicaDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
			ClusterRef:       "dr-cluster",
			UpstreamDatabase: "foo",
			Source: neo4jv1beta1.ReplicaSourceSpec{
				Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
				UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{Name: "foo-chain", Namespace: "prod"},
			},
		},
	}
}

// TestResolveUpstreamBackupPullURI_Ready confirms a same-Kubernetes-cluster
// upstream Neo4jBackup with status.replicationPullURI already populated
// resolves cleanly.
func TestResolveUpstreamBackupPullURI_Ready(t *testing.T) {
	replica := newTestReplicaDatabaseBackup("foo-replica", "dr")
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-chain", Namespace: "prod"},
		Status:     neo4jv1beta1.Neo4jBackupStatus{ReplicationPullURI: "s3://prod-backups/foo/foo-chain/"},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, backup).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jBackup{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	pullURI, ok, err := r.resolveUpstreamBackupPullURI(context.Background(), replica, replica.Spec.Source.UpstreamBackupRef)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, backup.Status.ReplicationPullURI, pullURI)
}

// TestResolveUpstreamBackupPullURI_NotFound confirms a missing upstream
// Neo4jBackup is an ordinary Pending/requeue condition, not a terminal
// error — it may simply not have been created yet.
func TestResolveUpstreamBackupPullURI_NotFound(t *testing.T) {
	replica := newTestReplicaDatabaseBackup("foo-replica", "dr")
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	pullURI, ok, err := r.resolveUpstreamBackupPullURI(context.Background(), replica, replica.Spec.Source.UpstreamBackupRef)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, pullURI)
}

// TestResolveUpstreamBackupPullURI_NotReady confirms an upstream Neo4jBackup
// that exists but hasn't run its first backup yet (empty
// replicationPullURI) is Pending/requeue, not a failure.
func TestResolveUpstreamBackupPullURI_NotReady(t *testing.T) {
	replica := newTestReplicaDatabaseBackup("foo-replica", "dr")
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-chain", Namespace: "prod"},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, backup).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jBackup{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	pullURI, ok, err := r.resolveUpstreamBackupPullURI(context.Background(), replica, replica.Spec.Source.UpstreamBackupRef)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, pullURI)
}

// TestResolveUpstreamBackupPullURI_DefaultsNamespace confirms an omitted
// UpstreamBackupRef.Namespace defaults to the Neo4jReplicaDatabase's own
// namespace.
func TestResolveUpstreamBackupPullURI_DefaultsNamespace(t *testing.T) {
	replica := newTestReplicaDatabaseBackup("foo-replica", "dr")
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-chain", Namespace: "dr"}, // same namespace as the replica
		Status:     neo4jv1beta1.Neo4jBackupStatus{ReplicationPullURI: "s3://dr-backups/foo/foo-chain/"},
	}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, backup).
		WithStatusSubresource(&neo4jv1beta1.Neo4jReplicaDatabase{}, &neo4jv1beta1.Neo4jBackup{}).
		Build()
	r := &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: record.NewFakeRecorder(50)}

	ref := &neo4jv1beta1.UpstreamBackupRef{Name: "foo-chain"} // Namespace omitted
	pullURI, ok, err := r.resolveUpstreamBackupPullURI(context.Background(), replica, ref)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, backup.Status.ReplicationPullURI, pullURI)
}
