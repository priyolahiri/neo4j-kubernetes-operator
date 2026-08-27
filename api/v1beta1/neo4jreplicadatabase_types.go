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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Replica source modes.
const (
	// ReplicaSourceModeBackup pulls a differential backup chain from object
	// storage. Requires no network path between the two Kubernetes clusters.
	ReplicaSourceModeBackup = "backup"

	// ReplicaSourceModeNetwork streams from the upstream cluster's endpoints.
	// Not supported by this operator — see the design note.
	ReplicaSourceModeNetwork = "network"
)

// Replica lifecycle phases.
const (
	ReplicaPhasePending     = "Pending"
	ReplicaPhaseSeeding     = "Seeding"
	ReplicaPhaseReplicating = "Replicating"
	ReplicaPhasePromoted    = "Promoted"
	ReplicaPhaseFailed      = "Failed"
)

// Neo4jReplicaDatabaseSpec defines a read-only cross-cluster replica of a
// database hosted on another Neo4j cluster.
//
// This CR is applied to the DOWNSTREAM cluster. The upstream lives in another
// Kubernetes cluster the operator cannot see; the only coupling is the object
// storage location the upstream's Neo4jBackup chain writes to.
//
// Requires Neo4j 2026.08+ on the downstream cluster.
type Neo4jReplicaDatabaseSpec struct {
	// ClusterRef is the downstream Neo4jEnterpriseCluster or
	// Neo4jEnterpriseStandalone in the same namespace that will host the
	// replica.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Name is the replica database name. Defaults to metadata.name when empty.
	//
	// Choose deliberately: Cypher has no RENAME DATABASE, so this name is
	// permanent and survives promotion. Pair the replica with a
	// Neo4jDatabaseAlias if applications should address it under the
	// upstream's name after failover.
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_.\-]*$`
	// +optional
	Name string `json:"name,omitempty"`

	// UpstreamDatabase is the name of the database being replicated, as it is
	// known on the upstream cluster. Becomes `replicaConfig.remote`.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	UpstreamDatabase string `json:"upstreamDatabase"`

	// Topology is how the replica is distributed across the downstream
	// cluster's servers. Both primaries and secondaries are read-only.
	// +optional
	Topology *DatabaseTopology `json:"topology,omitempty"`

	// Source describes where the replica pulls from. Immutable — Neo4j offers
	// no way to re-point an existing replica, so a change here would mean
	// dropping and re-seeding the database.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.source is immutable; delete and recreate the Neo4jReplicaDatabase to change the replication source"
	Source ReplicaSourceSpec `json:"source"`

	// PullInterval is how often the replica checks the pullURI for new
	// differential backups (Neo4j's `db.cluster.backup.pull_interval`,
	// default 1m). It bounds the replica's recovery point objective.
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m|h)$`
	// +optional
	PullInterval string `json:"pullInterval,omitempty"`

	// DeletionPolicy controls what happens to the replica database when this
	// CR is deleted.
	//   Delete (default): DROP DATABASE on CR deletion.
	//   Retain:           leave it in Neo4j, release the finalizer only.
	//
	// This applies only while the database is still a replica. Once promoted,
	// deletion NEVER drops regardless of this setting — a promoted database is
	// the live system, and removing a CR that no longer describes it must not
	// be a data-loss event.
	// +kubebuilder:validation:Enum=Delete;Retain
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// ReplicaSourceSpec describes where a replica pulls its data from.
type ReplicaSourceSpec struct {
	// Mode selects the replication mechanism.
	//
	// "backup" pulls a differential backup chain from object storage and
	// needs no network path between clusters.
	//
	// "network" streams directly from the upstream's cluster endpoints —
	// either listed explicitly (source.addresses) or resolved automatically
	// from an upstream on this same Kubernetes cluster
	// (source.upstreamClusterRef). For a genuinely separate upstream
	// Kubernetes cluster, source.addresses requires the upstream servers'
	// `server.cluster.advertised_address` to be externally routable — set
	// `spec.crossClusterReplication.enabled: true` on the upstream
	// Neo4jEnterpriseCluster and read the ready-to-use endpoint list from its
	// `status.crossClusterReplication.addresses`. See
	// docs/design/cross-cluster-replication.md §6.
	// +kubebuilder:validation:Enum=backup;network
	// +kubebuilder:default=backup
	// +optional
	Mode string `json:"mode,omitempty"`

	// PullURI is the object-storage directory holding the upstream's
	// differential backup chain. Becomes `replicaConfig.pullURI`.
	//
	// This must be the directory the upstream Neo4jBackup CR writes into —
	// read it from that CR's `status.replicationPullURI`. Pointing at the
	// bucket root, or at a directory a second backup CR also writes to, breaks
	// the chain.
	//
	// Example: "s3://prod-backups/foo-chain/"
	// +optional
	PullURI string `json:"pullURI,omitempty"`

	// SeedURI is the full backup artifact the replica is initially seeded
	// from, before it starts applying differentials. Must belong to the same
	// chain as PullURI.
	//
	// Example: "s3://prod-backups/foo-chain/foo-2026-08-01T01-00-00.backup"
	// +optional
	SeedURI string `json:"seedURI,omitempty"`

	// Addresses lists upstream cluster endpoints for network mode
	// (host:port, the upstream's port 6000). One reachable address is
	// sufficient: the upstream hands back the addresses the downstream then
	// actually uses (its own advertised cluster addresses), so this list only
	// needs to get the first connection made. Ignored in backup mode.
	// Mutually exclusive with UpstreamClusterRef — set exactly one for
	// network mode.
	// +optional
	Addresses []string `json:"addresses,omitempty"`

	// UpstreamClusterRef resolves Addresses automatically from an upstream
	// Neo4jEnterpriseCluster's status.internalAddresses, instead of listing
	// them by hand. Mutually exclusive with Addresses — set exactly one for
	// network mode.
	//
	// Only usable when the upstream is on this SAME Kubernetes cluster:
	// resolution is a live Get against this cluster's own API server, which
	// cannot reach a genuinely separate physical cluster. For an upstream on
	// a different Kubernetes cluster, use Addresses instead, populated from
	// the upstream's spec.crossClusterReplication proxy.
	// +optional
	UpstreamClusterRef *UpstreamClusterRef `json:"upstreamClusterRef,omitempty"`

	// CredentialsSecretRef names a Secret holding object-storage credentials
	// for PullURI/SeedURI, when the downstream cluster cannot reach the bucket
	// via workload identity. Same key layout as Neo4jBackup's cloud
	// credentials.
	// +optional
	CredentialsSecretRef string `json:"credentialsSecretRef,omitempty"`
}

// UpstreamClusterRef references an upstream Neo4jEnterpriseCluster to
// resolve network-mode addresses from automatically, instead of listing
// them in Addresses. Only usable when the upstream is on this SAME
// Kubernetes cluster: resolution is a live Get against this cluster's own
// API server, which cannot reach a genuinely separate physical cluster. For
// an upstream on a different Kubernetes cluster, use Addresses instead,
// populated from the upstream's spec.crossClusterReplication proxy.
type UpstreamClusterRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace of the upstream Neo4jEnterpriseCluster. Defaults to this
	// Neo4jReplicaDatabase's own namespace when omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// Neo4jReplicaDatabaseStatus describes the observed state of a replica.
type Neo4jReplicaDatabaseStatus struct {
	// Conditions reflects the latest reconcile state. Conditions used:
	//   Ready            — replica exists and is online
	//   ClusterNotReady  — referenced cluster is not Ready
	//   VersionTooOld    — downstream cluster predates 2026.08
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is a coarse summary: Pending, Seeding, Replicating, Promoted,
	// Failed.
	//
	// Promoted is TERMINAL. Once set, the controller stops touching the
	// database entirely — no create, no topology reconciliation, no drift
	// correction — because the database is no longer a replica and
	// "correcting" it back would mean dropping the promoted live system.
	// +kubebuilder:validation:Enum=Pending;Seeding;Replicating;Promoted;Failed
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message carries a short human-readable explanation of the current Phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation observed during the last
	// successful reconcile.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DatabaseType is the live `type` column from SHOW DATABASES — "replica"
	// while replicating, and a standard type after promotion.
	//
	// This is the operator's authoritative signal, not status.phase: status
	// can be lost to an etcd restore, so every mutating path re-reads this
	// value before acting.
	// +optional
	DatabaseType string `json:"databaseType,omitempty"`

	// LastCommittedTxn is the last transaction ID the replica has applied.
	// +optional
	LastCommittedTxn int64 `json:"lastCommittedTxn,omitempty"`

	// ReplicationLag is how many transactions the replica is behind, and
	// therefore the data loss that promoting right now would make permanent.
	// +optional
	ReplicationLag int64 `json:"replicationLag,omitempty"`

	// PromotedAt is when this replica was promoted to a standard database.
	// +optional
	PromotedAt *metav1.Time `json:"promotedAt,omitempty"`

	// PromotedBy names the Neo4jReplicaPromotion CR that promoted it, or
	// "out-of-band" when the operator observed a promotion it did not perform.
	// +optional
	PromotedBy string `json:"promotedBy,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=n4jreplica;n4jreplicas,categories=neo4j
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Upstream",type=string,JSONPath=`.spec.upstreamDatabase`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lag",type=integer,JSONPath=`.status.replicationLag`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jReplicaDatabase is the Schema for the neo4jreplicadatabases API.
type Neo4jReplicaDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jReplicaDatabaseSpec   `json:"spec,omitempty"`
	Status Neo4jReplicaDatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jReplicaDatabaseList contains a list of Neo4jReplicaDatabase.
type Neo4jReplicaDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jReplicaDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jReplicaDatabase{}, &Neo4jReplicaDatabaseList{})
}
