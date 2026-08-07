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

// Neo4jDatabaseAliasSpec defines a local database alias — a second name for an
// existing database in the same DBMS.
//
// The motivating case is cross-cluster replication failover. Cypher has no
// RENAME DATABASE, so a replica created as `foo-replica` keeps that name
// forever, including after promotion. An alias lets applications address `foo`
// on the downstream cluster throughout: while replicating it resolves to a
// read-only replica, and after promotion the same alias resolves to a
// read-write standard database with no client reconfiguration. Because aliases
// may target a database that is still a replica, the alias is created up front
// rather than during the failover window.
//
// Scope: LOCAL aliases only. Remote aliases (`... AT '<url>' USER ... PASSWORD
// ...`) and composite-database constituents are not modelled here.
type Neo4jDatabaseAliasSpec struct {
	// ClusterRef is the Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone in
	// the same namespace that hosts both the alias and its target.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Name is the alias name in Neo4j. Defaults to metadata.name when empty.
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_.\-]*$`
	// +optional
	Name string `json:"name,omitempty"`

	// TargetDatabase is the database this alias points at. It may be a
	// standard database or a cross-cluster replica; the alias survives the
	// replica's promotion unchanged.
	//
	// The target does not have to exist yet — the controller reports Pending
	// and retries rather than failing, so an alias and its replica can be
	// applied together.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetDatabase string `json:"targetDatabase"`

	// DeletionPolicy controls what happens to the alias in Neo4j when this CR
	// is deleted.
	//   Delete (default): DROP ALIAS.
	//   Retain:           leave it in place, release the finalizer only.
	// +kubebuilder:validation:Enum=Delete;Retain
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// Neo4jDatabaseAliasStatus describes the observed state of a Neo4jDatabaseAlias.
type Neo4jDatabaseAliasStatus struct {
	// Conditions reflects the latest reconcile state. Conditions used:
	//   Ready            — alias exists and points at spec.targetDatabase
	//   ClusterNotReady  — referenced cluster is not Ready
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is a coarse summary: Pending, Ready, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message carries a short human-readable explanation of the current Phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation observed during the last
	// successful reconcile.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ObservedTarget is the database the alias actually resolves to, read back
	// from SHOW ALIASES FOR DATABASE. Differs from spec.targetDatabase only
	// between an out-of-band change and the next reconcile.
	// +optional
	ObservedTarget string `json:"observedTarget,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=n4jalias;n4jaliases,categories=neo4j
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetDatabase`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jDatabaseAlias is the Schema for the neo4jdatabasealiases API.
type Neo4jDatabaseAlias struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jDatabaseAliasSpec   `json:"spec,omitempty"`
	Status Neo4jDatabaseAliasStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jDatabaseAliasList contains a list of Neo4jDatabaseAlias.
type Neo4jDatabaseAliasList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jDatabaseAlias `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jDatabaseAlias{}, &Neo4jDatabaseAliasList{})
}
