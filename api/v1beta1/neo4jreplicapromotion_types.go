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

// Promotion lifecycle phases.
const (
	PromotionPhasePending   = "Pending"
	PromotionPhasePromoting = "Promoting"
	PromotionPhaseCompleted = "Completed"
	PromotionPhaseFailed    = "Failed"
)

// Neo4jReplicaPromotionSpec requests the one-way promotion of a cross-cluster
// replica into an ordinary read-write database.
//
// Deliberately a separate one-shot CR rather than a field on
// Neo4jReplicaDatabase. Promotion is irreversible in Neo4j, so a boolean in a
// level-triggered spec would be dangerous: setting it back to false means
// "make it a replica again", which can only be honoured by dropping and
// re-seeding. A CEL immutability rule closes the interactive path but not the
// one that matters — a GitOps controller re-applying the pre-promotion
// manifest after a failover is byte-identical to a deliberate revert. A
// one-shot CR is inert to re-apply, and matches Neo4jBackup / Neo4jRestore.
type Neo4jReplicaPromotionSpec struct {
	// ReplicaRef is the Neo4jReplicaDatabase CR in the same namespace to
	// promote.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.replicaRef is immutable"
	ReplicaRef string `json:"replicaRef"`

	// Topology optionally changes the database's topology as part of
	// promotion, passed to dbms.promoteReplicaDatabase. When omitted the
	// replica's existing topology is retained.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.topology is immutable"
	Topology *DatabaseTopology `json:"topology,omitempty"`
}

// Neo4jReplicaPromotionStatus describes the observed state of a promotion.
type Neo4jReplicaPromotionStatus struct {
	// Conditions reflects the latest reconcile state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is Pending, Promoting, Completed or Failed. Completed and Failed
	// are terminal.
	// +kubebuilder:validation:Enum=Pending;Promoting;Completed;Failed
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message carries a short human-readable explanation of the current Phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation observed during the last
	// reconcile.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CompletionTime is when the promotion reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ObservedLagTxIds is the replica's replicationLag immediately before
	// promotion — the recovery point that this promotion made permanent.
	//
	// Recorded, never enforced. Blocking a failover on a lag threshold is the
	// wrong default: during a real outage the operator does not get to
	// second-guess the human. But the RPO actually taken must be auditable
	// afterwards.
	// +optional
	ObservedLagTxIds int64 `json:"observedLagTxIds,omitempty"`

	// LastCommittedTxn is the last transaction the replica had applied at
	// promotion time.
	// +optional
	LastCommittedTxn int64 `json:"lastCommittedTxn,omitempty"`

	// PromotedDatabase is the database name that was promoted.
	// +optional
	PromotedDatabase string `json:"promotedDatabase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=n4jpromo;n4jpromotions,categories=neo4j
// +kubebuilder:printcolumn:name="Replica",type=string,JSONPath=`.spec.replicaRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="LagTaken",type=integer,JSONPath=`.status.observedLagTxIds`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jReplicaPromotion is the Schema for the neo4jreplicapromotions API.
type Neo4jReplicaPromotion struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jReplicaPromotionSpec   `json:"spec,omitempty"`
	Status Neo4jReplicaPromotionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jReplicaPromotionList contains a list of Neo4jReplicaPromotion.
type Neo4jReplicaPromotionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jReplicaPromotion `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jReplicaPromotion{}, &Neo4jReplicaPromotionList{})
}
