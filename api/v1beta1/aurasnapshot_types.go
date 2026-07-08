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

// AuraSnapshotSpec requests an on-demand snapshot of an Aura instance. API
// credentials are resolved from the referenced AuraInstance's provider config.
//
// NOTE: the Aura API cannot DELETE snapshots. Deleting this CR releases its
// finalizer and drops it from cluster state; the Aura snapshot itself persists.
//
// +kubebuilder:validation:XValidation:rule="self.instanceRef == oldSelf.instanceRef",message="instanceRef is immutable"
type AuraSnapshotSpec struct {
	// InstanceRef is the AuraInstance (same namespace) to snapshot. Immutable.
	// +kubebuilder:validation:Required
	InstanceRef string `json:"instanceRef"`
}

// AuraSnapshotStatus is the observed state of the snapshot.
type AuraSnapshotStatus struct {
	// SnapshotID is the Aura snapshot ID (set once the snapshot is requested).
	// +optional
	SnapshotID string `json:"snapshotId,omitempty"`

	// Profile is AdHoc or Scheduled.
	// +optional
	Profile string `json:"profile,omitempty"`

	// Phase maps the Aura snapshot status: Pending, InProgress, Completed, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Exportable reports whether the snapshot can seed a new instance.
	// +optional
	Exportable bool `json:"exportable,omitempty"`

	// SnapshotTime is the snapshot's timestamp as reported by Aura.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aurasnap
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.spec.instanceRef`
// +kubebuilder:printcolumn:name="SnapshotID",type=string,JSONPath=`.status.snapshotId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraSnapshot takes an on-demand snapshot of an AuraInstance (the Aura
// equivalent of Neo4jBackup). Restore is a separate AuraRestore CR.
type AuraSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraSnapshotSpec   `json:"spec,omitempty"`
	Status AuraSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraSnapshotList contains a list of AuraSnapshot.
type AuraSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraSnapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraSnapshot{}, &AuraSnapshotList{})
}
