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

// AuraRestoreSpec restores an Aura instance in place from one of its snapshots
// (the Aura equivalent of Neo4jRestore). It is a one-shot action recorded as an
// auditable object: on completion the CR stays as history and does not re-fire.
// API credentials are resolved from the referenced AuraInstance's provider
// config. All fields are immutable.
//
// +kubebuilder:validation:XValidation:rule="has(self.snapshotId) != has(self.snapshotRef)",message="set exactly one of snapshotId or snapshotRef"
// +kubebuilder:validation:XValidation:rule="self.instanceRef == oldSelf.instanceRef",message="instanceRef is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.snapshotId) || (has(self.snapshotId) && self.snapshotId == oldSelf.snapshotId)",message="snapshotId is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.snapshotRef) || (has(self.snapshotRef) && self.snapshotRef == oldSelf.snapshotRef)",message="snapshotRef is immutable"
type AuraRestoreSpec struct {
	// InstanceRef is the target AuraInstance (same namespace), restored in place.
	// +kubebuilder:validation:Required
	InstanceRef string `json:"instanceRef"`

	// SnapshotID to restore from. Mutually exclusive with snapshotRef.
	// +optional
	SnapshotID string `json:"snapshotId,omitempty"`

	// SnapshotRef resolves the snapshot ID from an AuraSnapshot CR in the same
	// namespace. Mutually exclusive with snapshotId.
	// +optional
	SnapshotRef string `json:"snapshotRef,omitempty"`
}

// AuraRestoreStatus is the observed state of the restore action.
type AuraRestoreStatus struct {
	// Phase: Pending, Restoring, Completed, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// SnapshotID actually restored (resolved from snapshotRef when used).
	// +optional
	SnapshotID string `json:"snapshotId,omitempty"`

	// StartTime is when the restore was issued to Aura.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the instance returned to Running (or failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message carries the latest human-readable detail.
	// +optional
	Message string `json:"message,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aurarestore
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.spec.instanceRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=string,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraRestore restores an AuraInstance in place from one of its snapshots.
type AuraRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraRestoreSpec   `json:"spec,omitempty"`
	Status AuraRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraRestoreList contains a list of AuraRestore.
type AuraRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraRestore{}, &AuraRestoreList{})
}
