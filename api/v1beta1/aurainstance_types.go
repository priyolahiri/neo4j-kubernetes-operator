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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AuraInstanceSource clones a new instance from an existing one at create time
// (immutable). Exactly one of instanceRef / instanceId identifies the source.
type AuraInstanceSource struct {
	// InstanceRef is another AuraInstance (same namespace) to clone from.
	// +optional
	InstanceRef string `json:"instanceRef,omitempty"`

	// InstanceID is the Aura source instance ID (alternative to instanceRef).
	// +optional
	InstanceID string `json:"instanceId,omitempty"`

	// SnapshotID to clone from; requires an exportable snapshot of the source.
	// +optional
	SnapshotID string `json:"snapshotId,omitempty"`
}

// AuraInstanceSpec is the desired state of an Aura-hosted instance.
//
// Immutable fields (cloudProvider, region, type, version, projectId,
// customerManagedKeyId, source, instanceId) are enforced declaratively by the
// apiserver via CEL transition rules below — there is no admission webhook
// (project Invariant 1). Combinations of type/region/memory/version are further
// validated against the live per-project instance_configurations inline in the
// reconciler (the one check CEL cannot express).
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
// +kubebuilder:validation:XValidation:rule="self.type != 'free-db' || !has(self.storage)",message="storage is not configurable for free-db"
// +kubebuilder:validation:XValidation:rule="self.type == 'free-db' || has(self.memory)",message="memory is required for every tier except free-db (the Aura API requires it at create time)"
// +kubebuilder:validation:XValidation:rule="!has(self.secondariesCount) || self.type == 'enterprise-db'",message="secondariesCount is only valid for type enterprise-db (Virtual Dedicated Cloud)"
// +kubebuilder:validation:XValidation:rule="!has(self.cdcEnrichmentMode) || self.type == 'enterprise-db' || self.type == 'business-critical'",message="cdcEnrichmentMode is only valid for type enterprise-db or business-critical"
// +kubebuilder:validation:XValidation:rule="!has(self.customerManagedKeyId) || self.type == 'enterprise-db' || self.type == 'enterprise-ds'",message="customerManagedKeyId is only valid for type enterprise-db or enterprise-ds"
// +kubebuilder:validation:XValidation:rule="self.cloudProvider == oldSelf.cloudProvider",message="cloudProvider is immutable"
// +kubebuilder:validation:XValidation:rule="self.region == oldSelf.region",message="region is immutable"
// +kubebuilder:validation:XValidation:rule="self.type == oldSelf.type || (oldSelf.type == 'professional-db' && self.type == 'business-critical')",message="type is immutable except for the in-place professional-db → business-critical upgrade"
// +kubebuilder:validation:XValidation:rule="self.version == oldSelf.version",message="version is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.projectId) || (has(self.projectId) && self.projectId == oldSelf.projectId)",message="projectId is immutable once set"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.customerManagedKeyId) || (has(self.customerManagedKeyId) && self.customerManagedKeyId == oldSelf.customerManagedKeyId)",message="customerManagedKeyId is immutable once set"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.instanceId) || (has(self.instanceId) && self.instanceId == oldSelf.instanceId)",message="instanceId is immutable once set"
// +kubebuilder:validation:XValidation:rule="has(self.source) == has(oldSelf.source) && (!has(self.source) || self.source == oldSelf.source)",message="source is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.multiDatabase) == has(oldSelf.multiDatabase) && (!has(self.multiDatabase) || self.multiDatabase == oldSelf.multiDatabase)",message="multiDatabase is immutable: Aura fixes it when the instance is created and offers no way to convert an existing instance"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.organizationId) || (has(self.organizationId) && self.organizationId == oldSelf.organizationId)",message="organizationId is immutable once set"
// +kubebuilder:validation:XValidation:rule="!has(self.multiDatabase) || !self.multiDatabase || self.type in ['business-critical','enterprise-db']",message="multiDatabase is only supported on business-critical or enterprise-db (Virtual Dedicated Cloud); Aura refuses it on every other tier"
// +kubebuilder:validation:XValidation:rule="!has(self.multiDatabase) || !self.multiDatabase || (!has(self.storage) && !has(self.vectorOptimized) && !has(self.graphAnalyticsPlugin) && !has(self.secondariesCount) && !has(self.cdcEnrichmentMode) && !has(self.customerManagedKeyId) && !has(self.source))",message="multiDatabase creates the instance through the Aura v2beta1 API, which accepts only name/type/cloudProvider/region/memory and SILENTLY IGNORES every other field: unset storage, vectorOptimized, graphAnalyticsPlugin, secondariesCount, cdcEnrichmentMode, customerManagedKeyId and source"
type AuraInstanceSpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults +
	// rate limiter) in the same namespace. Mutually exclusive with
	// credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// ProjectID is the Aura project (API tenant_id). Immutable. If empty, the
	// referenced AuraProviderConfig's defaultProjectId is used (the operator
	// does not write it back into spec, to stay GitOps-idempotent).
	// +optional
	ProjectID string `json:"projectId,omitempty"`

	// OrganizationID is the Aura organization. Immutable once set. Only the
	// v2beta1 code paths need it (multiDatabase creation and the multi-database
	// status probe) — plain v1 management does not. If empty, the referenced
	// AuraProviderConfig's defaultOrganizationId is used.
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// CloudProvider hosting the instance. Immutable.
	// +kubebuilder:validation:Enum=aws;gcp;azure
	// +kubebuilder:validation:Required
	CloudProvider string `json:"cloudProvider"`

	// Region, e.g. europe-west1. Immutable. Valid values are per-project.
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// Type of instance. Immutable.
	//   free-db | professional-db | business-critical |
	//   enterprise-db (Virtual Dedicated Cloud) | professional-ds | enterprise-ds
	// +kubebuilder:validation:Enum=free-db;professional-db;business-critical;enterprise-db;professional-ds;enterprise-ds
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// Version is the coarse Aura Neo4j major version (e.g. "5"). Immutable.
	// +kubebuilder:validation:Required
	Version string `json:"version"`

	// Memory, e.g. 4GB. Mutable (drives online resize). Required except for
	// free-db, where the size is fixed by the tier.
	// +optional
	Memory string `json:"memory,omitempty"`

	// Storage, e.g. 8GB. Mutable. Not configurable for free-db.
	// +optional
	Storage string `json:"storage,omitempty"`

	// Name is the Aura instance name (1-30 chars). Defaults to metadata.name.
	// +kubebuilder:validation:MaxLength=30
	// +optional
	Name string `json:"name,omitempty"`

	// Paused expresses the desired paused state (drives pause / resume).
	// +optional
	Paused bool `json:"paused,omitempty"`

	// VectorOptimized enables vector optimization.
	// +optional
	VectorOptimized *bool `json:"vectorOptimized,omitempty"`

	// GraphAnalyticsPlugin enables the graph-analytics plugin.
	// +optional
	GraphAnalyticsPlugin *bool `json:"graphAnalyticsPlugin,omitempty"`

	// SecondariesCount — Virtual Dedicated Cloud (enterprise-db) only.
	// +optional
	SecondariesCount *int32 `json:"secondariesCount,omitempty"`

	// CDCEnrichmentMode — VDC / business-critical only.
	// +kubebuilder:validation:Enum=OFF;DIFF;FULL
	// +optional
	CDCEnrichmentMode string `json:"cdcEnrichmentMode,omitempty"`

	// CustomerManagedKeyID — VDC / AuraDS-Enterprise only. Immutable once set.
	// +optional
	CustomerManagedKeyID string `json:"customerManagedKeyId,omitempty"`

	// MultiDatabase requests a MULTI-DATABASE instance, the only kind that can
	// host more than the one database Aura creates with it — i.e. the only kind
	// AuraDatabase, AuraDatabaseBackup and AuraDatabaseRestore can be used
	// against. Supported on business-critical and enterprise-db (Virtual
	// Dedicated Cloud) only; Aura rejects it on free-db and professional-db.
	//
	// Immutable, because Aura fixes it at creation and publishes no way to
	// convert an existing instance. Only `true` changes anything: an unset or
	// false value leaves the normal v1 create path in place.
	//
	// Setting it switches the CREATE call to the Aura v2beta1 API (v1 has no
	// such field), which requires an organization ID and accepts a smaller set
	// of fields — see the CEL rules above. Everything after create (observe,
	// resize, pause/resume, upgrade, delete) still goes through v1.
	// +optional
	MultiDatabase *bool `json:"multiDatabase,omitempty"`

	// Source clones a new instance from an existing one at create. Immutable.
	// +optional
	Source *AuraInstanceSource `json:"source,omitempty"`

	// InstanceID adopts/imports an existing Aura instance by ID rather than
	// creating one. Immutable once set. The operator mirrors this into the
	// neo4j.com/external-instance-id annotation (its internal source of truth).
	// +optional
	InstanceID string `json:"instanceId,omitempty"`

	// ConnectionSecretName is the Secret the operator writes the instance
	// connection details (URI + one-time credentials) to. Defaults to
	// "<name>-conn" if empty.
	// +optional
	ConnectionSecretName string `json:"connectionSecretName,omitempty"`

	// ConnectionSecretFormat selects the key layout of the connection Secret.
	// +kubebuilder:validation:Enum=neo4j-driver;aura-dotenv;jdbc;servicebinding;custom
	// +kubebuilder:default=neo4j-driver
	// +optional
	ConnectionSecretFormat string `json:"connectionSecretFormat,omitempty"`

	// PublishConnectionDetailsTo names a ConfigMap to receive the non-secret
	// endpoint details (URI, instanceId, region, type). Credentials stay in the
	// connection Secret.
	// +optional
	PublishConnectionDetailsTo string `json:"publishConnectionDetailsTo,omitempty"`

	// DeletionPolicy controls what happens to the cloud instance when this CR is
	// deleted: Orphan (default; keep the running instance) or Delete.
	// +kubebuilder:validation:Enum=Orphan;Delete
	// +kubebuilder:default=Orphan
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// DeletionProtection blocks deletion of the cloud instance even when
	// deletionPolicy=Delete, until cleared.
	// +optional
	DeletionProtection bool `json:"deletionProtection,omitempty"`

	// ManagementPolicies restricts which actions the operator may take on the
	// cloud instance. Default ["*"] = full management. Use a subset to observe
	// only (["Observe"]), never delete (["Observe","Create","Update"]), etc.
	// +kubebuilder:validation:items:Enum=Observe;Create;Update;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraInstanceObservation mirrors the instance state last observed from the Aura
// API — the source of truth for drift detection and reporting.
type AuraInstanceObservation struct {
	// +optional
	Status string `json:"status,omitempty"`
	// +optional
	Memory string `json:"memory,omitempty"`
	// +optional
	Storage string `json:"storage,omitempty"`
	// +optional
	Type string `json:"type,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// +optional
	CloudProvider string `json:"cloudProvider,omitempty"`
	// +optional
	Name string `json:"name,omitempty"`

	// MultiDatabase reports whether the instance can host more than one
	// database, which decides whether AuraDatabase / AuraDatabaseBackup /
	// AuraDatabaseRestore can target it at all.
	//
	// Unset means UNKNOWN, not false. The flag lives only on the Aura v2beta1
	// instance detail — v1 does not return it — and that endpoint fails for
	// instances created through v1, so the operator cannot always learn the
	// answer. It is probed once (the value can never change) and cached.
	// +optional
	MultiDatabase *bool `json:"multiDatabase,omitempty"`

	// DefaultDatabaseID is the database Aura creates together with a
	// multi-database instance. It is reported only by the v2beta1 create
	// response, so it is populated only for instances this operator created with
	// multiDatabase set. Useful for telling that built-in database apart from
	// the ones AuraDatabase CRs own.
	// +optional
	DefaultDatabaseID string `json:"defaultDatabaseId,omitempty"`
}

// AuraServiceBinding is the Service Binding "Provisioned Service" pointer.
type AuraServiceBinding struct {
	// Name is the connection Secret conforming to the Service Binding spec.
	Name string `json:"name,omitempty"`
}

// AuraInstanceStatus is the observed state of an Aura instance.
type AuraInstanceStatus struct {
	// InstanceID is the Aura instance ID (mirror of the external-name annotation).
	// +optional
	InstanceID string `json:"instanceId,omitempty"`

	// Phase mirrors the Aura instance status (Creating, Running, Pausing,
	// Paused, Resuming, Updating, Restoring, Destroying, ...).
	// +optional
	Phase string `json:"phase,omitempty"`

	// ConnectionURL is the Bolt routing URL (neo4j+s://...).
	// +optional
	ConnectionURL string `json:"connectionUrl,omitempty"`

	// MetricsIntegrationURL is the Prometheus scrape endpoint, when available.
	// +optional
	MetricsIntegrationURL string `json:"metricsIntegrationUrl,omitempty"`

	// Binding exposes this instance as a Service Binding Provisioned Service.
	// +optional
	Binding *AuraServiceBinding `json:"binding,omitempty"`

	// Conditions: Ready (instance usable) and Synced (operator reconciled spec).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the instance was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`

	// AtProvider is the full instance state last observed from the Aura API.
	// +optional
	AtProvider *AuraInstanceObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aurainst
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="Memory",type=string,JSONPath=`.spec.memory`
// +kubebuilder:printcolumn:name="Connection",type=string,JSONPath=`.status.connectionUrl`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraInstance declaratively manages a Neo4j Aura cloud instance via the Aura
// REST API — provision, resize, pause/resume, and (optionally) delete. Distinct
// from spec.auraFleetManagement, which registers a self-managed cluster into the
// Aura console. See docs/design/aura-orchestration.md.
type AuraInstance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraInstanceSpec   `json:"spec,omitempty"`
	Status AuraInstanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraInstanceList contains a list of AuraInstance.
type AuraInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraInstance{}, &AuraInstanceList{})
}
