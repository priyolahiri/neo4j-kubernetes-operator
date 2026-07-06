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

package aura

import "strings"

// Instance status values, as returned in the `status` field of an Instance.
// These mirror the enum in the Aura API v1 OpenAPI spec.
const (
	InstanceStatusCreating      = "creating"
	InstanceStatusDestroying    = "destroying"
	InstanceStatusRunning       = "running"
	InstanceStatusPausing       = "pausing"
	InstanceStatusPaused        = "paused"
	InstanceStatusSuspending    = "suspending"
	InstanceStatusSuspended     = "suspended"
	InstanceStatusResuming      = "resuming"
	InstanceStatusLoading       = "loading"
	InstanceStatusLoadingFailed = "loading failed"
	InstanceStatusRestoring     = "restoring"
	InstanceStatusUpdating      = "updating"
	InstanceStatusOverwriting   = "overwriting"
)

// Snapshot status values, as returned in the `status` field of a Snapshot.
const (
	SnapshotStatusCompleted  = "Completed"
	SnapshotStatusInProgress = "InProgress"
	SnapshotStatusFailed     = "Failed"
	SnapshotStatusPending    = "Pending"
	SnapshotStatusCancelled  = "Cancelled"
)

// Snapshot profile values.
const (
	SnapshotProfileAdHoc     = "AdHoc"
	SnapshotProfileScheduled = "Scheduled"
)

// IsInstanceRunning reports whether status is the terminal "running" state — the
// instance is available for use.
func IsInstanceRunning(status string) bool {
	return normalizeStatus(status) == InstanceStatusRunning
}

// IsInstancePaused reports whether status is the terminal "paused" state.
func IsInstancePaused(status string) bool {
	return normalizeStatus(status) == InstanceStatusPaused
}

// IsInstanceTransient reports whether status is a transient (in-flight) state
// that will settle to a terminal state on its own — the caller should keep
// polling. Covers creating/pausing/resuming/updating/restoring/destroying/
// overwriting/loading (and their suspending sibling).
func IsInstanceTransient(status string) bool {
	switch normalizeStatus(status) {
	case InstanceStatusCreating,
		InstanceStatusPausing,
		InstanceStatusResuming,
		InstanceStatusUpdating,
		InstanceStatusRestoring,
		InstanceStatusDestroying,
		InstanceStatusOverwriting,
		InstanceStatusLoading,
		InstanceStatusSuspending:
		return true
	default:
		return false
	}
}

// Instance is the full detail of an Aura instance, as returned by
// GET /instances/{instanceId} (unwrapped from the `data` envelope).
type Instance struct {
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	Status                string `json:"status"`
	ConnectionURL         string `json:"connection_url"`
	MetricsIntegrationURL string `json:"metrics_integration_url,omitempty"`
	TenantID              string `json:"tenant_id"`
	CloudProvider         string `json:"cloud_provider"`
	Region                string `json:"region"`
	Type                  string `json:"type"`
	Memory                string `json:"memory"`
	Storage               string `json:"storage,omitempty"`
	CreatedAt             string `json:"created_at"`
	CustomerManagedKeyID  string `json:"customer_managed_key_id,omitempty"`
	GraphNodes            string `json:"graph_nodes,omitempty"`
	GraphRelationships    string `json:"graph_relationships,omitempty"`
	SecondariesCount      *int   `json:"secondaries_count,omitempty"`
	CDCEnrichmentMode     string `json:"cdc_enrichment_mode,omitempty"`
	VectorOptimized       *bool  `json:"vector_optimized,omitempty"`
	GraphAnalyticsPlugin  *bool  `json:"graph_analytics_plugin,omitempty"`
}

// InstanceSummary is the abbreviated instance shape returned by GET /instances.
type InstanceSummary struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	TenantID      string `json:"tenant_id"`
	CloudProvider string `json:"cloud_provider"`
	CreatedAt     string `json:"created_at"`
}

// CreateInstanceRequest is the request body for POST /instances. Optional fields
// use omitempty so they are omitted when unset. `version`, `region`, `memory`,
// `name`, `type`, `tenant_id`, and `cloud_provider` are required by the API.
type CreateInstanceRequest struct {
	Version              string `json:"version"`
	Region               string `json:"region"`
	Memory               string `json:"memory"`
	Name                 string `json:"name"`
	Type                 string `json:"type"`
	TenantID             string `json:"tenant_id"`
	CloudProvider        string `json:"cloud_provider"`
	Storage              string `json:"storage,omitempty"`
	SourceInstanceID     string `json:"source_instance_id,omitempty"`
	SourceSnapshotID     string `json:"source_snapshot_id,omitempty"`
	CustomerManagedKeyID string `json:"customer_managed_key_id,omitempty"`
	VectorOptimized      *bool  `json:"vector_optimized,omitempty"`
	GraphAnalyticsPlugin *bool  `json:"graph_analytics_plugin,omitempty"`
}

// CreateInstanceResponse is the 202 body of POST /instances (unwrapped from the
// `data` envelope). It carries the one-time initial credentials — the caller
// MUST persist Username/Password before the instance transitions to running, as
// they are returned only once.
type CreateInstanceResponse struct {
	ID                   string `json:"id"`
	ConnectionURL        string `json:"connection_url"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	TenantID             string `json:"tenant_id"`
	CloudProvider        string `json:"cloud_provider"`
	Region               string `json:"region"`
	Name                 string `json:"name"`
	Type                 string `json:"type"`
	CreatedAt            string `json:"created_at"`
	VectorOptimized      *bool  `json:"vector_optimized,omitempty"`
	GraphAnalyticsPlugin *bool  `json:"graph_analytics_plugin,omitempty"`
}

// PatchInstanceRequest is the request body for PATCH /instances/{instanceId}.
// All fields are optional pointers with omitempty so that unset fields are not
// serialized — only the properties the caller wants to change are sent.
type PatchInstanceRequest struct {
	Name                 *string `json:"name,omitempty"`
	Memory               *string `json:"memory,omitempty"`
	Storage              *string `json:"storage,omitempty"`
	SecondariesCount     *int    `json:"secondaries_count,omitempty"`
	CDCEnrichmentMode    *string `json:"cdc_enrichment_mode,omitempty"`
	VectorOptimized      *bool   `json:"vector_optimized,omitempty"`
	GraphAnalyticsPlugin *bool   `json:"graph_analytics_plugin,omitempty"`
}

// Tenant is the detail of an Aura project/tenant returned by
// GET /tenants/{tenantId} (unwrapped from the `data` envelope). Its
// InstanceConfigurations field is the authoritative oracle of which
// region/type/memory/storage/version combinations are valid for the project.
type Tenant struct {
	ID                     string                  `json:"id"`
	Name                   string                  `json:"name"`
	InstanceConfigurations []InstanceConfiguration `json:"instance_configurations"`
}

// InstanceConfiguration is a single supported instance shape within a tenant.
type InstanceConfiguration struct {
	Region        string `json:"region"`
	RegionName    string `json:"region_name"`
	Type          string `json:"type"`
	Memory        string `json:"memory"`
	Storage       string `json:"storage"`
	Version       string `json:"version"`
	CloudProvider string `json:"cloud_provider"`
}

// Snapshot describes a single Aura instance snapshot.
type Snapshot struct {
	InstanceID string `json:"instance_id"`
	SnapshotID string `json:"snapshot_id"`
	Profile    string `json:"profile"`
	Status     string `json:"status"`
	Timestamp  string `json:"timestamp"`
	Exportable bool   `json:"exportable"`
}

// normalizeStatus lowercases and trims an instance status for tolerant
// comparison. Retained as a helper in case callers compare raw API values.
func normalizeStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}
