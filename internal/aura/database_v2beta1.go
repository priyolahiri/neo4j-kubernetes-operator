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

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// ==========================================================================
// BETA — Aura API v2beta1 databases (+ per-database backups / restore).
//
// Databases are nested under an instance:
//   /organizations/{org}/projects/{project}/instances/{instance}/databases
// with per-database backups + restore under
//   .../databases/{id}/backups[/{backupId}]  and  .../databases/{id}/restore
//
// Unlike the ip-filters endpoints, these are wrapped in the standard v2beta1
// {"data": …} envelope (unwrapped here by doV2Data).
//
// Landmines the spec pins — do NOT "tidy" these back:
//
//  1. `legacy_status` belongs to INSTANCES only (CreateInstanceResponse /
//     InstanceDetails). Databases and backups do not have it: DatabaseSummary
//     carries ONLY `id`, and DatabaseBackup carries `status` with the enum
//     below. Reading a database/backup status from `legacy_status` silently
//     yields "" forever.
//
//  2. The RESTORE body field is `id` — NOT `backup_id`. Unlike create, this body
//     IS schema'd upstream, with `id` required, so `backup_id` is a hard 400.
//
//  3. DatabaseSummary has no `name`. The create body takes a name, but no
//     response echoes it back, so a database cannot be matched by name.
//
// Only the database CREATE body is un-schema'd upstream (the POST publishes no
// requestBody); it mirrors the documented conventions and is BETA/best-effort.
// Everything else here follows a published schema.
// ==========================================================================

// Backup status values (v2beta1 DatabaseBackup.status, a required field).
const (
	BackupStatusPending    = "Pending"
	BackupStatusInProgress = "InProgress"
	BackupStatusCompleted  = "Completed"
	BackupStatusFailed     = "Failed"
)

// Database is a v2beta1 database on an Aura instance.
//
// This is the full DatabaseSummary shape: the API returns ONLY an ID for a
// database — no name, no status. Do not add fields here without a schema to
// point at.
type Database struct {
	ID string `json:"id,omitempty"`
}

// CreateDatabaseRequest is the create body. The POST publishes no requestBody
// upstream, so this shape is BETA/best-effort (the 422 "invalid clone
// parameters" text implies it also accepts clone fields we do not model).
type CreateDatabaseRequest struct {
	Name string `json:"name"`
}

// DatabaseBackup is a per-database backup record (v2beta1 DatabaseBackup).
// `id`, `timestamp`, `status` and `exportable` are all required upstream.
//
// NOTE: the CREATE response is thinner — CreateDatabaseBackupResponse carries
// only `id` — so Status/Timestamp/Exportable are zero until a subsequent GET.
type DatabaseBackup struct {
	ID         string `json:"id,omitempty"`
	Status     string `json:"status,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
	Exportable bool   `json:"exportable,omitempty"`
}

// RestoreDatabaseRequest restores a database from one of its backups.
// The field is `id` (the backup's ID) and is required — see landmine 2 above.
type RestoreDatabaseRequest struct {
	BackupID string `json:"id"`
}

// dbCollectionPath builds the instance-scoped database collection path.
func dbCollectionPath(orgID, projectID, instanceID string) string {
	return "/organizations/" + url.PathEscape(orgID) +
		"/projects/" + url.PathEscape(projectID) +
		"/instances/" + url.PathEscape(instanceID) + "/databases"
}

// doV2Data performs a v2beta1 request and unwraps the {"data": …} envelope into
// out (which may be a struct or a slice). A nil out skips decoding.
func (c *Client) doV2Data(ctx context.Context, method, path string, body, out any) error {
	if out == nil {
		return c.doV2JSON(ctx, method, path, body, nil)
	}
	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := c.doV2JSON(ctx, method, path, body, &wrapper); err != nil {
		return err
	}
	if len(wrapper.Data) == 0 || string(wrapper.Data) == "null" {
		return nil
	}
	return json.Unmarshal(wrapper.Data, out)
}

// CreateDatabase creates a database on an Aura instance (v2beta1, beta).
func (c *Client) CreateDatabase(ctx context.Context, orgID, projectID, instanceID string, req CreateDatabaseRequest) (*Database, error) {
	var out Database
	if err := c.doV2Data(ctx, http.MethodPost, dbCollectionPath(orgID, projectID, instanceID), req, &out); err != nil {
		return nil, fmt.Errorf("creating database: %w", err)
	}
	return &out, nil
}

// GetDatabase returns a single database by ID (v2beta1, beta).
func (c *Client) GetDatabase(ctx context.Context, orgID, projectID, instanceID, id string) (*Database, error) {
	var out Database
	if err := c.doV2Data(ctx, http.MethodGet, dbCollectionPath(orgID, projectID, instanceID)+"/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, fmt.Errorf("getting database %q: %w", id, err)
	}
	return &out, nil
}

// ListDatabases lists an instance's databases (v2beta1, beta).
func (c *Client) ListDatabases(ctx context.Context, orgID, projectID, instanceID string) ([]Database, error) {
	var out []Database
	if err := c.doV2Data(ctx, http.MethodGet, dbCollectionPath(orgID, projectID, instanceID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing databases: %w", err)
	}
	return out, nil
}

// DeleteDatabase deletes a database (v2beta1, beta). Idempotent: a 404 is success.
func (c *Client) DeleteDatabase(ctx context.Context, orgID, projectID, instanceID, id string) error {
	if err := c.doV2Data(ctx, http.MethodDelete, dbCollectionPath(orgID, projectID, instanceID)+"/"+url.PathEscape(id), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting database %q: %w", id, err)
	}
	return nil
}

// CreateDatabaseBackup takes an on-demand backup of a database (v2beta1, beta).
func (c *Client) CreateDatabaseBackup(ctx context.Context, orgID, projectID, instanceID, databaseID string) (*DatabaseBackup, error) {
	var out DatabaseBackup
	path := dbCollectionPath(orgID, projectID, instanceID) + "/" + url.PathEscape(databaseID) + "/backups"
	if err := c.doV2Data(ctx, http.MethodPost, path, struct{}{}, &out); err != nil {
		return nil, fmt.Errorf("creating database backup: %w", err)
	}
	return &out, nil
}

// ListDatabaseBackups lists a database's backups (v2beta1, beta).
func (c *Client) ListDatabaseBackups(ctx context.Context, orgID, projectID, instanceID, databaseID string) ([]DatabaseBackup, error) {
	var out []DatabaseBackup
	path := dbCollectionPath(orgID, projectID, instanceID) + "/" + url.PathEscape(databaseID) + "/backups"
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("listing database backups: %w", err)
	}
	return out, nil
}

// GetDatabaseBackup returns a single backup by ID (v2beta1, beta).
func (c *Client) GetDatabaseBackup(ctx context.Context, orgID, projectID, instanceID, databaseID, backupID string) (*DatabaseBackup, error) {
	var out DatabaseBackup
	path := dbCollectionPath(orgID, projectID, instanceID) + "/" + url.PathEscape(databaseID) + "/backups/" + url.PathEscape(backupID)
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("getting database backup %q: %w", backupID, err)
	}
	return &out, nil
}

// RestoreDatabase restores a database from one of its backups (v2beta1, beta).
func (c *Client) RestoreDatabase(ctx context.Context, orgID, projectID, instanceID, databaseID string, req RestoreDatabaseRequest) error {
	path := dbCollectionPath(orgID, projectID, instanceID) + "/" + url.PathEscape(databaseID) + "/restore"
	if err := c.doV2Data(ctx, http.MethodPost, path, req, nil); err != nil {
		return fmt.Errorf("restoring database %q: %w", databaseID, err)
	}
	return nil
}
