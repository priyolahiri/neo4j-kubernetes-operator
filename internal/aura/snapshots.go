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
	"fmt"
	"net/url"
)

// snapshotCreateResponse is the 202 body of POST /instances/{id}/snapshots.
type snapshotCreateResponse struct {
	SnapshotID string `json:"snapshot_id"`
}

// CreateSnapshot starts an on-demand snapshot of an instance. Creation is async;
// the returned Snapshot carries only the new snapshot's ID (and instance ID) —
// poll GetSnapshot for its status.
func (c *Client) CreateSnapshot(ctx context.Context, instanceID string) (*Snapshot, error) {
	path := "/instances/" + url.PathEscape(instanceID) + "/snapshots"
	var env dataEnvelope[snapshotCreateResponse]
	if err := c.doJSON(ctx, "POST", path, struct{}{}, &env); err != nil {
		return nil, fmt.Errorf("creating snapshot for instance %q: %w", instanceID, err)
	}
	return &Snapshot{
		InstanceID: instanceID,
		SnapshotID: env.Data.SnapshotID,
	}, nil
}

// GetSnapshot returns detail for a single snapshot.
func (c *Client) GetSnapshot(ctx context.Context, instanceID, snapshotID string) (*Snapshot, error) {
	path := "/instances/" + url.PathEscape(instanceID) + "/snapshots/" + url.PathEscape(snapshotID)
	var env dataEnvelope[Snapshot]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("getting snapshot %q for instance %q: %w", snapshotID, instanceID, err)
	}
	return &env.Data, nil
}

// ListSnapshots returns the snapshots available for an instance on a given date
// (ISO YYYY-MM-DD). Pass an empty date to list snapshots for the current day.
func (c *Client) ListSnapshots(ctx context.Context, instanceID, date string) ([]Snapshot, error) {
	path := "/instances/" + url.PathEscape(instanceID) + "/snapshots"
	if date != "" {
		path += "?date=" + url.QueryEscape(date)
	}
	var env dataEnvelope[[]Snapshot]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("listing snapshots for instance %q: %w", instanceID, err)
	}
	return env.Data, nil
}

// RestoreSnapshot starts the async restore of an instance from a snapshot. Poll
// GetInstance until the status returns to running.
func (c *Client) RestoreSnapshot(ctx context.Context, instanceID, snapshotID string) error {
	path := "/instances/" + url.PathEscape(instanceID) + "/snapshots/" + url.PathEscape(snapshotID) + "/restore"
	if err := c.doJSON(ctx, "POST", path, struct{}{}, nil); err != nil {
		return fmt.Errorf("restoring instance %q from snapshot %q: %w", instanceID, snapshotID, err)
	}
	return nil
}
