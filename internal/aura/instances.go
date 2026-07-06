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

// ListInstances returns a summary of every instance the credential can see,
// optionally filtered to a single tenant/project. Pass an empty tenantID to
// list across all tenants.
func (c *Client) ListInstances(ctx context.Context, tenantID string) ([]InstanceSummary, error) {
	path := "/instances"
	if tenantID != "" {
		path += "?tenantId=" + url.QueryEscape(tenantID)
	}
	var env dataEnvelope[[]InstanceSummary]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("listing instances: %w", err)
	}
	return env.Data, nil
}

// GetInstance returns full detail for a single instance.
func (c *Client) GetInstance(ctx context.Context, id string) (*Instance, error) {
	path := "/instances/" + url.PathEscape(id)
	var env dataEnvelope[Instance]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("getting instance %q: %w", id, err)
	}
	return &env.Data, nil
}

// CreateInstance starts asynchronous creation of an instance. The 202 response
// carries the one-time initial credentials — the caller must persist them
// immediately (they are never returned again). Creation is async: poll
// GetInstance until the status becomes running.
func (c *Client) CreateInstance(ctx context.Context, req CreateInstanceRequest) (*CreateInstanceResponse, error) {
	var env dataEnvelope[CreateInstanceResponse]
	if err := c.doJSON(ctx, "POST", "/instances", req, &env); err != nil {
		return nil, fmt.Errorf("creating instance: %w", err)
	}
	return &env.Data, nil
}

// PatchInstance edits an instance's configuration (rename, resize, secondary
// count, CDC mode, vector optimization, graph analytics plugin). Only fields set
// in req are sent. The change is async; the returned Instance reflects the
// accepted request (typically with status "updating"). A 202 with an empty body
// yields a zero-valued Instance.
func (c *Client) PatchInstance(ctx context.Context, id string, req PatchInstanceRequest) (*Instance, error) {
	path := "/instances/" + url.PathEscape(id)
	var env dataEnvelope[Instance]
	if err := c.doJSON(ctx, "PATCH", path, req, &env); err != nil {
		return nil, fmt.Errorf("patching instance %q: %w", id, err)
	}
	return &env.Data, nil
}

// PauseInstance starts the async pause of an instance. Poll GetInstance until
// the status becomes paused.
func (c *Client) PauseInstance(ctx context.Context, id string) error {
	path := "/instances/" + url.PathEscape(id) + "/pause"
	if err := c.doJSON(ctx, "POST", path, struct{}{}, nil); err != nil {
		return fmt.Errorf("pausing instance %q: %w", id, err)
	}
	return nil
}

// ResumeInstance starts the async resume of a paused instance. Poll GetInstance
// until the status becomes running.
func (c *Client) ResumeInstance(ctx context.Context, id string) error {
	path := "/instances/" + url.PathEscape(id) + "/resume"
	if err := c.doJSON(ctx, "POST", path, struct{}{}, nil); err != nil {
		return fmt.Errorf("resuming instance %q: %w", id, err)
	}
	return nil
}

// DeleteInstance starts the async deletion of an instance. Deletion is
// idempotent: a 404 (the instance is already gone) is treated as success, per
// the Aura API guidance to treat both 202 and 404 as completion.
func (c *Client) DeleteInstance(ctx context.Context, id string) error {
	path := "/instances/" + url.PathEscape(id)
	if err := c.doJSON(ctx, "DELETE", path, nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting instance %q: %w", id, err)
	}
	return nil
}

// GetTenant returns detail for a tenant/project, including the
// InstanceConfigurations oracle of valid region/type/memory/storage/version
// combinations for that project.
func (c *Client) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	path := "/tenants/" + url.PathEscape(id)
	var env dataEnvelope[Tenant]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("getting tenant %q: %w", id, err)
	}
	return &env.Data, nil
}
