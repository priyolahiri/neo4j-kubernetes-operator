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
	"net/http"
	"net/url"
)

// ==========================================================================
// BETA — Aura API v2beta1 console RBAC (organization / project members + invites).
//
// This is Aura PLATFORM identity (who can access the console / manage the
// project) — NOT in-database Neo4j users. Endpoints:
//   /organizations/{org}/users[/{id}]                       (org members)
//   /organizations/{org}/projects/{project}/users[/{id}]    (project members)
//   /organizations/{org}/invites[/{id}]                     (email invites)
//
// Role values (verified against the Aura docs):
//   org:     ORG_OWNER | ORG_ADMIN | ORG_MEMBER
//   project: PROJECT_ADMIN | PROJECT_MEMBER | PROJECT_VIEWER | METRICS_READER
//
// Data-wrapped ({"data": …}, via doV2Data). Membership is granted by inviting an
// email; an existing member's role is changed with PATCH. The PATCH/invite
// request bodies are not fully schema'd upstream — BETA/best-effort.
// ==========================================================================

// Org-level role values.
const (
	OrgRoleOwner  = "ORG_OWNER"
	OrgRoleAdmin  = "ORG_ADMIN"
	OrgRoleMember = "ORG_MEMBER"
)

// Project-level role values.
const (
	ProjectRoleAdmin         = "PROJECT_ADMIN"
	ProjectRoleMember        = "PROJECT_MEMBER"
	ProjectRoleViewer        = "PROJECT_VIEWER"
	ProjectRoleMetricsReader = "METRICS_READER"
)

// Member is an organization- or project-level user (console identity).
type Member struct {
	ID    string `json:"id,omitempty"`
	Email string `json:"email,omitempty"`
	Role  string `json:"role,omitempty"`
}

// UpdateMemberRoleRequest changes a member's role (best-effort body).
type UpdateMemberRoleRequest struct {
	Role string `json:"role"`
}

// Invite is a pending email invitation to an organization or project.
type Invite struct {
	ID        string `json:"id,omitempty"`
	Email     string `json:"email,omitempty"`
	Role      string `json:"role,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
	Status    string `json:"legacy_status,omitempty"`
}

// CreateInviteRequest invites an email to an organization (optionally scoped to a
// project via ProjectID) with a role (best-effort body).
type CreateInviteRequest struct {
	Email     string `json:"email"`
	Role      string `json:"role"`
	ProjectID string `json:"project_id,omitempty"`
}

func orgUsersPath(orgID string) string {
	return "/organizations/" + url.PathEscape(orgID) + "/users"
}

func projectUsersPath(orgID, projectID string) string {
	return "/organizations/" + url.PathEscape(orgID) + "/projects/" + url.PathEscape(projectID) + "/users"
}

func orgInvitesPath(orgID string) string {
	return "/organizations/" + url.PathEscape(orgID) + "/invites"
}

// ---- Organization members -------------------------------------------------

// ListOrgMembers lists an organization's members (v2beta1, beta).
func (c *Client) ListOrgMembers(ctx context.Context, orgID string) ([]Member, error) {
	var out []Member
	if err := c.doV2Data(ctx, http.MethodGet, orgUsersPath(orgID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing org members: %w", err)
	}
	return out, nil
}

// UpdateOrgMemberRole changes an org member's role (v2beta1, beta).
func (c *Client) UpdateOrgMemberRole(ctx context.Context, orgID, userID, role string) (*Member, error) {
	var out Member
	if err := c.doV2Data(ctx, http.MethodPatch, orgUsersPath(orgID)+"/"+url.PathEscape(userID), UpdateMemberRoleRequest{Role: role}, &out); err != nil {
		return nil, fmt.Errorf("updating org member %q: %w", userID, err)
	}
	return &out, nil
}

// DeleteOrgMember removes a member from an organization (v2beta1, beta).
// Idempotent: a 404 is success.
func (c *Client) DeleteOrgMember(ctx context.Context, orgID, userID string) error {
	if err := c.doV2Data(ctx, http.MethodDelete, orgUsersPath(orgID)+"/"+url.PathEscape(userID), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting org member %q: %w", userID, err)
	}
	return nil
}

// ---- Project members ------------------------------------------------------

// ListProjectMembers lists a project's members (v2beta1, beta).
func (c *Client) ListProjectMembers(ctx context.Context, orgID, projectID string) ([]Member, error) {
	var out []Member
	if err := c.doV2Data(ctx, http.MethodGet, projectUsersPath(orgID, projectID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing project members: %w", err)
	}
	return out, nil
}

// UpdateProjectMemberRole changes a project member's role (v2beta1, beta).
func (c *Client) UpdateProjectMemberRole(ctx context.Context, orgID, projectID, userID, role string) (*Member, error) {
	var out Member
	if err := c.doV2Data(ctx, http.MethodPatch, projectUsersPath(orgID, projectID)+"/"+url.PathEscape(userID), UpdateMemberRoleRequest{Role: role}, &out); err != nil {
		return nil, fmt.Errorf("updating project member %q: %w", userID, err)
	}
	return &out, nil
}

// DeleteProjectMember removes a member from a project (v2beta1, beta).
// Idempotent: a 404 is success.
func (c *Client) DeleteProjectMember(ctx context.Context, orgID, projectID, userID string) error {
	if err := c.doV2Data(ctx, http.MethodDelete, projectUsersPath(orgID, projectID)+"/"+url.PathEscape(userID), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting project member %q: %w", userID, err)
	}
	return nil
}

// ---- Invites --------------------------------------------------------------

// CreateInvite invites an email to an organization (or project, if ProjectID is
// set) with a role (v2beta1, beta).
func (c *Client) CreateInvite(ctx context.Context, orgID string, req CreateInviteRequest) (*Invite, error) {
	var out Invite
	if err := c.doV2Data(ctx, http.MethodPost, orgInvitesPath(orgID), req, &out); err != nil {
		return nil, fmt.Errorf("creating invite: %w", err)
	}
	return &out, nil
}

// GetInvite returns a single invite by ID (v2beta1, beta).
func (c *Client) GetInvite(ctx context.Context, orgID, id string) (*Invite, error) {
	var out Invite
	if err := c.doV2Data(ctx, http.MethodGet, orgInvitesPath(orgID)+"/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, fmt.Errorf("getting invite %q: %w", id, err)
	}
	return &out, nil
}

// ListInvites lists an organization's pending invites (v2beta1, beta).
func (c *Client) ListInvites(ctx context.Context, orgID string) ([]Invite, error) {
	var out []Invite
	if err := c.doV2Data(ctx, http.MethodGet, orgInvitesPath(orgID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing invites: %w", err)
	}
	return out, nil
}

// DeleteInvite revokes a pending invite (v2beta1, beta). Idempotent: 404 is success.
func (c *Client) DeleteInvite(ctx context.Context, orgID, id string) error {
	if err := c.doV2Data(ctx, http.MethodDelete, orgInvitesPath(orgID)+"/"+url.PathEscape(id), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting invite %q: %w", id, err)
	}
	return nil
}
