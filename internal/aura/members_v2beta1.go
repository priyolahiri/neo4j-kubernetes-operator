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
//   /organizations/{org}/projects/{project}/users[/{id}]     (project members)
//   /organizations/{org}/invites  +  DELETE /invites/{id}    (email invites)
//
// Shapes below are taken from the official v2beta1 OpenAPI spec
// (https://api.neo4j.io/v2beta1/spec.json). Landmines the spec pins — respect
// them, do NOT "tidy" them back:
//
//  1. THREE DISTINCT ROLE VOCABULARIES, all lowercase-hyphenated:
//       org members     -> organization-owner | organization-admin | organization-member
//       project members -> project-admin | project-member | project-viewer |
//                          project-metrics-integration-reader
//       INVITES, for the project part -> namespace-viewer | namespace-member |
//                          namespace-admin | namespace-metrics-integration-reader
//     The invite body uses the `namespace-*` names for project roles even though
//     the project-members endpoint uses `project-*` for the same concept. There
//     is no SCREAMING_SNAKE form of any of these anywhere in the API.
//
//  2. ROLES ARE ARRAYS, not scalars, in every request and response — and both
//     PATCH bodies declare `additionalProperties: false` with the roles array
//     `required` and `minItems=maxItems=1`. A scalar `{"role": …}` body is a
//     deterministic 400: it both omits the required property and adds a
//     forbidden one.
//
//  3. The user identifier is `user_id`, not `id`.
//
//  4. There is NO `GET /organizations/{org}/invites/{id}` — the spec defines
//     only DELETE on that path. Read a single invite by listing and matching on
//     ID (see FindInvite). Do not add a Get; a 405 is not retryable and not a
//     404, so it would land in a permanent error.
//
//  5. Adding a user to a project takes `user_id` (a UUID) + `project_roles` —
//     NOT an email. Only invites take an email.
//
// Responses are `{"data": …}`-wrapped (via doV2Data), except POST project users
// which returns 201 with no body at all.
//
// v2beta1 is still beta (breaking changes allowed without a version bump), so
// re-diff against the live spec each Aura release.
// ==========================================================================

// Organization-level role values (v2beta1 `organization_roles`).
const (
	OrgRoleOwner  = "organization-owner"
	OrgRoleAdmin  = "organization-admin"
	OrgRoleMember = "organization-member"
)

// Project-level role values (v2beta1 `project_roles`, project-users endpoints).
const (
	ProjectRoleAdmin                  = "project-admin"
	ProjectRoleMember                 = "project-member"
	ProjectRoleViewer                 = "project-viewer"
	ProjectRoleMetricsIntegrationRead = "project-metrics-integration-reader"
)

// Project-level role values as spelled in the INVITE body
// (`project_invites[].project_roles`). Same concepts as the ProjectRole*
// constants above, different vocabulary — this is the API's own inconsistency.
const (
	NamespaceRoleViewer                 = "namespace-viewer"
	NamespaceRoleMember                 = "namespace-member"
	NamespaceRoleAdmin                  = "namespace-admin"
	NamespaceRoleMetricsIntegrationRead = "namespace-metrics-integration-reader"
)

// Invite status values (v2beta1 OrganizationInvite.status).
const (
	InviteStatusActive   = "active"
	InviteStatusAccepted = "accepted"
	InviteStatusRevoked  = "revoked"
	InviteStatusExpired  = "expired"
	InviteStatusDeclined = "declined"
)

// Member is an organization- or project-level user (console identity).
//
// One struct decodes both shapes: the org endpoints return `organization_roles`
// and the project endpoints return `project_roles`, so exactly one of the two
// slices is populated depending on which call produced it. Use Role() when you
// just want the effective single role.
type Member struct {
	UserID            string   `json:"user_id,omitempty"`
	Email             string   `json:"email,omitempty"`
	OrganizationRoles []string `json:"organization_roles,omitempty"`
	ProjectRoles      []string `json:"project_roles,omitempty"`
}

// Role returns the member's single effective role. The API constrains role
// arrays to exactly one entry on every write, and reads observed so far carry
// one, so the first element is the role; "" if the member carries none.
func (m Member) Role() string {
	if len(m.OrganizationRoles) > 0 {
		return m.OrganizationRoles[0]
	}
	if len(m.ProjectRoles) > 0 {
		return m.ProjectRoles[0]
	}
	return ""
}

// UpdateOrgMemberRoleRequest is the PATCH body for an org member's role.
// `organization_roles` is required with exactly one entry, and the schema sets
// additionalProperties:false — do not add fields.
type UpdateOrgMemberRoleRequest struct {
	OrganizationRoles []string `json:"organization_roles"`
}

// UpdateProjectMemberRoleRequest is the PATCH body for a project member's role.
// `project_roles` is required with exactly one entry (additionalProperties:false).
type UpdateProjectMemberRoleRequest struct {
	ProjectRoles []string `json:"project_roles"`
}

// AddProjectMemberRequest is the POST body for adding an EXISTING org user to a
// project. Both fields are required; UserID is a UUID, not an email
// (additionalProperties:false).
type AddProjectMemberRequest struct {
	UserID       string   `json:"user_id"`
	ProjectRoles []string `json:"project_roles"`
}

// ProjectInvite scopes an invite to one project, with the project roles to grant
// on acceptance. NOTE: these use the `namespace-*` vocabulary, not `project-*`.
// ProjectInvite carries the per-project roles of an invite. Note the vocabulary:
// these are `namespace-*` roles, NOT the `project-*` roles the project-members
// endpoints use — confirmed live by the API's own enum error:
// 'namespace-viewer', 'namespace-member', 'namespace-admin',
// 'namespace-metrics-integration-reader'.
//
// project_roles is required within an entry ("project_invites[0].project_roles:
// Field required"), so it carries no omitempty.
type ProjectInvite struct {
	ProjectID    string   `json:"project_id,omitempty"`
	ProjectRoles []string `json:"project_roles"`
}

// Invite is an email invitation to an organization, optionally carrying
// per-project roles (v2beta1 OrganizationInvite).
type Invite struct {
	ID                string          `json:"id,omitempty"`
	Email             string          `json:"email,omitempty"`
	OrganizationID    string          `json:"organization_id,omitempty"`
	OrganizationRoles []string        `json:"organization_roles,omitempty"`
	ProjectInvites    []ProjectInvite `json:"project_invites,omitempty"`
	Status            string          `json:"status,omitempty"`
	ExpiresAt         string          `json:"expires_at,omitempty"`
	InvitedBy         string          `json:"invited_by,omitempty"`
}

// CreateInviteRequest invites an email to an organization. Roles carries
// organization-level roles; ProjectInvites carries per-project roles using the
// `namespace-*` vocabulary.
// CreateInviteRequest is the invite body. Verified live 2026-08-01; the tags
// here are load-bearing and the omitempty that used to be on both slices made
// EVERY invite the operator could build a 400:
//
//   - `roles` must be PRESENT and carry AT LEAST ONE organization role. An empty
//     array is rejected ("List should have at least 1 item after validation"),
//     and null is rejected ("got null, want array"). So there is no such thing as
//     a project-only invite: every invitee gets an organization role.
//   - `project_invites` must be PRESENT but MAY be empty — `[]` is accepted, and
//     that is the normal shape for an organization-only invite. Omitting it is
//     "Field required"; null is rejected.
//
// Both fields therefore carry no omitempty, and CreateInvite normalises a nil
// ProjectInvites to `[]` so it can never marshal as null.
type CreateInviteRequest struct {
	Email          string          `json:"email"`
	Roles          []string        `json:"roles"`
	ProjectInvites []ProjectInvite `json:"project_invites"`
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
	body := UpdateOrgMemberRoleRequest{OrganizationRoles: []string{role}}
	if err := c.doV2Data(ctx, http.MethodPatch, orgUsersPath(orgID)+"/"+url.PathEscape(userID), body, &out); err != nil {
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

// AddProjectMember adds an existing organization user to a project with a role
// (v2beta1, beta). userID is the Aura user UUID — resolve it from an email via
// ListOrgMembers first. Returns no body (201, empty), per the spec.
func (c *Client) AddProjectMember(ctx context.Context, orgID, projectID, userID, role string) error {
	body := AddProjectMemberRequest{UserID: userID, ProjectRoles: []string{role}}
	if err := c.doV2Data(ctx, http.MethodPost, projectUsersPath(orgID, projectID), body, nil); err != nil {
		return fmt.Errorf("adding user %q to project %q: %w", userID, projectID, err)
	}
	return nil
}

// UpdateProjectMemberRole changes a project member's role (v2beta1, beta).
func (c *Client) UpdateProjectMemberRole(ctx context.Context, orgID, projectID, userID, role string) (*Member, error) {
	var out Member
	body := UpdateProjectMemberRoleRequest{ProjectRoles: []string{role}}
	if err := c.doV2Data(ctx, http.MethodPatch, projectUsersPath(orgID, projectID)+"/"+url.PathEscape(userID), body, &out); err != nil {
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

// CreateInvite invites an email to an organization (v2beta1, beta).
func (c *Client) CreateInvite(ctx context.Context, orgID string, req CreateInviteRequest) (*Invite, error) {
	// Aura requires at least one ORGANIZATION role on every invite; a
	// project-only invite is not expressible. Say so here rather than letting the
	// API answer "List should have at least 1 item after validation, not 0",
	// which never mentions that an organization role is the thing missing.
	if len(req.Roles) == 0 {
		return nil, fmt.Errorf("creating invite for %q: Aura requires at least one organization role on every "+
			"invite (organization-owner, organization-admin or organization-member) — a project-only invite is "+
			"not possible", req.Email)
	}
	// Never marshal as null: the API rejects null for both slices.
	if req.ProjectInvites == nil {
		req.ProjectInvites = []ProjectInvite{}
	}
	var out Invite
	if err := c.doV2Data(ctx, http.MethodPost, orgInvitesPath(orgID), req, &out); err != nil {
		return nil, fmt.Errorf("creating invite: %w", err)
	}
	return &out, nil
}

// ListInvites lists an organization's invites (v2beta1, beta).
func (c *Client) ListInvites(ctx context.Context, orgID string) ([]Invite, error) {
	var out []Invite
	if err := c.doV2Data(ctx, http.MethodGet, orgInvitesPath(orgID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing invites: %w", err)
	}
	return out, nil
}

// FindInvite returns the invite with the given ID, or nil if it is not present.
//
// There is deliberately no GetInvite: the spec defines only DELETE on
// /invites/{id}, so a single-invite read must go through the list endpoint.
func (c *Client) FindInvite(ctx context.Context, orgID, id string) (*Invite, error) {
	invites, err := c.ListInvites(ctx, orgID)
	if err != nil {
		return nil, err
	}
	for i := range invites {
		if invites[i].ID == id {
			return &invites[i], nil
		}
	}
	return nil, nil
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
