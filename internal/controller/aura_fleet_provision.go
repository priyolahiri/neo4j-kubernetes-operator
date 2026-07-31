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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ==========================================================================
// Aura Fleet Manager provisioning — "Phase 0" of the fleet integration.
//
// The fleet integration has three strictly ordered phases, and they must NOT be
// collapsed (see CLAUDE.md "Aura Fleet Management"):
//
//	Phase 0 (here, only when spec.provision is set)
//	    Register a Fleet Manager deployment via the Aura API and mint its token
//	    into a Secret. Runs regardless of cluster readiness — nothing about it
//	    needs a live DBMS.
//	Phase 1 (existing) merge "fleet-management" into NEO4J_PLUGINS, every reconcile.
//	Phase 2 (existing) once Ready, read the token Secret and register it over
//	    Bolt with CALL fleetManagement.registerToken($token).
//
// Phase 0 is deliberately NON-FATAL. The Aura API being unreachable must never
// wedge a cluster reconcile, so every failure path records a message on
// status.auraFleetManagement and returns nil.
// ==========================================================================

// AuraFleetDeploymentAnnotation pins the Aura-assigned deployment ID to the CR.
//
// This — not status — is the authoritative idempotency guard: it is written
// immediately after CreateDeployment and BEFORE a token is minted, so a crash
// in between re-observes an existing deployment instead of registering a second
// one. Same contract as the neo4j.com/external-*-id annotations on the Aura CRDs.
const AuraFleetDeploymentAnnotation = "neo4j.com/external-fleet-deployment-id"

// auraFleetTokenSecretKey is the key the minted token is written under.
const auraFleetTokenSecretKey = "token"

// auraRefusalError marks a decision the operator made DELIBERATELY — a policy
// refusal or an unrecoverable state — as opposed to an API or transport failure.
// Shared by every Aura controller, not just the fleet one.
//
// This distinction is load-bearing: aura.IsTransient() returns TRUE for any
// error that is not an *aura.APIError, on the reasonable assumption that such
// errors are transport-level blips worth retrying. A deliberate refusal is not
// an APIError either, so without this marker it would be misread as transient,
// silently retried forever, and its explanation never shown to the user.
type auraRefusalError struct{ msg string }

func (e *auraRefusalError) Error() string { return e.msg }

func refusef(format string, a ...any) error {
	return &auraRefusalError{msg: fmt.Sprintf(format, a...)}
}

func isAuraRefusal(err error) bool {
	var r *auraRefusalError
	return errors.As(err, &r)
}

// maxFleetTelemetryItems bounds the per-CR telemetry lists so a large fleet
// cannot bloat the CR. The true totals are reported separately.
const maxFleetTelemetryItems = 20

// auraFleetAPI is the subset of the Aura v2beta1 Fleet Manager client this helper
// needs. *aura.Client satisfies it. BETA — see internal/aura/fleet_v2beta1.go.
type auraFleetAPI interface {
	ListDeployments(ctx context.Context, orgID, projectID string) ([]aura.Deployment, error)
	GetDeployment(ctx context.Context, orgID, projectID, deploymentID string) (*aura.DetailedDeployment, error)
	CreateDeployment(ctx context.Context, orgID, projectID, name string) (string, error)
	DeleteDeployment(ctx context.Context, orgID, projectID, deploymentID string) error
	CreateDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) (string, error)
	RotateDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) (string, error)
	DeleteDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) error
	ListDeploymentServers(ctx context.Context, orgID, projectID, deploymentID string) ([]aura.FleetServer, error)
	// ListServerDatabases is per-SERVER on purpose: role / writer /
	// last_committed_txn / replication_lag / graph+property shards exist only on
	// that endpoint, not on the deployment-level databases list.
	ListServerDatabases(ctx context.Context, orgID, projectID, deploymentID, serverID string) ([]aura.FleetServerDatabase, error)
}

// auraFleetClientFactory builds an auraFleetAPI from resolved credentials.
type auraFleetClientFactory func(auraCredentials) auraFleetAPI

func defaultAuraFleetClientFactory(c auraCredentials) auraFleetAPI { return auraClientForCreds(c) }

func resolveFleetClient(factory auraFleetClientFactory, c auraCredentials) auraFleetAPI {
	if factory != nil {
		return factory(c)
	}
	return defaultAuraFleetClientFactory(c)
}

// fleetProvisionTarget is what a Cluster or Standalone CR must offer for the
// shared provisioning logic to work on it. Both implement it via the accessors
// in api/v1beta1/fleet_target.go.
type fleetProvisionTarget interface {
	client.Object
	GetFleetSpec() *neo4jv1beta1.AuraFleetManagementSpec
	GetFleetStatus() *neo4jv1beta1.AuraFleetManagementStatus
	SetFleetStatus(*neo4jv1beta1.AuraFleetManagementStatus)
}

// fleetProvisioner carries the collaborators Phase 0 needs, so the logic itself
// stays free of any particular reconciler type.
type fleetProvisioner struct {
	Client        client.Client
	Recorder      record.EventRecorder
	ClientFactory auraFleetClientFactory
	// StatusWriter persists a mutated status.auraFleetManagement. Each controller
	// supplies its own because the CR types differ.
	StatusWriter func(ctx context.Context, key types.NamespacedName, mutate func(*neo4jv1beta1.AuraFleetManagementStatus)) error
}

// fleetDeploymentName is the name to register in Aura.
//
// Defaults to "<namespace>-<name>" truncated to 30 characters: the API rejects
// names longer than that, and including the namespace stops two same-named
// clusters in different namespaces from colliding inside one Aura project.
func fleetDeploymentName(p *neo4jv1beta1.AuraFleetProvisionSpec, namespace, name string) string {
	if p.DeploymentName != "" {
		return p.DeploymentName
	}
	n := namespace + "-" + name
	if len(n) > 30 {
		n = n[:30]
	}
	return n
}

// fleetTokenSecretName is the Secret the minted token lands in.
func fleetTokenSecretName(p *neo4jv1beta1.AuraFleetProvisionSpec, name string) string {
	if p.TokenSecretName != "" {
		return p.TokenSecretName
	}
	return name + "-aura-fleet-token"
}

// ResolveFleetTokenSource returns the Secret name + key the registration phase
// must read, for either way a token can arrive: supplied by the user via
// spec.tokenSecretRef, or minted by Phase 0 into the provisioned Secret.
//
// This exists because the two are MUTUALLY EXCLUSIVE by CEL, and Phase 2
// originally keyed only off tokenSecretRef — so a provisioned cluster minted a
// token, wrote it to a Secret, and then never registered it, because
// tokenSecretRef was (necessarily) nil. Provisioning that stops one step short of
// registration defeats the entire point of the feature: closing the manual
// console-wizard step.
//
// ok is false when the feature cannot register yet (neither source configured).
func ResolveFleetTokenSource(spec *neo4jv1beta1.AuraFleetManagementSpec, objName string) (name, key string, ok bool) {
	if spec == nil {
		return "", "", false
	}
	if spec.TokenSecretRef != nil {
		key = spec.TokenSecretRef.Key
		if key == "" {
			key = auraFleetTokenSecretKey
		}
		return spec.TokenSecretRef.Name, key, true
	}
	if spec.Provision != nil {
		// Phase 0 always writes under auraFleetTokenSecretKey.
		return fleetTokenSecretName(spec.Provision, objName), auraFleetTokenSecretKey, true
	}
	return "", "", false
}

// reconcileAuraFleetProvision runs Phase 0. It is a no-op unless fleet
// management is enabled AND spec.provision is set.
//
// Always returns nil: provisioning problems are reported through
// status.auraFleetManagement.message, never by failing the caller's reconcile.
func (fp *fleetProvisioner) reconcileAuraFleetProvision(ctx context.Context, obj fleetProvisionTarget) error {
	logger := log.FromContext(ctx)
	spec := obj.GetFleetSpec()
	if spec == nil || !spec.Enabled || spec.Provision == nil {
		return nil
	}
	p := spec.Provision
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}

	creds, orgID, projectID, err := fp.resolveFleetTarget(ctx, obj, p)
	if err != nil {
		logger.Info("Aura fleet provisioning deferred: cannot resolve target", "reason", err.Error())
		return fp.note(ctx, key, fmt.Sprintf("fleet provisioning blocked: %v", err))
	}
	apiClient := resolveFleetClient(fp.ClientFactory, creds)

	// --- deployment ---------------------------------------------------------
	deploymentID, err := fp.ensureDeployment(ctx, obj, p, apiClient, orgID, projectID)
	if err != nil {
		// Refusals are checked BEFORE IsTransient — see the auraRefusalError doc.
		if !isAuraRefusal(err) && aura.IsTransient(err) {
			logger.Info("Aura fleet deployment lookup transient failure; will retry")
			return nil
		}
		return fp.note(ctx, key, fmt.Sprintf("cannot ensure fleet deployment: %v", err))
	}
	if deploymentID == "" {
		// No deployment and managementPolicies forbids Create.
		return fp.note(ctx, key, "no fleet deployment found and managementPolicies does not permit Create")
	}

	// --- token --------------------------------------------------------------
	if err := fp.ensureToken(ctx, obj, p, apiClient, orgID, projectID, deploymentID); err != nil {
		// Refusals are checked BEFORE IsTransient — see the auraRefusalError doc.
		if !isAuraRefusal(err) && aura.IsTransient(err) {
			return nil
		}
		return fp.note(ctx, key, err.Error())
	}

	// --- observed token metadata + optional telemetry ------------------------
	fp.syncObserved(ctx, obj, p, apiClient, orgID, projectID, deploymentID)
	return nil
}

// resolveFleetTarget resolves credentials plus the org and project the
// deployment lives in, falling back to the AuraProviderConfig defaults.
func (fp *fleetProvisioner) resolveFleetTarget(
	ctx context.Context, obj fleetProvisionTarget, p *neo4jv1beta1.AuraFleetProvisionSpec,
) (auraCredentials, string, string, error) {
	creds, err := resolveAuraCredentials(ctx, fp.Client, obj.GetNamespace(), p.ProviderConfigRef, p.CredentialsSecretRef)
	if err != nil {
		return creds, "", "", err
	}
	orgID := resolveProviderOrgID(ctx, fp.Client, obj.GetNamespace(), p.ProviderConfigRef, p.OrganizationID)
	if orgID == "" {
		return creds, "", "", fmt.Errorf("organizationId is required (set spec.auraFleetManagement.provision.organizationId " +
			"or defaultOrganizationId on the AuraProviderConfig)")
	}
	projectID := p.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	if projectID == "" {
		return creds, orgID, "", fmt.Errorf("projectId is required (set spec.auraFleetManagement.provision.projectId " +
			"or defaultProjectId on the AuraProviderConfig)")
	}
	return creds, orgID, projectID, nil
}

// ensureDeployment resolves the deployment ID, creating it if necessary.
//
// Resolution order — annotation, then name match, then create — is what makes
// this idempotent. The annotation is persisted IMMEDIATELY after a create and
// before any token is minted.
func (fp *fleetProvisioner) ensureDeployment(
	ctx context.Context, obj fleetProvisionTarget, p *neo4jv1beta1.AuraFleetProvisionSpec,
	apiClient auraFleetAPI, orgID, projectID string,
) (string, error) {
	if id := obj.GetAnnotations()[AuraFleetDeploymentAnnotation]; id != "" {
		return id, nil
	}

	name := fleetDeploymentName(p, obj.GetNamespace(), obj.GetName())
	existing, err := apiClient.ListDeployments(ctx, orgID, projectID)
	if err != nil {
		return "", fmt.Errorf("listing fleet deployments: %w", err)
	}
	for i := range existing {
		if existing[i].Name == name {
			if err := fp.setDeploymentAnnotation(ctx, obj, existing[i].ID); err != nil {
				return "", err
			}
			fp.Recorder.Eventf(obj, corev1.EventTypeNormal, EventReasonAuraFleetDeploymentAdopted,
				"Adopted existing Aura fleet deployment %q (%s)", name, existing[i].ID)
			return existing[i].ID, nil
		}
	}

	if !managementAllows(p.ManagementPolicies, auraPolicyCreate) {
		return "", nil
	}

	id, err := apiClient.CreateDeployment(ctx, orgID, projectID, name)
	if err != nil {
		return "", fmt.Errorf("creating fleet deployment %q: %w", name, err)
	}
	// Persist BEFORE minting a token — see the annotation's doc comment.
	if err := fp.setDeploymentAnnotation(ctx, obj, id); err != nil {
		return "", err
	}
	fp.Recorder.Eventf(obj, corev1.EventTypeNormal, EventReasonAuraFleetDeploymentCreated,
		"Registered Aura fleet deployment %q (%s)", name, id)
	return id, nil
}

// ensureToken makes sure the token Secret exists and is non-empty.
//
// The central hazard: a minted token is returned exactly ONCE and can never be
// read back. So "the deployment has a token but we have no Secret" is only
// recoverable by rotating — which invalidates whatever the DBMS already claimed.
// Under the default CreateIfMissing policy we therefore refuse to rotate a token
// that has already been registered successfully, and say so, rather than
// silently breaking a working deployment.
//
// HOW WE DECIDE create-vs-rotate, and why it is NOT what you would expect:
//
// We cannot ask Aura whether a token exists. GET deployment reports
// `token: null` for a deployment whose token has been minted but not yet CLAIMED
// by a running DBMS — indistinguishable from having no token at all (verified
// live 2026-07-31). And POST/PATCH are strictly complementary, each returning
// HTTP 500 in the state the other one handles.
//
// So we PROBE with POST, which is the non-destructive half, and treat its
// failure as evidence that a token already exists. Only then does policy decide
// whether to rotate. Note the registered check uses the operator's OWN status,
// not anything read back from Aura — that is the only trustworthy signal here.
func (fp *fleetProvisioner) ensureToken(
	ctx context.Context, obj fleetProvisionTarget, p *neo4jv1beta1.AuraFleetProvisionSpec,
	apiClient auraFleetAPI, orgID, projectID, deploymentID string,
) error {
	secretName := fleetTokenSecretName(p, obj.GetName())
	have, err := fp.readTokenSecret(ctx, obj.GetNamespace(), secretName)
	if err != nil {
		return fmt.Errorf("reading token Secret %q: %w", secretName, err)
	}
	if have != "" {
		return nil // already provisioned
	}

	if !managementAllows(p.ManagementPolicies, auraPolicyCreate) {
		return refusef("fleet deployment %q needs a token but managementPolicies does not permit Create", deploymentID)
	}

	// Probe: POST succeeds only when no token exists yet (the first-time case).
	token, postErr := apiClient.CreateDeploymentToken(ctx, orgID, projectID, deploymentID)
	if postErr == nil && strings.TrimSpace(token) != "" {
		return fp.storeToken(ctx, obj, secretName, token)
	}

	// POST failed, so a token almost certainly already exists — Aura signals this
	// with a 500 rather than a conflict, so we cannot distinguish it from a real
	// outage. Both paths below are safe: a genuine outage makes the PATCH fail
	// too and we simply requeue.
	st := obj.GetFleetStatus()
	if p.TokenPolicy != "Rotate" && st != nil && st.Registered {
		return refusef("token Secret %q is missing but fleet deployment %s appears to already hold a token "+
			"that has been registered, and the Aura API will not return it again; restore the Secret, or set "+
			"spec.auraFleetManagement.provision.tokenPolicy=Rotate to mint a replacement "+
			"(this invalidates the current registration). Underlying error: %v", secretName, deploymentID, postErr)
	}

	// Either the user opted into rotation, or nothing was ever registered so
	// there is no working state to protect.
	//
	// Clear `registered` FIRST, and persist it. Rotation invalidates whatever the
	// DBMS registered, so leaving the flag set makes Phase 2 short-circuit on it
	// and never register the replacement — the deployment would be left with a
	// valid token that nothing has claimed. The write happens before the PATCH on
	// purpose: if it fails we must NOT rotate, because the new token is returned
	// exactly once and would be stranded behind a stale flag.
	if st != nil && st.Registered {
		key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
		if err := fp.StatusWriter(ctx, key, func(fs *neo4jv1beta1.AuraFleetManagementStatus) {
			fs.Registered = false
			fs.Message = "token rotation in progress; the previous registration is being replaced"
		}); err != nil {
			return fmt.Errorf("clearing registered before rotating the token for fleet deployment %q: %w", deploymentID, err)
		}
		// Mirror onto the in-memory object so Phase 2 in THIS reconcile sees it.
		st.Registered = false
	}

	token, err = apiClient.RotateDeploymentToken(ctx, orgID, projectID, deploymentID)
	if err != nil {
		// Both legs are wrapped: POST and PATCH are complementary (landmine 3), so
		// which one failed and how is the whole diagnosis — and callers classify on
		// the API error, which would be lost if either leg were flattened to text.
		return fmt.Errorf("minting token for fleet deployment %q failed (post: %w; rotate: %w)", deploymentID, postErr, err)
	}
	if p.TokenPolicy == "Rotate" {
		fp.Recorder.Eventf(obj, corev1.EventTypeWarning, EventReasonAuraFleetTokenRotated,
			"Rotated the Aura fleet token for deployment %s; any previous registration is now invalid", deploymentID)
	}
	return fp.storeToken(ctx, obj, secretName, token)
}

// storeToken persists a freshly minted token. Called immediately after minting —
// the value is unrecoverable if dropped.
func (fp *fleetProvisioner) storeToken(ctx context.Context, obj fleetProvisionTarget, secretName, token string) error {
	if strings.TrimSpace(token) == "" {
		return refusef("Aura returned an empty token for Secret %q", secretName)
	}
	if err := fp.writeTokenSecret(ctx, obj, secretName, token); err != nil {
		return fmt.Errorf("storing minted token in Secret %q: %w", secretName, err)
	}
	fp.Recorder.Eventf(obj, corev1.EventTypeNormal, EventReasonAuraFleetTokenProvisioned,
		"Stored Aura fleet registration token in Secret %q", secretName)
	return nil
}

// syncObserved records the deployment/token metadata and, when requested,
// Aura's own view of the servers and databases.
//
// Strictly non-fatal, mirroring CollectDiagnostics: any failure lands in
// status.auraFleetManagement.telemetryError and is otherwise ignored.
func (fp *fleetProvisioner) syncObserved(
	ctx context.Context, obj fleetProvisionTarget, p *neo4jv1beta1.AuraFleetProvisionSpec,
	apiClient auraFleetAPI, orgID, projectID, deploymentID string,
) {
	logger := log.FromContext(ctx)
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	name := fleetDeploymentName(p, obj.GetNamespace(), obj.GetName())
	secretName := fleetTokenSecretName(p, obj.GetName())

	detail, derr := apiClient.GetDeployment(ctx, orgID, projectID, deploymentID)

	var servers []aura.FleetServer
	var databases []aura.FleetServerDatabase
	var telemetryErr string
	if p.CollectTelemetry {
		var serr error
		servers, serr = apiClient.ListDeploymentServers(ctx, orgID, projectID, deploymentID)
		if serr != nil {
			telemetryErr = fmt.Sprintf("listing fleet servers: %v", serr)
		} else {
			// Database detail is per-server, so this is an N+1 by necessity. Bound
			// it by the same cap as the reported lists so a large fleet cannot turn
			// one reconcile into hundreds of API calls.
			for i := range servers {
				if i >= maxFleetTelemetryItems {
					break
				}
				dbs, dberr := apiClient.ListServerDatabases(ctx, orgID, projectID, deploymentID, servers[i].ID)
				if dberr != nil {
					telemetryErr = fmt.Sprintf("listing databases for server %q: %v", servers[i].Name, dberr)
					break
				}
				// Stamp the reporting server: several servers report the same
				// database with different roles, so a bare name would be ambiguous.
				for j := range dbs {
					if dbs[j].ServerID == "" {
						dbs[j].ServerID = servers[i].ID
					}
					databases = append(databases, dbs[j])
				}
			}
		}
		if telemetryErr != "" {
			logger.V(1).Info("Aura fleet telemetry collection failed (non-fatal)", "error", telemetryErr)
		}
	}

	err := fp.StatusWriter(ctx, key, func(st *neo4jv1beta1.AuraFleetManagementStatus) {
		st.DeploymentID = deploymentID
		st.DeploymentName = name
		st.TokenSecretName = secretName
		st.Provisioned = true
		// Token metadata only exists once a running DBMS has CLAIMED the token:
		// until then Aura reports `token: null` even though a token was minted
		// (verified live 2026-07-31). So these stay empty between provisioning and
		// the DBMS's first registration — that is expected, not a failure, and is
		// exactly why token presence must NOT drive the mint decision.
		if derr == nil && detail != nil && detail.Token != nil {
			st.TokenAutoRotate = detail.Token.AutoRotate
			st.TokenCreationTime = parseFleetTime(detail.Token.CreationTime)
			st.TokenExpiryTime = parseFleetTime(detail.Token.ExpiryTime)
		}
		if p.CollectTelemetry {
			st.TelemetryError = telemetryErr
			st.ServerCount = len(servers)
			st.DatabaseCount = len(databases)
			st.Servers = fleetServersToStatus(servers)
			st.Databases = fleetDatabasesToStatus(databases)
		} else {
			// Feature turned off — drop any stale telemetry rather than leave a
			// frozen snapshot that looks current.
			st.Servers, st.Databases = nil, nil
			st.ServerCount, st.DatabaseCount, st.TelemetryError = 0, 0, ""
		}
	})
	if err != nil {
		logger.Error(err, "Failed to record Aura fleet provisioning status")
	}
}

func fleetServersToStatus(in []aura.FleetServer) []neo4jv1beta1.AuraFleetServerStatus {
	if len(in) > maxFleetTelemetryItems {
		in = in[:maxFleetTelemetryItems]
	}
	out := make([]neo4jv1beta1.AuraFleetServerStatus, 0, len(in))
	for _, s := range in {
		row := neo4jv1beta1.AuraFleetServerStatus{
			Name:           s.Name,
			Address:        s.Address,
			Status:         s.Status,
			Version:        s.Version,
			ModeConstraint: s.ModeConstraint,
			LastPing:       s.LastPing,
			PluginVersion:  s.PluginVersion,
		}
		if s.License != nil {
			row.LicenseState = s.License.State
			row.LicenseType = s.License.Type
		}
		out = append(out, row)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func fleetDatabasesToStatus(in []aura.FleetServerDatabase) []neo4jv1beta1.AuraFleetDatabaseStatus {
	if len(in) > maxFleetTelemetryItems {
		in = in[:maxFleetTelemetryItems]
	}
	out := make([]neo4jv1beta1.AuraFleetDatabaseStatus, 0, len(in))
	for _, d := range in {
		out = append(out, neo4jv1beta1.AuraFleetDatabaseStatus{
			Name:             d.Name,
			ServerID:         d.ServerID,
			CurrentStatus:    d.CurrentStatus,
			Role:             d.Role,
			Writer:           d.Writer,
			LastCommittedTxn: d.LastCommittedTxn,
			ReplicationLag:   d.ReplicationLag,
			GraphShards:      d.GraphShards,
			PropertyShards:   d.PropertyShards,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseFleetTime converts an Aura timestamp to a metav1.Time, returning nil for
// empty, zero or unparseable input (the field is decorative — never fail on it).
//
// Several formats are tried because Aura is NOT consistent here: the v2beta1
// spec declares these fields `format: date-time`, and the fleet token times do
// come back as RFC3339 — but the ip-filter `updated_at` on the same API returns
// RFC1123 ("Tue, 09 Jun 2026 14:45:10 GMT"), verified live 2026-07-30. Assuming
// one format silently drops the value.
func parseFleetTime(s string) *metav1.Time {
	if s == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.RFC1123, time.RFC1123Z} {
		t, err := time.Parse(layout, s)
		if err != nil {
			continue
		}
		// Aura uses the Go zero time ("0001-01-01T00:00:00Z") for "never", e.g. an
		// unused token's last_used_time. Report that as absent, not as year 1.
		if t.IsZero() {
			return nil
		}
		mt := metav1.NewTime(t)
		return &mt
	}
	return nil
}

// deprovisionAuraFleet removes the Aura-side deployment when the CR is deleted
// and deletionPolicy is Delete. Called from each controller's existing deletion
// path — no extra finalizer.
//
// Non-fatal by design: a stuck Aura API must not block CR deletion.
func (fp *fleetProvisioner) deprovisionAuraFleet(ctx context.Context, obj fleetProvisionTarget) {
	logger := log.FromContext(ctx)
	spec := obj.GetFleetSpec()
	if spec == nil || spec.Provision == nil || spec.Provision.DeletionPolicy != "Delete" {
		return
	}
	p := spec.Provision
	if !managementAllows(p.ManagementPolicies, auraPolicyDelete) {
		return
	}
	deploymentID := obj.GetAnnotations()[AuraFleetDeploymentAnnotation]
	if deploymentID == "" {
		return
	}

	creds, orgID, projectID, err := fp.resolveFleetTarget(ctx, obj, p)
	if err != nil {
		logger.Info("Skipping Aura fleet deprovision: cannot resolve target", "reason", err.Error())
		return
	}
	apiClient := resolveFleetClient(fp.ClientFactory, creds)

	// Revoke the token first: unregistering a deployment that still has a live
	// token would leave a usable credential behind.
	if err := apiClient.DeleteDeploymentToken(ctx, orgID, projectID, deploymentID); err != nil {
		logger.Error(err, "Failed to revoke Aura fleet token during deprovision", "deploymentId", deploymentID)
	}
	if err := apiClient.DeleteDeployment(ctx, orgID, projectID, deploymentID); err != nil {
		logger.Error(err, "Failed to unregister Aura fleet deployment during deprovision", "deploymentId", deploymentID)
		return
	}
	fp.Recorder.Eventf(obj, corev1.EventTypeNormal, EventReasonAuraFleetDeploymentDeleted,
		"Unregistered Aura fleet deployment %s", deploymentID)
}

// ---- small helpers --------------------------------------------------------

// note records a human-readable provisioning message without claiming success.
func (fp *fleetProvisioner) note(ctx context.Context, key types.NamespacedName, msg string) error {
	if err := fp.StatusWriter(ctx, key, func(st *neo4jv1beta1.AuraFleetManagementStatus) {
		st.Provisioned = false
		st.Message = msg
	}); err != nil {
		log.FromContext(ctx).Error(err, "Failed to record Aura fleet provisioning message")
	}
	return nil
}

// readTokenSecret returns the stored token, or "" when the Secret or key is absent.
func (fp *fleetProvisioner) readTokenSecret(ctx context.Context, namespace, name string) (string, error) {
	sec := &corev1.Secret{}
	err := fp.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sec)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(sec.Data[auraFleetTokenSecretKey])), nil
}

// writeTokenSecret creates or updates the token Secret, owned by the CR so it is
// garbage-collected with it.
func (fp *fleetProvisioner) writeTokenSecret(ctx context.Context, obj fleetProvisionTarget, name, token string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: obj.GetNamespace()},
		}
		_, err := controllerutil.CreateOrUpdate(ctx, fp.Client, sec, func() error {
			if sec.Data == nil {
				sec.Data = map[string][]byte{}
			}
			sec.Type = corev1.SecretTypeOpaque
			sec.Data[auraFleetTokenSecretKey] = []byte(token)
			if sec.Labels == nil {
				sec.Labels = map[string]string{}
			}
			sec.Labels["neo4j.com/managed-by"] = "neo4j-operator"
			sec.Labels["neo4j.com/aura-fleet-token"] = "true"
			return controllerutil.SetControllerReference(obj, sec, fp.Client.Scheme())
		})
		return err
	})
}

// setDeploymentAnnotation persists the Aura deployment ID onto the CR.
func (fp *fleetProvisioner) setDeploymentAnnotation(ctx context.Context, obj fleetProvisionTarget, id string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, ok := obj.DeepCopyObject().(fleetProvisionTarget)
		if !ok {
			return fmt.Errorf("unexpected object type %T", obj)
		}
		key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
		if err := fp.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		ann := latest.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		if ann[AuraFleetDeploymentAnnotation] == id {
			return nil
		}
		ann[AuraFleetDeploymentAnnotation] = id
		latest.SetAnnotations(ann)
		if err := fp.Client.Update(ctx, latest); err != nil {
			return err
		}
		// Keep the in-memory object consistent so callers see the annotation.
		local := obj.GetAnnotations()
		if local == nil {
			local = map[string]string{}
		}
		local[AuraFleetDeploymentAnnotation] = id
		obj.SetAnnotations(local)
		return nil
	})
}
