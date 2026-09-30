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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ==========================================================================
// Multi-database Aura instances.
//
// An Aura instance either can host several databases or it cannot, and that is
// decided when the instance is created and never again — Aura publishes no API
// to convert one. It also lives in only one place: the `multi_database` field of
// the v2beta1 instance API. v1, which the operator uses for everything else,
// neither accepts it at create time nor returns it on read.
//
// Two consequences this file exists to handle:
//
//   - An instance created through v1 (i.e. every instance this operator made
//     before this change) is NOT multi-database, so AuraDatabase,
//     AuraDatabaseBackup and AuraDatabaseRestore could never work against it.
//     The Aura API said so with a bare HTTP 409 — "Only multi database Instances
//     can add databases" — which the controller then retried every 30 seconds
//     forever. Requesting one now means asking v2beta1 to create it.
//
//   - Whether an EXISTING instance is multi-database is only knowable by asking
//     v2beta1, and that endpoint fails outright for v1-created instances (HTTP
//     500, see landmine 6 in internal/aura/instance_v2beta1.go). So the answer
//     is genuinely three-valued — yes / no / unknown — and the operator must
//     never collapse "unknown" into "no".
// ==========================================================================

// AuraMultiDatabaseProbedAnnotation records that the operator has already asked
// the v2beta1 API whether this instance is multi-database.
//
// The probe is ONE-SHOT by design: `multi_database` is immutable in Aura, so the
// answer cannot change, and asking again would burn an API call per reconcile
// forever on the (common) instances whose probe cannot succeed at all. The value
// is the outcome — "true", "false", or "unknown" — for operator-debugging; the
// authoritative report is status.atProvider.multiDatabase.
const AuraMultiDatabaseProbedAnnotation = "neo4j.com/multi-database-probed"

// probe outcomes stored in AuraMultiDatabaseProbedAnnotation.
const (
	multiDatabaseProbeTrue    = "true"
	multiDatabaseProbeFalse   = "false"
	multiDatabaseProbeUnknown = "unknown"
)

// auraInstanceV2API is the subset of the Aura v2beta1 client the instance
// controller needs for multi-database support. It is a SEPARATE interface from
// auraAPI (which is v1) so that the existing v1 test fakes keep compiling.
// *aura.Client satisfies it. BETA — see internal/aura/instance_v2beta1.go.
type auraInstanceV2API interface {
	CreateInstanceV2(ctx context.Context, orgID, projectID string, req aura.CreateInstanceV2Request) (*aura.CreateInstanceV2Response, error)
	GetInstanceV2(ctx context.Context, orgID, projectID, id string) (*aura.InstanceV2, error)
}

// auraInstanceV2ClientFactory builds an auraInstanceV2API from resolved
// credentials; the instance reconciler holds one (nil → the real shared client)
// so tests can inject a fake.
type auraInstanceV2ClientFactory func(auraCredentials) auraInstanceV2API

func defaultAuraInstanceV2ClientFactory(c auraCredentials) auraInstanceV2API {
	return auraClientForCreds(c)
}

// resolveInstanceV2Client returns the factory's client, or the default shared one.
func resolveInstanceV2Client(factory auraInstanceV2ClientFactory, c auraCredentials) auraInstanceV2API {
	if factory != nil {
		return factory(c)
	}
	return defaultAuraInstanceV2ClientFactory(c)
}

// resolvedGraphAnalytics is the graph-analytics mode the CR asks for, in Aura's
// own vocabulary (unavailable, serverless, plugin), or "" for unset.
// spec.graphAnalytics wins; the deprecated graphAnalyticsPlugin boolean maps to
// plugin/unavailable. CEL refuses setting both.
func resolvedGraphAnalytics(inst *neo4jv1beta1.AuraInstance) string {
	if inst.Spec.GraphAnalytics != "" {
		return inst.Spec.GraphAnalytics
	}
	if inst.Spec.GraphAnalyticsPlugin != nil {
		if *inst.Spec.GraphAnalyticsPlugin {
			return "plugin"
		}
		return "unavailable"
	}
	return ""
}

// wantsV2Create reports whether the instance must be created through the
// v2beta1 API because v1 cannot express what the CR asks for: a multi-database
// instance, or serverless graph analytics (v1 has only the graph_analytics_plugin
// boolean). Everything after the create stays on v1, which sees v2beta1-created
// instances.
func wantsV2Create(inst *neo4jv1beta1.AuraInstance) bool {
	return wantsMultiDatabase(inst) || resolvedGraphAnalytics(inst) == "serverless"
}

// v2CreateReason names what forced the v2beta1 create, for refusal messages.
func v2CreateReason(inst *neo4jv1beta1.AuraInstance) string {
	if wantsMultiDatabase(inst) {
		return "multiDatabase"
	}
	return "graphAnalytics: serverless"
}

// v2CreateRefusalReason is the condition reason for a create that cannot
// proceed. MultiDatabaseUnsupported predates serverless graph analytics, which
// also forces the v2beta1 create; a serverless instance refused for, say, a
// missing organization ID is not a multi-database problem and must not read
// like one.
func v2CreateRefusalReason(inst *neo4jv1beta1.AuraInstance) string {
	if !wantsMultiDatabase(inst) && wantsV2Create(inst) {
		return "ServerlessGraphAnalyticsUnsupported"
	}
	return "MultiDatabaseUnsupported"
}

// wantsMultiDatabase reports whether the CR asks for a multi-database instance.
// Only an explicit true counts: unset and false both leave the plain v1 create
// path in place.
func wantsMultiDatabase(inst *neo4jv1beta1.AuraInstance) bool {
	return inst.Spec.MultiDatabase != nil && *inst.Spec.MultiDatabase
}

// multiDatabaseCreateRequest translates the CR into the v2beta1 create body,
// refusing rather than guessing when the spec cannot be expressed there.
//
// Verified live 2026-09-30: the create honours storage and graph_analytics,
// refuses vector_optimized on a multi-database instance ("Vector optimized is
// not available on multi db instances"), and now REJECTS unknown fields where
// it used to ignore them. So v1-only fields are still refused here — with a
// message naming them, rather than the API's generic "additional properties" —
// and vectorOptimized with Aura's own reason. CEL rejects the same
// combinations on write; this is the belt-and-braces check for CRs that
// predate the rules.
func multiDatabaseCreateRequest(inst *neo4jv1beta1.AuraInstance, name string) (aura.CreateInstanceV2Request, error) {
	reason := v2CreateReason(inst)
	multiDB := wantsMultiDatabase(inst)
	v2Type, ok := aura.InstanceTypeV2(inst.Spec.Type)
	if !ok {
		return aura.CreateInstanceV2Request{}, refusef(
			"%s is not available for type %q: the Aura v2beta1 API, the only one that can create such an "+
				"instance, knows only free-db, professional-db, business-critical and enterprise-db",
			reason, inst.Spec.Type)
	}
	// Aura refuses multi_database outright on the smaller tiers (HTTP 400,
	// multi-database-tier-not-supported). Say so locally: type is immutable, so
	// the API's rejection would be terminal anyway, and only this message can
	// point at the actual choice the user has to make.
	if multiDB && !aura.SupportsMultiDatabase(v2Type) {
		return aura.CreateInstanceV2Request{}, refusef(
			"multiDatabase is not supported on type %q: Aura allows multi-database instances only on "+
				"business-critical and enterprise-db (Virtual Dedicated Cloud)", inst.Spec.Type)
	}

	if multiDB && inst.Spec.VectorOptimized != nil && *inst.Spec.VectorOptimized {
		return aura.CreateInstanceV2Request{}, refusef(
			"vectorOptimized is not available on a multi-database instance — Aura refuses the combination " +
				"(multi-database-capability-not-supported); drop one of them")
	}

	var dropped []string
	if inst.Spec.SecondariesCount != nil {
		dropped = append(dropped, "secondariesCount")
	}
	if inst.Spec.CDCEnrichmentMode != "" {
		dropped = append(dropped, "cdcEnrichmentMode")
	}
	if inst.Spec.CustomerManagedKeyID != "" {
		dropped = append(dropped, "customerManagedKeyId")
	}
	if inst.Spec.Source != nil {
		dropped = append(dropped, "source")
	}
	if len(dropped) > 0 {
		return aura.CreateInstanceV2Request{}, refusef(
			"%s creates the instance through the Aura v2beta1 API, which has no equivalent of %v "+
				"(it accepts name/type/cloudProvider/region/memory/storage/graphAnalytics); remove them or drop %s",
			reason, dropped, reason)
	}

	var multiDatabase *bool
	if multiDB {
		multiDatabase = &multiDB
	}
	// vector_optimized is a v2beta1 create field too (the API validates it);
	// only its combination with multi_database is refused, above.
	var vectorOptimized *bool
	if !multiDB {
		vectorOptimized = inst.Spec.VectorOptimized
	}
	return aura.CreateInstanceV2Request{
		Name:            name,
		Type:            v2Type,
		CloudProvider:   inst.Spec.CloudProvider,
		Region:          inst.Spec.Region,
		Memory:          inst.Spec.Memory,
		Storage:         inst.Spec.Storage,
		GraphAnalytics:  resolvedGraphAnalytics(inst),
		MultiDatabase:   multiDatabase,
		VectorOptimized: vectorOptimized,
		// No `version`: v2beta1 has no such field and picks the Neo4j version
		// itself. spec.version is still required by the CRD (v1 needs it) and is
		// simply not expressible here.
	}, nil
}

// v2CreateAsV1Response adapts the v2beta1 create response to the v1 shape, so
// the one-time-credentials path (reconcileConnectionOutputs) stays single-source.
// Both APIs return the initial password exactly once, at create.
func v2CreateAsV1Response(resp *aura.CreateInstanceV2Response) *aura.CreateInstanceResponse {
	if resp == nil {
		return nil
	}
	return &aura.CreateInstanceResponse{
		ID:              resp.ID,
		ConnectionURL:   resp.ConnectionURL,
		Username:        resp.Username,
		Password:        resp.Password,
		TenantID:        resp.ProjectID,
		CloudProvider:   resp.CloudProvider,
		Region:          resp.Region,
		Name:            resp.Name,
		Type:            resp.Type,
		CreatedAt:       resp.CreatedAt,
		VectorOptimized: resp.VectorOptimized,
	}
}

// auraInstanceMultiDatabase reads the multi-database verdict the instance
// controller recorded for the AuraInstance named instanceRef. A nil result means
// UNKNOWN — either not probed yet or unprobeable — and callers must not read it
// as "no".
func auraInstanceMultiDatabase(ctx context.Context, k8s client.Client, namespace, instanceRef string) *bool {
	inst := &neo4jv1beta1.AuraInstance{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: instanceRef, Namespace: namespace}, inst); err != nil {
		return nil
	}
	if inst.Status.AtProvider == nil {
		return nil
	}
	return inst.Status.AtProvider.MultiDatabase
}

// notMultiDatabaseRefusal is the single explanation every caller gives for
// "this instance cannot hold your database". It names the cause, the fix, and
// the fact that the fix cannot be applied in place.
func notMultiDatabaseRefusal(instanceRef, instanceID string) error {
	return refusef(
		"Aura instance %s (%s) is not a multi-database instance, so it cannot host additional databases. "+
			"Only an instance created with spec.multiDatabase: true can — and Aura fixes that at creation, "+
			"with no way to convert an existing instance, so this needs a new AuraInstance (and a migration of "+
			"any data). Instances created by earlier operator versions are never multi-database.",
		instanceRef, instanceID)
}

// probeMultiDatabase asks v2beta1 whether the instance is multi-database, at
// most once per instance, and returns the outcome to record in status.
//
// It is deliberately best-effort and non-fatal, for the reason in landmine 6:
// the endpoint returns HTTP 500 (not 404) for v1-created instances, which is the
// majority case, and that must never fail a reconcile or be mistaken for "false".
// known is false when the answer could not be established.
func probeMultiDatabase(
	ctx context.Context, apiClient auraInstanceV2API, orgID, projectID, instanceID string,
) (value bool, known bool) {
	observed, err := apiClient.GetInstanceV2(ctx, orgID, projectID, instanceID)
	if err != nil {
		log.FromContext(ctx).V(1).Info("multi-database probe unavailable for this instance; recording it as unknown",
			"instanceId", instanceID, "error", err.Error())
		return false, false
	}
	if observed.MultiDatabase == nil {
		return false, false
	}
	return *observed.MultiDatabase, true
}

// multiDatabaseProbeOutcome renders a probe result for the one-shot annotation.
func multiDatabaseProbeOutcome(value, known bool) string {
	switch {
	case !known:
		return multiDatabaseProbeUnknown
	case value:
		return multiDatabaseProbeTrue
	default:
		return multiDatabaseProbeFalse
	}
}

// resolveInstanceOrgID resolves the Aura organization for an AuraInstance:
// spec.organizationId if set, else the referenced provider config's
// defaultOrganizationId. Empty means unresolvable.
func resolveInstanceOrgID(ctx context.Context, k8s client.Client, inst *neo4jv1beta1.AuraInstance) string {
	return resolveProviderOrgID(ctx, k8s, inst.Namespace, inst.Spec.ProviderConfigRef, inst.Spec.OrganizationID)
}

// requireOrgIDForMultiDatabase turns a missing organization into an explanation
// instead of a confusing 404 from the v2beta1 path builder.
func requireOrgIDForMultiDatabase(orgID, reason string) error {
	if orgID != "" {
		return nil
	}
	return refusef("%s needs an Aura organization ID, because only the v2beta1 API can create such an "+
		"instance and its paths are organization-scoped; set spec.organizationId or "+
		"defaultOrganizationId on the AuraProviderConfig", reason)
}

// createMultiDatabaseInstance is observeOrCreate's create half for the one case
// v1 cannot serve. It mirrors the v1 path's ordering exactly, because the
// ordering is the idempotency guard: persist the external ID first (so a crash
// re-observes instead of creating a second paid instance), then capture the
// one-time credentials.
func (r *AuraInstanceReconciler) createMultiDatabaseInstance(
	ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance, projectID, name string,
) (id string, adopted bool, err error) {
	orgID := resolveInstanceOrgID(ctx, r.Client, inst)
	if err := requireOrgIDForMultiDatabase(orgID, v2CreateReason(inst)); err != nil {
		return "", false, err
	}
	createReq, err := multiDatabaseCreateRequest(inst, name)
	if err != nil {
		return "", false, err
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, inst.Namespace, inst.Spec.ProviderConfigRef, inst.Spec.CredentialsSecretRef)
	if err != nil {
		return "", false, err
	}
	resp, err := resolveInstanceV2Client(r.InstanceV2ClientFactory, creds).CreateInstanceV2(ctx, orgID, projectID, createReq)
	if err != nil {
		return "", false, err
	}

	id = resp.ID
	if err := r.setExternalID(ctx, req, id); err != nil {
		return "", false, err
	}
	if err := r.reconcileConnectionOutputs(ctx, inst, nil, v2CreateAsV1Response(resp)); err != nil {
		log.FromContext(ctx).Error(err, "failed to persist one-time connection credentials")
	}

	// The create response answers the multi-database question authoritatively, so
	// record it now and mark the probe done — no v2beta1 GET will ever be needed
	// for this instance.
	multiDB := resp.MultiDatabase != nil && *resp.MultiDatabase
	r.recordMultiDatabaseFacts(ctx, req, multiDB, true, resp.DefaultDatabaseID)

	r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceCreated,
		fmt.Sprintf("Created Aura instance %s via the Aura API v2beta1, which %s requires (multi_database=%t)",
			id, v2CreateReason(inst), multiDB))
	return id, false, nil
}

// reconcileMultiDatabaseStatus establishes the multi-database verdict for an
// instance the operator did not create as multi-database — typically an adopted
// or pre-existing one — exactly once.
//
// Non-fatal by contract: the probe cannot succeed at all for v1-created
// instances, which is the common case, so a failure records "unknown" and the
// reconcile carries on. It never returns an error.
func (r *AuraInstanceReconciler) reconcileMultiDatabaseStatus(
	ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance,
	v2Client auraInstanceV2API, projectID, instanceID string,
) {
	if inst.Annotations[AuraMultiDatabaseProbedAnnotation] != "" {
		return // already asked; the answer cannot have changed
	}
	orgID := resolveInstanceOrgID(ctx, r.Client, inst)
	if orgID == "" {
		// Without an organization there is no v2beta1 path to ask. Leave the
		// annotation unset so the probe runs if an org is configured later.
		return
	}
	value, known := probeMultiDatabase(ctx, v2Client, orgID, projectID, instanceID)
	r.recordMultiDatabaseFacts(ctx, req, value, known, "")
}

// recordMultiDatabaseFacts persists the verdict to status and stamps the
// one-shot annotation. Best-effort: these are reporting facts, so a write
// failure is logged, never propagated into the reconcile result.
func (r *AuraInstanceReconciler) recordMultiDatabaseFacts(
	ctx context.Context, req ctrl.Request, value, known bool, defaultDatabaseID string,
) {
	logger := log.FromContext(ctx)
	if err := r.patchInstance(ctx, req, func(o *neo4jv1beta1.AuraInstance) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraMultiDatabaseProbedAnnotation] = multiDatabaseProbeOutcome(value, known)
	}); err != nil {
		logger.Error(err, "failed to stamp the multi-database probe annotation")
		return
	}
	if !known && defaultDatabaseID == "" {
		return // nothing definite to report
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInstance{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		if latest.Status.AtProvider == nil {
			latest.Status.AtProvider = &neo4jv1beta1.AuraInstanceObservation{}
		}
		if known {
			v := value
			latest.Status.AtProvider.MultiDatabase = &v
		}
		if defaultDatabaseID != "" {
			latest.Status.AtProvider.DefaultDatabaseID = defaultDatabaseID
		}
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to record the multi-database verdict in status")
	}
}

// errAdoptionUnverified marks "the instance whose name matches is not known to be
// multi-database". It is deliberately NOT an auraRefusalError: a refusal is
// terminal, and this condition can clear on its own (the v2beta1 GET that could
// not answer may start answering), so the caller requeues with the explanation on
// status instead of stopping for good.
var errAdoptionUnverified = errors.New("cannot confirm the instance to adopt is multi-database")

// verifyAdoptableAsMultiDatabase refuses to adopt a name-matched instance into a
// `multiDatabase: true` CR unless v2beta1 confirms it really is one.
//
// Without this, adoption-by-name — which runs before the create branch, as the
// idempotency guard — would silently bind the CR to a pre-existing single-database
// instance (every instance created by an earlier operator version is one), and the
// multi-database create would never happen. The user's manifest would report Ready
// against an instance that can never host an AuraDatabase.
//
// A negative and an unconfirmed answer are treated the same way — do not adopt —
// but neither is terminal, because the two cannot be told apart: the v2beta1
// instance GET returns HTTP 500 both for a v1-created instance and for a genuine
// outage (landmine 6). Requeuing means the operator never binds the wrong instance
// and never creates a duplicate paid one, and recovers by itself if the GET starts
// working. The deliberate escape hatch is spec.instanceId, which imports an
// instance explicitly and bypasses name matching altogether.
func (r *AuraInstanceReconciler) verifyAdoptableAsMultiDatabase(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance,
	v2Client auraInstanceV2API, projectID, instanceID string,
) error {
	orgID := resolveInstanceOrgID(ctx, r.Client, inst)
	if orgID == "" {
		return fmt.Errorf("%w: instance %s matches this CR's name but no organization is configured, so "+
			"multi_database cannot be read (set spec.organizationId, or spec.instanceId to adopt it deliberately)",
			errAdoptionUnverified, instanceID)
	}
	value, known := probeMultiDatabase(ctx, v2Client, orgID, projectID, instanceID)
	switch {
	case known && value:
		return nil
	case known && !value:
		return fmt.Errorf("%w: instance %s matches this CR's name but is NOT a multi-database instance, and Aura "+
			"cannot convert it; rename this AuraInstance so a new multi-database instance is created, or drop "+
			"spec.multiDatabase to manage the existing one as-is", errAdoptionUnverified, instanceID)
	default:
		return fmt.Errorf("%w: instance %s matches this CR's name but the Aura v2beta1 API would not report its "+
			"multi_database flag, which is also what happens for instances created through v1 (never "+
			"multi-database); refusing to adopt it rather than bind this CR to an instance that may be unable to "+
			"host databases (set spec.instanceId to adopt it deliberately)", errAdoptionUnverified, instanceID)
	}
}

// v1GraphAnalyticsPlugin is the v1 create's graph_analytics_plugin for the CR:
// v1 has only that boolean, so plugin is true, unavailable is false, and unset
// is omitted. serverless never reaches the v1 path — wantsV2Create routes it to
// v2beta1.
func v1GraphAnalyticsPlugin(inst *neo4jv1beta1.AuraInstance) *bool {
	switch resolvedGraphAnalytics(inst) {
	case "plugin":
		t := true
		return &t
	case "unavailable":
		f := false
		return &f
	}
	return nil
}
