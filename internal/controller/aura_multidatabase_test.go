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
	"net/http"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ---------------------------------------------------------------------------
// Fake for the v2beta1 instance API.
// ---------------------------------------------------------------------------

type fakeInstanceV2API struct {
	createCalled  bool
	lastCreateReq aura.CreateInstanceV2Request
	lastCreateOrg string
	createFn      func(aura.CreateInstanceV2Request) (*aura.CreateInstanceV2Response, error)

	getCalls int
	getFn    func(id string) (*aura.InstanceV2, error)
}

func (f *fakeInstanceV2API) CreateInstanceV2(_ context.Context, orgID, _ string, req aura.CreateInstanceV2Request) (*aura.CreateInstanceV2Response, error) {
	f.createCalled = true
	f.lastCreateReq = req
	f.lastCreateOrg = orgID
	if f.createFn != nil {
		return f.createFn(req)
	}
	multiDB := true
	return &aura.CreateInstanceV2Response{
		ID: "v2-created", Name: req.Name, Type: req.Type,
		MultiDatabase: &multiDB, DefaultDatabaseID: "defdb-1",
		Username: "neo4j", Password: "one-time", ConnectionURL: "neo4j+s://v2-created.databases.neo4j.io",
	}, nil
}

func (f *fakeInstanceV2API) GetInstanceV2(_ context.Context, _, _, id string) (*aura.InstanceV2, error) {
	f.getCalls++
	if f.getFn != nil {
		return f.getFn(id)
	}
	multiDB := true
	return &aura.InstanceV2{ID: id, MultiDatabase: &multiDB, LegacyStatus: "running"}, nil
}

func instV2Factory(f *fakeInstanceV2API) auraInstanceV2ClientFactory {
	return func(auraCredentials) auraInstanceV2API { return f }
}

func ptrTo[T any](v T) *T { return &v }

// conditionMessage complements conditionStatus in aura_controllers_test.go:
// these assertions turn on the Message as well as the Status — a refusal is only
// useful if it explains itself. (findCondition already lives in conditions.go.)
func conditionMessage(conds []metav1.Condition, condType string) string {
	if c := meta.FindStatusCondition(conds, condType); c != nil {
		return c.Message
	}
	return ""
}

// ---------------------------------------------------------------------------
// Translating the CR into the v2beta1 create body.
// ---------------------------------------------------------------------------

func TestMultiDatabaseCreateRequest(t *testing.T) {
	base := func() *neo4jv1beta1.AuraInstance {
		return &neo4jv1beta1.AuraInstance{Spec: neo4jv1beta1.AuraInstanceSpec{
			CloudProvider: "gcp", Region: "europe-west1", Type: "business-critical",
			Version: "5", Memory: "2GB", MultiDatabase: ptrTo(true),
		}}
	}

	got, err := multiDatabaseCreateRequest(base(), "inst")
	if err != nil {
		t.Fatalf("happy path: %v", err)
	}
	if got.Type != aura.InstanceTypeV2BusinessCritical {
		t.Errorf("Type = %q, want the v2beta1 vocabulary", got.Type)
	}
	if got.MultiDatabase == nil || !*got.MultiDatabase {
		t.Error("multi_database must be requested explicitly — it is the entire point of this path")
	}

	// enterprise-db maps to virtual-dedicated-cloud, the other multi-database
	// capable tier — it must be accepted.
	vdc := base()
	vdc.Spec.Type = "enterprise-db"
	if got, err := multiDatabaseCreateRequest(vdc, "inst"); err != nil {
		t.Errorf("enterprise-db must be accepted: %v", err)
	} else if got.Type != aura.InstanceTypeV2VirtualDedicatedCloud {
		t.Errorf("Type = %q, want %q", got.Type, aura.InstanceTypeV2VirtualDedicatedCloud)
	}

	// The AuraDS tiers have no v2beta1 equivalent, so this must refuse rather
	// than send a plausible-looking substitute to an irreversible create.
	ds := base()
	ds.Spec.Type = "enterprise-ds"
	if _, err := multiDatabaseCreateRequest(ds, "inst"); err == nil {
		t.Error("enterprise-ds must be refused: it has no v2beta1 tier")
	} else if !isAuraRefusal(err) {
		t.Errorf("must be a refusal (never retried, always explained), got %T", err)
	}

	// Aura refuses multi_database on the smaller tiers (verified live: HTTP 400
	// multi-database-tier-not-supported for both `free` and `professional`).
	// Refusing locally is what makes the message actionable — the CEL rule blocks
	// this on write, so only a pre-existing CR reaches here.
	for _, tier := range []string{"free-db", "professional-db"} {
		small := base()
		small.Spec.Type = tier
		_, err := multiDatabaseCreateRequest(small, "inst")
		if err == nil {
			t.Errorf("%s: must be refused — Aura rejects multi_database on this tier", tier)
			continue
		}
		if !isAuraRefusal(err) {
			t.Errorf("%s: must be a refusal, got %T", tier, err)
		}
		if !strings.Contains(err.Error(), "business-critical") {
			t.Errorf("%s: refusal must name the tiers that DO work, got %q", tier, err.Error())
		}
	}

	// Fields v2beta1 silently ignores must be refused, not dropped: dropping them
	// hands the user an instance that does not match their manifest, with nothing
	// in the status to say why.
	for _, tc := range []struct {
		name  string
		apply func(*neo4jv1beta1.AuraInstance)
	}{
		{"storage", func(i *neo4jv1beta1.AuraInstance) { i.Spec.Storage = "8GB" }},
		{"vectorOptimized", func(i *neo4jv1beta1.AuraInstance) { i.Spec.VectorOptimized = ptrTo(true) }},
		{"graphAnalyticsPlugin", func(i *neo4jv1beta1.AuraInstance) { i.Spec.GraphAnalyticsPlugin = ptrTo(true) }},
		{"secondariesCount", func(i *neo4jv1beta1.AuraInstance) { i.Spec.SecondariesCount = ptrTo(int32(1)) }},
		{"cdcEnrichmentMode", func(i *neo4jv1beta1.AuraInstance) { i.Spec.CDCEnrichmentMode = "DIFF" }},
		{"customerManagedKeyId", func(i *neo4jv1beta1.AuraInstance) { i.Spec.CustomerManagedKeyID = "k" }},
		{"source", func(i *neo4jv1beta1.AuraInstance) {
			i.Spec.Source = &neo4jv1beta1.AuraInstanceSource{InstanceID: "src"}
		}},
	} {
		inst := base()
		tc.apply(inst)
		_, err := multiDatabaseCreateRequest(inst, "inst")
		if err == nil {
			t.Errorf("%s: must be refused — v2beta1 create ignores it silently", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.name) {
			t.Errorf("%s: the refusal must name the offending field, got %q", tc.name, err.Error())
		}
	}
}

func TestWantsMultiDatabaseOnlyOnExplicitTrue(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *bool
		want bool
	}{
		{"unset", nil, false},
		{"false", ptrTo(false), false},
		{"true", ptrTo(true), true},
	} {
		inst := &neo4jv1beta1.AuraInstance{Spec: neo4jv1beta1.AuraInstanceSpec{MultiDatabase: tc.spec}}
		if got := wantsMultiDatabase(inst); got != tc.want {
			t.Errorf("%s: wantsMultiDatabase = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// AuraInstance: creating a multi-database instance goes through v2beta1.
// ---------------------------------------------------------------------------

func TestAuraInstance_MultiDatabaseCreatesViaV2beta1(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "mdb", Namespace: testNS},
		Spec: neo4jv1beta1.AuraInstanceSpec{
			CredentialsSecretRef: credRef(), ProjectID: "proj-1", OrganizationID: "org-1",
			CloudProvider: "gcp", Region: "europe-west1", Type: "enterprise-db",
			Version: "5", Memory: "8GB", MultiDatabase: ptrTo(true),
		},
	}
	v1API := &fakeAuraAPI{}
	v2API := &fakeInstanceV2API{}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory:           func(auraCredentials) auraAPI { return v1API },
		InstanceV2ClientFactory: instV2Factory(v2API),
	}
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if v1API.createCalled {
		t.Error("v1 CreateInstance must NOT be used: it cannot express multi_database, so it would " +
			"silently produce an instance that can never hold an AuraDatabase")
	}
	if !v2API.createCalled {
		t.Fatal("expected CreateInstanceV2 to be called")
	}
	if v2API.lastCreateOrg != "org-1" {
		t.Errorf("org = %q, want org-1 (v2beta1 paths are organization-scoped)", v2API.lastCreateOrg)
	}
	if v2API.lastCreateReq.Type != aura.InstanceTypeV2VirtualDedicatedCloud {
		t.Errorf("type = %q, want %q — enterprise-db must be translated",
			v2API.lastCreateReq.Type, aura.InstanceTypeV2VirtualDedicatedCloud)
	}

	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalIDAnnotation]; id != "v2-created" {
		t.Errorf("external-instance-id = %q, want v2-created", id)
	}
	if got.Annotations[AuraMultiDatabaseProbedAnnotation] != multiDatabaseProbeTrue {
		t.Errorf("probe annotation = %q, want %q: the create response already answered the question, "+
			"so no v2beta1 GET should ever be needed for this instance",
			got.Annotations[AuraMultiDatabaseProbedAnnotation], multiDatabaseProbeTrue)
	}
	if v2API.getCalls != 0 {
		t.Errorf("GetInstanceV2 called %d times; the create response is authoritative", v2API.getCalls)
	}
	if got.Status.AtProvider == nil || got.Status.AtProvider.MultiDatabase == nil || !*got.Status.AtProvider.MultiDatabase {
		t.Fatalf("status.atProvider.multiDatabase must report true, got %+v", got.Status.AtProvider)
	}
	if got.Status.AtProvider.DefaultDatabaseID != "defdb-1" {
		t.Errorf("defaultDatabaseId = %q, want defdb-1 (only the v2beta1 create reports it)",
			got.Status.AtProvider.DefaultDatabaseID)
	}

	// syncStatus rebuilds atProvider wholesale from the v1 observation, which
	// knows nothing about multi_database. A second pass must not erase the facts.
	if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
		t.Fatalf("Reconcile #2: %v", err)
	}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.AtProvider.MultiDatabase == nil || !*got.Status.AtProvider.MultiDatabase {
		t.Error("multiDatabase was dropped by the next reconcile's status rebuild")
	}
	if got.Status.AtProvider.DefaultDatabaseID != "defdb-1" {
		t.Error("defaultDatabaseId was dropped by the next reconcile's status rebuild")
	}
}

func TestAuraInstance_MultiDatabaseRefusesWithoutAnOrganization(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "mdb-noorg", Namespace: testNS},
		Spec: neo4jv1beta1.AuraInstanceSpec{
			CredentialsSecretRef: credRef(), ProjectID: "proj-1",
			CloudProvider: "gcp", Region: "europe-west1", Type: "business-critical",
			Version: "5", Memory: "2GB", MultiDatabase: ptrTo(true),
		},
	}
	v2API := &fakeInstanceV2API{}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory:           func(auraCredentials) auraAPI { return &fakeAuraAPI{} },
		InstanceV2ClientFactory: instV2Factory(v2API),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if v2API.createCalled {
		t.Error("must not attempt a v2beta1 create without an organization ID")
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(context.Background(), reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	msg := conditionMessage(got.Status.Conditions, "Ready")
	if !strings.Contains(msg, "organization") {
		t.Errorf("Ready message must explain the missing organization, got %q", msg)
	}
}

// The probe cannot succeed for v1-created instances (HTTP 500, not 404). That
// must be recorded as UNKNOWN, never as "not multi-database", and must not be
// retried on every reconcile.
func TestAuraInstance_ProbeFailureRecordsUnknownAndStopsAsking(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "adopted", Namespace: testNS,
			Annotations: map[string]string{AuraExternalIDAnnotation: "inst-v1"},
		},
		Spec: neo4jv1beta1.AuraInstanceSpec{
			CredentialsSecretRef: credRef(), ProjectID: "proj-1", OrganizationID: "org-1",
			CloudProvider: "gcp", Region: "europe-west1", Type: "enterprise-db", Version: "5", Memory: "8GB",
		},
	}
	v2API := &fakeInstanceV2API{getFn: func(string) (*aura.InstanceV2, error) {
		return nil, &aura.APIError{StatusCode: http.StatusInternalServerError,
			Message: "invalid status code 404 [GET /aura-instances/{{.Instance_id}}]"}
	}}
	// The v1 observation must match the spec, or reconcileDrift PATCHes and
	// returns before the probe is ever reached.
	v1API := &fakeAuraAPI{getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
		return &aura.Instance{
			ID: id, Name: "adopted", Status: aura.InstanceStatusRunning,
			Memory: "8GB", Type: "enterprise-db", Region: "europe-west1", CloudProvider: "gcp",
		}, nil
	}}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory:           func(auraCredentials) auraAPI { return v1API },
		InstanceV2ClientFactory: instV2Factory(v2API),
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
			t.Fatalf("Reconcile #%d must not fail on a probe error: %v", i, err)
		}
	}
	if v2API.getCalls != 1 {
		t.Errorf("GetInstanceV2 called %d times, want exactly 1: multi_database is immutable, so the "+
			"probe is one-shot and must not burn an API call every reconcile", v2API.getCalls)
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Annotations[AuraMultiDatabaseProbedAnnotation] != multiDatabaseProbeUnknown {
		t.Errorf("probe annotation = %q, want %q",
			got.Annotations[AuraMultiDatabaseProbedAnnotation], multiDatabaseProbeUnknown)
	}
	if got.Status.AtProvider != nil && got.Status.AtProvider.MultiDatabase != nil {
		t.Errorf("multiDatabase must stay UNSET (unknown), got %v — recording false here would wrongly "+
			"block every AuraDatabase against this instance", *got.Status.AtProvider.MultiDatabase)
	}
	// Ready must not be dragged down by a best-effort probe.
	if s := conditionStatus(got.Status.Conditions, "Ready"); s != metav1.ConditionTrue {
		t.Errorf("Ready = %q, want True: the probe is non-fatal", s)
	}
}

// ---------------------------------------------------------------------------
// AuraDatabase: refusing a single-database instance, terminally.
// ---------------------------------------------------------------------------

func newDBAgainstInstance(t *testing.T, multiDB *bool) (*neo4jv1beta1.AuraDatabase, *neo4jv1beta1.AuraInstance) {
	t.Helper()
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "single", Namespace: testNS,
			Annotations: map[string]string{AuraExternalIDAnnotation: "inst-1"},
		},
		Spec: neo4jv1beta1.AuraInstanceSpec{CredentialsSecretRef: credRef(), ProjectID: "proj-1"},
		Status: neo4jv1beta1.AuraInstanceStatus{
			AtProvider: &neo4jv1beta1.AuraInstanceObservation{MultiDatabase: multiDB},
		},
	}
	db := &neo4jv1beta1.AuraDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-db", Namespace: testNS},
		Spec: neo4jv1beta1.AuraDatabaseSpec{
			InstanceRef: "single", Name: "extra", OrganizationID: "org-1",
		},
	}
	return db, inst
}

func TestAuraDatabase_RefusesKnownSingleDatabaseInstanceWithoutCallingTheAPI(t *testing.T) {
	scheme := auraTestScheme(t)
	db, inst := newDBAgainstInstance(t, ptrTo(false))
	api := &fakeDatabaseAPI{}
	c := newAuraFakeClient(t, scheme, inst, db)
	r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: dbFactory(api)}

	res, err := r.Reconcile(context.Background(), reqFor(db))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if api.createCalled {
		t.Error("must not call CreateDatabase when the instance is known to be single-database")
	}
	// Terminal: requeueing would rewrite the same status forever and bury the
	// explanation. The spec change that fixes it triggers its own reconcile.
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0: the refusal is permanent", res.RequeueAfter)
	}

	got := &neo4jv1beta1.AuraDatabase{}
	if err := c.Get(context.Background(), reqFor(db).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	cond := findCondition(got.Status.Conditions, "Ready")
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready = %+v, want False", cond)
	}
	if cond.Reason != "InstanceNotMultiDatabase" {
		t.Errorf("Reason = %q, want InstanceNotMultiDatabase", cond.Reason)
	}
	for _, want := range []string{"multiDatabase", "no way to convert"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("message must contain %q so the user knows the fix is a new instance, got %q", want, cond.Message)
		}
	}
}

func TestAuraDatabase_UnknownVerdictStillAttemptsTheCreate(t *testing.T) {
	scheme := auraTestScheme(t)
	// nil = unknown, which is the normal state for instances whose probe cannot
	// succeed. Treating it as "no" would block every such instance outright.
	db, inst := newDBAgainstInstance(t, nil)
	api := &fakeDatabaseAPI{}
	c := newAuraFakeClient(t, scheme, inst, db)
	r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: dbFactory(api)}
	if _, err := r.Reconcile(context.Background(), reqFor(db)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !api.createCalled {
		t.Error("an unknown verdict must not block the create — only an explicit false may")
	}
}

func TestAuraDatabase_TranslatesTheLiveMultiDBOnly409(t *testing.T) {
	scheme := auraTestScheme(t)
	db, inst := newDBAgainstInstance(t, nil) // unknown, so the create is attempted
	api := &fakeDatabaseAPI{createErr: &aura.APIError{
		StatusCode: http.StatusConflict,
		Reason:     aura.ReasonMultiDBOnly,
		Message:    "Only multi database Instances can add databases",
	}}
	c := newAuraFakeClient(t, scheme, inst, db)
	r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: dbFactory(api)}

	res, err := r.Reconcile(context.Background(), reqFor(db))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 — the bare 409 reads as \"retry later\" but never succeeds", res.RequeueAfter)
	}
	got := &neo4jv1beta1.AuraDatabase{}
	if err := c.Get(context.Background(), reqFor(db).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	cond := findCondition(got.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "InstanceNotMultiDatabase" {
		t.Fatalf("Ready = %+v, want reason InstanceNotMultiDatabase", cond)
	}
	// The user must get the explanation, not the API's four-word 409.
	if strings.TrimSpace(cond.Message) == "Only multi database Instances can add databases" {
		t.Error("the raw API message was surfaced verbatim; it says nothing about what to do")
	}
	if !strings.Contains(cond.Message, "spec.multiDatabase") {
		t.Errorf("message must point at spec.multiDatabase, got %q", cond.Message)
	}
}

func TestAuraInstanceMultiDatabaseReadsTheVerdict(t *testing.T) {
	scheme := auraTestScheme(t)
	_, inst := newDBAgainstInstance(t, ptrTo(true))
	c := newAuraFakeClient(t, scheme, inst)
	ctx := context.Background()

	if v := auraInstanceMultiDatabase(ctx, c, testNS, "single"); v == nil || !*v {
		t.Errorf("verdict = %v, want true", v)
	}
	// A missing instance is unknown, not false.
	if v := auraInstanceMultiDatabase(ctx, c, testNS, "does-not-exist"); v != nil {
		t.Errorf("verdict for a missing instance = %v, want nil (unknown)", *v)
	}
}

func TestV2CreateAsV1ResponseKeepsTheOneTimeCredentials(t *testing.T) {
	multiDB := true
	got := v2CreateAsV1Response(&aura.CreateInstanceV2Response{
		ID: "i-1", Username: "neo4j", Password: "one-time", ConnectionURL: "neo4j+s://i-1",
		ProjectID: "proj-1", MultiDatabase: &multiDB,
	})
	if got.Username != "neo4j" || got.Password != "one-time" || got.ConnectionURL != "neo4j+s://i-1" {
		t.Errorf("credentials lost in translation: %+v", got)
	}
	// v1 calls the project a tenant; losing the mapping breaks the ConfigMap the
	// connection outputs publish.
	if got.TenantID != "proj-1" {
		t.Errorf("TenantID = %q, want proj-1 (v2beta1's project_id)", got.TenantID)
	}
	if v2CreateAsV1Response(nil) != nil {
		t.Error("nil in must give nil out")
	}
}

func TestMultiDatabaseProbeOutcome(t *testing.T) {
	if got := multiDatabaseProbeOutcome(false, false); got != multiDatabaseProbeUnknown {
		t.Errorf("unknown probe = %q, want %q", got, multiDatabaseProbeUnknown)
	}
	if got := multiDatabaseProbeOutcome(true, true); got != multiDatabaseProbeTrue {
		t.Errorf("true probe = %q", got)
	}
	if got := multiDatabaseProbeOutcome(false, true); got != multiDatabaseProbeFalse {
		t.Errorf("false probe = %q", got)
	}
	// An unknown answer must never render as a definite one.
	if multiDatabaseProbeOutcome(true, false) != multiDatabaseProbeUnknown {
		t.Error("known=false must win over the value")
	}
}

func TestNotMultiDatabaseRefusalIsARefusalNotATransientError(t *testing.T) {
	err := notMultiDatabaseRefusal("my-instance", "inst-1")
	// aura.IsTransient returns true for anything that is not an *aura.APIError,
	// so without the refusal marker this would be retried forever in silence.
	if !isAuraRefusal(err) {
		t.Fatal("must be marked as a deliberate refusal")
	}
	var apiErr *aura.APIError
	if errors.As(err, &apiErr) {
		t.Fatal("must not masquerade as an APIError")
	}
	if !strings.Contains(err.Error(), "my-instance") || !strings.Contains(err.Error(), "inst-1") {
		t.Errorf("refusal must name the instance, got %q", err.Error())
	}
}
