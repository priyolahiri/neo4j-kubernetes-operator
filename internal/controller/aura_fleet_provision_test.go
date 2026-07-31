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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ---------------------------------------------------------------------------
// Fake Aura Fleet Manager API.
//
// Models the two properties that make this surface dangerous: a token is
// returned exactly ONCE (so the fake never hands it back on a later read), and
// POST token fails when the deployment already has one.
// ---------------------------------------------------------------------------

type fakeFleetAPI struct {
	deployments []aura.Deployment
	// hasToken reports whether the deployment already holds a token Aura will
	// not return again. NOT observable through GetDeployment — see GetDeployment.
	hasToken bool
	// claimed reports whether a DBMS has claimed the token, which is the ONLY
	// condition under which Aura exposes token metadata on GET.
	claimed bool

	listCalled   bool
	createCalled bool
	createdName  string
	mintCalled   bool
	rotateCalled bool
	delTokCalled bool
	delDepCalled bool

	servers       []aura.FleetServer
	databases     []aura.FleetServerDatabase
	serverDBCalls []string
	telemErr      error
}

func (f *fakeFleetAPI) ListDeployments(_ context.Context, _, _ string) ([]aura.Deployment, error) {
	f.listCalled = true
	return f.deployments, nil
}

// Live behaviour: `token` is populated ONLY once a running DBMS has CLAIMED the
// token. A freshly-minted, never-used token reports token:null — so the fake
// keys off `claimed`, NOT `hasToken`. If it keyed off hasToken it would hand the
// provisioner a signal the real API never provides.
func (f *fakeFleetAPI) GetDeployment(_ context.Context, _, _, id string) (*aura.DetailedDeployment, error) {
	d := &aura.DetailedDeployment{ID: id, Name: "ns-cluster"}
	if f.claimed {
		d.Token = &aura.DeploymentToken{
			AutoRotate:   true,
			CreationTime: "2026-07-01T00:00:00Z",
			ExpiryTime:   "2026-10-01T00:00:00Z",
		}
	}
	return d, nil
}

func (f *fakeFleetAPI) CreateDeployment(_ context.Context, _, _, name string) (string, error) {
	f.createCalled = true
	f.createdName = name
	return "dep-new", nil
}

func (f *fakeFleetAPI) DeleteDeployment(_ context.Context, _, _, _ string) error {
	f.delDepCalled = true
	return nil
}

// POST and PATCH are STRICTLY COMPLEMENTARY on the live API, and each signals
// the wrong state with an unhelpful HTTP 500 rather than a conflict:
//
//	POST  works only when NO token exists
//	PATCH works only when a token DOES exist
//
// Modelling that here is the whole point — the provisioner's create-vs-rotate
// logic exists to cope with it, and a fake that always succeeds would hide it.
func (f *fakeFleetAPI) CreateDeploymentToken(_ context.Context, _, _, _ string) (string, error) {
	f.mintCalled = true
	if f.hasToken {
		return "", &aura.APIError{StatusCode: 500, Message: "failed to create api key: no rows in result set"}
	}
	f.hasToken = true
	return "minted-token", nil
}

func (f *fakeFleetAPI) RotateDeploymentToken(_ context.Context, _, _, _ string) (string, error) {
	f.rotateCalled = true
	if !f.hasToken {
		return "", &aura.APIError{StatusCode: 500, Message: "failed to create api key: no rows in result set"}
	}
	return "rotated-token", nil
}

func (f *fakeFleetAPI) DeleteDeploymentToken(_ context.Context, _, _, _ string) error {
	f.delTokCalled = true
	return nil
}

func (f *fakeFleetAPI) ListDeploymentServers(_ context.Context, _, _, _ string) ([]aura.FleetServer, error) {
	if f.telemErr != nil {
		return nil, f.telemErr
	}
	return f.servers, nil
}

// Per-server on purpose: the shard / txn / lag / role / writer fields exist only
// on this endpoint, not the deployment-level databases list.
func (f *fakeFleetAPI) ListServerDatabases(_ context.Context, _, _, _, serverID string) ([]aura.FleetServerDatabase, error) {
	if f.telemErr != nil {
		return nil, f.telemErr
	}
	f.serverDBCalls = append(f.serverDBCalls, serverID)
	return f.databases, nil
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func fleetCluster(name string, provision *neo4jv1beta1.AuraFleetProvisionSpec) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AuraFleetManagement: &neo4jv1beta1.AuraFleetManagementSpec{
				Enabled:   true,
				Provision: provision,
			},
		},
	}
}

func fleetProvisionSpec() *neo4jv1beta1.AuraFleetProvisionSpec {
	return &neo4jv1beta1.AuraFleetProvisionSpec{
		CredentialsSecretRef: credRef(),
		OrganizationID:       "org-1",
		ProjectID:            "proj-1",
		TokenPolicy:          "CreateIfMissing",
		ManagementPolicies:   []string{"*"},
	}
}

// runFleetProvision wires the shared helper against a fake k8s client + fake API.
func runFleetProvision(
	t *testing.T, cl *neo4jv1beta1.Neo4jEnterpriseCluster, api *fakeFleetAPI, extra ...client.Object,
) (*fleetProvisioner, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	scheme := auraTestScheme(t)
	// newAuraFakeClient already seeds the shared Aura credentials Secret.
	objs := append([]client.Object{cl}, extra...)
	c := newAuraFakeClient(t, scheme, objs...)

	r := &Neo4jEnterpriseClusterReconciler{
		Client:             c,
		Scheme:             scheme,
		Recorder:           record.NewFakeRecorder(50),
		FleetClientFactory: func(auraCredentials) auraFleetAPI { return api },
	}
	fp := r.newFleetProvisioner()
	if err := fp.reconcileAuraFleetProvision(context.Background(), cl); err != nil {
		t.Fatalf("reconcileAuraFleetProvision returned an error; Phase 0 must always be non-fatal: %v", err)
	}
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: cl.Name}, got); err != nil {
		t.Fatalf("Get cluster: %v", err)
	}
	fp.Client = c
	return fp, got
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Happy path: register the deployment, mint a token, store it in a Secret.
func TestFleetProvision_CreatesDeploymentAndMintsToken(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	api := &fakeFleetAPI{}
	fp, got := runFleetProvision(t, cl, api)

	if !api.createCalled {
		t.Fatal("expected the deployment to be registered")
	}
	// Default name is "<namespace>-<name>" capped at 30 chars, so two same-named
	// clusters in different namespaces cannot collide in one Aura project.
	if want := testNS + "-cluster"; api.createdName != want {
		t.Errorf("deployment name = %q, want %q", api.createdName, want)
	}
	if !api.mintCalled {
		t.Error("expected a token to be minted")
	}
	if api.rotateCalled {
		t.Error("must not rotate when the deployment had no token")
	}

	// The annotation is the idempotency guard.
	if id := got.Annotations[AuraFleetDeploymentAnnotation]; id != "dep-new" {
		t.Errorf("deployment annotation = %q, want dep-new", id)
	}

	tok, err := fp.readTokenSecret(context.Background(), testNS, "cluster-aura-fleet-token")
	if err != nil || tok != "minted-token" {
		t.Errorf("token Secret = %q, err=%v; want the minted token", tok, err)
	}
	if got.Status.AuraFleetManagement == nil || !got.Status.AuraFleetManagement.Provisioned {
		t.Errorf("status.provisioned not set: %+v", got.Status.AuraFleetManagement)
	}
	// Token metadata is absent until a DBMS CLAIMS the token — Aura reports
	// token:null for a freshly minted one. Expecting it here would encode a
	// behaviour the real API does not have.
	if got.Status.AuraFleetManagement.TokenExpiryTime != nil {
		t.Error("tokenExpiryTime must stay empty until the token is claimed")
	}
}

// With the annotation already set, we must not list or re-create.
func TestFleetProvision_AnnotationPreventsDuplicateDeployment(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-existing"}
	api := &fakeFleetAPI{}
	_, _ = runFleetProvision(t, cl, api)

	if api.createCalled {
		t.Error("must NOT create a second deployment when the annotation pins one")
	}
	if api.listCalled {
		t.Error("must not need to list when the annotation pins a deployment")
	}
}

// An existing deployment with a matching name is adopted, not duplicated.
func TestFleetProvision_AdoptsDeploymentByName(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	api := &fakeFleetAPI{
		deployments: []aura.Deployment{{ID: "dep-found", Name: testNS + "-cluster"}},
	}
	_, got := runFleetProvision(t, cl, api)

	if api.createCalled {
		t.Error("must adopt the same-named deployment instead of creating another")
	}
	if id := got.Annotations[AuraFleetDeploymentAnnotation]; id != "dep-found" {
		t.Errorf("annotation = %q, want dep-found", id)
	}
}

// THE safety rule: a token that has already been registered must never be
// rotated just because the Secret went missing — rotating would invalidate the
// DBMS's working registration, and the old token can never be recovered.
func TestFleetProvision_RefusesToRotateARegisteredToken(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
	cl.Status.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementStatus{Registered: true}
	api := &fakeFleetAPI{hasToken: true} // Aura holds a token; no Secret exists

	_, got := runFleetProvision(t, cl, api)

	if api.rotateCalled {
		t.Fatal("must NOT rotate a token that has already been registered successfully")
	}
	// POST *is* attempted, deliberately: it is the only way to probe whether a
	// token exists (GET cannot tell us, and POST is the non-destructive half).
	// Its failure is what proves a token is already there.
	if !api.mintCalled {
		t.Error("expected the POST probe to be attempted before deciding to refuse")
	}
	msg := ""
	if got.Status.AuraFleetManagement != nil {
		msg = got.Status.AuraFleetManagement.Message
	}
	if msg == "" {
		t.Error("expected an explanatory status message about the unrecoverable token")
	}
	if got.Status.AuraFleetManagement.Provisioned {
		t.Error("provisioned must be false while the token Secret is missing")
	}
}

// tokenPolicy: Rotate is the explicit opt-in to break the existing registration.
func TestFleetProvision_RotatePolicyReplacesTheToken(t *testing.T) {
	p := fleetProvisionSpec()
	p.TokenPolicy = "Rotate"
	cl := fleetCluster("cluster", p)
	cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
	cl.Status.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementStatus{Registered: true}
	api := &fakeFleetAPI{hasToken: true}

	fp, _ := runFleetProvision(t, cl, api)

	if !api.rotateCalled {
		t.Fatal("tokenPolicy=Rotate must rotate when the Secret is missing")
	}
	tok, _ := fp.readTokenSecret(context.Background(), testNS, "cluster-aura-fleet-token")
	if tok != "rotated-token" {
		t.Errorf("token Secret = %q, want rotated-token", tok)
	}
}

// Never-registered + Aura already holds a token: nothing works today, so
// replacing it costs nothing and is the only way forward.
func TestFleetProvision_ReplacesUnusableTokenWhenNeverRegistered(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
	api := &fakeFleetAPI{hasToken: true} // registered == false (no status)

	_, got := runFleetProvision(t, cl, api)

	if !api.rotateCalled {
		t.Error("expected the unusable token to be replaced when it was never registered")
	}
	if got.Status.AuraFleetManagement == nil || !got.Status.AuraFleetManagement.Provisioned {
		t.Error("expected provisioned=true after storing a usable token")
	}
}

// An existing, non-empty Secret means there is nothing to do — never re-mint.
func TestFleetProvision_ExistingSecretIsLeftAlone(t *testing.T) {
	cl := fleetCluster("cluster", fleetProvisionSpec())
	cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-aura-fleet-token", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("already-here")},
	}
	api := &fakeFleetAPI{hasToken: true}

	fp, _ := runFleetProvision(t, cl, api, existing)

	if api.mintCalled || api.rotateCalled {
		t.Error("must not touch the token when a usable Secret already exists")
	}
	tok, _ := fp.readTokenSecret(context.Background(), testNS, "cluster-aura-fleet-token")
	if tok != "already-here" {
		t.Errorf("token = %q, want the pre-existing value untouched", tok)
	}
}

// Telemetry is opt-in, bounded, and strictly non-fatal.
func TestFleetProvision_Telemetry(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		cl := fleetCluster("cluster", fleetProvisionSpec())
		api := &fakeFleetAPI{servers: []aura.FleetServer{{ID: "s0", Name: "s0"}}}
		_, got := runFleetProvision(t, cl, api)
		if got.Status.AuraFleetManagement.Servers != nil {
			t.Error("telemetry must not be collected unless collectTelemetry is true")
		}
	})

	t.Run("collected when enabled", func(t *testing.T) {
		p := fleetProvisionSpec()
		p.CollectTelemetry = true
		cl := fleetCluster("cluster", p)
		api := &fakeFleetAPI{
			servers: []aura.FleetServer{{
				Name: "server-0", ID: "srv-1", ModeConstraint: "PRIMARY", PluginVersion: "1.2.3",
				License: &aura.License{State: "VALID", Type: "COMMERCIAL"},
			}},
			databases: []aura.FleetServerDatabase{{
				Name: "neo4j", Writer: true, LastCommittedTxn: 42,
				PropertyShards: []string{"p1", "p2"},
			}},
		}
		_, got := runFleetProvision(t, cl, api)
		st := got.Status.AuraFleetManagement
		if len(st.Servers) != 1 || st.Servers[0].ModeConstraint != "PRIMARY" {
			t.Fatalf("servers = %+v", st.Servers)
		}
		if st.Servers[0].LicenseState != "VALID" || st.Servers[0].LicenseType != "COMMERCIAL" {
			t.Errorf("license not flattened into status: %+v", st.Servers[0])
		}
		if len(st.Databases) != 1 || !st.Databases[0].Writer || st.Databases[0].LastCommittedTxn != 42 {
			t.Fatalf("databases = %+v", st.Databases)
		}
		if len(st.Databases[0].PropertyShards) != 2 {
			t.Errorf("propertyShards = %v", st.Databases[0].PropertyShards)
		}
		if st.ServerCount != 1 || st.DatabaseCount != 1 {
			t.Errorf("counts = %d/%d, want 1/1", st.ServerCount, st.DatabaseCount)
		}
	})

	t.Run("bounded", func(t *testing.T) {
		p := fleetProvisionSpec()
		p.CollectTelemetry = true
		cl := fleetCluster("cluster", p)
		api := &fakeFleetAPI{}
		for i := 0; i < maxFleetTelemetryItems+7; i++ {
			api.servers = append(api.servers, aura.FleetServer{ID: "s", Name: "s"})
		}
		_, got := runFleetProvision(t, cl, api)
		st := got.Status.AuraFleetManagement
		if len(st.Servers) != maxFleetTelemetryItems {
			t.Errorf("servers truncated to %d, want %d", len(st.Servers), maxFleetTelemetryItems)
		}
		if st.ServerCount != maxFleetTelemetryItems+7 {
			t.Errorf("serverCount = %d, want the untruncated total %d", st.ServerCount, maxFleetTelemetryItems+7)
		}
	})

	t.Run("failure is non-fatal", func(t *testing.T) {
		p := fleetProvisionSpec()
		p.CollectTelemetry = true
		cl := fleetCluster("cluster", p)
		api := &fakeFleetAPI{telemErr: context.DeadlineExceeded}
		// runFleetProvision already fails the test if an error is returned.
		_, got := runFleetProvision(t, cl, api)
		st := got.Status.AuraFleetManagement
		if st.TelemetryError == "" {
			t.Error("expected telemetryError to record the failure")
		}
		if !st.Provisioned {
			t.Error("a telemetry failure must not undo provisioning")
		}
	})
}

// Provisioning is a no-op unless both enabled and provision are set.
func TestFleetProvision_NoOpWithoutProvision(t *testing.T) {
	cl := fleetCluster("cluster", nil) // enabled, but no provision block
	api := &fakeFleetAPI{}
	_, got := runFleetProvision(t, cl, api)
	if api.listCalled || api.createCalled || api.mintCalled {
		t.Error("must be a no-op when spec.provision is absent (tokenSecretRef flow)")
	}
	if got.Status.AuraFleetManagement != nil && got.Status.AuraFleetManagement.Provisioned {
		t.Error("must not claim provisioned without a provision block")
	}
}

// Deprovision honours deletionPolicy and revokes the token before unregistering.
func TestFleetProvision_Deprovision(t *testing.T) {
	t.Run("orphan default leaves Aura alone", func(t *testing.T) {
		p := fleetProvisionSpec()
		p.DeletionPolicy = "Orphan"
		cl := fleetCluster("cluster", p)
		cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
		api := &fakeFleetAPI{}
		fp, _ := runFleetProvision(t, cl, api)
		fp.deprovisionAuraFleet(context.Background(), cl)
		if api.delDepCalled || api.delTokCalled {
			t.Error("Orphan must not unregister the Aura deployment")
		}
	})

	t.Run("delete revokes token then unregisters", func(t *testing.T) {
		p := fleetProvisionSpec()
		p.DeletionPolicy = "Delete"
		cl := fleetCluster("cluster", p)
		cl.Annotations = map[string]string{AuraFleetDeploymentAnnotation: "dep-1"}
		api := &fakeFleetAPI{}
		fp, _ := runFleetProvision(t, cl, api)
		fp.deprovisionAuraFleet(context.Background(), cl)
		if !api.delTokCalled {
			t.Error("must revoke the token so no usable credential is left behind")
		}
		if !api.delDepCalled {
			t.Error("must unregister the deployment")
		}
	})
}

// fleetDeploymentName caps at the API's 30-character limit.
func TestFleetDeploymentNameCapped(t *testing.T) {
	p := &neo4jv1beta1.AuraFleetProvisionSpec{}
	got := fleetDeploymentName(p, "a-very-long-namespace-name-here", "and-a-long-cluster-name")
	if len(got) > 30 {
		t.Errorf("name = %q (%d chars), must be capped at 30 — the API rejects longer", got, len(got))
	}
	explicit := &neo4jv1beta1.AuraFleetProvisionSpec{DeploymentName: "chosen"}
	if fleetDeploymentName(explicit, "ns", "cl") != "chosen" {
		t.Error("an explicit deploymentName must win")
	}
}
