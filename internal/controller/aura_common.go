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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/metrics"
)

const (
	// AuraExternalIDAnnotation stores the Aura instance ID as the external-name:
	// the operator's source of truth for idempotent create + adopt. It is
	// written before status so a crash between create and status can't produce a
	// duplicate paid instance.
	AuraExternalIDAnnotation = "neo4j.com/external-instance-id"

	// AuraExternalCMKAnnotation stores the Aura customer-managed-key ID as the
	// external-name for an AuraCustomerManagedKey: the operator's source of truth
	// for idempotent create + adopt, written before status so a crash between
	// create and status cannot register a duplicate key.
	AuraExternalCMKAnnotation = "neo4j.com/external-cmk-id"

	// AuraExternalIPFilterAnnotation stores the Aura v2beta1 IP-filter ID as the
	// external-name for an AuraIPFilter (idempotent create + adopt).
	AuraExternalIPFilterAnnotation = "neo4j.com/external-ipfilter-id"

	// defaultAuraRatePerMinute is the conservative Aura API rate limit (trial
	// keys are 25/min; paid are 125/min). We default to the safe floor.
	defaultAuraRatePerMinute = 25

	// AuraPausedAnnotation, when "true", suspends reconciliation of a resource
	// (including deletion) — the operator observes nothing and takes no action
	// until it is cleared. Standard cloud-native pause knob for incident
	// response / manual intervention without deleting the CR.
	AuraPausedAnnotation = "neo4j.com/paused"
)

// Management policy actions.
const (
	auraPolicyObserve = "Observe"
	auraPolicyCreate  = "Create"
	auraPolicyUpdate  = "Update"
	auraPolicyDelete  = "Delete"
)

// managementAllows reports whether an action is permitted by the resource's
// managementPolicies. Empty (or containing "*") means full management.
func managementAllows(policies []string, action string) bool {
	if len(policies) == 0 {
		return true
	}
	for _, p := range policies {
		if p == "*" || p == action {
			return true
		}
	}
	return false
}

// auraCredentials is the resolved information needed to build an Aura API client.
type auraCredentials struct {
	baseURL      string
	clientID     string
	clientSecret string
	perMinute    int
	projectID    string // default project (tenant_id), from the provider config
}

func (a auraCredentials) cacheKey() string {
	sum := sha256.Sum256([]byte(a.clientSecret))
	return a.baseURL + "|" + a.clientID + "|" + hex.EncodeToString(sum[:8])
}

var (
	auraClientsMu sync.Mutex
	auraClients   = map[string]*aura.Client{}
)

// auraClientForCreds returns a client shared by all resources that resolve to
// the same credential (keyed by base URL + client ID + a hash of the secret so
// a rotated secret yields a fresh client). Sharing means one OAuth token cache
// and one rate-limit budget per credential, as intended by the provider-config
// design.
func auraClientForCreds(c auraCredentials) *aura.Client {
	key := c.cacheKey()
	auraClientsMu.Lock()
	defer auraClientsMu.Unlock()
	if cl, ok := auraClients[key]; ok {
		return cl
	}
	perMin := c.perMinute
	if perMin <= 0 {
		perMin = defaultAuraRatePerMinute
	}
	cl := aura.NewClient(aura.Config{
		BaseURL:      c.baseURL,
		ClientID:     c.clientID,
		ClientSecret: c.clientSecret,
		PerMinute:    perMin,
		// Feed operator metrics without coupling the aura package to Prometheus.
		Observer: func(operation string, duration time.Duration, err error) {
			metrics.RecordAuraAPICall(operation, duration, err == nil)
		},
	})
	auraClients[key] = cl
	return cl
}

// auraAPI is the subset of the Aura client the controllers depend on. Declaring
// it here (rather than using *aura.Client directly) lets tests inject a fake via
// each reconciler's ClientFactory, so controller envtests need no real Aura
// account. *aura.Client satisfies this interface.
type auraAPI interface {
	ListInstances(ctx context.Context, tenantID string) ([]aura.InstanceSummary, error)
	GetInstance(ctx context.Context, id string) (*aura.Instance, error)
	CreateInstance(ctx context.Context, req aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error)
	PatchInstance(ctx context.Context, id string, req aura.PatchInstanceRequest) (*aura.Instance, error)
	PauseInstance(ctx context.Context, id string) error
	ResumeInstance(ctx context.Context, id string) error
	UpgradeInstance(ctx context.Context, id string) error
	DeleteInstance(ctx context.Context, id string) error
	GetTenant(ctx context.Context, id string) (*aura.Tenant, error)
	CreateSnapshot(ctx context.Context, instanceID string) (*aura.Snapshot, error)
	GetSnapshot(ctx context.Context, instanceID, snapshotID string) (*aura.Snapshot, error)
	RestoreSnapshot(ctx context.Context, instanceID, snapshotID string) error
}

// auraCMKAPI is the subset of the Aura client the customer-managed-key
// controller depends on. It is a separate interface (not folded into auraAPI) so
// the instance/snapshot/restore test fakes need not implement CMK methods.
// *aura.Client satisfies this interface.
type auraCMKAPI interface {
	CreateCustomerManagedKey(ctx context.Context, req aura.CreateCMKRequest) (*aura.CustomerManagedKey, error)
	GetCustomerManagedKey(ctx context.Context, id string) (*aura.CustomerManagedKey, error)
	ListCustomerManagedKeys(ctx context.Context, tenantID string) ([]aura.CustomerManagedKey, error)
	DeleteCustomerManagedKey(ctx context.Context, id string) error
}

// auraClientFactory builds an auraAPI from resolved credentials; reconcilers
// hold one (nil → the real, shared cached client) so tests can inject a fake.
type auraClientFactory func(auraCredentials) auraAPI

func defaultAuraClientFactory(c auraCredentials) auraAPI { return auraClientForCreds(c) }

// resolveClient returns the factory's client, or the default shared client.
func resolveClient(factory auraClientFactory, c auraCredentials) auraAPI {
	if factory != nil {
		return factory(c)
	}
	return defaultAuraClientFactory(c)
}

// auraCMKClientFactory builds an auraCMKAPI from resolved credentials; the CMK
// reconciler holds one (nil → the real, shared cached client) so tests can
// inject a fake.
type auraCMKClientFactory func(auraCredentials) auraCMKAPI

func defaultAuraCMKClientFactory(c auraCredentials) auraCMKAPI { return auraClientForCreds(c) }

// resolveCMKClient returns the factory's CMK client, or the default shared client.
func resolveCMKClient(factory auraCMKClientFactory, c auraCredentials) auraCMKAPI {
	if factory != nil {
		return factory(c)
	}
	return defaultAuraCMKClientFactory(c)
}

// auraIPFilterAPI is the subset of the Aura v2beta1 client the IP-filter
// controller depends on. Separate interface so the other test fakes need not
// implement v2beta1 methods. *aura.Client satisfies it. BETA — see
// internal/aura/ipfilter_v2beta1.go.
type auraIPFilterAPI interface {
	CreateIPFilter(ctx context.Context, orgID, projectID string, req aura.CreateIPFilterRequest) (*aura.IPFilter, error)
	GetIPFilter(ctx context.Context, orgID, projectID, id string) (*aura.IPFilter, error)
	ListIPFilters(ctx context.Context, orgID, projectID string) ([]aura.IPFilter, error)
	UpdateIPFilter(ctx context.Context, orgID, projectID, id string, req aura.UpdateIPFilterRequest) (*aura.IPFilter, error)
	DeleteIPFilter(ctx context.Context, orgID, projectID, id string) error
}

// auraIPFilterClientFactory builds an auraIPFilterAPI from resolved credentials.
type auraIPFilterClientFactory func(auraCredentials) auraIPFilterAPI

func defaultAuraIPFilterClientFactory(c auraCredentials) auraIPFilterAPI {
	return auraClientForCreds(c)
}

// resolveIPFilterClient returns the factory's client, or the default shared client.
func resolveIPFilterClient(factory auraIPFilterClientFactory, c auraCredentials) auraIPFilterAPI {
	if factory != nil {
		return factory(c)
	}
	return defaultAuraIPFilterClientFactory(c)
}

// resolveAuraCredentials resolves API credentials from either a
// providerConfigRef (preferred: also carries baseURL + default project) or an
// inline credentialsSecretRef, reading the referenced Secret in the given
// namespace. Exactly one of the two refs should be non-nil (enforced by CEL on
// the resource spec).
func resolveAuraCredentials(
	ctx context.Context,
	k8s client.Client,
	namespace string,
	providerConfigRef *corev1.LocalObjectReference,
	inline *neo4jv1beta1.AuraCredentialsSecretRef,
) (auraCredentials, error) {
	var (
		secretRef neo4jv1beta1.AuraCredentialsSecretRef
		creds     auraCredentials
	)
	switch {
	case providerConfigRef != nil:
		pc := &neo4jv1beta1.AuraProviderConfig{}
		if err := k8s.Get(ctx, types.NamespacedName{Name: providerConfigRef.Name, Namespace: namespace}, pc); err != nil {
			return creds, fmt.Errorf("resolving providerConfigRef %q: %w", providerConfigRef.Name, err)
		}
		secretRef = pc.Spec.CredentialsSecretRef
		creds.baseURL = pc.Spec.BaseURL
		creds.projectID = pc.Spec.DefaultProjectID
	case inline != nil:
		secretRef = *inline
	default:
		return creds, fmt.Errorf("no Aura credentials: set spec.providerConfigRef or spec.credentialsSecretRef")
	}

	idKey := secretRef.ClientIDKey
	if idKey == "" {
		idKey = "clientId"
	}
	secretKey := secretRef.ClientSecretKey
	if secretKey == "" {
		secretKey = "clientSecret"
	}

	sec := &corev1.Secret{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: namespace}, sec); err != nil {
		return creds, fmt.Errorf("reading Aura credentials Secret %q: %w", secretRef.Name, err)
	}
	id := string(sec.Data[idKey])
	secret := string(sec.Data[secretKey])
	if id == "" || secret == "" {
		return creds, fmt.Errorf("credentials Secret %q is missing key %q and/or %q", secretRef.Name, idKey, secretKey)
	}
	creds.clientID = id
	creds.clientSecret = secret
	return creds, nil
}

// resolveInstanceCredsAndID fetches the referenced AuraInstance (same
// namespace) and returns its resolved credentials + external Aura ID. Callers
// (the snapshot and restore controllers, which act on an instance they don't
// own) build the client via their own ClientFactory so tests can inject a fake.
func resolveInstanceCredsAndID(
	ctx context.Context, k8s client.Client, namespace, instanceRef string,
) (auraCredentials, string, *neo4jv1beta1.AuraInstance, error) {
	inst := &neo4jv1beta1.AuraInstance{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: instanceRef, Namespace: namespace}, inst); err != nil {
		return auraCredentials{}, "", nil, fmt.Errorf("resolving instanceRef %q: %w", instanceRef, err)
	}
	creds, err := resolveAuraCredentials(ctx, k8s, namespace, inst.Spec.ProviderConfigRef, inst.Spec.CredentialsSecretRef)
	if err != nil {
		return auraCredentials{}, "", inst, err
	}
	externalID := inst.Annotations[AuraExternalIDAnnotation]
	if externalID == "" {
		externalID = inst.Status.InstanceID
	}
	if externalID == "" {
		return auraCredentials{}, "", inst, fmt.Errorf("AuraInstance %q has no external instance ID yet (not created/adopted)", instanceRef)
	}
	return creds, externalID, inst, nil
}

// auraConnectionSecretName returns the connection Secret name for an instance:
// spec.connectionSecretName, or "<name>-conn" by default. Package-level so the
// target resolver can find an Aura instance's admin credentials.
func auraConnectionSecretName(inst *neo4jv1beta1.AuraInstance) string {
	if inst.Spec.ConnectionSecretName != "" {
		return inst.Spec.ConnectionSecretName
	}
	return inst.Name + "-conn"
}

// auraCondStatus maps a boolean to a Kubernetes condition status.
func auraCondStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
