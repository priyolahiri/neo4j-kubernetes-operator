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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

const (
	// AuraExternalIDAnnotation stores the Aura instance ID as the external-name:
	// the operator's source of truth for idempotent create + adopt. It is
	// written before status so a crash between create and status can't produce a
	// duplicate paid instance.
	AuraExternalIDAnnotation = "neo4j.com/external-instance-id"

	// defaultAuraRatePerMinute is the conservative Aura API rate limit (trial
	// keys are 25/min; paid are 125/min). We default to the safe floor.
	defaultAuraRatePerMinute = 25
)

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
	})
	auraClients[key] = cl
	return cl
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

// auraCondStatus maps a boolean to a Kubernetes condition status.
func auraCondStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
