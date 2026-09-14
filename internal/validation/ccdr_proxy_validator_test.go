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

package validation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func certManagerTLS() *neo4jv1beta1.TLSSpec {
	return &neo4jv1beta1.TLSSpec{
		Mode:      resources.CertManagerMode,
		IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
	}
}

func clusterWithCCDR(enabled bool, tls *neo4jv1beta1.TLSSpec) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			TLS:                     tls,
			CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationSpec{Enabled: enabled},
		},
	}
}

// The proxy publishes Neo4j's transaction-shipping port through a load
// balancer and authenticates nothing itself — HAProxy in `mode tcp`
// terminates nothing and inspects nothing. So the cluster SSL policy's
// client_auth=REQUIRE is the only access control that port has, and without
// spec.tls it has none: anyone who can reach the load balancer's address can
// stream the whole database, in cleartext.
//
// There is no deployment in which that is the intended outcome, so it is
// refused rather than warned about. The field is unreleased (absent from
// v1.14.0), so nothing in the wild is configured this way — refusing before
// the API is public is the cheapest this decision will ever be.
func TestValidateCrossClusterReplication_RequiresTLS(t *testing.T) {
	path := field.NewPath("spec", "crossClusterReplication")

	t.Run("enabled with no spec.tls is refused", func(t *testing.T) {
		errs := validateCrossClusterReplication(clusterWithCCDR(true, nil), path)
		require.Len(t, errs, 1)
		assert.Equal(t, "spec.crossClusterReplication.enabled", errs[0].Field)
		// The message has to say what to do, not just that it said no.
		assert.Contains(t, errs[0].Detail, "spec.tls.mode=cert-manager")
		assert.Contains(t, errs[0].Detail, "additionalClusterTrustCAs")
	})

	t.Run("enabled with a non-cert-manager tls mode is refused", func(t *testing.T) {
		tls := &neo4jv1beta1.TLSSpec{Mode: "manual"}
		require.Len(t, validateCrossClusterReplication(clusterWithCCDR(true, tls), path), 1)
	})

	t.Run("enabled with cert-manager TLS is allowed", func(t *testing.T) {
		assert.Empty(t, validateCrossClusterReplication(clusterWithCCDR(true, certManagerTLS()), path))
	})

	// strictPeerValidation false is trust_all=true: encrypted, every peer
	// certificate accepted. Weaker, and Neo4j documents it as debugging-only —
	// but a deliberate opt-out with a narrow legitimate use, so it is
	// permitted and reported on the CrossClusterProxySecure condition instead.
	// The line is drawn at "no TLS at all", the case with no defensible
	// reading.
	t.Run("strictPeerValidation false is permitted, not refused", func(t *testing.T) {
		tls := certManagerTLS()
		off := false
		tls.StrictPeerValidation = &off
		assert.Empty(t, validateCrossClusterReplication(clusterWithCCDR(true, tls), path))
	})

	t.Run("a disabled proxy needs no TLS", func(t *testing.T) {
		assert.Empty(t, validateCrossClusterReplication(clusterWithCCDR(false, nil), path))
	})

	t.Run("no crossClusterReplication block at all is fine", func(t *testing.T) {
		assert.Empty(t, validateCrossClusterReplication(&neo4jv1beta1.Neo4jEnterpriseCluster{}, path))
	})
}

// additionalClusterTrustCAs is read ONLY when tls.mode is cert-manager, so
// setting it without the mode silently drops every peer CA while the field
// looks configured. The published network-mode example had exactly that shape.
func TestValidateClusterTrustCAsNeedTLS(t *testing.T) {
	path := field.NewPath("spec", "tls")
	withCAs := func(tls *neo4jv1beta1.TLSSpec) *neo4jv1beta1.Neo4jEnterpriseCluster {
		if tls != nil {
			tls.AdditionalClusterTrustCAs = []neo4jv1beta1.TrustedCASecret{{Name: "peer-ca"}}
		}
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{TLS: tls},
		}
	}

	t.Run("peer CAs without a TLS mode are refused", func(t *testing.T) {
		errs := validateClusterTrustCAsNeedTLS(withCAs(&neo4jv1beta1.TLSSpec{}), path)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Detail, "cert-manager")
	})

	t.Run("peer CAs with cert-manager are fine", func(t *testing.T) {
		assert.Empty(t, validateClusterTrustCAsNeedTLS(withCAs(certManagerTLS()), path))
	})

	t.Run("no peer CAs, nothing to say", func(t *testing.T) {
		assert.Empty(t, validateClusterTrustCAsNeedTLS(withCAs(nil), path))
	})
}

// The rule has to hold through the real entry point, not just its own unit:
// the reconciler and `kubectl neo4j validate` both go through validateCluster,
// and a check that is never wired in is a check that does not exist.
func TestValidateCluster_RefusesCCDRProxyWithoutTLS(t *testing.T) {
	cluster := clusterWithCCDR(true, nil)
	cluster.Name = "prod"
	cluster.Spec.Topology = neo4jv1beta1.TopologyConfiguration{Servers: 3}

	errs := NewClusterValidator(nil).ValidateCreateWithWarnings(t.Context(), cluster).Errors

	var found bool
	for _, e := range errs {
		if strings.Contains(e.Field, "crossClusterReplication") {
			found = true
		}
	}
	assert.True(t, found,
		"validateCluster must reject an untls'd CCDR proxy — got %v", errs)
}
