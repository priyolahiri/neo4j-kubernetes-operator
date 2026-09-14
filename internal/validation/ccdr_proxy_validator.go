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
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// validateCrossClusterReplication refuses the exposure proxy on a cluster with
// no TLS.
//
// spec.crossClusterReplication puts Neo4j's transaction-shipping port behind a
// type: LoadBalancer Service. The proxy is HAProxy in `mode tcp` — it
// terminates nothing, inspects nothing and authenticates nothing — so the
// cluster SSL policy's client_auth=REQUIRE is the ONLY access control in front
// of that port. Without spec.tls there is none at all: anyone who can reach
// the load balancer's address can stream the entire database, in cleartext.
//
// That is not a configuration worth supporting. There is no deployment in
// which publishing an unauthenticated database replication port is the
// intended outcome, and the one plausible alternative — terminating TLS at the
// load balancer — does not apply here, because the catchup protocol's
// authentication IS the certificate exchange the SSL policy performs. A proxy
// that terminated it would be authenticating on the database's behalf without
// the database ever knowing who connected.
//
// So this is an error, not the warning it was until 2026-09-14. The field is
// unreleased (absent from v1.14.0), so nothing in the wild is configured this
// way and no one is broken by refusing it. Refusing before it is public is the
// cheapest this decision will ever be.
//
// What is NOT refused: spec.tls set with strictPeerValidation false. That is
// encryption without peer authentication — weaker, and Neo4j itself documents
// it as debugging-only — but it is a deliberate opt-out with a legitimate
// narrow use, and the cluster reports CrossClusterProxySecure=False to say so.
// The line is drawn at "no TLS at all", which is the case with no defensible
// reading.
func validateCrossClusterReplication(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if cluster.Spec.CrossClusterReplication == nil || !cluster.Spec.CrossClusterReplication.Enabled {
		return allErrs
	}
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == resources.CertManagerMode {
		return allErrs
	}

	allErrs = append(allErrs, field.Forbidden(
		path.Child("enabled"),
		"requires spec.tls.mode=cert-manager. The proxy publishes Neo4j's "+
			"transaction-shipping port through a load balancer and authenticates "+
			"nothing itself — it is a TCP passthrough — so the cluster SSL policy is "+
			"the only access control in front of that port. Without TLS anyone who "+
			"can reach the load balancer can stream the database in cleartext. Set "+
			"spec.tls.mode=cert-manager with an issuerRef (strictPeerValidation "+
			"defaults to true, which is what requires a peer certificate), and give "+
			"each cluster the other's CA in spec.tls.additionalClusterTrustCAs",
	))
	return allErrs
}

// validateClusterTrustCAsNeedTLS catches the other half of the same mistake:
// spec.tls.additionalClusterTrustCAs is read ONLY when tls.mode is
// cert-manager, so setting it without the mode silently does nothing. The
// published network-mode example had exactly that shape, which made the field
// look configured while the peer CAs were dropped.
func validateClusterTrustCAsNeedTLS(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if cluster.Spec.TLS == nil || len(cluster.Spec.TLS.AdditionalClusterTrustCAs) == 0 {
		return allErrs
	}
	if cluster.Spec.TLS.Mode == resources.CertManagerMode {
		return allErrs
	}
	allErrs = append(allErrs, field.Invalid(
		path.Child("additionalClusterTrustCAs"),
		cluster.Spec.TLS.AdditionalClusterTrustCAs,
		"has no effect unless spec.tls.mode is cert-manager: peer CAs are projected "+
			"into the cluster SSL policy's trust directory, which only exists when TLS "+
			"is configured. Set spec.tls.mode=cert-manager, or remove this field",
	))
	return allErrs
}
