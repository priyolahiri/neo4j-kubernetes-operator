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

package main

// Cloud-substrate checks for preflight.
//
// The install of the operator is identical on GKE, AKS and EKS — kubectl has
// already authenticated by the time this binary runs, and there is no
// cloud-specific install step to perform. What genuinely differs per cloud is
// the SUBSTRATE the operator's objects land on, and the sharpest example is
// the CCDR proxy's private load balancer.
//
// The operator asks for a private LB by emitting the union of five provider
// annotations, because a controller cannot know which cloud it is running on
// and a provider ignores keys it does not recognise. That is the right call
// in the operator. But it means the request is only honoured on a provider
// the union happens to cover: on any other, `loadBalancerInternal: true` is
// accepted, reported, and silently does nothing — leaving a public load
// balancer in front of the cluster's transaction-shipping port.
//
// That is not hypothetical. The field defaulted to true, documented itself as
// the main mitigation against public exposure, and was computed and thrown
// away for the whole of its existence until it was fixed. The annotations now
// land; whether the cloud in front of you reads any of them is the part no
// unit test can answer, and this is where it gets answered.
//
// Still shape, not reachability: this reads Node and Service objects. It does
// not dial the load balancer or resolve its hostname.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// cloudProvider is what a node's spec.providerID says it is running on.
type cloudProvider struct {
	id   string // "aws", "gce", "azure", "kind", … ; "" when undetermined
	name string // human name for output
}

// providerIDPrefixes maps the scheme of Node.spec.providerID to a provider.
// The kubelet sets it from the cloud provider, so it is the most reliable
// in-cluster signal — more so than node labels, which a user can relabel.
var providerIDPrefixes = []struct {
	prefix string
	id     string
	name   string
}{
	{"aws://", "aws", "AWS (EKS or self-managed)"},
	{"gce://", "gce", "Google Cloud (GKE)"},
	{"azure://", "azure", "Azure (AKS)"},
	{"kind://", "kind", "Kind"},
	{"openstack://", "openstack", "OpenStack"},
	{"vsphere://", "vsphere", "vSphere"},
	{"digitalocean://", "digitalocean", "DigitalOcean"},
	{"oci://", "oci", "Oracle Cloud"},
	{"ibm://", "ibm", "IBM Cloud"},
	{"hcloud://", "hcloud", "Hetzner Cloud"},
	{"equinixmetal://", "equinixmetal", "Equinix Metal"},
	{"linode://", "linode", "Linode"},
	{"scaleway://", "scaleway", "Scaleway"},
}

// internalLBAnnotationOwners says which provider reads each of the annotations
// the operator emits for a private load balancer.
//
// Derived from the operator's own list at call time rather than copied, so a
// key added there without a row here is reported as a gap in this table
// instead of silently widening the set of providers we claim to cover.
var internalLBAnnotationOwners = map[string]string{
	"service.beta.kubernetes.io/aws-load-balancer-internal":   "aws",
	"service.beta.kubernetes.io/aws-load-balancer-scheme":     "aws",
	"service.beta.kubernetes.io/azure-load-balancer-internal": "azure",
	"networking.gke.io/load-balancer-type":                    "gce",
	"cloud.google.com/load-balancer-type":                     "gce",
}

// detectCloudProvider reads one node's providerID. Any node will do: a cluster
// whose nodes span providers is not a configuration the operator supports, and
// reading one keeps this to a single cheap List.
func detectCloudProvider(ctx context.Context, c client.Client) cloudProvider {
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes, client.Limit(1)); err != nil || len(nodes.Items) == 0 {
		return cloudProvider{}
	}
	pid := nodes.Items[0].Spec.ProviderID
	for _, p := range providerIDPrefixes {
		if strings.HasPrefix(pid, p.prefix) {
			return cloudProvider{id: p.id, name: p.name}
		}
	}
	if pid == "" {
		return cloudProvider{}
	}
	// A providerID we do not recognise still tells us it is not one of the
	// three the operator's annotations cover, which is the answer that matters.
	scheme := pid
	if i := strings.Index(pid, "://"); i > 0 {
		scheme = pid[:i]
	}
	return cloudProvider{id: scheme, name: scheme}
}

// providersCoveredByInternalLBAnnotations returns the providers the operator's
// current annotation set can actually reach, and any key it emits that this
// file has no owner for.
func providersCoveredByInternalLBAnnotations() (covered map[string]bool, unknownKeys []string) {
	covered = map[string]bool{}
	for key := range resources.CCDRInternalLoadBalancerAnnotations() {
		owner, ok := internalLBAnnotationOwners[key]
		if !ok {
			unknownKeys = append(unknownKeys, key)
			continue
		}
		covered[owner] = true
	}
	sort.Strings(unknownKeys)
	return covered, unknownKeys
}

// checkCCDRProxyExposure is the cloud-substrate check.
//
// Three questions, in the order they bite:
//  1. Did the user turn the private LB off? Then the transaction port is
//     public on purpose, and should be said out loud once.
//  2. Will the private-LB request be understood by THIS cloud?
//  3. If the Service already exists, what address did it actually get?
//
// (3) is the one that settles it. An annotation table is a claim about how a
// provider behaves; the address the provider assigned is what it did.
func checkCCDRProxyExposure(ctx context.Context, c client.Client, ns string,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster) []symptom {
	if cluster.Spec.CrossClusterReplication == nil || !cluster.Spec.CrossClusterReplication.Enabled {
		return nil
	}

	if !resources.CCDRProxyLoadBalancerInternalEffective(cluster) {
		return []symptom{{
			mark:    markWarning,
			subject: "ccdr proxy",
			what:    "loadBalancerInternal is false, so the load balancer will be public",
			action: "Every server's transaction-shipping port becomes reachable from the " +
				"internet. That is a deliberate setting, so this is not an error — but " +
				"restrict it with spec.crossClusterReplication.annotations (a source-range " +
				"annotation your provider reads) or a NetworkPolicy, and require TLS.",
		}}
	}

	var symptoms []symptom
	provider := detectCloudProvider(ctx, c)
	covered, unknownKeys := providersCoveredByInternalLBAnnotations()

	if len(unknownKeys) > 0 {
		// The operator grew a key this file does not know the owner of. Do not
		// guess it widens coverage — say the table needs updating.
		symptoms = append(symptoms, symptom{
			mark:    markWarning,
			subject: "ccdr proxy",
			what: "the operator emits private-LB annotation(s) this check does not recognise: " +
				strings.Join(unknownKeys, ", "),
			action: "Add them to internalLBAnnotationOwners in preflight_cloud.go so the " +
				"provider-coverage check below accounts for them.",
		})
	}

	switch {
	case provider.id == "":
		symptoms = append(symptoms, symptom{
			mark:    markWarning,
			subject: "ccdr proxy",
			what:    "the cloud provider could not be determined, so private-LB support is unknown",
			action: "No node carries a providerID. Confirm your load-balancer controller reads " +
				"one of: " + strings.Join(sortedKeys(resources.CCDRInternalLoadBalancerAnnotations()), ", ") +
				" — otherwise loadBalancerInternal has no effect and the proxy will be public.",
		})
	case provider.id == "kind":
		symptoms = append(symptoms, symptom{
			mark:    markWaiting,
			subject: "ccdr proxy",
			what:    "Kind has no cloud load-balancer controller",
			action: "The Service stays Pending without MetalLB or an equivalent. That is a " +
				"development limitation, not an exposure risk — nothing is reachable from " +
				"outside the host.",
		})
	case !covered[provider.id]:
		symptoms = append(symptoms, symptom{
			mark:    markProblem,
			subject: "ccdr proxy",
			what: fmt.Sprintf("loadBalancerInternal is true, but %s reads none of the annotations the operator emits",
				provider.name),
			action: "The private-LB request will be silently ignored and the load balancer " +
				"will be PUBLIC, in front of every server's transaction-shipping port. Add " +
				"your provider's own internal-LB annotation under " +
				"spec.crossClusterReplication.annotations — it is applied after the " +
				"operator's set and wins on any shared key.",
		})
	}

	symptoms = append(symptoms, checkCCDRProxyAssignedAddress(ctx, c, ns, cluster, provider)...)
	return symptoms
}

// checkCCDRProxyAssignedAddress reads the address the provider actually
// assigned. This is the only check here that can prove exposure rather than
// predict it, so it runs whatever the annotation table said.
func checkCCDRProxyAssignedAddress(ctx context.Context, c client.Client, ns string,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster, provider cloudProvider) []symptom {
	name := resources.CCDRProxyName(cluster.Name)
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &svc); err != nil {
		// Not yet created is the normal case before apply, and says nothing.
		if apierrors.IsNotFound(err) {
			return nil
		}
		return []symptom{{
			mark: markWarning, subject: "service " + name,
			what: "could not be read, so the address it was assigned is unknown", detail: err.Error(),
		}}
	}

	var symptoms []symptom
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		switch {
		case ing.IP != "":
			ip := net.ParseIP(ing.IP)
			if ip == nil || isPrivateIP(ip) {
				continue
			}
			symptoms = append(symptoms, symptom{
				mark:    markProblem,
				subject: "service " + name,
				what: fmt.Sprintf("was assigned the PUBLIC address %s despite loadBalancerInternal: true",
					ing.IP),
				action: "The private-LB annotations did not take effect on " + providerLabel(provider) +
					". Every server's transaction-shipping port is reachable from the internet " +
					"right now. Add your provider's internal-LB annotation under " +
					"spec.crossClusterReplication.annotations, or delete the Service and the " +
					"cluster's CCDR exposure until it is fixed.",
			})
		case ing.Hostname != "":
			// AWS hands out a hostname. Resolving it would be reachability,
			// which this command does not do — so report what is known rather
			// than implying the address was checked.
			symptoms = append(symptoms, symptom{
				mark:    markWarning,
				subject: "service " + name,
				what:    "was assigned the hostname " + ing.Hostname + ", which is not resolved here",
				action: "preflight reads objects and does not resolve DNS. Confirm it points at a " +
					"private address: dig +short " + ing.Hostname,
			})
		}
	}
	return symptoms
}

// isPrivateIP reports whether an address is one a load balancer can only be
// reached at from inside the network — RFC1918, the shared CGNAT range,
// loopback and link-local, and their IPv6 equivalents.
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsPrivate() {
		return true
	}
	// 100.64.0.0/10 (RFC 6598) is not covered by IsPrivate but is never
	// internet-routable; several providers use it for internal LBs.
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127
	}
	return false
}

func providerLabel(p cloudProvider) string {
	if p.name == "" {
		return "this cluster's provider"
	}
	return p.name
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
