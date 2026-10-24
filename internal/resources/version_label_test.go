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

package resources

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestVersionLabelValue(t *testing.T) {
	for tag, want := range map[string]string{
		"2026.08.1-enterprise":                            "2026.08.1-enterprise",
		"2026.08.1-enterprise@sha256:" + hex(64):          "2026.08.1-enterprise",
		"5.26-enterprise":                                 "5.26-enterprise",
		"2026.08.1-enterprise-" + strings.Repeat("x", 60): ("2026.08.1-enterprise-" + strings.Repeat("x", 60))[:63],
		"2026.08.1-" + strings.Repeat("y", 52) + "-z":     "2026.08.1-" + strings.Repeat("y", 52), // the cut ends on '-'
		"": "",
	} {
		got := VersionLabelValue(tag)
		assert.Equal(t, want, got, tag)
		assert.Empty(t, validation.IsValidLabelValue(got), "%q is not a valid label value", got)
	}
}

func hex(n int) string { return strings.Repeat("0123456789abcdef", n/16+1)[:n] }

// Every object the operator labels with the image tag carries a valid label
// value, whatever spec.image.tag says: a tag pinned to its digest, which the
// image validation accepts, made the API server refuse the ConfigMap and the
// cluster went Failed at once (#562 walk).
func TestEveryObjectLabelledWithTheTagHasAValidLabel(t *testing.T) {
	for _, tag := range []string{
		"2026.08.1-enterprise@sha256:" + hex(64),
		"2026.08.1-enterprise-" + strings.Repeat("x", 60),
	} {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
				Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:  neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				TLS:      &neo4jv1beta1.TLSSpec{Mode: CertManagerMode, IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca", Kind: "ClusterIssuer"}},
				MCP: &neo4jv1beta1.MCPServerSpec{Enabled: true,
					Image: &neo4jv1beta1.ImageSpec{Repo: "mcp/neo4j", Tag: "v1.4.0@sha256:" + hex(64)}},
			},
		}
		sts := BuildServerStatefulSetForEnterprise(cluster)
		labelled := map[string]map[string]string{
			"StatefulSet":       sts.Labels,
			"pod template":      sts.Spec.Template.Labels,
			"ConfigMap":         BuildConfigMapForEnterprise(cluster).Labels,
			"headless Service":  BuildHeadlessServiceForEnterprise(cluster).Labels,
			"internals Service": BuildInternalsServiceForEnterprise(cluster).Labels,
			"client Service":    BuildClientServiceForEnterprise(cluster).Labels,
			"Certificate":       BuildCertificateForEnterprise(cluster).Labels,
			"ServiceAccount":    BuildDiscoveryServiceAccountForEnterprise(cluster).Labels,
			"MCP Deployment":    BuildMCPDeploymentForCluster(cluster).Labels,
			"MCP pod template":  BuildMCPDeploymentForCluster(cluster).Spec.Template.Labels,
			"MCP Service":       BuildMCPServiceForCluster(cluster).Labels,
		}
		for what, labels := range labelled {
			for key, value := range labels {
				assert.Empty(t, validation.IsValidLabelValue(value), "%s label %s=%q (tag %q)", what, key, value, tag)
			}
		}
		assert.Equal(t, VersionLabelValue(tag), sts.Labels["app.kubernetes.io/version"])
	}

	// A standalone's own objects carry no tag in a label; its MCP server's do.
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise@sha256:" + hex(64)},
			MCP: &neo4jv1beta1.MCPServerSpec{Enabled: true,
				Image: &neo4jv1beta1.ImageSpec{Repo: "mcp/neo4j", Tag: "v1.4.0@sha256:" + hex(64)}},
		},
	}
	deployment := BuildMCPDeploymentForStandalone(standalone)
	for what, labels := range map[string]map[string]string{
		"standalone MCP Deployment":   deployment.Labels,
		"standalone MCP pod template": deployment.Spec.Template.Labels,
		"standalone MCP Service":      BuildMCPServiceForStandalone(standalone).Labels,
	} {
		for key, value := range labels {
			assert.Empty(t, validation.IsValidLabelValue(value), "%s label %s=%q", what, key, value)
		}
	}
	assert.Equal(t, "v1.4.0", deployment.Labels["app.kubernetes.io/version"])
}
