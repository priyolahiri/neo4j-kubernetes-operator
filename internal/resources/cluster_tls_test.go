package resources_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/neo4j-partners/neo4j-kubernetes-operator/internal/resources"
)

func TestBuildConfigMapForEnterprise_TLSConfiguration(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-cluster",
			Namespace: "default",
		},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{
				Repo: "neo4j",
				Tag:  "5.26.0-enterprise",
			},
			Topology: neo4jv1beta1.TopologyConfiguration{
				Servers: 5, // 3 + 2 total servers
			},
			TLS: &neo4jv1beta1.TLSSpec{
				Mode: "cert-manager",
				IssuerRef: &neo4jv1beta1.IssuerRef{
					Name: "ca-cluster-issuer",
					Kind: "ClusterIssuer",
				},
			},
			Storage: neo4jv1beta1.StorageSpec{
				ClassName: "standard",
				Size:      "10Gi",
			},
		},
	}

	configMap := resources.BuildConfigMapForEnterprise(cluster)

	// Test that neo4j.conf contains TLS configuration
	neo4jConf := configMap.Data["neo4j.conf"]

	// Test HTTPS SSL policy
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.https.enabled=true", "should enable HTTPS SSL policy")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.https.base_directory=/ssl", "should set HTTPS base directory")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.https.private_key=tls.key", "should set HTTPS private key")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.https.public_certificate=tls.crt", "should set HTTPS certificate")

	// Test Bolt SSL policy
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.bolt.enabled=true", "should enable Bolt SSL policy")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.bolt.base_directory=/ssl", "should set Bolt base directory")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.bolt.private_key=tls.key", "should set Bolt private key")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.bolt.public_certificate=tls.crt", "should set Bolt certificate")

	// Test Cluster SSL policy — strictPeerValidation defaults to true, so
	// the operator emits trust_all=false + client_auth=REQUIRE +
	// verify_hostname=true to match Neo4j's canonical production posture.
	// The old "trust_all=true" assertion was a lock-in of debugging-only
	// config; see the strictPeerValidation=false test below for the
	// opt-out path.
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.enabled=true", "should enable cluster SSL policy")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.trust_all=false", "default posture: peers validated against /ssl/trusted/ca.crt")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.client_auth=REQUIRE", "default posture: mutual TLS")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.verify_hostname=true", "default posture: explicit hostname verification")
	assert.NotContains(t, neo4jConf, "dbms.ssl.policy.cluster.trust_all=true", "must not emit legacy trust_all=true by default")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.base_directory=/ssl", "should set cluster base directory")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.private_key=tls.key", "should set cluster private key")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.public_certificate=tls.crt", "should set cluster certificate")

	// Test connector configuration
	assert.Contains(t, neo4jConf, "server.https.enabled=true", "should enable HTTPS")
	assert.Contains(t, neo4jConf, "server.bolt.tls_level=REQUIRED", "should set Bolt TLS level to REQUIRED when TLS is enabled")

	// Test startup script for parallel pod management compatibility
	startupScript := configMap.Data["startup.sh"]
	assert.Contains(t, startupScript, "dbms.cluster.minimum_initial_system_primaries_count=${TOTAL_SERVERS}", "should use TOTAL_SERVERS as minimum to prevent split-brain")
	assert.Contains(t, startupScript, `BOOTSTRAP_STRATEGY="me"`, "should have me/other bootstrap strategy")
}

func TestBuildStatefulSetForEnterprise_TLSClusterFormation(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-parallel-test",
			Namespace: "default",
		},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{
				Repo: "neo4j",
				Tag:  "5.26.0-enterprise",
			},
			Topology: neo4jv1beta1.TopologyConfiguration{
				Servers: 5, // 3 + 2 total servers
			},
			TLS: &neo4jv1beta1.TLSSpec{
				Mode: "cert-manager",
				IssuerRef: &neo4jv1beta1.IssuerRef{
					Name: "ca-cluster-issuer",
					Kind: "ClusterIssuer",
				},
			},
			Storage: neo4jv1beta1.StorageSpec{
				ClassName: "standard",
				Size:      "10Gi",
			},
		},
	}

	// Test that TLS clusters use parallel pod management
	serverStatefulSets := resources.BuildServerStatefulSetsForEnterprise(cluster)
	require.Len(t, serverStatefulSets, 5, "should create 5 StatefulSets for 5 servers")

	// Test the first StatefulSet as representative
	serverSts := serverStatefulSets[0]
	assert.Equal(t, serverSts.Spec.PodManagementPolicy, appsv1.ParallelPodManagement,
		"TLS clusters must use ParallelPodManagement for reliable formation")
}

// TestBuildConfigMapForEnterprise_TLSStrictPeerValidationOptOut covers the
// escape hatch for users whose external issuer doesn't populate ca.crt in
// the cert-manager Secret. With strictPeerValidation=false we revert to
// the legacy debugging-only posture (trust_all=true, client_auth=NONE).
func TestBuildConfigMapForEnterprise_TLSStrictPeerValidationOptOut(t *testing.T) {
	optOut := false
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-loose", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			TLS: &neo4jv1beta1.TLSSpec{
				Mode:                 "cert-manager",
				IssuerRef:            &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
				StrictPeerValidation: &optOut,
			},
			Storage: neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "10Gi"},
		},
	}

	neo4jConf := resources.BuildConfigMapForEnterprise(cluster).Data["neo4j.conf"]

	// Legacy posture: opt-out flips back to trust_all=true + client_auth=NONE.
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.trust_all=true")
	assert.Contains(t, neo4jConf, "dbms.ssl.policy.cluster.client_auth=NONE")
	assert.NotContains(t, neo4jConf, "dbms.ssl.policy.cluster.trust_all=false")
	assert.NotContains(t, neo4jConf, "dbms.ssl.policy.cluster.verify_hostname=true",
		"opt-out should not emit verify_hostname — leave Neo4j to its version default")
}

// TestBuildStatefulSet_TLSVolumeProjectsCAToTrustedDir locks in the Secret
// items projection that places ca.crt at /ssl/trusted/ca.crt — the path
// Neo4j's cluster SSL policy reads when trust_all=false. Without the
// projection, strict peer validation would have no trust anchors and
// every peer connection would fail.
func TestBuildStatefulSet_TLSVolumeProjectsCAToTrustedDir(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-vol", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			TLS: &neo4jv1beta1.TLSSpec{
				Mode:      "cert-manager",
				IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
			},
			Storage: neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "10Gi"},
		},
	}

	sts := resources.BuildServerStatefulSetsForEnterprise(cluster)[0]

	var certsVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		v := &sts.Spec.Template.Spec.Volumes[i]
		if v.Name == "certs" {
			certsVol = v
			break
		}
	}
	require.NotNil(t, certsVol, "certs volume must be present when TLS enabled")
	require.NotNil(t, certsVol.Secret, "certs volume must be backed by a Secret")
	require.Equal(t, "tls-vol-tls-secret", certsVol.Secret.SecretName)

	// Items must include the trusted/ca.crt projection.
	paths := map[string]string{}
	for _, item := range certsVol.Secret.Items {
		paths[item.Key+"->"+item.Path] = item.Path
	}
	require.Contains(t, paths, "ca.crt->trusted/ca.crt",
		"Secret projection must put ca.crt at /ssl/trusted/ca.crt for cluster SSL trust anchors")
	require.Contains(t, paths, "tls.crt->tls.crt", "tls.crt projected at root")
	require.Contains(t, paths, "tls.key->tls.key", "tls.key projected at root")
}
