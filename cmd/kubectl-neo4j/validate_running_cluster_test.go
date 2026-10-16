package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// validate ran ValidateCreateWithWarnings, which did not include the
// spec.upgradeStrategy rules the operator ran on every pass: a timeout of
// "5 minutes" passed validate and the cluster was then refused (Failed).
func TestValidate_ChecksTheUpgradeStrategyLikeTheOperator(t *testing.T) {
	path := writeManifest(t, validCluster+`  upgradeStrategy:
    upgradeTimeout: "5 minutes"
`)
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
	assert.Equal(t, "spec.upgradeStrategy.upgradeTimeout", results[0].findings[0].path)
}

const runningClusterManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata: {name: prod, namespace: neo4j}
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "2025.10.0-enterprise"}
  topology: {servers: 3}
  storage: {size: 10Gi}
`

func runningCluster(version string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status:     neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "Ready", Version: version},
	}
}

// With --connect, a cluster manifest is checked against the cluster already in
// the API server under its name, as the operator checks it once applied: the
// version its servers run comes from the live status.
func TestValidate_Connected_ChecksTheTagAgainstTheRunningVersion(t *testing.T) {
	t.Run("a downgrade is refused", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, runningClusterManifest),
			testClient(t, runningCluster("2026.08.1-enterprise")), "")
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Equal(t, "spec.image.tag", results[0].findings[0].path)
		assert.Contains(t, results[0].findings[0].detail, "2026.08.1-enterprise (status.version)")
	})

	t.Run("an upgrade passes", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, runningClusterManifest),
			testClient(t, runningCluster("2025.06.0-enterprise")), "")
		require.NoError(t, err)
		assert.Empty(t, results[0].findings)
	})

	t.Run("a cluster that does not exist yet has nothing to compare", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, runningClusterManifest), testClient(t), "")
		require.NoError(t, err)
		assert.Empty(t, results[0].findings)
	})

	t.Run("a cluster that cannot be read is a warning, not a pass", func(t *testing.T) {
		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
					return apierrors.NewForbidden(schema.GroupResource{Group: "neo4j.neo4j.com", Resource: "neo4jenterpriseclusters"}, key.Name, nil)
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
		results, err := validateSource(writeManifest(t, runningClusterManifest), c, "")
		require.NoError(t, err)
		assert.Equal(t, 0, results[0].errorCount())
		require.Equal(t, 1, results[0].warningCount(), "%s", findingsText(results[0]))
		assert.Contains(t, findingsText(results[0]), "spec.image.tag was not checked")
	})
}

// Offline there is no running cluster. A status in the manifest (a saved
// `kubectl get -o yaml`) is not what the operator reads — apply drops it — so
// it is not used, and the summary says which rule needs --connect.
func TestValidate_Offline_SaysTheTagIsCheckedOnlyWhenConnected(t *testing.T) {
	path := writeManifest(t, runningClusterManifest+`status:
  version: 2026.08.1-enterprise
`)
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].findings, "offline, the status in a file is not the cluster's")

	out, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer out.Close()
	assert.Equal(t, exitOK, report(results, out, false, false, false))
	printed, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	assert.Contains(t, string(printed),
		"run with --connect to also check a Neo4jEnterpriseCluster's spec.image.tag against the version the cluster already runs")
	assert.Equal(t, 1, strings.Count(string(printed), "spec.image.tag against"), "said once, not per document")

	connected, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer connected.Close()
	withClient, err := validateSource(path, testClient(t), "")
	require.NoError(t, err)
	report(withClient, connected, false, false, true)
	printed, err = os.ReadFile(connected.Name())
	require.NoError(t, err)
	assert.NotContains(t, string(printed), "spec.image.tag against", "connected, the rule ran")
}
