package main

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// operatorClusterErrors is what the cluster controller refuses a manifest with:
// it applies its defaults to the cluster, then runs ValidateWithWarnings.
func operatorClusterErrors(t *testing.T, doc string) []string {
	t.Helper()
	var obj neo4jv1beta1.Neo4jEnterpriseCluster
	require.NoError(t, yaml.Unmarshal([]byte(doc), &obj))
	v := validation.NewClusterValidator(nil)
	v.ApplyDefaults(context.Background(), &obj)
	var out []string
	for _, e := range v.ValidateWithWarnings(context.Background(), &obj).Errors {
		out = append(out, e.Field+": "+e.ErrorBody())
	}
	sort.Strings(out)
	return out
}

func cliErrors(t *testing.T, doc string) []string {
	t.Helper()
	results, err := validateSource(writeManifest(t, doc), nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	var out []string
	for _, f := range results[0].findings {
		if f.severity == "error" {
			out = append(out, f.path+": "+f.detail)
		}
	}
	sort.Strings(out)
	return out
}

// The operator applies its defaults to a cluster before it validates one, and
// validate never did, so a manifest could pass one and fail the other. A
// cluster that names authorizationProviders and no authenticationProviders is
// one: the operator's defaults replace both lists, so it never reports the
// authorization provider that validate refused (operations.md id 144).
func TestValidate_JudgesAClusterAfterTheOperatorsDefaults(t *testing.T) {
	manifests := map[string]string{
		"a clean cluster": validCluster,
		"authorization providers without authentication providers": validCluster + `  auth:
    authorizationProviders: [kerberos-x]
`,
		"cert-manager without an issuer": validCluster + `  tls:
    mode: cert-manager
`,
	}
	for name, doc := range manifests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, operatorClusterErrors(t, doc), cliErrors(t, doc))
		})
	}
}

// The defaults go only into the copy that is validated, and add nothing an
// error can be reported on: the refusal of a cert-manager cluster without an
// issuer names spec.tls.issuerRef, the field that is missing, not
// spec.tls.issuerRef.name of an issuerRef the user never wrote.
func TestValidate_ReportsNoFieldTheUserDidNotWrite(t *testing.T) {
	errs := cliErrors(t, validCluster+`  tls:
    mode: cert-manager
`)
	require.Len(t, errs, 1)
	assert.True(t, strings.HasPrefix(errs[0], "spec.tls.issuerRef: Required value"), errs[0])
}

const runningStandaloneManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseStandalone
metadata: {name: dev, namespace: neo4j}
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "2025.10.0-enterprise"}
  storage: {size: 10Gi}
`

func runningStandalone(version string) *neo4jv1beta1.Neo4jEnterpriseStandalone {
	return &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "neo4j"},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: "Ready", Version: version},
	}
}

// validate ran a standalone's ValidateCreate, as the controller did; the
// update-only rules were never called by either. Both now run the same entry
// point, which checks the tag against the version the standalone runs, read
// from the live standalone when connected.
func TestValidate_Connected_ChecksAStandalonesTagAgainstTheRunningVersion(t *testing.T) {
	t.Run("a downgrade is refused", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, runningStandaloneManifest),
			testClient(t, runningStandalone("2026.08.1-enterprise")), "")
		require.NoError(t, err)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Equal(t, "spec.image.tag", results[0].findings[0].path)
		assert.Contains(t, results[0].findings[0].detail, "the standalone runs 2026.08.1-enterprise (status.version)")
	})

	t.Run("an upgrade passes", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, runningStandaloneManifest),
			testClient(t, runningStandalone("2025.06.0-enterprise")), "")
		require.NoError(t, err)
		assert.Empty(t, results[0].findings)
	})

	t.Run("the admin password's shape is checked, as the operator checks it", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: "neo4j"},
			Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("-dash")},
		}
		results, err := validateSource(writeManifest(t, runningStandaloneManifest), testClient(t, secret), "")
		require.NoError(t, err)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Equal(t, "spec.auth.adminSecret", results[0].findings[0].path)
	})

	t.Run("a standalone that cannot be read is a warning, not a pass", func(t *testing.T) {
		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseStandalone); ok {
					return apierrors.NewForbidden(schema.GroupResource{Group: "neo4j.neo4j.com", Resource: "neo4jenterprisestandalones"}, key.Name, nil)
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
		results, err := validateSource(writeManifest(t, runningStandaloneManifest), c, "")
		require.NoError(t, err)
		assert.Equal(t, 0, results[0].errorCount())
		require.Equal(t, 1, results[0].warningCount(), "%s", findingsText(results[0]))
		assert.Contains(t, findingsText(results[0]), "spec.image.tag was not checked")
	})
}

// Offline a standalone's status in the file is not used, and the summary says
// the tag rule needs --connect.
func TestValidate_Offline_SaysAStandalonesTagIsCheckedOnlyWhenConnected(t *testing.T) {
	path := writeManifest(t, runningStandaloneManifest+`status:
  version: 2026.08.1-enterprise
`)
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	assert.Empty(t, results[0].findings, "offline, the status in a file is not the standalone's")

	out, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer out.Close()
	report(results, out, false, false, false)
	printed, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	assert.Contains(t, string(printed),
		"run with --connect to also check a Neo4jEnterpriseStandalone's spec.image.tag against the version it already runs")
}

// The usage text said the operator stops validating after certain critical
// errors, so a clean run might hide more. Since the cluster validator stopped
// returning early on an invalid image (#354), every validator but the sharded
// database's reports everything it finds.
func TestValidate_UsageSaysOneRunListsEveryError(t *testing.T) {
	stderr, err := os.CreateTemp(t.TempDir(), "stderr")
	require.NoError(t, err)
	defer stderr.Close()
	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	require.NoError(t, err)
	defer stdout.Close()

	assert.Equal(t, exitUsage, runValidate(nil, stdout, stderr))
	usage, err := os.ReadFile(stderr.Name())
	require.NoError(t, err)
	assert.NotContains(t, string(usage), "stops validating")
	assert.Contains(t, string(usage), "reports every error it finds")
	assert.Contains(t, string(usage), "Neo4jShardedDatabase")
}

// The operator's upgrade timeouts must be above zero, and validate refuses what
// the operator refuses.
func TestValidate_RefusesAnUpgradeTimeoutOfZeroOrLess(t *testing.T) {
	for _, value := range []string{"0s", "-5m"} {
		errs := cliErrors(t, validCluster+"  upgradeStrategy:\n    stabilizationTimeout: \""+value+"\"\n")
		require.Len(t, errs, 1, value)
		assert.True(t, strings.HasPrefix(errs[0], "spec.upgradeStrategy.stabilizationTimeout: Invalid value"), errs[0])
	}
}
