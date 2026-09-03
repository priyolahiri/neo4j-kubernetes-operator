package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func readyPodObj(name, namespace string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// THE security property of this command: the admin password is referenced by
// variable name so the shell expands it inside the pod. Passing -p <value>
// would put the secret in this process's argv, in the pod's argv, and verbatim
// in the Kubernetes audit log, which records an exec request's command array.
func TestCypherShellArgs_NeverCarriesThePasswordValue(t *testing.T) {
	tgt := target{tls: false}
	cmd := tgt.cypherShellArgs("")

	assert.Contains(t, cmd, `-p "$DB_PASSWORD"`,
		"the password must be referenced by variable name, expanded inside the pod")
	assert.Contains(t, cmd, `-u "$DB_USERNAME"`)
	assert.NotContains(t, cmd, "secret")
	assert.NotContains(t, cmd, "password=")
}

func TestCypherShellArgs_SchemeFollowsTLS(t *testing.T) {
	solo := target{kind: "Neo4jEnterpriseStandalone", name: "dev", namespace: "neo4j"}
	assert.Contains(t, solo.cypherShellArgs(""), "bolt://dev-client.neo4j.svc.cluster.local:7687")

	// With TLS on, the operator rejects plain bolt:// — guessing wrong fails
	// opaquely, which is the whole reason this command resolves it.
	solo.tls = true
	assert.Contains(t, solo.cypherShellArgs(""), "bolt+s://dev-client.neo4j.svc.cluster.local:7687")
}

// The CLI never composes Cypher of its own; it passes the user's text through.
// Quoting is therefore the only thing it must get right.
func TestCypherShellArgs_QueryPassthroughEscapesQuotes(t *testing.T) {
	cmd := target{}.cypherShellArgs(`RETURN 'it''s fine'`)
	assert.Contains(t, cmd, `'\''`, "embedded single quotes must be escaped for the container shell")
	assert.True(t, strings.HasPrefix(strings.TrimSpace(cmd), "cypher-shell"))
}

func TestResolveTarget_PicksTheOnlyDeployment(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{TLS: &neo4jv1beta1.TLSSpec{Mode: "cert-manager"}},
		},
		readyPodObj("prod-server-0", "neo4j", map[string]string{"neo4j.com/cluster": "prod"}),
	)

	tgt, err := resolveTarget(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	assert.Equal(t, "prod", tgt.name)
	assert.Equal(t, "prod-server-0", tgt.pod)
	assert.Equal(t, "neo4j", tgt.container)
	assert.True(t, tgt.tls, "TLS must be read from spec, not guessed")
	assert.Equal(t, "bolt+s", tgt.scheme())
}

// Ambiguity must be an error that names the options, not a silent guess.
func TestResolveTarget_AmbiguousNamespaceAsksRatherThanGuessing(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		&neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "neo4j"}},
	)

	_, err := resolveTarget(context.Background(), c, "neo4j", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prod")
	assert.Contains(t, err.Error(), "dev", "the error must name the candidates so the user can pick")
}

func TestResolveTarget_StandaloneUsesItsOwnPodSelector(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "neo4j"}},
		readyPodObj("dev-0", "neo4j", map[string]string{"app": "dev"}),
	)

	tgt, err := resolveTarget(context.Background(), c, "neo4j", "dev")
	require.NoError(t, err)
	assert.Equal(t, "Neo4jEnterpriseStandalone", tgt.kind)
	assert.Equal(t, "dev-0", tgt.pod)
	assert.False(t, tgt.tls)
}

// "connection refused" from a pod that was never going to answer is a worse
// error than naming the actual problem.
func TestResolveTarget_NoReadyPodSaysSoAndPointsSomewhere(t *testing.T) {
	notReady := readyPodObj("prod-server-0", "neo4j", map[string]string{"neo4j.com/cluster": "prod"})
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		notReady,
	)

	_, err := resolveTarget(context.Background(), c, "neo4j", "prod")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "none are Ready")
	assert.Contains(t, err.Error(), "status", "point the user at the command that explains why")
}

func TestResolveTarget_UnknownNameIsAClearError(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
	})
	_, err := resolveTarget(context.Background(), c, "neo4j", "typo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"typo"`)
}

// A one-shot query MUST be non-interactive. Without it cypher-shell executes
// the argument and then reads stdin for more statements: the user's terminal
// hangs and an orphaned shell keeps its session open inside the database pod.
// Found on a real cluster, where twelve of them accumulated and the deployment
// stopped answering over both Bolt and HTTP while the pod still read Ready.
func TestCypherShellArgs_OneShotQueryIsNonInteractive(t *testing.T) {
	withQuery := target{}.cypherShellArgs("RETURN 1")
	assert.Contains(t, withQuery, "--non-interactive",
		"a -c query must not fall through to reading stdin")

	// An interactive session is the opposite case and must NOT carry it.
	assert.NotContains(t, target{}.cypherShellArgs(""), "--non-interactive")
}

// On a cluster the default `neo4j` database has ONE primary, so a session
// pinned to localhost lands on a server that does not host it in 2 cases out
// of 3 and answers "Database neo4j not found" on a healthy cluster.
func TestCypherShellArgs_ClusterSessionIsRouted(t *testing.T) {
	cluster := target{kind: "Neo4jEnterpriseCluster", name: "prod", namespace: "neo4j"}
	cmd := cluster.cypherShellArgs("SHOW DATABASES")

	assert.Contains(t, cmd, "neo4j://prod-client.neo4j.svc.cluster.local:7687",
		"a cluster session must be routed through the client Service")
	assert.NotContains(t, cmd, "localhost:7687")

	// TLS picks the secure routing scheme, not the secure direct one.
	secure := target{kind: "Neo4jEnterpriseCluster", name: "prod", namespace: "neo4j", tls: true}
	assert.Contains(t, secure.cypherShellArgs(""), "neo4j+s://prod-client.neo4j.svc.cluster.local:7687")

	// A standalone needs no routing — one server hosts everything — but it does
	// need the Service NAME, because the certificate's SANs cover the client
	// Service and the pod, never `localhost`. A session dialled at localhost
	// cannot pass hostname verification against a TLS deployment.
	solo := target{kind: "Neo4jEnterpriseStandalone", name: "dev", namespace: "neo4j"}
	assert.Contains(t, solo.cypherShellArgs(""), "bolt://dev-client.neo4j.svc.cluster.local:7687")

	soloTLS := target{kind: "Neo4jEnterpriseStandalone", name: "dev", namespace: "neo4j", tls: true}
	assert.Contains(t, soloTLS.cypherShellArgs(""), "bolt+s://dev-client.neo4j.svc.cluster.local:7687")
}

// The operator's certificates never carry a `localhost` SAN — for either Kind
// — so no session may be dialled there. bolt+s and neo4j+s verify the
// hostname, so doing that failed against every TLS deployment.
func TestCypherShellArgs_NeverDialsLocalhost(t *testing.T) {
	for _, tgt := range []target{
		{kind: "Neo4jEnterpriseCluster", name: "prod", namespace: "neo4j"},
		{kind: "Neo4jEnterpriseCluster", name: "prod", namespace: "neo4j", tls: true},
		{kind: "Neo4jEnterpriseStandalone", name: "dev", namespace: "neo4j"},
		{kind: "Neo4jEnterpriseStandalone", name: "dev", namespace: "neo4j", tls: true},
	} {
		assert.NotContains(t, tgt.cypherShellArgs(""), "localhost",
			"%s/%s (tls=%v) must be reached by a name the certificate covers", tgt.kind, tgt.name, tgt.tls)
	}
}
