package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// bundleClient builds a controller-runtime fake whose List calls can be made to
// fail selectively, which is how a real collector meets RBAC denials, a CRD that
// is not installed, or an API server that is struggling — the moments a support
// bundle is wanted most.
func bundleClient(t *testing.T, failList func(client.ObjectList) error, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failList != nil {
					if err := failList(list); err != nil {
						return err
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
}

// logsFail makes every container-log read fail, optionally only previous logs.
func logsFail(cs *kfake.Clientset, onlyPrevious bool) {
	cs.PrependReactor("get", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "log" {
			return false, nil, nil
		}
		if onlyPrevious {
			opts, _ := a.(k8stesting.GenericAction).GetValue().(*corev1.PodLogOptions)
			if opts == nil || !opts.Previous {
				return false, nil, nil
			}
		}
		return true, nil, errors.New("the log stream is unavailable")
	})
}

func testPod(name string, restarts int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "neo4j", RestartCount: restarts},
		}},
	}
}

func operatorObjects() []client.Object {
	return []client.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: "neo4j-operator-controller-manager", Namespace: "neo4j-operator",
				Labels: map[string]string{"app.kubernetes.io/name": "neo4j-operator"},
			},
			Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"control-plane": "controller-manager"},
			}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "op-0", Namespace: "neo4j-operator",
				Labels: map[string]string{"control-plane": "controller-manager"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "manager"}}},
		},
	}
}

func joinFailures(f []string) string { return strings.Join(f, "\n") }

// The defect: every one of these reads was dropped with a bare `continue`, and
// the comment above the first one promised an errors.txt that nothing wrote. A
// bundle collected by a user who cannot list Secrets was therefore
// indistinguishable from one where the namespace simply had none.
func TestCollectBundle_RecordsEveryFailedRead(t *testing.T) {
	c := bundleClient(t, func(l client.ObjectList) error {
		switch v := l.(type) {
		case *corev1.EventList:
			return errors.New(`events is forbidden: User "u" cannot list resource "events"`)
		case *corev1.SecretList:
			return errors.New(`secrets is forbidden: User "u" cannot list resource "secrets"`)
		case *unstructured.UnstructuredList:
			if v.GetKind() == "Neo4jBackupList" {
				return errors.New("no matches for kind Neo4jBackup")
			}
		}
		return nil
	}, append(operatorObjects(),
		testPod("fresh-0", 0),
		testPod("restarted-0", 2),
	)...)
	cs := kfake.NewClientset()
	logsFail(cs, false)

	files, _, failures := collectBundle(context.Background(), c, cs, "neo4j", 100)
	out := joinFailures(failures)

	assert.Contains(t, out, "list Neo4jBackup: no matches for kind Neo4jBackup", "a failed CR-kind list")
	assert.Contains(t, out, `list events: events is forbidden`)
	assert.Contains(t, out, `list secrets (names and key names only): secrets is forbidden`)
	assert.Contains(t, out, "log of container neo4j in pod neo4j/fresh-0: ", "a container log")
	assert.Contains(t, out, "the log stream is unavailable", "the underlying error text is kept")
	assert.Contains(t, out, "log of container neo4j in pod neo4j/restarted-0")
	assert.Contains(t, out, "log of container manager in pod neo4j-operator/op-0", "the operator's own log")

	// A previous log can only exist if the container restarted. Reporting its
	// absence for a pod that never did would stamp a false failure on every
	// healthy pod in the namespace.
	assert.Contains(t, out, "previous log of container neo4j in pod neo4j/restarted-0")
	assert.NotContains(t, out, "previous log of container neo4j in pod neo4j/fresh-0")
	assert.NotContains(t, out, "previous log of container manager in pod neo4j-operator/op-0")

	// Collection carries on regardless: the pod status files are still there.
	var names []string
	for _, f := range files {
		names = append(names, f.name)
	}
	assert.Contains(t, names, "pods/fresh-0/status.txt")
	assert.Contains(t, names, "operator/neo4j-operator/op-0/status.txt")
}

func TestCollectBundle_RecordsFailedPodAndOperatorLists(t *testing.T) {
	c := bundleClient(t, func(l client.ObjectList) error {
		switch l.(type) {
		case *corev1.PodList:
			return errors.New("pods is forbidden")
		case *appsv1.DeploymentList:
			return errors.New("deployments.apps is forbidden at the cluster scope")
		}
		return nil
	})

	_, _, failures := collectBundle(context.Background(), c, kfake.NewClientset(), "neo4j", 100)
	out := joinFailures(failures)
	assert.Contains(t, out, "list pods: pods is forbidden")
	assert.Contains(t, out, "operator logs (cannot list deployments cluster-wide): deployments.apps is forbidden")
}

// "No operator found" used to be filed in REDACTIONS.txt, under "every value
// listed below was replaced" and counted as a redaction. It is a read that did
// not happen, and belongs with the others.
func TestCollectBundle_AbsentOperatorIsAReadFailureNotARedaction(t *testing.T) {
	_, notes, failures := collectBundle(context.Background(), bundleClient(t, nil), kfake.NewClientset(), "neo4j", 100)
	assert.Contains(t, joinFailures(failures), "no Deployment labelled app.kubernetes.io/name=neo4j-operator")
	for _, n := range notes {
		assert.NotContains(t, n, "could not be collected", "a failed read is not a redaction: %q", n)
	}
}

// Failed reads must not be the only thing a healthy cluster's bundle contains,
// and a clean run must say so: an errors.txt that is merely absent cannot be
// told apart from one that an older kubectl-neo4j never wrote.
func TestCollectBundle_CleanRunReportsNoFailures(t *testing.T) {
	c := bundleClient(t, nil, append(operatorObjects(), testPod("fresh-0", 0))...)
	files, notes, failures := collectBundle(context.Background(), c, kfake.NewClientset(), "neo4j", 100)
	assert.Empty(t, failures, "a fully readable namespace reported failures:\n%s", joinFailures(failures))

	finished := finishBundle(files, notes, failures)
	var errorsTxt string
	for _, f := range finished {
		if f.name == "errors.txt" {
			errorsTxt = string(f.body)
		}
	}
	assert.Contains(t, errorsTxt, "(every read succeeded)")
}

// Only a previous log of a container that DID restart is worth listing when it
// fails; a current-log failure is always listed.
func TestCollectContainerLogs_PreviousLogOnlyExpectedAfterARestart(t *testing.T) {
	cs := kfake.NewClientset()
	logsFail(cs, true) // current logs read fine, previous ones do not

	_, fresh := collectContainerLogs(context.Background(), cs, testPod("a", 0), 10, "pods/a")
	assert.Empty(t, fresh)

	files, restarted := collectContainerLogs(context.Background(), cs, testPod("b", 1), 10, "pods/b")
	require.Len(t, restarted, 1)
	assert.Contains(t, restarted[0], "previous log of container neo4j in pod neo4j/b")
	require.Len(t, files, 1, "the current log is still collected")
	assert.Equal(t, "pods/b/neo4j.log", files[0].name)
}

func TestFinishBundle_AlwaysAddsErrorsAndRedactions(t *testing.T) {
	got := finishBundle([]bundleFile{{name: "meta.txt", body: []byte("m")}}, nil, []string{"list events: boom"})
	byName := map[string]string{}
	for _, f := range got {
		byName[f.name] = string(f.body)
	}
	assert.Contains(t, byName, "REDACTIONS.txt")
	assert.Contains(t, byName["errors.txt"], "- list events: boom")
	assert.Contains(t, byName["errors.txt"], "MISSING from this archive",
		"the recipient must be told that listed reads are absent, not empty")

	// And it survives the round trip through the real archive writer.
	target := filepath.Join(t.TempDir(), "bundle.tar.gz")
	require.NoError(t, writeArchive(target, got))
	assert.Contains(t, readArchive(t, target)["bundle/errors.txt"], "list events: boom")
}

// errors.txt is shared with a stranger like the rest of the bundle, so it
// carries the error and what was being read — nothing else. A multi-line error
// must not be able to forge an extra entry, and a pathological one is bounded.
func TestReadFailure_IsOneBoundedLineOfErrorTextOnly(t *testing.T) {
	line := readFailure("list secrets", errors.New("first\n- forged: entry\n\tthird"))
	assert.NotContains(t, line, "\n")
	assert.Equal(t, "list secrets: first - forged: entry third", line)

	long := readFailure("x", errors.New(strings.Repeat("é", 5000)))
	assert.LessOrEqual(t, len([]rune(long)), len("x: ")+maxErrorLen+1)
	assert.True(t, strings.HasSuffix(long, "…"))
}

// The redaction guarantee must be unaffected by the new file: a Secret's value
// can be in the namespace being read, and must appear nowhere in the output.
func TestCollectBundle_SecretValuesStillNeverShipAndNeverReachErrors(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin", Namespace: "neo4j"},
		Data:       map[string][]byte{"password": []byte("hunter2-do-not-ship")},
	}
	c := bundleClient(t, func(l client.ObjectList) error {
		if _, ok := l.(*corev1.EventList); ok {
			return errors.New("events unavailable")
		}
		return nil
	}, append(operatorObjects(), secret)...)
	cs := kfake.NewClientset()
	logsFail(cs, false)

	files, notes, failures := collectBundle(context.Background(), c, cs, "neo4j", 100)
	for _, f := range finishBundle(files, notes, failures) {
		assert.NotContains(t, string(f.body), "hunter2-do-not-ship", "%s leaked a Secret value", f.name)
	}
}
