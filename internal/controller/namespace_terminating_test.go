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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// namespaceTerminatingError is the API server's refusal of a create in a
// namespace being deleted, as its NamespaceLifecycle admission plugin builds
// it: Forbidden, with the NamespaceTerminating cause.
func namespaceTerminatingError(resource, name, namespace string) error {
	err := apierrors.NewForbidden(schema.GroupResource{Resource: resource}, name,
		fmt.Errorf("unable to create new content in namespace %s because it is being terminated", namespace))
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{
		Type:    corev1.NamespaceTerminatingCause,
		Message: fmt.Sprintf("namespace %s is being terminated", namespace),
		Field:   "metadata.namespace",
	})
	return err
}

// terminatingNamespace refuses creates the way the API server does in a
// namespace being deleted, once it is on — for every object, or for the kinds
// only allows.
type terminatingNamespace struct {
	on   atomic.Bool
	only func(client.Object) bool
}

func (n *terminatingNamespace) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if n.on.Load() && (n.only == nil || n.only(obj)) {
				return namespaceTerminatingError(strings.ToLower(fmt.Sprintf("%T", obj)), obj.GetName(), obj.GetNamespace())
			}
			return c.Create(ctx, obj, opts...)
		},
	}
}

// endingPass runs one pass of r the way the manager does — through
// endsWithItsNamespace — with a logger that records every line.
func endingPass(t *testing.T, r reconcile.Reconciler, key client.ObjectKey) ([]logEntry, error) {
	t.Helper()
	logger, entries := newCaptureLogger()
	_, err := endsWithItsNamespace(r).Reconcile(log.IntoContext(context.Background(), logger), ctrl.Request{NamespacedName: key})
	return entries(), err
}

func TestNamespaceTerminating(t *testing.T) {
	refused := namespaceTerminatingError("services", "fms-client", "u4")
	assert.True(t, namespaceTerminating(refused))
	assert.True(t, namespaceTerminating(fmt.Errorf("failed to reconcile Service: %w",
		fmt.Errorf("failed to create or update client Service: %w", refused))), "wrapped, as the passes return it")
	assert.False(t, namespaceTerminating(nil))
	assert.False(t, namespaceTerminating(apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "x", errors.New("RBAC"))),
		"a Forbidden for another reason is a failure")
	assert.False(t, namespaceTerminating(errors.New("unable to create new content in namespace u4 because it is being terminated")),
		"the cause decides, not the text")
}

// #619 S4, the journey's U4 teardown: deleting the namespace of a standalone
// logged 8 × ERROR "Failed to reconcile standalone deployment … services
// "fms-client" is forbidden: … being terminated", each with a "Reconciler
// error". The namespace controller had deleted the Services before marking the
// CR, and every pass created them again. A refusal for that is not a failure:
// no ERROR, no reconcile error, no Failed status, no ReconcileFailed event.
func TestStandaloneReconcile_ANamespaceBeingDeletedIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "fms", Namespace: "u4", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
	ns := &terminatingNamespace{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).WithInterceptorFuncs(ns.funcs()).Build()
	rec := record.NewFakeRecorder(1000)
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute,
		Validator: validation.NewStandaloneValidator(),
	}
	key := client.ObjectKeyFromObject(standalone)
	_, err := endingPass(t, r, key)
	require.NoError(t, err)
	before := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, c.Get(ctx, key, before))
	drainEvents(rec)

	// The namespace controller deletes the Services; the namespace refuses
	// new content.
	for _, name := range []string{"fms-client", "fms-headless"} {
		require.NoError(t, c.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "u4"}}))
	}
	ns.on.Store(true)

	for range 3 {
		entries, err := endingPass(t, r, key)
		assert.NoError(t, err, "no reconcile error, so no \"Reconciler error\" and no backoff")
		assert.Empty(t, errorEntries(entries), "no ERROR")
	}
	after := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, c.Get(ctx, key, after))
	assert.Equal(t, before.Status.Phase, after.Status.Phase, "the status is not Failed")
	assert.NotContains(t, after.Status.Message, standaloneReconcileFailedPrefix)
	for _, e := range drainEvents(rec) {
		assert.NotContains(t, e, EventReasonReconcileFailed)
	}

	// Any other refusal is still a failure.
	ns.on.Store(false)
	r.Client = refuseCreates{Client: c, err: apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "fms-headless", errors.New("RBAC"))}
	entries, err := endingPass(t, r, key)
	assert.Error(t, err)
	assert.NotEmpty(t, errorEntries(entries))
}

// refuseCreates refuses every create with err.
type refuseCreates struct {
	client.Client
	err error
}

func (c refuseCreates) Create(context.Context, client.Object, ...client.CreateOption) error {
	return c.err
}

// The cluster's passes end the same way where a write the namespace refuses
// would have written Failed (failPass).
func TestClusterReconcile_ANamespaceBeingDeletedIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("loop", "u3")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	ns := &terminatingNamespace{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).WithInterceptorFuncs(ns.funcs()).Build()
	rec := record.NewFakeRecorder(1000)
	r := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute,
		Validator: validation.NewClusterValidator(c),
	}
	pass := func() ([]logEntry, error) {
		cm := NewConfigMapManager(c)
		cm.LiveConfig = nil
		r.ConfigMapManager = cm
		return endingPass(t, r, client.ObjectKeyFromObject(cluster))
	}
	_, err := pass()
	require.NoError(t, err)
	before := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), before))
	require.NotEqual(t, "Failed", before.Status.Phase)

	for _, deleted := range []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "loop-client", Namespace: "u3"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "loop-config", Namespace: "u3"}},
	} {
		require.NoError(t, c.Delete(ctx, deleted))
	}
	ns.on.Store(true)

	for range 3 {
		entries, err := pass()
		assert.NoError(t, err)
		assert.Empty(t, errorEntries(entries), "no ERROR")
	}
	after := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), after))
	assert.Equal(t, before.Status.Phase, after.Status.Phase, "the status is not Failed")
	assert.NotContains(t, after.Status.Message, "being terminated")
}

// A scheduled backup whose CronJob the namespace controller deleted first:
// recreating it is refused, and that is not a failed schedule.
func TestBackupReconcile_ANamespaceBeingDeletedIsNotAFailedSchedule(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	cluster := minimalClusterForRestore("c", "ns")
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ns", Finalizers: []string{BackupFinalizer}},
		Spec: neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", Database: "neo4j", Schedule: "0 2 * * *",
			Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups", Size: "1Gi"}}},
	}
	ns := &terminatingNamespace{only: func(o client.Object) bool { _, ok := o.(*batchv1.CronJob); return ok }}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup).
		WithStatusSubresource(&neo4jv1beta1.Neo4jBackup{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(ns.funcs()).Build()
	rec := record.NewFakeRecorder(1000)
	r := &Neo4jBackupReconciler{Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute}
	key := client.ObjectKeyFromObject(backup)
	_, err := endingPass(t, r, key)
	require.NoError(t, err)
	before := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, c.Get(ctx, key, before))
	require.Equal(t, neo4jv1beta1.PhaseScheduled, before.Status.Phase, before.Status.Message)

	require.NoError(t, c.Delete(ctx, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "nightly-backup-cron", Namespace: "ns"}}))
	ns.on.Store(true)
	entries, err := endingPass(t, r, key)
	assert.NoError(t, err)
	assert.Empty(t, errorEntries(entries), "no ERROR")
	after := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, c.Get(ctx, key, after))
	assert.Equal(t, neo4jv1beta1.PhaseScheduled, after.Status.Phase, "not Failed")
}

// endsWithItsNamespace is every kind's: an ERROR whose error is the refusal is
// written at V(1), and the pass returns no error; anything else is untouched.
func TestEndsWithItsNamespace(t *testing.T) {
	refused := fmt.Errorf("failed to create Secret: %w", namespaceTerminatingError("secrets", "conn", "ns"))
	other := errors.New("something else")
	pass := func(err error) reconcile.Reconciler {
		return reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
			log.FromContext(ctx).WithValues("k", "v").WithName("n").Error(err, "Failed to create the connection Secret")
			return reconcile.Result{RequeueAfter: time.Minute}, err
		})
	}

	sink := &depthSink{}
	ctx := log.IntoContext(context.Background(), logr.New(sink))
	result, err := endsWithItsNamespace(pass(refused)).Reconcile(ctx, reconcile.Request{})
	assert.NoError(t, err)
	assert.Zero(t, result)
	assert.Zero(t, sink.errors(), "no ERROR")
	assert.Contains(t, sink.infos(), "Failed to create the connection Secret", "written at V(1)")

	sink = &depthSink{}
	ctx = log.IntoContext(context.Background(), logr.New(sink))
	result, err = endsWithItsNamespace(pass(other)).Reconcile(ctx, reconcile.Request{})
	assert.Equal(t, other, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Equal(t, 1, sink.errors())
	assert.Equal(t, 1, sink.lastDepth(), "the wrapper's own frame is skipped, so the line names the reconciler's")
}

// depthSink records the calls it gets and the call depth it was given.
type depthSink struct {
	mu     sync.Mutex
	depth  int
	errs   int
	msgs   []string
	depths []int
}

type depthSinkView struct {
	s     *depthSink
	depth int
}

func (s *depthSink) Init(logr.RuntimeInfo) {}
func (s *depthSink) Enabled(int) bool      { return true }
func (s *depthSink) Info(_ int, msg string, _ ...any) {
	s.record(false, msg, s.depth)
}
func (s *depthSink) Error(_ error, msg string, _ ...any) {
	s.record(true, msg, s.depth)
}
func (s *depthSink) WithValues(...any) logr.LogSink { return s }
func (s *depthSink) WithName(string) logr.LogSink   { return s }
func (s *depthSink) WithCallDepth(d int) logr.LogSink {
	return &depthSinkView{s: s, depth: s.depth + d}
}

func (s *depthSink) record(isErr bool, msg string, depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if isErr {
		s.errs++
	}
	s.msgs = append(s.msgs, msg)
	s.depths = append(s.depths, depth)
}

func (s *depthSink) errors() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errs
}

func (s *depthSink) infos() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.msgs, "\n")
}

func (s *depthSink) lastDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.depths[len(s.depths)-1]
}

func (v *depthSinkView) Init(logr.RuntimeInfo) {}
func (v *depthSinkView) Enabled(int) bool      { return true }
func (v *depthSinkView) Info(_ int, msg string, _ ...any) {
	v.s.record(false, msg, v.depth)
}
func (v *depthSinkView) Error(_ error, msg string, _ ...any) {
	v.s.record(true, msg, v.depth)
}
func (v *depthSinkView) WithValues(...any) logr.LogSink { return v }
func (v *depthSinkView) WithName(string) logr.LogSink   { return v }
func (v *depthSinkView) WithCallDepth(d int) logr.LogSink {
	return &depthSinkView{s: v.s, depth: v.depth + d}
}

// The journey's U3 teardown logged ERROR "Failed to perform split-brain
// detection … (3/3 failed)" while the namespace delete stopped the pods.
// Servers being deleted that refuse the connection are servers going away.
func TestDetectSplitBrain_ServersBeingDeletedAreNotAFailedDetection(t *testing.T) {
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("loop", "u3")
	cluster.Spec.Topology.Servers = 3
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	pods := func(deleting bool) []client.Object {
		var out []client.Object
		for i := range 3 {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("loop-server-%d", i), Namespace: "u3",
					Labels: map[string]string{"neo4j.com/cluster": "loop", "neo4j.com/clustering": "true"}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			if deleting {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				p.Finalizers = []string{"test/keep"}
			}
			out = append(out, p)
		}
		return out
	}

	// No admin Secret: no server can be reached, as none can while it stops.
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(pods(true), cluster)...).Build()
	analysis, err := NewSplitBrainDetector(c).DetectSplitBrain(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, analysis.IsSplitBrain)
	assert.Equal(t, RepairActionWaitForming, analysis.RepairAction)
	assert.Equal(t, "3 of 3 server pods are being deleted", analysis.ErrorMessage)

	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(pods(false), cluster)...).Build()
	_, err = NewSplitBrainDetector(c).DetectSplitBrain(context.Background(), cluster)
	assert.Error(t, err, "servers that stay and cannot be reached are still a failed detection")
}
