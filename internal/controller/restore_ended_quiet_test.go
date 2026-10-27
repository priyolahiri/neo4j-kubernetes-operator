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

// A restore that ended at its generation (#606, knowledge id 215): a refused
// restore logged its refusal once at info (#602), then "Restore previously
// failed; not retrying …" at info about every 30 seconds — every resync of the
// dev-mode cache (configureDevelopmentCache) ran the terminal pass, and the
// pass logged at info. Each pass after the end now returns no requeue, writes
// nothing, emits nothing and logs at V(1) only; a new generation runs the
// restore again.

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// writeCountingRestoreReconciler is statusRestoreReconciler with every write
// the client makes — of any object, status or not — counted.
func writeCountingRestoreReconciler(t *testing.T, objs ...runtime.Object) (*Neo4jRestoreReconciler, *int) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	writes := new(int)
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jRestore{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				*writes++
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				*writes++
				return cl.Update(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				*writes++
				return cl.Patch(ctx, obj, patch, opts...)
			},
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				*writes++
				return cl.Delete(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				*writes++
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				*writes++
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	return &Neo4jRestoreReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16), RequeueAfter: 30 * time.Second}, writes
}

// endedRestore is restore "ns/r" into standalone "sa", at generation 1, which
// ended in phase at that generation.
func endedRestore(phase, message string) *neo4jv1beta1.Neo4jRestore {
	restore := pointInTimeIntoStandalone()
	restore.Generation = 1
	restore.Finalizers = []string{RestoreFinalizer}
	restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: phase, Message: message, ObservedGeneration: 1}
	return restore
}

// finishedRestoreJob is the restore's Job, finished with condition, still there
// before its TTL removes it.
func finishedRestoreJob(condition batchv1.JobConditionType) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "r-restore", Namespace: "ns"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: condition, Status: corev1.ConditionTrue,
		}}},
	}
}

// infoLines returns every line logged at info (V(0)), and errorLines every
// ERROR.
func infoLines(sink levelCaptureSink) []string {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	var out []string
	for _, l := range *sink.lines {
		if l.level == 0 {
			out = append(out, l.msg)
		}
	}
	return out
}

func TestRestoreReconcile_AnEndedRestoreIsQuietOnTheNextPasses(t *testing.T) {
	cases := []struct {
		name string
		// setup returns the reconciler and the passes that end the restore
		// (0 when it has ended already).
		setup func(t *testing.T) (*Neo4jRestoreReconciler, *int, int)
		phase string
		// endLine is the one line at info the pass that ended it logs ("" for
		// a restore that had ended before the first pass).
		endLine string
		// quietLine is what the passes after log at V(1).
		quietLine string
	}{
		{
			name: "refused: database system",
			setup: func(t *testing.T) (*Neo4jRestoreReconciler, *int, int) {
				restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "system-1.backup"})
				restore.Spec.Database = "system"
				restore.Generation = 1
				restore.Finalizers = []string{RestoreFinalizer}
				r, writes := writeCountingRestoreReconciler(t, minimalStandaloneForRestore("sa", "ns"), restore)
				return r, writes, 1
			},
			phase:     StatusFailed,
			endLine:   "Restore validation failed",
			quietLine: "Restore previously failed",
		},
		{
			name: "refused: no spec.instanceRef",
			setup: func(t *testing.T) (*Neo4jRestoreReconciler, *int, int) {
				restore := minimalRestore("r", "ns", "")
				restore.Generation = 1
				restore.Finalizers = []string{RestoreFinalizer}
				r, writes := writeCountingRestoreReconciler(t, restore)
				return r, writes, 1
			},
			phase:     StatusFailed,
			endLine:   "Restore refused: spec.instanceRef is required",
			quietLine: "Restore previously failed",
		},
		{
			name: "failed: its Job failed, the Job not yet removed",
			setup: func(t *testing.T) (*Neo4jRestoreReconciler, *int, int) {
				r, writes := writeCountingRestoreReconciler(t, minimalStandaloneForRestore("sa", "ns"),
					endedRestore(StatusFailed, "Restore job r-restore failed (BackoffLimitExceeded)"),
					finishedRestoreJob(batchv1.JobFailed))
				return r, writes, 0
			},
			phase:     StatusFailed,
			quietLine: "Restore previously failed",
		},
		{
			name: "completed, its Job not yet removed",
			setup: func(t *testing.T) (*Neo4jRestoreReconciler, *int, int) {
				r, writes := writeCountingRestoreReconciler(t, minimalStandaloneForRestore("sa", "ns"),
					endedRestore(StatusCompleted, "Restore completed successfully"),
					finishedRestoreJob(batchv1.JobComplete))
				return r, writes, 0
			},
			phase:     StatusCompleted,
			quietLine: "Restore already completed",
		},
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "r"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, writes, endingPasses := tc.setup(t)
			endSink := newLevelCaptureSink()
			for i := range endingPasses {
				_, err := r.Reconcile(log.IntoContext(context.Background(), logr.New(endSink)), req)
				require.NoError(t, err, "ending pass %d", i+1)
			}
			if tc.endLine != "" {
				assert.Equal(t, []string{tc.endLine}, infoLines(endSink), "the pass that ends it logs it once at info")
			}
			ended := restoreNamed(t, r)
			require.Equal(t, tc.phase, ended.Status.Phase)
			restoreEvents(t, r) // what the ending pass announced is not the passes' after

			// Three more passes: a resync, the Job's TTL, an operator restart.
			sink := newLevelCaptureSink()
			before := *writes
			for i := range 3 {
				res, err := r.Reconcile(log.IntoContext(context.Background(), logr.New(sink)), req)
				require.NoError(t, err, "pass %d", i+1)
				assert.Equal(t, ctrl.Result{}, res, "pass %d: no requeue on a timer", i+1)
			}
			assert.Equal(t, before, *writes, "no write on a pass after the end")
			assert.Empty(t, infoLines(sink), "nothing at info on a pass after the end")
			assert.Empty(t, sink.errors(), "no ERROR")
			assert.Len(t, sink.mentioning(1, tc.quietLine), 3, "said at V(1)")
			assert.Empty(t, restoreEvents(t, r), "nothing announced")

			got := restoreNamed(t, r)
			assert.Equal(t, ended.ResourceVersion, got.ResourceVersion, "the restore is as the end left it")
			assert.Equal(t, ended.Status, got.Status)
		})
	}
}

// A spec change is a new generation: the restore that ended at the last one
// runs again — here it looks its target up, finds none, and waits for it.
func TestRestoreReconcile_ANewGenerationRunsAnEndedRestoreAgain(t *testing.T) {
	for _, phase := range []string{StatusFailed, StatusCompleted} {
		t.Run(phase, func(t *testing.T) {
			restore := endedRestore(phase, "ended at generation 1")
			restore.Generation = 2
			restore.Spec.InstanceRef = "not-created-yet"
			r, writes := writeCountingRestoreReconciler(t, restore)
			sink := newLevelCaptureSink()

			res, err := r.Reconcile(log.IntoContext(context.Background(), logr.New(sink)),
				ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "r"}})
			require.NoError(t, err)
			assert.Equal(t, r.RequeueAfter, res.RequeueAfter, "a wait for the target requeues")
			assert.Positive(t, *writes)

			got := restoreNamed(t, r)
			assert.Equal(t, StatusPending, got.Status.Phase)
			assert.Equal(t, int64(2), got.Status.ObservedGeneration)
			assert.Empty(t, sink.mentioning(1, "Restore previously failed"))
			assert.Empty(t, sink.mentioning(1, "Restore already completed"))
		})
	}
}

// Kubernetes removes a finished restore Job (its TTL), not a requeue of the
// restore: the Job carries ttlSecondsAfterFinished, so a restore that ended
// and is never run again still loses its Job.
func TestRestoreJob_IsRemovedByItsTTLNotByARequeue(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := pointInTimeIntoStandalone()
	r := statusRestoreReconciler(t, sa, restore)

	_, err := r.startRestore(context.Background(), restore, standaloneAsCluster(sa))
	require.NoError(t, err)

	job := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "r-restore"}, job))
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished)
	assert.Positive(t, *job.Spec.TTLSecondsAfterFinished)
}
