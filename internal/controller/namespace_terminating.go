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

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A namespace being deleted is not an operator failure (knowledge
// operations.md id 232).
//
// The namespace controller deletes what a namespace holds in no fixed order,
// so a deployment's Services, ConfigMap or CronJob can go before its CR is
// marked for deletion. The operator watches what it owns: it reconciles the
// CR and creates them again, and the API server refuses each create with
// Forbidden, cause NamespaceTerminating ("unable to create new content in
// namespace … because it is being terminated"). The v1.24.0 journey's
// namespace deletes logged 8 ERROR "Failed to reconcile standalone deployment"
// lines, each with a controller-runtime "Reconciler error" (#619 S4). Nothing
// had failed: the namespace controller marks the CR next, and its finalizer
// does the rest. client-go itself logs the same refusal of an event at V(5).

// namespaceTerminating reports an error the API server returned because the
// object's namespace is being deleted, however deep it is wrapped (%w).
func namespaceTerminating(err error) bool {
	return err != nil && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// logNamespaceTerminating logs, at V(1), a pass that ends because its
// namespace is being deleted.
func logNamespaceTerminating(logger logr.Logger, err error) {
	logger.V(1).Info("The namespace is being deleted; the pass ends here and the deletion goes on through the finalizer",
		"refused", err.Error())
}

// failPass ends a cluster pass whose write failed: an ERROR, the cluster
// Failed with statusMessage, and err returned so the pass is retried. A write
// refused because the namespace is being deleted is none of those: the pass
// ends without an error, no status is written, and the deletion goes on.
func (r *Neo4jEnterpriseClusterReconciler) failPass(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	err error, logMessage, statusMessage string, keysAndValues ...any) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if namespaceTerminating(err) {
		logNamespaceTerminating(logger, err)
		return ctrl.Result{}, nil
	}
	logger.Error(err, logMessage, keysAndValues...)
	_ = r.updateClusterStatus(ctx, cluster, "Failed", statusMessage)
	return ctrl.Result{}, err
}

// endsWithItsNamespace wraps every reconciler the operator registers
// (SetupWithManager), so no kind reports a namespace being deleted as a
// failure, whichever write ran into it:
//   - the pass's logger writes an ERROR whose error is that refusal at V(1)
//     instead (namespaceEndingSink), wherever in the pass it is logged;
//   - a pass that returns that refusal returns no error, so controller-runtime
//     logs no "Reconciler error" and does not retry it with backoff. The CR is
//     marked for deletion next, which is a new event.
//
// A kind that writes a Failed status for a failed write checks
// namespaceTerminating first (the standalone, the cluster's failPass, the
// backup), so the status does not say Failed either.
func endsWithItsNamespace(r reconcile.Reconciler) reconcile.Reconciler {
	return namespaceEndingReconciler{Reconciler: r}
}

type namespaceEndingReconciler struct {
	reconcile.Reconciler
}

func (n namespaceEndingReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	if sink := logger.GetSink(); sink != nil {
		ctx = log.IntoContext(ctx, logger.WithSink(newNamespaceEndingSink(sink)))
	}
	result, err := n.Reconciler.Reconcile(ctx, req)
	if namespaceTerminating(err) {
		logNamespaceTerminating(logger, err)
		return reconcile.Result{}, nil
	}
	return result, err
}

// namespaceEndingSink passes every call on to the sink it wraps, except an
// Error whose error is a namespace-terminating refusal: that one is written at
// V(1), with the error as a value.
type namespaceEndingSink struct {
	delegate logr.LogSink
}

// newNamespaceEndingSink wraps sink. Every call goes through one more frame,
// this sink's own method, so a sink that names the caller is told to skip it:
// the line still names the reconciler's.
func newNamespaceEndingSink(sink logr.LogSink) logr.LogSink {
	if cd, ok := sink.(logr.CallDepthLogSink); ok {
		sink = cd.WithCallDepth(1)
	}
	return namespaceEndingSink{delegate: sink}
}

func (s namespaceEndingSink) Init(info logr.RuntimeInfo) { s.delegate.Init(info) }

func (s namespaceEndingSink) Enabled(level int) bool { return s.delegate.Enabled(level) }

func (s namespaceEndingSink) Info(level int, msg string, keysAndValues ...any) {
	s.delegate.Info(level, msg, keysAndValues...)
}

func (s namespaceEndingSink) Error(err error, msg string, keysAndValues ...any) {
	if !namespaceTerminating(err) {
		s.delegate.Error(err, msg, keysAndValues...)
		return
	}
	if s.delegate.Enabled(1) {
		s.delegate.Info(1, msg, append(append([]any{}, keysAndValues...), "error", err.Error(), "namespaceTerminating", true)...)
	}
}

func (s namespaceEndingSink) WithValues(keysAndValues ...any) logr.LogSink {
	return namespaceEndingSink{delegate: s.delegate.WithValues(keysAndValues...)}
}

func (s namespaceEndingSink) WithName(name string) logr.LogSink {
	return namespaceEndingSink{delegate: s.delegate.WithName(name)}
}

func (s namespaceEndingSink) WithCallDepth(depth int) logr.LogSink {
	if cd, ok := s.delegate.(logr.CallDepthLogSink); ok {
		return namespaceEndingSink{delegate: cd.WithCallDepth(depth)}
	}
	return s
}
