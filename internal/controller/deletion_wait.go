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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// deletionCleanup is one deletion that has to remove something in Neo4j — a
// user, a role, a grant, an auth rule, a database, a composite, an alias — and
// what it does while that cannot be done yet (knowledge rule 178).
//
// Its target CR exists, so what the deletion would remove may still be in
// Neo4j: the target restarts, reads Ready before its Bolt port accepts
// connections, has its servers stopped by a stopCluster restore, or the
// statement failed. Releasing the finalizer then removed the CR and left the
// user, the grant or the rule in place — access the user had asked to revoke.
// The deletion waits instead: the reason is recorded in status and announced
// once, and it retries. Once finalizerDeletionGracePeriod has passed since the
// deletion was requested, it releases the finalizer — a CR that cannot be
// deleted wedges a namespace — with one Warning naming what was left in Neo4j
// and the statement that removes it by hand. A target CR that no longer exists
// is not this case: the caller releases at once, as nothing can be left in a
// deployment that is gone.
type deletionCleanup struct {
	obj      client.Object
	recorder record.EventRecorder
	// reason is the kind's Warning reason for a deletion that cannot finish.
	reason string
	// leftBehind names what stays in Neo4j if the finalizer is released now,
	// e.g. `user "alice" on clusterRef "s1"`.
	leftBehind string
	// byHand is the statement that removes it, e.g. "DROP USER `alice`".
	byHand string
	// record writes msg to the object's status and reports whether that
	// changed it, so the wait is announced once per cause (rule 145).
	record func(msg string) bool
	// release removes the finalizer.
	release func() error
	requeue time.Duration
}

// targetDownCause is the cause of a deletion that waits on a target that is not
// Ready or does not accept connections yet. Both are one restart, so they are
// one cause, announced once; the error itself is logged at info.
func targetDownCause(clusterRef string) string {
	return fmt.Sprintf("%s is not Ready or does not accept connections yet", targetRefDisplay(clusterRef))
}

// connectFailedCause is the cause of a deletion whose connection failed for a
// reason other than reaching the target: the admin Secret cannot be read, the
// login is refused. Unreachable is targetDownCause.
func connectFailedCause(clusterRef string, err error) string {
	if targetUnreachable(err) {
		return targetDownCause(clusterRef)
	}
	return fmt.Sprintf("cannot connect to %s: %v", targetRefDisplay(clusterRef), err)
}

// statementFailedCause is the cause of a deletion whose cleanup statement
// failed. A target that went away mid-statement is targetDownCause.
func statementFailedCause(clusterRef, statement string, err error) string {
	if targetUnreachable(err) {
		return targetDownCause(clusterRef)
	}
	return fmt.Sprintf("%s failed: %v", statement, err)
}

// failed is blocked for a cleanup err stopped, with cause naming it. A
// transient Neo4j asks to be retried is retried quietly within the grace
// period — logged at info, nothing on status, no event, since the retry
// usually succeeds (rule 164) — and is the wait's release Warning only if it
// lasts past it. A deadlock between the DROPs of a bulk delete raised a
// RoleDeletionFailed Warning before the retry succeeded (round-8 walk).
func (d deletionCleanup) failed(ctx context.Context, cause string, err error) (ctrl.Result, error) {
	if d.retryTransient(ctx, err) {
		return ctrl.Result{RequeueAfter: transientRetry}, nil
	}
	return d.blocked(ctx, cause)
}

// retryTransient reports whether err is failed's quiet retry — the caller
// requeues after transientRetry — for a deletion that reports other failures
// its own way (the Neo4jDatabase's DROP).
func (d deletionCleanup) retryTransient(ctx context.Context, err error) bool {
	if !statementTransient(err) || deletionGracePeriodExceeded(d.obj) {
		return false
	}
	log.FromContext(ctx).Info("Neo4j asked for the cleanup to be retried; retrying",
		"leftBehind", d.leftBehind, "error", err.Error())
	return true
}

// deletionDeadline is when a deletion that cannot finish stops waiting
// (finalizerDeletionGracePeriod after it was requested).
func deletionDeadline(obj client.Object) time.Time {
	ts := obj.GetDeletionTimestamp()
	if ts == nil {
		return time.Now().Add(finalizerDeletionGracePeriod)
	}
	return ts.Add(finalizerDeletionGracePeriod)
}

// blocked records that the cleanup cannot run because of cause and retries,
// or, once the grace period is over, releases the finalizer and says what was
// left behind. Logged at info: a deletion during a restart is routine (rule
// 164); the Warning is the record.
func (d deletionCleanup) blocked(ctx context.Context, cause string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !deletionGracePeriodExceeded(d.obj) {
		msg := fmt.Sprintf("deletion waits to remove %s: %s; if that still holds at %s, the finalizer is "+
			"released and it is left in Neo4j", d.leftBehind, cause,
			deletionDeadline(d.obj).UTC().Format(time.RFC3339))
		if d.record(msg) {
			d.recorder.Event(d.obj, corev1.EventTypeWarning, d.reason, msg)
		}
		logger.Info("Deletion waits for its target", "leftBehind", d.leftBehind, "cause", cause)
		requeue := d.requeue
		if requeue <= 0 {
			// A zero RequeueAfter would retry only on a watch event.
			requeue = 30 * time.Second
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	d.recorder.Event(d.obj, corev1.EventTypeWarning, d.reason, fmt.Sprintf(
		"Released the finalizer without removing %s: %s, %s after the deletion was requested. "+
			"It is still in Neo4j; remove it by hand: %s", d.leftBehind, cause, finalizerDeletionGracePeriod, d.byHand))
	logger.Info("Deletion stopped waiting; finalizer released", "leftBehind", d.leftBehind, "cause", cause)
	return ctrl.Result{}, d.release()
}

// targetFate is what becomes of what a dependent's deletion would remove —
// a user, a grant, a database — when its target CR is being deleted or is
// gone. It turns on the target's volumes, which hold the system database:
// storage.retentionPolicy Delete (the default) deletes them with the target,
// Retain keeps them, and a deployment created again from kept volumes has
// everything they held (knowledge rule 178).
type targetFate int

const (
	// targetHolds: the target exists and is not being deleted, or is being
	// deleted but keeps its volumes. The cleanup runs, or waits, as for any
	// target: a target that is being deleted may still serve the DROP.
	targetHolds targetFate = iota
	// targetTakesIt: the target is gone, or being deleted, and its volumes
	// with it. Nothing can be left; the finalizer is released at once.
	targetTakesIt
	// targetVolumesKept: the target CR is gone but volumes of it remain. The
	// deletion waits, bounded, for a deployment created again from them, then
	// releases naming what they still hold.
	targetVolumesKept
)

// fateOfTarget reads the fate of target, which ResolveClusterRef resolved
// from ref in namespace: from its deletionTimestamp and retention policy while
// it exists, from the PersistentVolumeClaims of the instance once it is gone
// (the ones its deletion leaves; a claim being deleted does not count).
func fateOfTarget(ctx context.Context, c client.Client, namespace, ref string, target ResolvedTarget) (targetFate, error) {
	switch {
	case target.Cluster != nil:
		return fateOfPresent(target.Cluster, target.Cluster.Spec.Storage.RetentionPolicy), nil
	case target.Standalone != nil:
		return fateOfPresent(target.Standalone, target.Standalone.Spec.Storage.RetentionPolicy), nil
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace),
		client.MatchingLabels(resources.PVCSelectorByInstance(ref))); err != nil {
		return targetHolds, err
	}
	for i := range claims.Items {
		if claims.Items[i].DeletionTimestamp.IsZero() {
			return targetVolumesKept, nil
		}
	}
	return targetTakesIt, nil
}

func fateOfPresent(target client.Object, retentionPolicy string) targetFate {
	if target.GetDeletionTimestamp().IsZero() || retentionPolicy == "Retain" {
		return targetHolds
	}
	return targetTakesIt
}

// volumesKeptCause is the cause of a deletion that waits on a target CR that
// is gone while volumes of it remain.
func volumesKeptCause(ref string) string {
	return fmt.Sprintf("%s was deleted and its volumes were kept (storage.retentionPolicy: Retain); "+
		"a deployment created again from them still has it", targetRefDisplay(ref))
}

// settleByTarget decides the deletion by its target's fate: done with the
// finalizer released when nothing can be left, done and waiting when the
// target is gone but its volumes are kept, not done when the cleanup should
// run (targetHolds).
func (d deletionCleanup) settleByTarget(ctx context.Context, c client.Client, namespace, ref string, target ResolvedTarget) (done bool, res ctrl.Result, err error) {
	fate, err := fateOfTarget(ctx, c, namespace, ref, target)
	switch {
	case err != nil:
		return true, ctrl.Result{}, err
	case fate == targetTakesIt:
		return true, ctrl.Result{}, d.release()
	case fate == targetVolumesKept:
		res, err := d.blocked(ctx, volumesKeptCause(ref))
		return true, res, err
	}
	return false, ctrl.Result{}, nil
}
