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
