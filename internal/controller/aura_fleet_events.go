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
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fleetFailureTracker remembers, per cluster/standalone, the last Aura Fleet
// Management registration failure that was announced as an event.
//
// Registration is retried every reconcile, so an unchanged failure would raise
// the same Warning each time. The tracker lets the controllers announce a
// failure when it first appears or when its message changes, and stay quiet
// while it repeats. It is in-memory on purpose: an operator restart re-announces
// a still-failing registration once, which is the right thing to do, and no
// status field has to carry dedupe bookkeeping (status.auraFleetManagement.message
// is also written by the provisioning phase, so it is not a reliable "last
// announced" marker).
//
// The zero value is ready to use.
type fleetFailureTracker struct {
	last sync.Map // client.ObjectKey -> string (the last announced failure message)
}

// shouldAnnounce records msg as the latest failure for key and reports whether
// it differs from the previous one (or there was none).
func (t *fleetFailureTracker) shouldAnnounce(key client.ObjectKey, msg string) bool {
	prev, loaded := t.last.Swap(key, msg)
	if !loaded {
		return true
	}
	return prev.(string) != msg
}

// clear forgets key's last failure, so a recurrence after a successful
// registration (or after the resource is deleted) is announced again.
func (t *fleetFailureTracker) clear(key client.ObjectKey) {
	t.last.Delete(key)
}

// announceFleetFailure raises the AuraFleetManagementFailed Warning for a
// registration failure, but only when its message changed since the last one
// announced for obj. The caller still records the message in
// status.auraFleetManagement; the event is the notification, the status is the
// state.
func announceFleetFailure(rec record.EventRecorder, t *fleetFailureTracker, obj client.Object, msg string) {
	if rec == nil || !t.shouldAnnounce(client.ObjectKeyFromObject(obj), msg) {
		return
	}
	rec.Eventf(obj, corev1.EventTypeWarning, EventReasonAuraFleetFailed,
		"Aura Fleet Management registration failed: %s", msg)
}
