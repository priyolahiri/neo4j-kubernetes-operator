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
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// advisoryWarningRefresh is how often a warning that still applies is
// announced again. Events expire (the API server's --event-ttl, one hour by
// default), so a warning announced only once would vanish from kubectl
// describe and kubectl neo4j diagnose while it is still true. Announcing it on
// every reconcile pass instead was the bug: 35 identical events in three
// minutes on one standalone (v1.21.0 journey), every one of them spending the
// object's event budget — client-go's spam filter allows a burst of 25 per
// object, then one every five minutes, so the flood could crowd out the events
// that report real changes.
const advisoryWarningRefresh = 30 * time.Minute

// AdvisoryWarningRefresh is advisoryWarningRefresh for readers outside this
// package: an advisory Warning last seen within it may still apply, because it
// would have been announced again by now if it did.
const AdvisoryWarningRefresh = advisoryWarningRefresh

// AdvisoryWarningReasons are the reasons the reconcilers announce through
// advisoryWarnings (record and recordScoped): Warnings that stay true while
// the object is otherwise fine, announced again every advisoryWarningRefresh
// while they apply. `kubectl neo4j diagnose` reads one seen within that time
// as still applying, whatever the object's conditions did since
// (operations.md id 168). EventReasonUpgradeFailed is left out: the
// standalone's pre-upgrade block is announced this way, but the reason is a
// one-time failure everywhere else. TestAdvisoryWarningReasons_ListsEveryRecordedReason
// fails for a reason recorded here and not listed.
var AdvisoryWarningReasons = []string{
	EventReasonValidationWarning,
	EventReasonTopologyWarning,
	EventReasonTopologyZoneDiscoveryDegraded,
	EventReasonRouteAPINotFound,
	EventReasonMCPApocMissing,
	EventReasonReplicationSourceCaveat,
	EventReasonBackupValidateUnsupported,
	EventReasonServiceAccountAnnotationConflict,
	EventReasonRestoreShardedNotCovered,
	EventReasonRestoreFromChainParent,
	EventReasonPrivilegesDriftKept,
}

// advisoryWarnings remembers which advisory warnings each object was last
// given, and when each was announced, so the reconciler can emit a warning when
// it starts to apply and every advisoryWarningRefresh while it still does — not
// on every pass. The zero value is ready to use. State is in memory: an
// operator restart announces each warning once more, which is fine.
//
// A warning that stops applying is forgotten, so one that comes back is
// announced again at once.
type advisoryWarnings struct {
	mu        sync.Mutex
	announced map[advisoryWarningsKey]map[string]time.Time
	lastSweep time.Time
	// now replaces the clock in tests; nil means time.Now.
	now func() time.Time
}

type advisoryWarningsKey struct {
	object string // type, namespace, name and UID: a recreated object starts afresh
	reason string
	scope  string // see recordScoped
}

// record emits each of warnings as a Warning event with reason on obj, unless
// it was already announced for this object within advisoryWarningRefresh.
//
// warnings is everything that applies for reason right now: a warning left out
// is forgotten. Where independent checks share one reason, use recordScoped.
func (a *advisoryWarnings) record(rec record.EventRecorder, obj client.Object, reason string, warnings []string) {
	a.recordScoped(rec, obj, reason, "", warnings)
}

// recordScoped is record for one of several independent checks that share a
// reason (the cluster's Route and its MCP Route both report RouteAPINotFound):
// each scope keeps its own set, so a pass of one check does not make another's
// warning look new.
func (a *advisoryWarnings) recordScoped(rec record.EventRecorder, obj client.Object, reason, scope string, warnings []string) {
	if rec == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	if a.now != nil {
		now = a.now()
	}
	if a.announced == nil {
		a.announced = map[advisoryWarningsKey]map[string]time.Time{}
	}
	a.sweepLocked(now)

	key := advisoryWarningsKey{
		object: fmt.Sprintf("%T/%s/%s/%s", obj, obj.GetNamespace(), obj.GetName(), obj.GetUID()),
		reason: reason,
		scope:  scope,
	}
	previous := a.announced[key]
	current := make(map[string]time.Time, len(warnings))
	for _, warning := range warnings {
		if _, dup := current[warning]; dup {
			continue
		}
		if at, ok := previous[warning]; ok && now.Sub(at) < advisoryWarningRefresh {
			current[warning] = at
			continue
		}
		rec.Event(obj, corev1.EventTypeWarning, reason, warning)
		current[warning] = now
	}
	if len(current) == 0 {
		delete(a.announced, key)
		return
	}
	a.announced[key] = current
}

// sweepLocked drops, at most once per advisoryWarningRefresh, the objects
// whose every warning was announced longer ago than that — deleted objects,
// mostly. Dropping them changes nothing: each would be announced again on its
// next pass anyway.
func (a *advisoryWarnings) sweepLocked(now time.Time) {
	if now.Sub(a.lastSweep) < advisoryWarningRefresh {
		return
	}
	a.lastSweep = now
	for key, announced := range a.announced {
		stale := true
		for _, at := range announced {
			if now.Sub(at) < advisoryWarningRefresh {
				stale = false
				break
			}
		}
		if stale {
			delete(a.announced, key)
		}
	}
}
