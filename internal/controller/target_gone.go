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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// targetGoneNoticeDelay is how long a dependent whose target CR went away
// waits before its ClusterNotFound Warning. A teardown deletes the dependent
// within seconds of its target — a namespace deletion, a `kubectl delete -f`
// of a directory that lists the cluster first — and a dependent being deleted
// takes the deletion path, which raises no ClusterNotFound (knowledge rule
// 178), so the Warning is never emitted there.
const targetGoneNoticeDelay = time.Minute

// targetGoneNotices decides, for one reconciler, when a dependent's wait for
// a target CR that does not exist is announced (knowledge rule 182). A
// dependent applied before its target — it has never found one — is
// announced when the wait starts, as before. One whose target went away is
// announced only once the wait has lasted targetGoneNoticeDelay and the
// dependent is still there: in a teardown every dependent warned
// ClusterNotFound for a target deleted a moment before it (v1.23.0 journey,
// #594). The wait itself is on status at once either way. An operator restart
// during the delay forgets it; the wait is already on status, and nothing is
// announced for it, as for any wait a restarted operator finds (rule 136).
type targetGoneNotices struct{ m sync.Map }

// announce reports whether the ClusterNotFound Warning is due on this pass.
// entered is whether this pass's status write started the wait; hadTarget
// whether the dependent's status, before the write, recorded a pass past its
// target lookup (hadFoundTarget).
func (n *targetGoneNotices) announce(obj client.Object, entered, hadTarget bool, now time.Time) bool {
	key := client.ObjectKeyFromObject(obj).String()
	if entered {
		if !hadTarget {
			n.m.Delete(key)
			return true
		}
		n.m.Store(key, now)
		return false
	}
	v, ok := n.m.Load(key)
	if !ok {
		return false
	}
	if since, ok := v.(time.Time); ok && now.Sub(since) < targetGoneNoticeDelay {
		return false
	}
	n.m.Delete(key)
	return true
}

// hadFoundTarget reports whether a dependent's conditions record a pass that
// found its target: a Ready condition for any reason but the missing target.
// A new object has none.
func hadFoundTarget(conditions []metav1.Condition) bool {
	ready := meta.FindStatusCondition(conditions, ConditionTypeReady)
	return ready != nil && ready.Reason != EventReasonClusterNotFound
}
