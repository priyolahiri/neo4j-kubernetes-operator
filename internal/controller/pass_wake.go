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

	ctrl "sigs.k8s.io/controller-runtime"
)

// passWakes is, per cluster (key ns/name), how soon something the pass waits
// for is due: the end of a scale-down drain's settle after its cordon, the
// next look at a raise held for the removed servers' pods and volumes
// (operations.md ids 229, 231). Nothing in Kubernetes changes when such a
// wait ends — a time passes, a volume the garbage collector deletes is not
// watched — so the pass asks to run again by then. A cluster is never
// reconciled twice at once, so one entry per key is the pass's own.
type passWakes struct{ m sync.Map }

// note asks for the cluster's next pass no later than after from now.
func (w *passWakes) note(key string, after time.Duration) {
	if after <= 0 {
		return
	}
	if prev, ok := w.m.Load(key); ok {
		if d, isDuration := prev.(time.Duration); isDuration && d <= after {
			return
		}
	}
	w.m.Store(key, after)
}

// apply takes what the pass noted and brings res forward to it. A pass that
// failed is retried on controller-runtime's backoff, which ignores the
// result; one that asked to run again at once keeps that.
func (w *passWakes) apply(key string, res ctrl.Result, err error) ctrl.Result {
	v, ok := w.m.LoadAndDelete(key)
	if !ok || err != nil {
		return res
	}
	after, isDuration := v.(time.Duration)
	if !isDuration || after <= 0 {
		return res
	}
	if res.RequeueAfter > 0 && res.RequeueAfter <= after {
		return res
	}
	if res.RequeueAfter == 0 && res.Requeue { //nolint:staticcheck // Requeue is what the pass returned
		return res
	}
	res.RequeueAfter = after
	return res
}
