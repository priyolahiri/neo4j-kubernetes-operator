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
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// transientRetry is how soon a statement Neo4j refused with an error it asks
// to be retried — a deadlock in a bulk change, a leader switch, a conflicting
// transaction — is tried again.
const transientRetry = 5 * time.Second

// statementTransient reports an error Neo4j asks the client to retry
// (IsTransientError: Neo.TransientError.*, NotALeader, a conflicting
// transaction state) that is not the target being down: that one has its own
// wait, reported (targetUnreachable; rules 172, 178).
func statementTransient(err error) bool {
	return err != nil && !targetUnreachable(err) && neo4jclient.IsTransientError(err)
}

// transientStreakGap is how long a pass of an object can go without meeting
// a transient before its streak ends: a pass in between succeeded, or failed
// for another reason.
const transientStreakGap = 2 * time.Minute

type transientStreak struct{ first, last time.Time }

// transientStreaks remembers, for a reconciler, since when the passes of each
// object keep meeting a statement Neo4j asks to be retried. The retry stays
// quiet only as long as such an error usually lasts: a deadlock between two
// writes of a bulk change resolves on the next try. A user, role, binding or
// auth rule whose GRANT or CREATE hit one went Failed with a Warning and the
// error went back to controller-runtime, logged at ERROR, before the retry
// succeeded (knowledge rule 164).
type transientStreaks struct{ m sync.Map }

// quiet reports whether err, which the pass of obj met at step label, is
// retried quietly: a transient (statementTransient) whose streak is younger
// than targetUnreachableGrace, logged at info, nothing on status, no event.
// Past the grace it is the object's failure, reported as any other.
func (s *transientStreaks) quiet(ctx context.Context, obj client.Object, label string, err error) bool {
	if !statementTransient(err) {
		return false
	}
	key := client.ObjectKeyFromObject(obj).String()
	now := time.Now()
	streak := transientStreak{first: now, last: now}
	if v, ok := s.m.Load(key); ok {
		if prev, ok := v.(transientStreak); ok && now.Sub(prev.last) <= transientStreakGap {
			streak.first = prev.first
		}
	}
	s.m.Store(key, streak)
	if now.Sub(streak.first) > targetUnreachableGrace {
		return false
	}
	log.FromContext(ctx).Info("Neo4j asked for the statement to be retried; retrying",
		"step", label, "error", err.Error())
	return true
}
