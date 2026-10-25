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

// An all-databases restore announced each database's seed progress on every
// 30s poll. Every event spends the object's event budget — client-go's spam
// filter allows a burst of 25 per source, object and event type, then one
// every five minutes — so a long seed, or many databases, left nothing for the
// restore's outcome (#527). That outcome was not announced at all on this path
// either: it ended in status alone.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// budgetRecorder runs each event through client-go's own EventCorrelator — the
// aggregation and the spam filter the manager's broadcaster applies — and
// keeps the reasons that would reach the API server. Recorders made from one
// budgetBroadcaster share its correlator, as the recorders the manager hands
// out share one broadcaster; each has its own source component.
type budgetBroadcaster struct {
	mu         sync.Mutex
	clock      *clocktesting.FakeClock
	correlator *record.EventCorrelator
	emitted    map[string]int // reason → events the operator emitted
	delivered  map[string]int // reason → events that reached the API server
}

func newBudgetBroadcaster(clock *clocktesting.FakeClock) *budgetBroadcaster {
	return &budgetBroadcaster{
		clock: clock, correlator: record.NewEventCorrelator(clock),
		emitted: map[string]int{}, delivered: map[string]int{},
	}
}

func (b *budgetBroadcaster) recorder(component string) record.EventRecorder {
	return &budgetRecorder{b: b, component: component}
}

type budgetRecorder struct {
	b         *budgetBroadcaster
	component string
}

func (r *budgetRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	obj, ok := object.(client.Object)
	if !ok {
		panic(fmt.Sprintf("budgetRecorder: %T is not a client.Object", object))
	}
	now := metav1.NewTime(r.b.clock.Now())
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s.%x", obj.GetName(), now.UnixNano()), Namespace: obj.GetNamespace()},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Neo4jRestore", APIVersion: neo4jv1beta1.GroupVersion.String(),
			Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: obj.GetUID(),
		},
		Reason: reason, Message: message, Type: eventtype,
		Source:         corev1.EventSource{Component: r.component},
		FirstTimestamp: now, LastTimestamp: now, Count: 1,
	}
	r.b.mu.Lock()
	defer r.b.mu.Unlock()
	r.b.emitted[reason]++
	result, err := r.b.correlator.EventCorrelate(event)
	if err != nil || result.Skip {
		return
	}
	r.b.correlator.UpdateState(result.Event)
	r.b.delivered[reason]++
}

func (r *budgetRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *budgetRecorder) AnnotatedEventf(object runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

// seedingServer is a scripted Neo4j for the all-databases loop: no database
// exists, and each one comes online after the given number of polls, through
// the states a seed reports.
type seedingServer struct {
	pollsToOnline map[string]int
	polls         map[string]int
}

func (s *seedingServer) DatabaseExists(context.Context, string) (bool, error) { return false, nil }

func (s *seedingServer) RecreateDatabaseWithSeedURI(context.Context, *neo4j.Version, string, string) (bool, error) {
	return true, nil
}

func (s *seedingServer) CreateDatabaseWithSeedURIOptions(context.Context, string, string, bool) error {
	return nil
}

func (s *seedingServer) DatabaseOnlineState(_ context.Context, db string) (int, int, string, error) {
	s.polls[db]++
	n, last := s.polls[db], s.pollsToOnline[db]
	switch {
	case n >= last:
		return 3, 3, "", nil
	case n == 1:
		return 0, 0, "", nil // the recreate has not allocated it yet
	case n == last-1:
		return 2, 3, "", nil
	default:
		return 0, 3, "", nil
	}
}

func (s *seedingServer) Close() error { return nil }

// Thirty databases (the #488 upgrade test's count), one of which takes two
// hours to seed: the restore's RestoreCompleted reaches the API, and progress
// is announced per state and per heartbeat, not per poll.
func TestAllDatabasesRestore_TheOutcomeIsAnnouncedAfterALongSeed(t *testing.T) {
	ctx := context.Background()
	clock := clocktesting.NewFakeClock(time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC))
	broadcaster := newBudgetBroadcaster(clock)

	const databases = 30
	const poll = 30 * time.Second
	server := &seedingServer{pollsToOnline: map[string]int{}, polls: map[string]int{}}
	snap := neo4jv1beta1.ResolvedRestoreSource{BackupRef: "all", BackupPath: "all/",
		Storage: &neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"}}}
	for i := range databases {
		db := fmt.Sprintf("db%02d", i)
		snap.DatabaseArtifacts = append(snap.DatabaseArtifacts,
			neo4jv1beta1.DatabaseArtifact{Database: db, Filename: db + "-2026-10-24T11-00-00.backup", Type: "FULL"})
		server.pollsToOnline[db] = 5
	}
	server.pollsToOnline["db00"] = int(2 * time.Hour / poll)

	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "r-uid", Generation: 1, Finalizers: []string{RestoreFinalizer}},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "c", AllDatabases: true,
			Source: neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "all"},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{ResolvedSource: &snap},
	}
	r := statusRestoreReconciler(t, minimalClusterForRestore("c", "ns"), restore)
	r.Recorder = broadcaster.recorder("neo4j-restore-controller")
	r.ProgressRecorder = broadcaster.recorder("neo4j-restore-progress")
	r.RequeueAfter = poll
	r.seedProgress.now = clock.Now
	r.newAllDatabasesClient = func(restoreTarget) (allDatabasesRestoreClient, error) { return server, nil }

	passes := 0
	for ; passes < 2000; passes++ {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)})
		require.NoError(t, err)
		got := &neo4jv1beta1.Neo4jRestore{}
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(restore), got))
		if got.Status.Phase == StatusCompleted {
			break
		}
		clock.Step(poll)
	}
	require.Less(t, passes, 2000, "the restore completes")
	t.Logf("%d passes; emitted %v; delivered %v", passes, broadcaster.emitted, broadcaster.delivered)

	assert.Equal(t, 1, broadcaster.emitted[EventReasonRestoreCompleted], "the outcome is announced once")
	assert.Equal(t, 1, broadcaster.delivered[EventReasonRestoreCompleted], "and reaches the API")
	// One RestoreStarted for the restore, not one per database: 30 of them
	// outran the progress source's budget and 13 reached the API (#573).
	assert.Equal(t, 1, broadcaster.emitted[EventReasonRestoreStarted], "the start is announced once")
	assert.Equal(t, 1, broadcaster.delivered[EventReasonRestoreStarted], "and reaches the API")
	// One progress summary for the restore on its first seed poll and one
	// every ten minutes, not one per database and state: those were 101, and
	// 55 reached the API (#573). Every summary is delivered.
	elapsed := time.Duration(passes) * poll
	assert.LessOrEqual(t, broadcaster.emitted[EventReasonRestoreSeedProgress], 1+int(elapsed/seedProgressRefresh),
		"one summary every %s over %s, not one per database change", seedProgressRefresh, elapsed)
	assert.Positive(t, broadcaster.emitted[EventReasonRestoreSeedProgress])
	assert.Equal(t, broadcaster.emitted[EventReasonRestoreSeedProgress], broadcaster.delivered[EventReasonRestoreSeedProgress],
		"every progress event reaches the API server")
}

// existingServer is seedingServer with some databases already present.
type existingServer struct {
	*seedingServer
	exists map[string]bool
}

func (s *existingServer) DatabaseExists(_ context.Context, db string) (bool, error) {
	return s.exists[db], nil
}

// What an all-databases restore says in events (#573): its start once, on its
// own source; a database that cannot be restored once, as a Warning from the
// progress source, while the others carry on; the outcome once. A database
// starting or finishing is in status.databaseResults.
func TestAllDatabasesRestore_AnnouncesItsStartAndEachFailedDatabaseOnce(t *testing.T) {
	ctx := context.Background()
	server := &existingServer{
		seedingServer: &seedingServer{pollsToOnline: map[string]int{"a": 2, "b": 2, "c": 2}, polls: map[string]int{}},
		exists:        map[string]bool{"b": true}, // and spec.options.replaceExisting is not set
	}
	snap := neo4jv1beta1.ResolvedRestoreSource{BackupRef: "all", BackupPath: "all/",
		Storage: &neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"}}}
	for _, db := range []string{"a", "b", "c"} {
		snap.DatabaseArtifacts = append(snap.DatabaseArtifacts,
			neo4jv1beta1.DatabaseArtifact{Database: db, Filename: db + "-2026-10-24T11-00-00.backup", Type: "FULL"})
	}
	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "r-uid", Generation: 1, Finalizers: []string{RestoreFinalizer}},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "c", AllDatabases: true,
			Source: neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "all"},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{ResolvedSource: &snap},
	}
	r := statusRestoreReconciler(t, minimalClusterForRestore("c", "ns"), restore)
	own, progress := record.NewFakeRecorder(100), record.NewFakeRecorder(100)
	r.Recorder, r.ProgressRecorder = own, progress
	r.newAllDatabasesClient = func(restoreTarget) (allDatabasesRestoreClient, error) { return server, nil }

	for range 20 {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)})
		require.NoError(t, err)
	}
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(restore), got))
	require.Equal(t, StatusFailed, got.Status.Phase, got.Status.Message)

	ownEvents, progressEvents := drainEvents(own), drainEvents(progress)
	started := eventsWithReason(ownEvents, EventReasonRestoreStarted)
	require.Len(t, started, 1, "the restore's start, once: %q", ownEvents)
	assert.Contains(t, started[0], `3 database(s), "a", "b", "c"`)
	assert.Empty(t, eventsWithReason(progressEvents, EventReasonRestoreStarted), "no RestoreStarted per database")
	failed := eventsWithReason(progressEvents, EventReasonRestoreDatabaseFailed)
	require.Len(t, failed, 1, "%q", progressEvents)
	assert.True(t, strings.HasPrefix(failed[0], corev1.EventTypeWarning+" "), failed[0])
	assert.Contains(t, failed[0], `database "b" failed: database already exists`)
	assert.Len(t, eventsWithReason(ownEvents, EventReasonRestoreFailed), 1, "the outcome, once")
}

func TestAllDatabasesRestoreStartedMessage(t *testing.T) {
	var results []neo4jv1beta1.DatabaseRestoreResult
	for i := range 12 {
		results = append(results, neo4jv1beta1.DatabaseRestoreResult{Database: fmt.Sprintf("db%02d", i)})
	}
	msg := allDatabasesRestoreStartedMessage(results)
	assert.Contains(t, msg, `12 database(s), "db00", "db01"`)
	assert.Contains(t, msg, `"db09" and 2 more`)
	assert.NotContains(t, msg, "db10")
}

// The summary on its own: one per restore at its first poll, then one every
// seedProgressRefresh whatever the databases and states in between; it counts
// the databases from status and says how long the database being seeded has
// shown its progress; two restores have one summary each, and a forgotten
// restore starts afresh.
func TestSeedProgressEvents(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	s := &seedProgressEvents{now: func() time.Time { return now }}
	rec := record.NewFakeRecorder(20)
	results := []neo4jv1beta1.DatabaseRestoreResult{
		{Database: "a", Phase: StatusCompleted}, {Database: "b", Phase: StatusFailed},
		{Database: "c", Phase: StatusRunning}, {Database: "d", Phase: StatusPending},
	}
	r1 := &neo4jv1beta1.Neo4jRestore{ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "ns", UID: "1"},
		Status: neo4jv1beta1.Neo4jRestoreStatus{DatabaseResults: results}}
	r2 := &neo4jv1beta1.Neo4jRestore{ObjectMeta: metav1.ObjectMeta{Name: "r2", Namespace: "ns", UID: "2"},
		Status: neo4jv1beta1.Neo4jRestoreStatus{DatabaseResults: results}}

	s.announce(rec, r1, "c", "seeding from backup; 0/3 allocations online")
	events := drainEvents(rec)
	require.Len(t, events, 1, "the first poll is announced")
	assert.Equal(t, `Normal RestoreSeedProgress 1 of 4 database(s) restored, 1 failed; seeding "c": 0/3 allocations online, unchanged for under a minute`, events[0])

	for _, progress := range []string{"1/3 allocations online", "2/3 allocations online", "2/3 allocations online"} {
		now = now.Add(2 * time.Minute)
		s.announce(rec, r1, "c", progress)
	}
	assert.Empty(t, drainEvents(rec), "a change is not an event of its own")

	now = now.Add(seedProgressRefresh - 6*time.Minute)
	s.announce(rec, r1, "c", "2/3 allocations online")
	events = drainEvents(rec)
	require.Len(t, events, 1, "one summary a refresh period after the last")
	assert.Contains(t, events[0], `seeding "c": 2/3 allocations online, unchanged for 6m`, "how long it has stood still")

	// Two hours more of polls every five minutes: a summary every ten.
	for range 24 {
		now = now.Add(5 * time.Minute)
		s.announce(rec, r1, "c", "2/3 allocations online")
	}
	events = drainEvents(rec)
	require.Len(t, events, 12)
	assert.Contains(t, events[11], "unchanged for 2h06m", "a stall shows as its length")

	s.announce(rec, r2, "c", "2/3 allocations online")
	assert.Len(t, drainEvents(rec), 1, "another restore has a summary of its own")

	s.forget(r1)
	s.announce(rec, r1, "d", "seeding from backup; last status poll FAILED: boom")
	events = drainEvents(rec)
	require.Len(t, events, 1, "a forgotten restore starts afresh")
	assert.True(t, strings.HasSuffix(events[0], `seeding "d": last status poll FAILED: boom, unchanged for under a minute`), events[0])
}
