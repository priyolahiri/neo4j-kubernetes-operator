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

// Shard-level status of a Neo4jShardedDatabase: status.graphShard,
// status.propertyShards, status.virtualDatabase and status.creationTime.
//
// Everything here is derived from the SHOW DATABASES rows updateShardStatus
// already fetches; nothing extra is queried. SHOW DATABASES carries no store
// size, server address or property count, so shard size, servers, lastError and
// propertyCount, the virtual database's endpoint and metrics, and
// status.totalSize are deliberately left unpopulated (documented as such on the
// API types) rather than guessed.

import (
	"context"
	"regexp"
	"sort"
	"strconv"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

const (
	shardTypeGraph    = "graph"
	shardTypeProperty = "property"
	// shardStateOnline is the SHOW DATABASES currentStatus of a healthy copy.
	shardStateOnline = "online"

	// shardSettleWindow bounds how long after creation a sharded database is
	// re-read quickly while a shard is still settling, and shardSettleRequeue
	// is how soon. Bounded so a family someone deliberately stopped is not
	// re-read every few seconds forever: past the window the ordinary periodic
	// reconcile picks up any change.
	shardSettleWindow  = 10 * time.Minute
	shardSettleRequeue = 15 * time.Second
)

// shardFamilySettling reports whether the recorded shard status is a snapshot
// of a family that is still starting, and so worth re-reading soon. The status
// is written from the SHOW DATABASES pass right after CREATE returns, when
// Neo4j can still report a shard as "starting"; nothing re-read it before the
// five-minute periodic reconcile, so a Ready sharded database showed a starting
// graph shard for minutes (found on the v1.18.0 journey).
func shardFamilySettling(status *neo4jv1beta1.Neo4jShardedDatabaseStatus, now time.Time) bool {
	if status.CreationTime == nil || now.Sub(status.CreationTime.Time) > shardSettleWindow {
		return false
	}
	if status.GraphShard == nil || !status.GraphShard.Ready {
		return true
	}
	for _, shard := range status.PropertyShards {
		if !shard.Ready {
			return true
		}
	}
	return false
}

// mirrorShardObservation copies what applyShardStatus just wrote onto the
// reconcile's own copy of the object, so the end of the reconcile can decide
// from it (shardFamilySettling) how soon to look again. applyShardStatus works
// on a freshly read copy, which the caller never sees.
func mirrorShardObservation(shardedDB *neo4jv1beta1.Neo4jShardedDatabase, obs shardStatusObservation, recreated bool, now time.Time) {
	shardedDB.Status.GraphShard = obs.GraphShard
	shardedDB.Status.PropertyShards = obs.PropertyShards
	shardedDB.Status.VirtualDatabase = obs.VirtualDatabase
	if recreated {
		shardedDB.Status.CreationTime = nil
	}
	if obs.Observed && shardedDB.Status.CreationTime == nil {
		created := metav1.NewTime(now)
		shardedDB.Status.CreationTime = &created
	}
}

// shardStatusObservation is what one SHOW DATABASES pass says about one sharded
// database's family: the parent (virtual) database, its graph shard and its
// property shards.
type shardStatusObservation struct {
	GraphShard      *neo4jv1beta1.ShardStatus
	PropertyShards  []neo4jv1beta1.ShardStatus
	VirtualDatabase *neo4jv1beta1.VirtualDatabaseStatus
	// Observed reports whether the database has been created as far as the
	// operator can tell: its graph shard is visible. It gates creationTime.
	Observed bool
}

// summarizeShardCopies folds the per-server rows of one database into the
// shard's state and readiness.
//
// state is "online" when every copy is online and otherwise the first
// non-online state (by name, so the answer does not depend on the order SHOW
// DATABASES returned the rows in and the status cannot flap on it). ready means
// at least one copy is online and no copy that is supposed to be online is not
// -- the same "requested online but not online" test DatabasesHealthy uses --
// so a copy being rebuilt shows as not ready while a deliberately stopped one
// beside a serving copy does not.
func summarizeShardCopies(rows []neo4j.DatabaseInfo) (state string, ready bool) {
	anyOnline, degraded := false, false
	var notOnline []string
	for _, row := range rows {
		if row.Status == shardStateOnline {
			anyOnline = true
			continue
		}
		notOnline = append(notOnline, row.Status)
		if row.RequestedStatus == shardStateOnline {
			degraded = true
		}
	}
	if len(notOnline) == 0 {
		return shardStateOnline, anyOnline
	}
	sort.Strings(notOnline)
	return notOnline[0], anyOnline && !degraded
}

// observeShardFamily reads the family of the logical database `logical` out of
// a SHOW DATABASES result. Only names that are exactly the logical name, its
// graph shard `<logical>-g000`, or a property shard `<logical>-pNNN` count:
// a database that merely shares the prefix (`orders2`, `orders-archive`) is not
// part of the family.
func observeShardFamily(logical string, databases []neo4j.DatabaseInfo) shardStatusObservation {
	graphName := logical + "-g000"
	propertyPattern := regexp.MustCompile(`^` + regexp.QuoteMeta(logical) + `-p(\d{3})$`)

	var parent, graph []neo4j.DatabaseInfo
	property := map[int][]neo4j.DatabaseInfo{}
	for _, db := range databases {
		switch db.Name {
		case logical:
			parent = append(parent, db)
		case graphName:
			graph = append(graph, db)
		default:
			if m := propertyPattern.FindStringSubmatch(db.Name); m != nil {
				if idx, err := strconv.Atoi(m[1]); err == nil {
					property[idx] = append(property[idx], db)
				}
			}
		}
	}

	obs := shardStatusObservation{Observed: len(graph) > 0}

	if len(graph) > 0 {
		state, ready := summarizeShardCopies(graph)
		obs.GraphShard = &neo4jv1beta1.ShardStatus{
			Name: graphName, Type: shardTypeGraph, State: state, Ready: ready,
		}
	}

	indexes := make([]int, 0, len(property))
	for idx := range property {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	for _, idx := range indexes {
		state, ready := summarizeShardCopies(property[idx])
		i := int32(idx) // #nosec G115 -- bounded by the three-digit name pattern
		obs.PropertyShards = append(obs.PropertyShards, neo4jv1beta1.ShardStatus{
			Name:               property[idx][0].Name,
			Type:               shardTypeProperty,
			State:              state,
			Ready:              ready,
			PropertyShardIndex: &i,
		})
	}

	if len(parent) > 0 {
		_, ready := summarizeShardCopies(parent)
		obs.VirtualDatabase = &neo4jv1beta1.VirtualDatabaseStatus{Name: logical, Ready: ready}
	}
	return obs
}

// applyShardStatus writes the observed shard state onto the CR's status.
//
// It writes only when something differs from what is already persisted: every
// status write is a watch event that reconciles the CR again, so an
// unconditional write would turn the periodic reconcile into a self-triggering
// loop. The object is re-fetched inside the conflict-retry closure.
//
// creationTime is set once, the first time the database's graph shard is seen
// (the operator's observation of the database existing, not the CR's own
// creationTimestamp), and kept across reconciles. recreated is true for the
// reconcile that dropped and recreated the database (replaceExisting + force):
// that is a new database, so the stamp restarts rather than reporting the
// dropped one's age.
func (r *Neo4jShardedDatabaseReconciler) applyShardStatus(
	ctx context.Context,
	shardedDB *neo4jv1beta1.Neo4jShardedDatabase,
	obs shardStatusObservation,
	recreated bool,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jShardedDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(shardedDB), latest); err != nil {
			return err
		}
		changed := false

		if !apiequality.Semantic.DeepEqual(latest.Status.GraphShard, obs.GraphShard) {
			latest.Status.GraphShard = obs.GraphShard
			changed = true
		}
		// Semantic equality treats nil and empty slices as equal, so "no
		// property shards observed" on an already-empty status is not a change.
		if !apiequality.Semantic.DeepEqual(latest.Status.PropertyShards, obs.PropertyShards) {
			latest.Status.PropertyShards = obs.PropertyShards
			changed = true
		}
		if !apiequality.Semantic.DeepEqual(latest.Status.VirtualDatabase, obs.VirtualDatabase) {
			latest.Status.VirtualDatabase = obs.VirtualDatabase
			changed = true
		}

		if recreated && latest.Status.CreationTime != nil {
			latest.Status.CreationTime = nil
			changed = true
		}
		if obs.Observed && latest.Status.CreationTime == nil {
			now := metav1.Now()
			latest.Status.CreationTime = &now
			changed = true
		}

		if !changed {
			return nil
		}
		return r.Status().Update(ctx, latest)
	})
}
