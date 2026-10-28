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
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// parseServerOrdinal extracts the StatefulSet pod ordinal N from a Neo4j server
// address or name of the form "<cluster>-server-<N>[...]". Server identity in
// SHOW SERVERS is by address; the operator's pods are "<cluster>-server-<N>",
// so the ordinal tells us which server a row corresponds to (rule 47).
func parseServerOrdinal(s, clusterName string) (int, bool) {
	prefix := clusterName + "-server-"
	idx := strings.Index(s, prefix)
	if idx < 0 {
		return 0, false
	}
	rest := s[idx+len(prefix):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0, false
	}
	return n, true
}

// ---------------------------------------------------------------------------
// #173 PR2: automated scale-down drain (cordon -> deallocate -> wait -> drop).
// ---------------------------------------------------------------------------

// ScaleDownDrainingServersAnnotation holds the comma-separated Neo4j serverIds
// the operator is draining for an in-progress scale-down. Its presence gates
// the StatefulSet apply (scaleDownDrainInProgress) to HOLD replicas until the
// drain finishes. Tracking by serverId — NOT pod ordinal — is essential: once a
// server enters Deallocating its SHOW SERVERS address becomes NULL (and name is
// just the UUID), so the ordinal can no longer be derived. The id set is
// captured once at detection (while addresses are present), then driven until
// every id is Dropped/gone. Also makes the drain resumable after an operator
// restart.
const ScaleDownDrainingServersAnnotation = "neo4j.com/scale-down-draining-servers"

// ScaleDownCordonedServersAnnotation holds the comma-separated Neo4j serverIds
// the drain itself cordoned, written before the CORDON runs. It is what a
// scale-down set back before its drain ended gives back with ENABLE SERVER,
// and nothing else: SHOW SERVERS does not say who cordoned a server, and the
// drain set alone would include a server cordoned by hand, which the drain
// deallocates without cordoning it (operations.md id 224).
const ScaleDownCordonedServersAnnotation = "neo4j.com/scale-down-cordoned-servers"

func scaleDownDrainInProgress(owner client.Object) bool {
	if owner == nil {
		return false
	}
	return strings.TrimSpace(owner.GetAnnotations()[ScaleDownDrainingServersAnnotation]) != ""
}

func drainServerIDsFromAnnotation(owner client.Object) []string {
	return annotationIDs(owner, ScaleDownDrainingServersAnnotation)
}

// cordonedByDrain is the record of the servers the drain cordoned.
func cordonedByDrain(owner client.Object) []string {
	return annotationIDs(owner, ScaleDownCordonedServersAnnotation)
}

// annotationIDs splits a comma-separated id list annotation.
func annotationIDs(owner client.Object, key string) []string {
	if owner == nil {
		return nil
	}
	var ids []string
	for _, p := range strings.Split(owner.GetAnnotations()[key], ",") {
		if p = strings.TrimSpace(p); p != "" {
			ids = append(ids, p)
		}
	}
	return ids
}

// blockScaleDown records why the scale-down is blocked in the
// ServersPendingDrain condition (replicas stay held) and announces
// ScaleDownBlocked when that write changed the condition: a new block, or a
// block for another cause, whose message says what. It is never decided from
// this pass's copy of the condition, which the informer cache can hand over
// from before the last pass's write: two passes on such a copy announced the
// same block twice (#565; operations.md id 145).
func (r *Neo4jEnterpriseClusterReconciler) blockScaleDown(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, msg string) {
	if r.setScaleDownConditionPersisted(ctx, cluster, metav1.ConditionTrue, ConditionReasonScaleDownBlocked, msg) {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonScaleDownBlocked, msg)
	}
}

func serverIdentifier(s neo4jclient.ServerInfo) string {
	if s.ID != "" {
		return s.ID
	}
	if s.Name != "" {
		return s.Name
	}
	return s.Address
}

// initialRemovedServerIDs returns the serverIds to drain for a scale-down,
// computed from servers whose pod ordinal is >= desiredServers (matched on the
// still-present address/name). Dropped servers excluded. Used at detection time
// while addresses are intact; thereafter the set is tracked by id. A server
// that is down reports no address; the caller fills in the one it last
// reported first (withKnownAddresses, operations.md id 213).
func initialRemovedServerIDs(servers []neo4jclient.ServerInfo, clusterName string, desiredServers int) []string {
	var ids []string
	for _, s := range servers {
		if strings.EqualFold(s.State, "dropped") {
			continue
		}
		if ord, ok := serverOrdinal(s, clusterName); ok && ord >= desiredServers {
			ids = append(ids, serverIdentifier(s))
		}
	}
	return ids
}

// serverOrdinal is the pod ordinal a SHOW SERVERS row names, by its address,
// else its name.
func serverOrdinal(s neo4jclient.ServerInfo, clusterName string) (int, bool) {
	if ord, ok := parseServerOrdinal(s.Address, clusterName); ok {
		return ord, true
	}
	return parseServerOrdinal(s.Name, clusterName)
}

// unidentifiedRemovedServerIDs returns the servers SHOW SERVERS lists, not
// dropped, whose ordinal neither their address — as reported, or as last
// reported — nor their name tells: a server that has been down since before
// this operator saw its address (after an operator restart, with no
// diagnostics recorded). When every server the scale-down keeps (ordinals
// below desired) is named by another row, they are none of those, and are
// drained with the removed ones. Otherwise it returns why it cannot tell, and
// the drain keeps holding replicas until it can: one of them could be a server
// the scale-down keeps (operations.md id 213).
func unidentifiedRemovedServerIDs(servers []neo4jclient.ServerInfo, clusterName string, desired int) ([]string, string) {
	kept := map[int]bool{}
	var unknown []string
	for _, s := range servers {
		if strings.EqualFold(s.State, "dropped") {
			continue
		}
		ord, ok := serverOrdinal(s, clusterName)
		if !ok {
			unknown = append(unknown, serverIdentifier(s))
			continue
		}
		if ord < desired {
			kept[ord] = true
		}
	}
	if len(unknown) == 0 {
		return nil, ""
	}
	sort.Strings(unknown)
	var missing []string
	for ord := 0; ord < desired; ord++ {
		if !kept[ord] {
			missing = append(missing, fmt.Sprintf("%s-server-%d", clusterName, ord))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Sprintf("SHOW SERVERS lists %s with no address this operator knows, and no server reports being %s: "+
			"the drain waits until it can tell which servers the scale-down removes",
			strings.Join(unknown, ", "), strings.Join(missing, ", "))
	}
	return unknown, ""
}

type scaleDownPhase int

const (
	scaleDownNone scaleDownPhase = iota
	scaleDownCordon
	scaleDownDeallocate
	scaleDownWaitDeallocating
	scaleDownDrop
)

type scaleDownStep struct {
	phase     scaleDownPhase
	serverIDs []string
}

// planScaleDownStep decides the next SINGLE drain action from the live states
// of the still-active drain-target servers (one action per call —
// requeue-driven): cordon any not yet cordoned, then deallocate the cordoned
// ones, then wait while any are deallocating, then drop the deallocated ones.
// Pure + unit-tested.
func planScaleDownStep(active []neo4jclient.ServerInfo) scaleDownStep {
	if len(active) == 0 {
		return scaleDownStep{phase: scaleDownNone}
	}
	var toCordon, toDeallocate, waiting, toDrop []string
	for _, s := range active {
		id := serverIdentifier(s)
		switch strings.ToLower(s.State) {
		case "cordoned":
			toDeallocate = append(toDeallocate, id)
		case "deallocating", "deallocated":
			// A draining server is droppable once it hosts ONLY `system` (all
			// user databases relocated). It often never leaves the
			// "Deallocating" state — a system-primary server's `system` copy is
			// only released by DROP itself — so we must NOT wait for the
			// "Deallocated" state; gate on hosting instead. DROP enforces the
			// system voting-member floor and refuses if the result is below it.
			if hostsOnlySystem(s) {
				toDrop = append(toDrop, id)
			} else {
				waiting = append(waiting, id)
			}
		default: // enabled / free / unknown → not yet cordoned
			toCordon = append(toCordon, id)
		}
	}
	switch {
	case len(toCordon) > 0:
		return scaleDownStep{phase: scaleDownCordon, serverIDs: toCordon}
	case len(toDeallocate) > 0:
		return scaleDownStep{phase: scaleDownDeallocate, serverIDs: toDeallocate}
	case len(toDrop) > 0:
		return scaleDownStep{phase: scaleDownDrop, serverIDs: toDrop}
	case len(waiting) > 0:
		return scaleDownStep{phase: scaleDownWaitDeallocating, serverIDs: waiting}
	default:
		return scaleDownStep{phase: scaleDownNone}
	}
}

// drainAlreadyUnderway reports whether Neo4j refused a drain step because the
// server is already past it — "Server '…' is already deallocating." or "…
// is already dropped." (2026.08.1). It means the pass acted on a view older
// than Neo4j's, so it is progress, never a reason to report the scale-down as
// blocked (#476).
func drainAlreadyUnderway(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"is already deallocating", "is already deallocated", "is already dropped"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// hostsOnlySystem reports whether a server hosts no user databases — only the
// `system` database (or nothing). Such a draining server is safe to DROP.
func hostsOnlySystem(s neo4jclient.ServerInfo) bool {
	for _, h := range s.Hosting {
		if !strings.EqualFold(h, "system") {
			return false
		}
	}
	return true
}

// reconcileScaleDownDrain drives the automated drain. It runs BEFORE the
// StatefulSet apply so the annotation it sets holds replicas this same
// reconcile. Detects a pending scale-down (current STS replicas >
// spec.topology.servers), captures the removed serverIds, and executes one
// drain step per reconcile (cordon → DRYRUN-feasibility → deallocate → wait →
// drop) tracked BY ID. Only once every target is Dropped/gone does it clear the
// annotation, releasing the hold so a later reconcile lowers replicas. Non-fatal
// on connect/query errors — it keeps holding and retries (never removes pods it
// couldn't drain).
func (r *Neo4jEnterpriseClusterReconciler) reconcileScaleDownDrain(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)
	desired := int(cluster.Spec.Topology.Servers)
	if desired <= 0 {
		return nil
	}

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, sts); err != nil {
		return nil // STS not created yet — nothing to drain
	}
	current := 0
	if sts.Spec.Replicas != nil {
		current = int(*sts.Spec.Replicas)
	}

	if current <= desired {
		// Not scaling down, or a scale-down set back before its drain ended:
		// the drain gives back what it took, or refuses the set-back when
		// Neo4j cannot undo it (setBackScaleDown, operations.md id 224).
		return r.setBackScaleDown(ctx, cluster, current)
	}

	// Engage the replica hold THIS reconcile, before we touch Neo4j. The hold is
	// gated on the draining annotation; if we returned on a connect/SHOW SERVERS
	// error below WITHOUT a hold already in place, this same reconcile's
	// StatefulSet apply would lower replicas and delete pods before any drain
	// ran. So if no drain is recorded yet, seed the annotation with the
	// to-be-removed pod NAMES (derived from STS ordinals — no Neo4j needed).
	// They only need to be non-empty to hold; once SHOW SERVERS succeeds they
	// are replaced by real serverIds (and evicted from the set — they never
	// match a live server, which is keyed by id).
	if !scaleDownDrainInProgress(cluster) {
		if err := r.setDrainServers(ctx, cluster, provisionalDrainHoldNames(cluster.Name, current, desired)); err != nil {
			return err
		}
	}

	nc, err := r.scaleDownClientFor(ctx, cluster)
	if err != nil {
		logger.Info("Scale-down drain: cannot connect yet, holding replicas", "error", err)
		return nil
	}
	defer func() { _ = nc.Close() }()

	reported, err := nc.ListServersFromLeader(ctx)
	if err != nil {
		logger.Info("Scale-down drain: SHOW SERVERS failed, holding replicas", "error", err)
		return nil
	}
	// A server that is down — its pod being replaced, say — is listed with no
	// address, and its name is its id: its ordinal is not in the row. It comes
	// from the address the server last reported (the address book and
	// status.diagnostics), else by elimination. Matched on the address alone,
	// the drain left it out: a 5→3 scale-down issued while server-3's pod was
	// being replaced drained only server-4, released the hold, and server-3
	// stayed Enabled/Unavailable for good (#594; operations.md id 213).
	servers := withKnownAddresses(cluster, reported)
	unidentified, undecided := unidentifiedRemovedServerIDs(servers, cluster.Name, desired)

	live := map[string]neo4jclient.ServerInfo{}
	for _, s := range servers {
		live[serverIdentifier(s)] = s
	}

	// A scale-down set back part of the way (5→3, then 4) keeps servers the
	// drain had taken. One Neo4j can no longer give back — deallocating, or
	// dropped and still running in a pod the StatefulSet keeps — refuses the
	// set-back: the replicas stay held and nothing more is drained until the
	// spec lets the drain finish it (operations.md id 224).
	if past := serversPastUndoing(servers, reported, cluster.Name, desired, drainTouched(cluster), false); len(past) > 0 {
		r.blockScaleDown(ctx, cluster, setBackRefusal(past, cluster.Name, current))
		return nil
	}

	// Drain-target id set = (currently removable by ordinal) ∪ (persisted ids
	// still present as live servers). The union absorbs further scale-downs
	// mid-drain; keeping a persisted id only while it's still a live server
	// covers a Deallocating server whose address went NULL (initialRemoved can't
	// derive its ordinal, but its id row persists) AND evicts both the
	// provisional pod-name hold seeded above and servers already Dropped + gone.
	// A persisted server whose ordinal the spec now keeps leaves the set: the
	// drain gives it back (keptByTheSpec).
	idSet := map[string]bool{}
	for _, id := range initialRemovedServerIDs(servers, cluster.Name, desired) {
		idSet[id] = true
	}
	for _, id := range unidentified {
		idSet[id] = true
	}
	owners := reportedOrdinals(reported, cluster.Name)
	var keep []string
	for _, id := range drainServerIDsFromAnnotation(cluster) {
		s, ok := live[id]
		if !ok || strings.EqualFold(s.State, "dropped") {
			continue
		}
		if keptByTheSpec(s, owners, cluster.Name, desired) {
			keep = append(keep, id)
			continue
		}
		idSet[id] = true
	}
	if len(keep) > 0 {
		if err := r.giveBackCordoned(ctx, cluster, nc, servers, keep); err != nil {
			return nil // retried next pass; the replicas stay held
		}
	}
	if len(idSet) == 0 && undecided == "" {
		// Nothing left to drain: every server the scale-down removes is
		// dropped, or gone. The condition of a drain the operator recorded
		// says the drain is complete, and the passes after it leave it so
		// (settleScaleDownCondition). This branch used to reset it as for an
		// aborted scale-down ("not in progress"), and the completion's message
		// sat further down, where no pass reached it: every id in the set is
		// a live server not dropped, so nothing is active only when the set is
		// empty (#606).
		logger.Info("Scale-down drain complete; releasing replica hold", "desired", desired)
		r.settleScaleDownCondition(ctx, cluster, scaleDownDrainCompleteMessage)
		return r.clearDrainRecords(ctx, cluster)
	}

	var active []neo4jclient.ServerInfo
	for id := range idSet {
		s, present := live[id]
		if !present {
			continue // dropped + pod stopped → gone
		}
		if strings.EqualFold(s.State, "dropped") {
			continue // dropped; pod will stop once we release the hold
		}
		active = append(active, s)
	}

	if len(active) == 0 && undecided != "" {
		// Nothing left it can name, and a server it cannot: keep holding.
		logger.Info("Scale-down drain: "+undecided, "desired", desired)
		r.setScaleDownConditionPersisted(ctx, cluster, metav1.ConditionTrue, ConditionReasonServersPendingDrain,
			fmt.Sprintf("Scale-down to %d server(s) waits: %s", desired, undecided))
		return r.setDrainServers(ctx, cluster, provisionalDrainHoldNames(cluster.Name, current, desired))
	}

	// Persist/refresh the target set (also holds replicas via the annotation).
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if err := r.setDrainServers(ctx, cluster, ids); err != nil {
		return err
	}

	// Pre-flight floor: a cluster cannot be scaled below the system database's
	// minimum voting members (dbms.cluster.minimum_initial_system_primaries_count,
	// default 3). Neo4j only rejects this at DROP time — AFTER the irreversible
	// deallocate — which permanently strands the server (a Deallocating server
	// can't be re-enabled). Refuse up front, holding replicas, WITHOUT cordoning
	// or deallocating anything.
	if minPrimaries := nc.MinimumSystemPrimaries(ctx); desired < minPrimaries {
		msg := fmt.Sprintf("Refusing to scale this cluster to %d server(s): a Neo4j cluster cannot drop below %d servers — the system database requires at least %d voting members (dbms.cluster.minimum_initial_system_primaries_count). Scaling lower would strand servers (a deallocated server can't rejoin). Keep at least %d servers, or use Neo4jEnterpriseStandalone for a single node. Replicas held at %d.",
			desired, minPrimaries, minPrimaries, minPrimaries, current)
		r.blockScaleDown(ctx, cluster, msg)
		return nil
	}

	// Fail fast: while no target has started deallocating yet, dry-run the whole
	// set up front so an infeasible scale-down (e.g. a single-primary database on
	// a removed server, or a topology the survivors can't satisfy) blocks
	// IMMEDIATELY — before we cordon anything (cordon is a visible side-effect)
	// and without a reconcile round-trip. The per-phase dry-run below still
	// guards the moment of the real deallocate.
	preDeallocate := true
	for _, s := range active {
		if st := strings.ToLower(s.State); st == "deallocating" || st == "deallocated" {
			preDeallocate = false
			break
		}
	}
	if preDeallocate {
		activeIDs := make([]string, 0, len(active))
		for _, s := range active {
			activeIDs = append(activeIDs, serverIdentifier(s))
		}
		if derr := nc.DeallocateServers(ctx, activeIDs, true); derr != nil {
			if drainAlreadyUnderway(derr) {
				logger.V(1).Info("Scale-down drain: a server is already further along than this read showed; next pass", "error", derr.Error())
				return nil
			}
			msg := fmt.Sprintf("Scale-down to %d server(s) is blocked: DEALLOCATE dry-run failed: %v. Give single-primary databases an additional primary (ALTER DATABASE ... SET TOPOLOGY) or keep the servers — the operator will not auto-reduce topology. No server has been cordoned; replicas are held until this is resolvable.", desired, derr)
			r.blockScaleDown(ctx, cluster, msg)
			return nil
		}
	}

	step := planScaleDownStep(active)
	switch step.phase {
	case scaleDownCordon:
		// Recorded before Neo4j is asked, so an operator restart between
		// the two cannot leave a server cordoned with no record of it; a
		// server the record lists that is not cordoned is left as it is
		// when given back (operations.md id 224).
		if err := r.setScaleDownIDs(ctx, cluster, ScaleDownCordonedServersAnnotation,
			unionIDs(cordonedByDrain(cluster), step.serverIDs)); err != nil {
			return err
		}
		for _, id := range step.serverIDs {
			if cerr := nc.CordonServer(ctx, id); cerr != nil {
				logger.Error(cerr, "Scale-down drain: cordon failed", "server", id)
				return nil
			}
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonScaleDownDraining,
			"Scale-down: cordoned %d server(s): %s", len(step.serverIDs), strings.Join(step.serverIDs, ", "))
	case scaleDownDeallocate:
		if derr := nc.DeallocateServers(ctx, step.serverIDs, true); derr != nil {
			if drainAlreadyUnderway(derr) {
				logger.V(1).Info("Scale-down drain: a server is already further along than this read showed; next pass", "error", derr.Error())
				return nil
			}
			msg := fmt.Sprintf("Scale-down to %d server(s) is blocked: DEALLOCATE dry-run failed: %v. Reduce database topology (ALTER DATABASE ... SET TOPOLOGY) or keep the servers — the operator will not auto-reduce topology. Replicas are held until this is resolvable.", desired, derr)
			r.blockScaleDown(ctx, cluster, msg)
			return nil
		}
		if derr := nc.DeallocateServers(ctx, step.serverIDs, false); derr != nil {
			if !drainAlreadyUnderway(derr) {
				logger.Error(derr, "Scale-down drain: deallocate failed", "servers", step.serverIDs)
			}
			return nil
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonScaleDownDraining,
			"Scale-down: deallocating databases from %d server(s): %s", len(step.serverIDs), strings.Join(step.serverIDs, ", "))
	case scaleDownWaitDeallocating:
		logger.Info("Scale-down drain: waiting for servers to finish deallocating", "servers", step.serverIDs)
	case scaleDownDrop:
		for _, id := range step.serverIDs {
			if derr := nc.DropServer(ctx, id); derr != nil {
				if drainAlreadyUnderway(derr) {
					continue // dropped already: progress, not a block (#476)
				}
				// A DROP failure here is typically the system-db minimum-voting-
				// members floor (defensive — the pre-flight above normally
				// catches it). Surface it as blocked rather than looping.
				msg := fmt.Sprintf("Scale-down to %d server(s) is blocked: DROP SERVER %s failed: %v. This usually means the drop would take the system database below its minimum voting members. Keep at least the system minimum number of servers. Replicas are held.", desired, id, derr)
				r.blockScaleDown(ctx, cluster, msg)
				return nil
			}
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonScaleDownDraining,
			"Scale-down: dropped %d drained server(s): %s", len(step.serverIDs), strings.Join(step.serverIDs, ", "))
	}

	r.setScaleDownConditionPersisted(ctx, cluster, metav1.ConditionTrue, ConditionReasonServersPendingDrain,
		fmt.Sprintf("Scale-down to %d server(s) in progress: draining %d server(s)", desired, len(active)))
	return nil
}

// scaleDownClient is what the drain asks Neo4j: *neo4jclient.Client in
// production, a fake in tests (drainClient).
type scaleDownClient interface {
	ListServersFromLeader(ctx context.Context) ([]neo4jclient.ServerInfo, error)
	MinimumSystemPrimaries(ctx context.Context) int
	CordonServer(ctx context.Context, serverID string) error
	EnableServer(ctx context.Context, serverID string) error
	DeallocateServers(ctx context.Context, serverIDs []string, dryRun bool) error
	DropServer(ctx context.Context, serverID string) error
	Close() error
}

// scaleDownClientFor is the drain's client: drainClient when a test set it,
// else the cluster's own.
func (r *Neo4jEnterpriseClusterReconciler) scaleDownClientFor(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) {
	if r.drainClient != nil {
		return r.drainClient(ctx, cluster)
	}
	c, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// provisionalDrainHoldNames returns the StatefulSet pod names of the servers a
// scale-down will remove (ordinals [desired, current)). Used ONLY to seed the
// draining annotation so the replica hold engages before the operator can reach
// Neo4j; once SHOW SERVERS succeeds these are replaced by real serverIds.
func provisionalDrainHoldNames(clusterName string, current, desired int) []string {
	var names []string
	for ord := desired; ord < current; ord++ {
		names = append(names, fmt.Sprintf("%s-server-%d", clusterName, ord))
	}
	return names
}

// The messages of a ServersPendingDrain condition that is False: a drain that
// ran to its end, and a scale-down that stopped before it did (scaled back
// up).
const (
	scaleDownDrainCompleteMessage = "Scale-down drain complete"
	scaleDownNotInProgressMessage = "Scale-down not in progress"
)

// setBackScaleDown is the drain's pass while the StatefulSet runs no more
// servers than the spec asks for: every pass of a cluster that is not scaling
// down, and the pass that finds a scale-down set back before its drain ended.
//
// With no drain on record — the steady state of every cluster — it asks Neo4j
// nothing, and settles a ServersPendingDrain condition left True, as before.
// A drain on record is resolved against SHOW SERVERS:
//   - a server the drain took that Neo4j cannot give back (deallocating,
//     deallocated, or dropped and still running in a pod the StatefulSet
//     keeps) refuses the set-back: the records and the replica hold stay, and
//     ServersPendingDrain is True (ScaleDownBlocked) naming them and the spec
//     that lets the drain finish them;
//   - otherwise each server the drain itself cordoned that is still Cordoned
//     is enabled again (ENABLE SERVER), never one cordoned by hand, the
//     records go, and the condition reads "Scale-down not in progress".
//
// It used to release the hold and reset the condition without asking Neo4j:
// a 3→5→3→5 set-back within a second left server-3 and server-4 Cordoned, and
// the cluster Forming until a later scale-down dropped them (#608). While
// Neo4j cannot be asked, the records stay and the next pass asks again; a
// pass that finds nothing left to give back leaves Neo4j as it is, so the
// passes after a set-back, or one on a copy of the cluster from before its
// writes, enable nothing twice (operations.md id 224).
func (r *Neo4jEnterpriseClusterReconciler) setBackScaleDown(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, current int) error {
	drained := drainServerIDsFromAnnotation(cluster)
	if len(drained) == 0 && len(cordonedByDrain(cluster)) == 0 {
		r.settleScaleDownCondition(ctx, cluster, scaleDownNotInProgressMessage)
		return nil
	}
	logger := log.FromContext(ctx)
	nc, err := r.scaleDownClientFor(ctx, cluster)
	if err != nil {
		logger.Info("Scale-down set back: cannot connect yet; the drain's records stay until Neo4j can be asked", "error", err)
		return nil
	}
	defer func() { _ = nc.Close() }()
	reported, err := nc.ListServersFromLeader(ctx)
	if err != nil {
		logger.Info("Scale-down set back: SHOW SERVERS failed; the drain's records stay until Neo4j can be asked", "error", err)
		return nil
	}
	servers := withKnownAddresses(cluster, reported)
	if past := serversPastUndoing(servers, reported, cluster.Name, current, drainTouched(cluster), true); len(past) > 0 {
		r.blockScaleDown(ctx, cluster, setBackRefusal(past, cluster.Name, current))
		return nil
	}
	if err := r.giveBackCordoned(ctx, cluster, nc, servers, cordonedByDrain(cluster)); err != nil {
		return nil // retried next pass
	}
	r.settleScaleDownCondition(ctx, cluster, scaleDownNotInProgressMessage)
	return r.clearDrainRecords(ctx, cluster)
}

// giveBackCordoned enables again each server of ids that the drain's record
// says it cordoned and SHOW SERVERS lists Cordoned, then takes ids out of the
// record. A server of ids the drain did not cordon — one cordoned by hand,
// which the drain deallocates without cordoning it — is left as Neo4j has it,
// and so is one the record lists that is no longer Cordoned. It announces one
// ScaleDownSetBack for the servers it enabled, none when it enabled none.
func (r *Neo4jEnterpriseClusterReconciler) giveBackCordoned(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	nc scaleDownClient, servers []neo4jclient.ServerInfo, ids []string) error {
	record := map[string]bool{}
	for _, id := range cordonedByDrain(cluster) {
		record[id] = true
	}
	byID := map[string]neo4jclient.ServerInfo{}
	for _, s := range servers {
		byID[serverIdentifier(s)] = s
	}
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	given := map[string]bool{}
	var enabled []string
	for _, id := range sorted {
		given[id] = true
		if !record[id] {
			continue
		}
		s, ok := byID[id]
		if !ok || !strings.EqualFold(s.State, "cordoned") {
			continue
		}
		if err := nc.EnableServer(ctx, id); err != nil {
			log.FromContext(ctx).Info("Scale-down set back: ENABLE SERVER failed; the next pass tries again", "server", id, "error", err.Error())
			return err
		}
		enabled = append(enabled, serverLabel(s, cluster.Name))
	}
	var remaining []string
	for _, id := range cordonedByDrain(cluster) {
		if !given[id] {
			remaining = append(remaining, id)
		}
	}
	if err := r.setScaleDownIDs(ctx, cluster, ScaleDownCordonedServersAnnotation, remaining); err != nil {
		return err
	}
	if len(enabled) > 0 {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonScaleDownSetBack,
			"Scale-down set back: enabled %d server(s) the drain had cordoned: %s", len(enabled), strings.Join(enabled, ", "))
	}
	return nil
}

// serverPastUndoing reports a SHOW SERVERS row a drain cannot give back:
// deallocating or deallocated — "This state is irreversible" (Operations
// Manual, "Managing servers in a cluster", 5.x and current) — or dropped,
// which "cannot rejoin a cluster".
func serverPastUndoing(s neo4jclient.ServerInfo) bool {
	switch strings.ToLower(s.State) {
	case "deallocating", "deallocated", "dropped":
		return true
	}
	return false
}

// reportedOrdinals maps each pod ordinal to the id of the row whose own
// reported address (not one filled in from memory) names it: the server that
// runs in that pod now.
func reportedOrdinals(reported []neo4jclient.ServerInfo, clusterName string) map[int]string {
	owners := map[int]string{}
	for _, s := range reported {
		if ord, ok := parseServerOrdinal(s.Address, clusterName); ok {
			if _, taken := owners[ord]; !taken {
				owners[ord] = serverIdentifier(s)
			}
		}
	}
	return owners
}

// runsInOrdinal reports whether a row whose ordinal is ord can be the server
// in that pod: no other row reports the pod's address as its own. An earlier
// identity of a pod (its volume replaced) is listed with no address, and the
// one it last reported fills it in; the pod's server now reports it.
func runsInOrdinal(owners map[int]string, ord int, id string) bool {
	owner, ok := owners[ord]
	return !ok || owner == id
}

// keptByTheSpec reports whether a server of the drain set is one the spec now
// keeps: its pod's ordinal is below spec.topology.servers, and it is that
// pod's server.
func keptByTheSpec(s neo4jclient.ServerInfo, owners map[int]string, clusterName string, desired int) bool {
	ord, ok := serverOrdinal(s, clusterName)
	return ok && ord < desired && runsInOrdinal(owners, ord, serverIdentifier(s))
}

// drainTouched is every server the drain on record took: its drain set and
// the servers it cordoned (which keeps one it dropped since, and the drain
// set no longer lists).
func drainTouched(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) map[string]bool {
	touched := map[string]bool{}
	for _, id := range drainServerIDsFromAnnotation(cluster) {
		touched[id] = true
	}
	for _, id := range cordonedByDrain(cluster) {
		touched[id] = true
	}
	return touched
}

// serversPastUndoing returns the servers the drain took (touched) past
// undoing — deallocating, deallocated or dropped — that the StatefulSet keeps:
// those that run in a pod below keptBelow, or, with no ordinal known, any of
// them when every pod is kept (allKept: a full set-back). A server the drain
// never touched — one deallocated by hand — is not the set-back's to refuse.
func serversPastUndoing(servers, reported []neo4jclient.ServerInfo, clusterName string, keptBelow int,
	touched map[string]bool, allKept bool) []neo4jclient.ServerInfo {
	owners := reportedOrdinals(reported, clusterName)
	var past []neo4jclient.ServerInfo
	for _, s := range servers {
		id := serverIdentifier(s)
		if !serverPastUndoing(s) || !touched[id] {
			continue
		}
		if ord, ok := serverOrdinal(s, clusterName); ok {
			if ord < keptBelow && runsInOrdinal(owners, ord, id) {
				past = append(past, s)
			}
			continue
		}
		if allKept {
			past = append(past, s)
		}
	}
	return past
}

// setBackRefusal is the ServersPendingDrain message of a set-back that comes
// too late for some servers: it names them and the spec that lets the drain
// finish them.
func setBackRefusal(past []neo4jclient.ServerInfo, clusterName string, replicas int) string {
	names := make([]string, 0, len(past))
	lowest := -1
	for _, s := range past {
		names = append(names, fmt.Sprintf("%s (%s)", serverLabel(s, clusterName), s.State))
		if ord, ok := serverOrdinal(s, clusterName); ok && (lowest < 0 || ord < lowest) {
			lowest = ord
		}
	}
	sort.Strings(names)
	advice := "Lower spec.topology.servers again so the drain can finish them, then raise it to add new servers in their place."
	if lowest >= 0 {
		advice = fmt.Sprintf("Set spec.topology.servers to %d so the drain can finish them, then raise it to add new servers in their place.", lowest)
	}
	return fmt.Sprintf("Scale-down set back too late for %s: Neo4j cannot enable a server again once it deallocates, and a dropped server cannot rejoin. %s Replicas are held at %d.",
		strings.Join(names, ", "), advice, replicas)
}

// serverLabel names a server by its pod, with its id, when its ordinal is
// known; else by its id.
func serverLabel(s neo4jclient.ServerInfo, clusterName string) string {
	if ord, ok := serverOrdinal(s, clusterName); ok {
		return fmt.Sprintf("%s-server-%d (%s)", clusterName, ord, serverIdentifier(s))
	}
	return serverIdentifier(s)
}

// unionIDs is a and b together, sorted, each once.
func unionIDs(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range append(append([]string(nil), a...), b...) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// clearDrainRecords releases the replica hold and drops the record of the
// servers the drain cordoned: a drain that ended, or a set-back that gave
// back what it took.
func (r *Neo4jEnterpriseClusterReconciler) clearDrainRecords(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	if err := r.setScaleDownIDs(ctx, cluster, ScaleDownCordonedServersAnnotation, nil); err != nil {
		return err
	}
	return r.setDrainServers(ctx, cluster, nil)
}

// settleScaleDownCondition sets the ServersPendingDrain condition False
// (NoServersPendingDrain) with message, when it is True as stored: decided in
// the write, from the object it re-reads, never from this pass's copy. The
// pass after a drain completed reads, from a cache that has not seen the
// "complete" write, the condition still True; judged from that copy, it
// rewrote "Scale-down drain complete" into "Scale-down not in progress"
// (#606). A condition that is False already, or absent, is left as it is.
func (r *Neo4jEnterpriseClusterReconciler) settleScaleDownCondition(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, message string) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); getErr != nil {
			return getErr
		}
		if c := findCondition(latest.Status.Conditions, ConditionTypeServersPendingDrain); c == nil || c.Status != metav1.ConditionTrue {
			return nil
		}
		SetNamedCondition(&latest.Status.Conditions, ConditionTypeServersPendingDrain,
			latest.Generation, metav1.ConditionFalse, ConditionReasonNoServersPendingDrain, message)
		if updateErr := r.Status().Update(ctx, latest); updateErr != nil {
			return updateErr
		}
		cluster.Status.Conditions = latest.Status.Conditions
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to set scale-down condition (non-fatal)")
	}
}

// drainDeferralKey is where deferredUpgradeTargets remembers the image change
// a scale-down drain holds back. The Ready gate's (holdImageDriftUntilReady)
// is under clusterKey.
func drainDeferralKey(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	return clusterKey(cluster) + "#drain"
}

// announceUpgradeDeferredByDrain emits UpgradeDeferred for an image change a
// scale-down drain holds back, once per target. The upgrade machine asks again
// every upgradeStepRequeue (15s) while the drain runs, and each ask announced
// it again (#527; operations.md id 136). Nothing in status records the
// deferral, so the memory is this process's; setDrainServers forgets it when
// the drain releases its hold, so a later drain announces again.
func (r *Neo4jEnterpriseClusterReconciler) announceUpgradeDeferredByDrain(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	target := cluster.Spec.Image.Tag
	if previous, loaded := r.deferredUpgradeTargets.Swap(drainDeferralKey(cluster), target); loaded && previous == target {
		return
	}
	r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeDeferred,
		"Rolling upgrade to %s deferred until the in-progress scale-down drain completes", target)
}

// forgetDeferredUpgrades drops what the reconciler remembers about a deleted
// cluster's held image changes.
func (r *Neo4jEnterpriseClusterReconciler) forgetDeferredUpgrades(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	r.deferredUpgradeTargets.Delete(clusterKey(cluster))
	r.deferredUpgradeTargets.Delete(drainDeferralKey(cluster))
}

// setDrainServers sets (or clears, when ids is empty) the draining-servers
// annotation on the cluster CR — both in memory (so THIS reconcile's
// StatefulSet apply holds replicas) and persisted (refetch + RetryOnConflict),
// as a patch of the annotation alone (operations.md id 150).
func (r *Neo4jEnterpriseClusterReconciler) setDrainServers(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, ids []string) error {
	if len(ids) == 0 {
		// No drain holds an upgrade back any more.
		r.deferredUpgradeTargets.Delete(drainDeferralKey(cluster))
	}
	return r.setScaleDownIDs(ctx, cluster, ScaleDownDrainingServersAnnotation, ids)
}

// setScaleDownIDs sets (or removes, when ids is empty) one of the drain's id
// list annotations on the cluster CR, in memory and persisted, as a patch of
// that annotation alone (operations.md id 150).
func (r *Neo4jEnterpriseClusterReconciler) setScaleDownIDs(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, key string, ids []string) error {
	val := strings.Join(ids, ",")
	if cluster.GetAnnotations()[key] == val {
		return nil
	}
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	if val == "" {
		delete(cluster.Annotations, key)
	} else {
		cluster.Annotations[key] = val
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		return patchObject(ctx, r.Client, latest, func() {
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			if val == "" {
				delete(latest.Annotations, key)
			} else {
				latest.Annotations[key] = val
			}
		})
	})
}

// setScaleDownConditionPersisted writes the ServersPendingDrain condition
// (refetch + RetryOnConflict — never a stale write) and reports whether the
// write changed its status, reason or message, read in the attempt that
// succeeded: what a caller announces from (operations.md id 145).
func (r *Neo4jEnterpriseClusterReconciler) setScaleDownConditionPersisted(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, status metav1.ConditionStatus, reason, message string) bool {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); getErr != nil {
			return getErr
		}
		before := findCondition(latest.Status.Conditions, ConditionTypeServersPendingDrain)
		changed = before == nil || before.Status != status || before.Reason != reason || before.Message != message
		SetNamedCondition(&latest.Status.Conditions, ConditionTypeServersPendingDrain,
			latest.Generation, status, reason, message)
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to set scale-down condition (non-fatal)")
		return false
	}
	return changed
}
