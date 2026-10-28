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

// Requeue-driven rolling-upgrade state machine (#174).
//
// The previous implementation (RollingUpgradeOrchestrator.ExecuteRollingUpgrade)
// ran the whole partition walk synchronously inside one Reconcile — blocking up
// to ~30 minutes per pod. With one worker on the cluster controller (as it had
// then), one upgrading cluster starved reconciliation of every other cluster. This file replaces the blocking loop with one non-blocking step per
// reconcile:
//
//	Staging     — apply the FULL target-version pod template (image AND the
//	              resource-builder env for the new tag — discovery config keys
//	              differ between 5.26 and CalVer) with the StatefulSet
//	              RollingUpdate partition frozen at replicas, then reconcile
//	              the ConfigMap under the same freeze.
//	Rolling     — lower the partition one ordinal at a time (highest first,
//	              ordinal 0 / system-DB seed last), gating each step on the
//	              StatefulSet rollout AND the restarted server re-appearing as
//	              Enabled+Available in SHOW SERVERS.
//	Stabilizing — one-shot cluster health + consensus + replication gate.
//	Verifying   — per-server version verification, then Completed.
//
// State is persisted in status.upgradeStatus (phase, currentPartition,
// stepStartTime), so an operator restart resumes mid-walk instead of wedging.

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/metrics"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Upgrade phases persisted in status.upgradeStatus.phase.
const (
	upgradePhaseStaging     = "Staging"
	upgradePhaseRolling     = "Rolling"
	upgradePhaseStabilizing = "Stabilizing"
	upgradePhaseVerifying   = "Verifying"
	upgradePhaseCompleted   = "Completed"
	upgradePhaseFailed      = "Failed"
	upgradePhasePaused      = "Paused"
	// upgradePhaseLegacyInProgress is the marker older operator versions
	// persisted while their blocking loop ran; resumed as Staging.
	upgradePhaseLegacyInProgress = "InProgress"
)

// upgradeStepRequeue is the poll interval between state-machine steps. Short
// enough to keep per-pod latency low, long enough that an upgrading cluster
// leaves plenty of reconcile bandwidth for other clusters.
const upgradeStepRequeue = 15 * time.Second

// upgradeTransitionRequeue is used right after a phase transition, where the
// next step is expected to make progress immediately.
const upgradeTransitionRequeue = 5 * time.Second

// upgradeStateMachineActive reports whether a rolling upgrade is mid-flight
// and the state machine must keep driving it (bypassing the normal reconcile
// path, which would otherwise stomp the partition freeze and re-apply
// replicas). Terminal phases (Completed/Failed/Paused) are NOT active: the
// normal path takes over and isUpgradeRequired decides whether to re-enter.
// After Failed or Paused the normal path keeps the partition the machine left
// (upgradeHeld in upgrade_hold.go, operations.md id 149).
func upgradeStateMachineActive(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	if cluster.Status.UpgradeStatus == nil {
		return false
	}
	switch cluster.Status.UpgradeStatus.Phase {
	case upgradePhaseStaging, upgradePhaseRolling, upgradePhaseStabilizing,
		upgradePhaseVerifying, upgradePhaseLegacyInProgress:
		return true
	}
	return false
}

// upgradeRollObservation is everything the Rolling step needs to decide its
// next action, gathered by the caller so the decision itself is pure and
// unit-testable (same pattern as planScaleDownStep in scale_down.go).
type upgradeRollObservation struct {
	Replicas           int32
	Partition          int32
	Generation         int64
	ObservedGeneration int64
	UpdatedReplicas    int32
	ReadyReplicas      int32
	// LastRolledAvailable is the SHOW SERVERS Enabled+Available gate for the
	// most recently rolled pod (ordinal == Partition). Ignored while
	// Partition == Replicas (nothing rolled yet).
	LastRolledAvailable bool
	// LastRolledDatabasesCaughtUp is the database gate for the same pod
	// (#488): every database copy on it replicating and applying, and at its
	// requested status. See upgrade_database_gate.go.
	LastRolledDatabasesCaughtUp bool
}

type upgradeRollDecision int

const (
	// rollWait — the StatefulSet rollout or the Neo4j rejoin gate for the
	// current partition step is still pending; requeue and re-observe.
	rollWait upgradeRollDecision = iota
	// rollLower — current step verified; lower the partition by one to roll
	// the next ordinal.
	rollLower
	// rollDone — partition is 0 and ordinal 0 is verified; the walk is done.
	rollDone
)

// planUpgradeRollStep decides the next Rolling action from one observation.
func planUpgradeRollStep(o upgradeRollObservation) upgradeRollDecision {
	// StatefulSet controller hasn't observed the latest spec yet.
	if o.ObservedGeneration < o.Generation {
		return rollWait
	}
	// Pods at ordinals >= partition must all be on the update revision.
	if o.UpdatedReplicas < o.Replicas-o.Partition {
		return rollWait
	}
	// Every pod must be Ready before touching the next one.
	if o.ReadyReplicas < o.Replicas {
		return rollWait
	}
	// Kubernetes readiness reads the server's own view, and stops waiting for
	// it after ReadyWindowSeconds (operations.md id 181); the restarted server
	// must also be Enabled+Available in SHOW SERVERS before the next pod rolls.
	if o.Partition < o.Replicas && !o.LastRolledAvailable {
		return rollWait
	}
	// Available is a server-level state: its databases can still be starting
	// or catching up, and restarting the next server then would leave them
	// short of a majority (#488).
	if o.Partition < o.Replicas && !o.LastRolledDatabasesCaughtUp {
		return rollWait
	}
	if o.Partition == 0 {
		return rollDone
	}
	return rollLower
}

// patchUpgradeStatus applies mutate to status.upgradeStatus on a refetched
// object with conflict retry (see #207 — bare Status().Update on the stale
// reconcile-start object silently loses writes), mirroring the result back
// onto the in-memory cluster.
func (r *Neo4jEnterpriseClusterReconciler) patchUpgradeStatus(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	mutate func(*neo4jv1beta1.UpgradeStatus),
) error {
	// A phase transition ends the previous phase: observe its duration
	// (upgrade_duration_seconds) once the write that moves the persisted phase
	// has succeeded — see rolling_upgrade_phase_metrics.go for why that is
	// exactly-once. Reset per attempt: the closure re-runs on conflict.
	var ended *endedUpgradePhase
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ended = nil
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		if latest.Status.UpgradeStatus == nil {
			latest.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{}
		}
		us := latest.Status.UpgradeStatus
		prevPhase, prevPhaseStart := us.Phase, snapshotPhaseStart(us.PhaseStartTime)
		mutate(us)
		ended = stampUpgradePhaseTransition(us, prevPhase, prevPhaseStart, metav1.Now())
		// Stalled follows the record in this same write (operations.md id 149).
		syncStalledCondition(latest)
		cluster.Status.UpgradeStatus = us
		return r.Status().Update(ctx, latest)
	})
	if err == nil {
		recordEndedUpgradePhase(metrics.NewUpgradeMetrics(cluster.Name, cluster.Namespace), ended)
	}
	return err
}

// upgradeStepDeadlineExceeded reports whether the current step has been
// running longer than its budget.
func upgradeStepDeadlineExceeded(us *neo4jv1beta1.UpgradeStatus, budget time.Duration, now time.Time) bool {
	if us == nil || us.StepStartTime == nil {
		return false
	}
	return now.After(us.StepStartTime.Add(budget))
}

// startRollingUpgrade runs the pre-upgrade gates and, when they pass,
// transitions the state machine into Staging. Called when image drift is
// detected and no upgrade is mid-flight.
func (r *Neo4jEnterpriseClusterReconciler) startRollingUpgrade(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")

	// Mutual exclusion with the scale-down drain (#173): finish an in-progress
	// drain before starting an upgrade (Neo4j docs: don't change topology
	// mid-upgrade). Advance the drain here so it can complete — the normal
	// reconcile path is not reached from the upgrade branch.
	if scaleDownDrainInProgress(cluster) {
		logger.Info("Deferring rolling upgrade: scale-down drain in progress")
		if err := r.reconcileScaleDownDrain(ctx, cluster); err != nil {
			logger.Error(err, "Scale-down drain step failed while deferring upgrade (non-fatal)")
		}
		r.announceUpgradeDeferredByDrain(cluster)
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}

	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)

	// The servers the walk covers are the StatefulSet's: an upgrade changes no
	// topology, and a change of spec.topology.servers waits for it (Staging
	// keeps the replica count; holdTopologyWhileUpgradeHolds).
	servers := cluster.Spec.Topology.Servers
	if live, err := orch.getServerStatefulSet(ctx, cluster); err == nil && live.Spec.Replicas != nil {
		servers = *live.Spec.Replicas
	}
	previousVersion := cluster.Status.Version
	targetVersion := cluster.Spec.Image.Tag
	// What the held upgrade's servers started on, read now: the version rule
	// below holds the tag to it, and validation may have read it before a
	// server started (an image that became pullable meanwhile).
	r.recordStartedTarget(ctx, cluster)
	// The record this attempt writes, also when a gate below refuses it: a
	// Paused or Failed record names the target it stopped on, which is what
	// the user changes to resume a Paused one (upgradeWaitsForUser). An
	// attempt that takes up a held upgrade keeps that upgrade's progress
	// (newUpgradeRecord, operations.md id 149).
	held := cluster.Status.UpgradeStatus.DeepCopy()
	resumed := sameHeldUpgrade(held, previousVersion)
	record := func(us *neo4jv1beta1.UpgradeStatus) {
		*us = newUpgradeRecord(held, previousVersion, targetVersion, servers, metav1.Now())
	}
	// A held upgrade whose restarted servers cannot start on its target goes
	// on towards the new tag although they are down: replacing them is the
	// only way they come back, and it touches no server that is up
	// (heldUpgradeRepair, operations.md id 162).
	repair := r.heldUpgradeRepair(ctx, cluster)

	// The version rule, as validation ran it on this pass (no Neo4j I/O).
	if err := orch.validateVersionCompatibility(cluster); err != nil {
		return r.refuseUpgradeStart(ctx, cluster, fmt.Errorf("version compatibility check failed: %w", err), record)
	}
	if repair == nil {
		if err := orch.validateStatefulSetsReady(ctx, cluster); err != nil {
			// Not an upgrade failure — the cluster isn't quiescent yet. Retry.
			logger.Info("Upgrade waiting for StatefulSet readiness", "reason", err.Error())
			return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
		}
	}

	// Pre-upgrade Neo4j health gate (one-shot), unless disabled. A repair
	// skips it: the cluster is short of the servers it is about to replace.
	if repair == nil && (cluster.Spec.UpgradeStrategy == nil || cluster.Spec.UpgradeStrategy.PreUpgradeHealthCheck) {
		if err := r.preUpgradeCheck(ctx, cluster); err != nil {
			return r.refuseUpgradeStart(ctx, cluster, err, record)
		}
	}

	if err := r.patchUpgradeStatus(ctx, cluster, record); err != nil {
		logger.Error(err, "Failed to initialize upgrade status")
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	// The image change is no longer held back, and the cluster's message no
	// longer says the upgrade is paused or failed (#553).
	r.deferredUpgradeTargets.Delete(clusterKey(cluster))
	r.noteUpgradeInClusterMessage(ctx, cluster)

	if repair != nil {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeStarted,
			"Rolling upgrade resumed after it was %s: %s -> %s; %s cannot start on %s and is replaced first",
			held.Phase, previousVersion, targetVersion, strings.Join(repair.failing, ", "), held.TargetVersion)
		logger.Info("Rolling upgrade resumed to replace servers that cannot start on the earlier target",
			"after", held.Phase, "from", previousVersion, "to", targetVersion, "servers", repair.failing)
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	if resumed {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeStarted,
			"Rolling upgrade resumed after it was %s: %s -> %s", held.Phase, previousVersion, targetVersion)
		logger.Info("Rolling upgrade resumed", "after", held.Phase, "from", previousVersion, "to", targetVersion)
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeStarted,
		"Rolling upgrade started: %s -> %s", previousVersion, targetVersion)
	logger.Info("Rolling upgrade started", "from", previousVersion, "to", targetVersion)
	return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
}

// buildDesiredServerStatefulSet computes the full desired server StatefulSet —
// builder output for the current spec plus topology constraints — exactly as
// the normal reconcile path applies it. Staging MUST go through this (not an
// image-only patch): the builder's env for the target tag carries the
// version-correct discovery config (5.26 uses dbms.cluster.discovery.v2.*;
// CalVer uses dbms.cluster.endpoints), so each pod restarts into a
// self-consistent target template.
func (r *Neo4jEnterpriseClusterReconciler) buildDesiredServerStatefulSet(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (*appsv1.StatefulSet, error) {
	sts := resources.BuildServerStatefulSetForEnterprise(cluster)
	if r.TopologyScheduler != nil {
		placement, err := r.TopologyScheduler.CalculateTopologyPlacement(ctx, cluster)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate topology placement: %w", err)
		}
		if placement != nil {
			if err := r.TopologyScheduler.ApplyTopologyConstraints(ctx, sts, cluster, placement); err != nil {
				return nil, fmt.Errorf("failed to apply topology constraints: %w", err)
			}
		}
	}
	return sts, nil
}

// storedUpgradeStatus reads the cluster's upgrade record as stored, uncached
// (APIReader); false when it cannot be read or there is none.
func (r *Neo4jEnterpriseClusterReconciler) storedUpgradeStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jv1beta1.UpgradeStatus, bool) {
	stored := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(cluster), stored); err != nil || stored.Status.UpgradeStatus == nil {
		return nil, false
	}
	return stored.Status.UpgradeStatus, true
}

// stagingPartition is the partition Staging freezes the target at: the
// replica count, so no server restarts until the walk releases it — unless
// the live template already carries the target (a Staging run again after a
// crash between its two writes, a held upgrade retried towards the same
// target, a legacy resume) and its partition is lower. The servers at or
// above that partition run the target already, or are being replaced on it;
// raising it over them would make a server being replaced come back on the
// StatefulSet's current revision, the version the upgrade leaves, and then
// restart again onto the target (#562 re-walk), and one the walk had rolled
// would go back down if it restarted.
func stagingPartition(live, desired *appsv1.StatefulSet, replicas int32) int32 {
	if imageChanges(live, desired) {
		return replicas
	}
	if p := statefulSetPartition(live); p != nil && *p >= 0 && *p < replicas {
		return *p
	}
	return replicas
}

// serverImage returns the image of sts's server container, or "".
func serverImage(sts *appsv1.StatefulSet) string {
	if c := sts.Spec.Template.Spec.Containers; len(c) > 0 {
		return c[0].Image
	}
	return ""
}

// stepUpgradeStaging applies the full target template with the partition
// frozen at replicas (atomic in one StatefulSet update — no window where the
// new template can roll pods uncontrolled), then reconciles the ConfigMap
// under the same freeze, then transitions to Rolling. It runs once per
// target: a pass whose cached copy still says Staging after it has run leaves
// the StatefulSet as it is (storedUpgradeStatus), and a second run on the same
// template keeps the partition (stagingPartition).
func (r *Neo4jEnterpriseClusterReconciler) stepUpgradeStaging(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")
	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)

	if upgradeStepDeadlineExceeded(cluster.Status.UpgradeStatus, orch.getUpgradeTimeout(cluster), time.Now()) {
		return r.failUpgrade(ctx, cluster, fmt.Errorf("timed out staging the target pod template"))
	}

	// Staging decides from what is stored, not from the cache. The pass after
	// a Staging write is started by the watch event of that write, and its
	// cached copy of this cluster could still say Staging: it staged again, the
	// template already the target, and froze every server — server-2, which the
	// first write was replacing, came back on the StatefulSet's current
	// revision (#562 re-walk: twice in one second, partition 2 then 3). A
	// Staging that has run is not run again.
	if fresh, ok := r.storedUpgradeStatus(ctx, cluster); ok &&
		(fresh.Phase != upgradePhaseStaging || fresh.TargetVersion != cluster.Status.UpgradeStatus.TargetVersion) {
		logger.V(1).Info("Staging has run: the stored upgrade record moved on", "phase", fresh.Phase, "target", fresh.TargetVersion)
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	live := &appsv1.StatefulSet{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, live); err != nil {
		return r.failUpgrade(ctx, cluster, fmt.Errorf("failed to get server StatefulSet: %w", err))
	}
	replicas := int32(0)
	if live.Spec.Replicas != nil {
		replicas = *live.Spec.Replicas
	}
	if replicas == 0 {
		return r.failUpgrade(ctx, cluster, fmt.Errorf("server StatefulSet has zero replicas configured"))
	}

	desired, err := r.buildDesiredServerStatefulSet(ctx, cluster)
	if err != nil {
		return r.failUpgrade(ctx, cluster, err)
	}
	// A StatefulSet created again after a storage expansion waits for its held
	// template to be written back (operations.md id 171); the write here does
	// that once it can, and nothing else, and Staging runs on the next pass.
	if _, pending := restorePending(live); pending {
		logger.Info("Staging waits for the StatefulSet created again after a storage expansion to get its held template back")
		if err := r.createOrUpdateResource(ctx, desired, cluster); err != nil {
			logger.V(1).Info("Could not write the held template back yet", "error", err.Error())
		}
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	// An upgrade changes no topology: the builder renders spec.topology.servers,
	// and a scale-down applied here removed servers with no drain, a scale-up
	// added them on the target mid-upgrade (#538). The change waits for the
	// upgrade to complete (holdTopologyWhileUpgradeHolds).
	desired.Spec.Replicas = &replicas
	// Freeze every pod restart in the SAME update that lands the new template.
	if desired.Spec.UpdateStrategy.RollingUpdate == nil {
		desired.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	partition := stagingPartition(live, desired, replicas)
	// The pods, from the API server: the partition is decided from them
	// (operations.md ids 180, 203).
	pods, err := r.serverPodsUncached(ctx, cluster)
	if err != nil {
		logger.Info("Staging waits: the server pods could not be read", "error", err.Error())
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	// Servers that cannot start on the image they run are replaced on the
	// target first, when it is another image: they are down, and freezing them
	// too left them down for good — the walk waits for every pod to be ready
	// before it lowers the partition (operations.md id 162).
	down := r.serversDownIn(ctx, cluster, live, pods)
	if down != nil && down.images[serverImage(desired)] {
		down = nil // the same image cannot start there either
	}
	if down != nil && down.partition < partition {
		partition = down.partition
		logger.Info("Staging the target above the servers that cannot start on the image they run; they are replaced first",
			"partition", partition, "servers", down.failing)
	}
	// A server the upgrade has moved on from the version below the partition
	// stays at or above it (operations.md id 180).
	moved, waitFor := retargetPartition(live, pods, partition, down != nil)
	if waitFor != "" {
		logger.Info("Staging waits: "+waitFor, "partition", moved)
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	if moved < partition {
		partition = moved
		logger.Info("Staging the target at the lowest server that runs a newer revision than the StatefulSet's current one; the StatefulSet controller moves those servers on to the target, highest first",
			"partition", partition)
	}
	if imageChanges(live, desired) && partition < replicas {
		if notReady := serverNotReadyBelow(live, pods, partition, down); notReady != "" {
			logger.Info("Staging waits: the target would restart servers at or above the partition while "+notReady+" is not ready",
				"partition", partition)
			return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
		}
	}
	desired.Spec.UpdateStrategy.RollingUpdate.Partition = &partition

	if err := r.createOrUpdateResource(ctx, desired, cluster); err != nil {
		logger.Error(err, "Failed to stage target StatefulSet template")
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}

	// ConfigMap content can also be version-specific. Reconcile it AFTER the
	// partition freeze so its config-hash restart annotation lands gated, and
	// each pod picks up config + template in a single partition-driven restart.
	if err := r.ConfigMapManager.ReconcileConfigMap(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile ConfigMap during upgrade staging")
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}

	now := metav1.Now()
	if err := r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
		us.Phase = upgradePhaseRolling
		us.CurrentPartition = &partition
		us.StepStartTime = &now
		us.CurrentStep = "Rolling servers to the target version"
		us.Message = us.CurrentStep
	}); err != nil {
		logger.Error(err, "Failed to persist Rolling transition")
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}
	logger.Info("Upgrade staged: target template applied, partition frozen", "partition", partition)
	return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
}

// stepUpgradeRolling advances the partition walk by at most one ordinal per
// reconcile. The system-DB seed (ordinal 0) is always rolled last — the
// StatefulSet partition strategy restarts highest-ordinal first by
// construction.
func (r *Neo4jEnterpriseClusterReconciler) stepUpgradeRolling(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")
	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)
	// Whether a server has started on the target yet: from then on a tag
	// below it is refused (operations.md id 149).
	r.recordStartedTarget(ctx, cluster)
	us := cluster.Status.UpgradeStatus
	// Mid-flight target changes are handled centrally in handleRollingUpgrade
	// (re-stage from any active phase) before this step is dispatched.

	// From the API server: a cached copy from before Staging wrote a new
	// target counts the servers on the old one as updated, and the step would
	// lower the partition onto the new template with no check (operations.md
	// id 203).
	live, err := r.serverStatefulSetUncached(ctx, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.failUpgrade(ctx, cluster, fmt.Errorf("failed to get server StatefulSet: %w", err))
		}
		logger.Info("Rolling waits: the server StatefulSet could not be read", "error", err.Error())
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	replicas := int32(0)
	if live.Spec.Replicas != nil {
		replicas = *live.Spec.Replicas
	}
	if replicas == 0 {
		return r.failUpgrade(ctx, cluster, fmt.Errorf("server StatefulSet has zero replicas configured"))
	}
	// A server the update replaces that cannot start is replaced now: the
	// StatefulSet controller does not always do it (operations.md id 204).
	if live.Status.ReadyReplicas < replicas {
		r.replaceStuckServerPodsUncached(ctx, cluster, live)
	}

	// Surface (don't act on) a topology change requested mid-upgrade: Staging
	// kept the replica count, and the change — a scale-down's drain, a
	// scale-up's new servers — is applied after the upgrade completes (#173 /
	// #174 mutual exclusion — upgrade wins; operations.md id 163).
	r.reportTopologyDeferral(ctx, cluster, cluster.Spec.Topology.Servers, replicas)

	// Resolve the partition: the machine's persisted currentPartition is
	// authoritative — the live StatefulSet value can be a STALE informer read
	// (e.g. partition=0 left by a PREVIOUS completed upgrade, observed just
	// after Staging re-froze it) and trusting it could fast-forward the walk.
	// The persisted value is at worst one step BEHIND the StatefulSet (crash
	// between the STS write and the status patch), which converges safely:
	// the step's gates re-verify and the partition write is idempotent. Fall
	// back to the live value (legacy resume without the field), then to a
	// full freeze.
	partition := replicas
	if us.CurrentPartition != nil {
		partition = *us.CurrentPartition
	} else if live.Spec.UpdateStrategy.RollingUpdate != nil && live.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
		partition = *live.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	obs := upgradeRollObservation{
		Replicas:           replicas,
		Partition:          partition,
		Generation:         live.Generation,
		ObservedGeneration: live.Status.ObservedGeneration,
		UpdatedReplicas:    live.Status.UpdatedReplicas,
		ReadyReplicas:      live.Status.ReadyReplicas,
	}

	// Only pay for the SHOW SERVERS round-trip when the Kubernetes-level
	// rollout for the current step is already complete.
	kubernetesSettled := obs.ObservedGeneration >= obs.Generation &&
		obs.UpdatedReplicas >= obs.Replicas-obs.Partition &&
		obs.ReadyReplicas >= obs.Replicas
	var lagging []string
	if kubernetesSettled && partition < replicas {
		podName := fmt.Sprintf("%s-server-%d", cluster.Name, partition)
		available, version, err := r.isServerAvailable(ctx, cluster, podName)
		if err != nil {
			logger.V(1).Info("SHOW SERVERS gate not yet answerable (retrying)", "pod", podName, "error", err.Error())
		}
		obs.LastRolledAvailable = available
		if available {
			// The server runs the target now; for a target without a patch,
			// what it reports is the only record of the patch it runs, and the
			// version rule refuses a tag below it (operations.md id 153).
			if resolved, changed := resolvedTargetVersionFor(us, version); changed {
				if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
					if want, ok := resolvedTargetVersionFor(s, version); ok {
						s.ResolvedTargetVersion = want
					}
				}); err != nil {
					logger.V(1).Info("Failed to record the version the target resolved to", "error", err.Error())
				} else {
					logger.Info("Recorded the version the upgrade's target resolved to",
						"pod", podName, "target", us.TargetVersion, "version", resolved)
				}
				us = cluster.Status.UpgradeStatus
			}
			caughtUp, notReady, err := r.lastRolledDatabasesCaughtUp(ctx, cluster, podName)
			switch {
			case err != nil:
				logger.V(1).Info("Database gate not yet answerable (retrying)", "pod", podName, "error", err.Error())
			case !caughtUp:
				lagging = notReady
				logger.Info("Waiting for the restarted server's databases to catch up", "pod", podName, "databases", notReady)
				if msg := databaseGateMessage(partition, notReady); us.Message != msg {
					if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) { s.Message = msg }); err != nil {
						logger.V(1).Info("Failed to record the database gate message", "error", err.Error())
					}
				}
			}
			obs.LastRolledDatabasesCaughtUp = caughtUp
		}
	}

	switch planUpgradeRollStep(obs) {
	case rollWait:
		if upgradeStepDeadlineExceeded(us, orch.getUpgradeTimeout(cluster), time.Now()) {
			if len(lagging) > 0 {
				return r.failUpgrade(ctx, cluster, fmt.Errorf(
					"timed out waiting for the databases on server ordinal %d to catch up (%s)", partition, databaseNameList(lagging)))
			}
			return r.failUpgrade(ctx, cluster, fmt.Errorf(
				"timed out waiting for server ordinal %d to roll and rejoin the cluster", partition))
		}
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil

	case rollDone:
		now := metav1.Now()
		if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
			s.Phase = upgradePhaseStabilizing
			s.StepStartTime = &now
			s.CurrentStep = "All servers rolled; waiting for cluster stabilization"
			s.Message = s.CurrentStep
			if s.Progress != nil {
				s.Progress.Upgraded = replicas
				s.Progress.Pending = 0
				s.Progress.InProgress = 0
			}
		}); err != nil {
			logger.Error(err, "Failed to persist Stabilizing transition")
		}
		logger.Info("All servers rolled to the target version")
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil

	default: // rollLower
		newPartition := partition - 1
		if _, err := orch.updateServerStatefulSet(ctx, cluster, func(sts *appsv1.StatefulSet) {
			if sts.Spec.UpdateStrategy.RollingUpdate == nil {
				sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
			}
			p := newPartition
			sts.Spec.UpdateStrategy.RollingUpdate.Partition = &p
		}); err != nil {
			logger.Error(err, "Failed to lower StatefulSet partition", "partition", newPartition)
			return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
		}
		now := metav1.Now()
		if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
			s.CurrentPartition = &newPartition
			s.StepStartTime = &now
			s.CurrentStep = fmt.Sprintf("Rolling server ordinal %d", newPartition)
			s.Message = s.CurrentStep
			if s.Progress != nil {
				s.Progress.Upgraded = replicas - partition
				s.Progress.InProgress = 1
				s.Progress.Pending = partition - 1
				if s.Progress.Pending < 0 {
					s.Progress.Pending = 0
				}
			}
		}); err != nil {
			logger.Error(err, "Failed to persist partition advance")
		}
		logger.Info("Lowered upgrade partition", "ordinal", newPartition)
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
}

// stepUpgradeStabilizing gates on one-shot cluster health + consensus +
// replication checks. With Verifying re-checking replication ~10s later, the
// upgrade completes only after two independent healthy observations —
// equivalent rigor to the old WaitForClusterStabilization consecutive-check
// loop, without blocking the reconcile.
func (r *Neo4jEnterpriseClusterReconciler) stepUpgradeStabilizing(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")
	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)

	// Post-upgrade health gates can be explicitly disabled.
	if cluster.Spec.UpgradeStrategy != nil && !cluster.Spec.UpgradeStrategy.PostUpgradeHealthCheck {
		return r.transitionToVerifying(ctx, cluster, "Post-upgrade health check disabled; verifying versions")
	}

	healthy := false
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err == nil {
		defer func() { _ = neo4jClient.Close() }()
		ok, herr := neo4jClient.IsClusterHealthy(ctx)
		if herr == nil && ok {
			consensus, cerr := neo4jClient.GetClusterConsensusState(ctx)
			if cerr == nil && consensus {
				replicating, rerr := neo4jClient.IsClusterReplicationHealthy(ctx)
				healthy = rerr == nil && replicating
			}
		}
	}

	if !healthy {
		if upgradeStepDeadlineExceeded(cluster.Status.UpgradeStatus, orch.getStabilizationTimeout(cluster), time.Now()) {
			return r.failUpgrade(ctx, cluster, fmt.Errorf("cluster failed to stabilize after the rolling upgrade"))
		}
		logger.V(1).Info("Cluster not yet stable after upgrade; retrying")
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}

	return r.transitionToVerifying(ctx, cluster, "Cluster stable; verifying server versions")
}

func (r *Neo4jEnterpriseClusterReconciler) transitionToVerifying(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	step string,
) (ctrl.Result, error) {
	now := metav1.Now()
	if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
		s.Phase = upgradePhaseVerifying
		s.StepStartTime = &now
		s.CurrentStep = step
		s.Message = step
	}); err != nil {
		log.FromContext(ctx).Error(err, "Failed to persist Verifying transition")
	}
	return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
}

// stepUpgradeVerifying confirms every server reports the target version, then
// completes the upgrade.
func (r *Neo4jEnterpriseClusterReconciler) stepUpgradeVerifying(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")
	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)

	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		if upgradeStepDeadlineExceeded(cluster.Status.UpgradeStatus, orch.getHealthCheckTimeout(cluster), time.Now()) {
			return r.failUpgrade(ctx, cluster, fmt.Errorf("failed to create Neo4j client for verification: %w", err))
		}
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	defer func() { _ = neo4jClient.Close() }()

	reported, err := orch.verifyVersionUpgrade(ctx, cluster, neo4jClient)
	if err != nil {
		if upgradeStepDeadlineExceeded(cluster.Status.UpgradeStatus, orch.getHealthCheckTimeout(cluster), time.Now()) {
			return r.failUpgrade(ctx, cluster, fmt.Errorf("post-upgrade version verification failed: %w", err))
		}
		logger.V(1).Info("Version verification not yet passing; retrying", "error", err.Error())
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}

	// Completed: stamps CompletionTime, LastUpgradeTime and Status.Version on
	// a refetched object (see updateUpgradeStatus), with the version the
	// servers reported to the verification for a tag without a patch
	// (status.runningVersion, operations.md id 153), then the cluster phase +
	// version in one write (#207 fix pattern). The Completed persist GATES the
	// Ready write: if it fails, retry the (idempotent) verification next
	// reconcile rather than reporting Ready while upgradeStatus still says
	// Verifying — that combination would keep the upgrade branch blocking
	// normal reconciliation with no record of why.
	startTime := time.Now()
	if us := cluster.Status.UpgradeStatus; us != nil && us.StartTime != nil {
		startTime = us.StartTime.Time
	}
	if err := r.completeUpgrade(ctx, cluster, orch, reported); err != nil {
		logger.Error(err, "Failed to persist Completed upgrade phase; retrying")
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}

	orch.upgradeMetrics.RecordUpgrade(ctx, true, time.Since(startTime))
	r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonUpgradeCompleted, "Rolling upgrade completed successfully")
	logger.Info("Rolling upgrade completed successfully", "version", cluster.Spec.Image.Tag)
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

// completeUpgrade records a verified upgrade: the record Completed, with what
// the servers reported (status.runningVersion for a tag without a patch), then
// the cluster's Ready status and version in one write. That write announces
// ClusterReady only when it takes the cluster into Ready — a walk that
// replaced servers which could not start on a cluster Degraded or Forming —
// never for a cluster that stayed Ready through its upgrade (operations.md id
// 136, #606).
func (r *Neo4jEnterpriseClusterReconciler) completeUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, orch *RollingUpgradeOrchestrator, reported string) error {
	if err := orch.updateUpgradeStatus(ctx, cluster, upgradePhaseCompleted, "Rolling upgrade completed successfully", "", reported); err != nil {
		return err
	}
	_ = r.updateClusterStatusWithVersion(ctx, cluster, neo4jv1beta1.PhaseReady, "Rolling upgrade completed successfully", cluster.Spec.Image.Tag)
	return nil
}

// failUpgrade routes a step failure: Paused when autoPauseOnFailure is set
// (manual intervention required, never auto-resumed), Failed otherwise (the
// normal path takes over and records a server short; isUpgradeRequired
// re-enters once every server is available). Either way the StatefulSet keeps the partition the step left,
// so no server the upgrade has not rolled restarts onto the target before the
// upgrade goes on (upgradeHeld, operations.md id 149). prepare runs first in
// the same status write: startRollingUpgrade passes the record of the attempt
// a gate refused.
func (r *Neo4jEnterpriseClusterReconciler) failUpgrade(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	cause error,
	prepare ...func(*neo4jv1beta1.UpgradeStatus),
) (ctrl.Result, error) {
	// A stopped upgrade is a state the user acts on, recorded in
	// status.upgradeStatus.lastError and announced by an UpgradePaused or
	// UpgradeFailed Warning: logged at info once its write lands, not as an
	// ERROR, and not returned as a reconcile error, which controller-runtime
	// logs as an ERROR too (#562 re-walk).
	logger := log.FromContext(ctx).WithName("rolling-upgrade")

	startTime := time.Now()
	if us := cluster.Status.UpgradeStatus; us != nil && us.StartTime != nil {
		startTime = us.StartTime.Time
	}
	orch := NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace)
	// Metric recorded only after the terminal phase persists (below) — a
	// failed persist retries this whole routing and would double-count.
	recordFailureMetric := func() { orch.upgradeMetrics.RecordUpgrade(ctx, false, time.Since(startTime)) }

	prepared := func(s *neo4jv1beta1.UpgradeStatus) {
		for _, p := range prepare {
			p(s)
		}
	}

	paused := autoPauseOnFailure(cluster)
	phase, step := upgradePhaseFailed, "Rolling upgrade failed"
	if paused {
		phase, step = upgradePhasePaused, "Upgrade paused due to failure - manual intervention required"
	}
	announce := false
	if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
		prepared(s)
		// A Failed upgrade the operator retried that stops again with the same
		// cause was announced already: the retry keeps the record's last
		// error (newUpgradeRecord), and the same target and cause are one
		// event (#565: UpgradeFailed on every retry cycle).
		announce = s.LastError != cause.Error()
		s.Phase = phase
		s.LastError = cause.Error()
		s.CurrentStep = step
		s.Message = s.CurrentStep
	}); err != nil {
		// The terminal phase MUST land before any other side effect: if it
		// doesn't, upgradeStateMachineActive keeps treating the upgrade as
		// mid-flight and the steps keep driving after we reported a stop.
		// Requeue and re-route the same failure next reconcile.
		logger.Error(err, "Failed to persist the stopped upgrade's phase; retrying", "phase", phase)
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	recordFailureMetric()
	// The cluster keeps its phase, as for any held upgrade (id 149): Ready with
	// every server available, its message naming the stopped upgrade, or what
	// the next normal pass reports for a cluster short of a server. It was set
	// to Paused or Failed — phases the cluster documents for neither — and the
	// next pass wrote Ready again with a ClusterReady, on every retry of a
	// Failed upgrade (#565).
	r.noteUpgradeInClusterMessage(ctx, cluster)
	switch {
	case !announce:
		logger.V(1).Info("Rolling upgrade stopped again with the same cause", "phase", phase, "reason", cause.Error())
	case paused:
		logger.Info("Rolling upgrade paused", "reason", cause.Error())
		r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonUpgradePaused, fmt.Sprintf("Upgrade paused: %v", cause))
	default:
		logger.Info("Rolling upgrade failed; it is retried once every server is available", "reason", cause.Error())
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonUpgradeFailed, "Rolling upgrade failed: %v", cause)
	}
	if paused {
		return ctrl.Result{}, nil // no auto-requeue: manual intervention required
	}
	// The retry follows on the next pass.
	return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
}

// isServerAvailable is a one-shot SHOW SERVERS check: does the server whose
// bolt address contains podName report Enabled+Available? When it does, the
// version it reports comes with it, from the same row.
func (r *Neo4jEnterpriseClusterReconciler) isServerAvailable(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	podName string,
) (bool, string, error) {
	if r.UpgradeGates != nil && r.UpgradeGates.serverAvailable != nil {
		return r.UpgradeGates.serverAvailable(ctx, cluster, podName)
	}
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		return false, "", err
	}
	defer func() { _ = neo4jClient.Close() }()

	servers, err := neo4jClient.GetServerList(ctx)
	if err != nil {
		return false, "", err
	}
	for _, s := range servers {
		if addressIsPod(s.Address, podName) && s.State == "Enabled" && s.Health == "Available" {
			return true, s.Version, nil
		}
	}
	return false, "", nil
}
