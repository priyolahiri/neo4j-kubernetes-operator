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

// What happens when a check refuses to start an upgrade (operations.md id
// 149).
//
// startRollingUpgrade runs its checks — the version rule, then the pre-upgrade
// check: cluster health, consensus, a leader, and the target against what the
// servers report — before it writes anything to the server StatefulSet. A
// refusal there used to go through failUpgrade like a failure mid-walk: the
// cluster's phase turned Failed, the next pass (no longer Ready, so no retry)
// held the image change and wrote Ready again, and the pass after that, Ready
// with the image still changed, tried again. A check that kept refusing
// flipped the cluster between Failed and Ready, with an UpgradeFailed and a
// ClusterReady event on every cycle, and every dependent paused at each Failed
// (#538, #531).
//
// Nothing was rolled, so the cluster stays as it was — Ready, its message
// naming the stopped upgrade — and the attempt is recorded in
// status.upgradeStatus, announced only when the record changes (rule 145). A
// Failed one is tried again after upgradeStartRetry, and the normal pass runs
// in between with the image change held; a Paused one waits for the user, as
// every Paused upgrade does.

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// upgradeStartRetry is how long an upgrade a check refused to start waits
// before it is tried again (startRefusedRecently).
const upgradeStartRetry = time.Minute

// startRefusedRecently reports whether the upgrade to tag was refused before
// it staged anything less than upgradeStartRetry ago: its record is Failed
// for tag, and the attempt began (stepStartTime) within the interval. The
// caller has seen that the template does not carry tag yet.
func startRefusedRecently(us *neo4jv1beta1.UpgradeStatus, tag string, now time.Time) bool {
	return us != nil && us.Phase == upgradePhaseFailed && us.TargetVersion == tag &&
		us.StepStartTime != nil && now.Sub(us.StepStartTime.Time) < upgradeStartRetry
}

// preUpgradeCheck is the pre-upgrade check: the cluster healthy enough to start
// (Client.ValidateUpgradeSafety), and spec.image.tag not below a version a
// server reports. The version rule already held the tag to what the operator
// recorded (status.runningVersion, resolvedTargetVersion); this reads what the
// servers report now, which the operator may not have recorded yet — a
// floating tag pulled again can start a server on a newer patch (#550).
func (r *Neo4jEnterpriseClusterReconciler) preUpgradeCheck(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	var servers []neo4jclient.ServerInfo
	if r.UpgradeGates != nil && r.UpgradeGates.preUpgrade != nil {
		s, err := r.UpgradeGates.preUpgrade(ctx, cluster)
		if err != nil {
			return err
		}
		servers = s
	} else {
		c, err := r.createNeo4jClient(ctx, cluster)
		if err != nil {
			return fmt.Errorf("failed to create Neo4j client: %w", err)
		}
		defer func() { _ = c.Close() }()
		if err := c.ValidateUpgradeSafety(ctx); err != nil {
			return fmt.Errorf("upgrade safety validation failed: %w", err)
		}
		if servers, err = c.GetServerList(ctx); err != nil {
			return fmt.Errorf("failed to read the versions the servers run: %w", err)
		}
	}
	tag := cluster.Spec.Image.Tag
	if reported := highestReportedVersion(servers, nil); validation.TagBelowReportedVersion(reported, tag) {
		return fmt.Errorf("a server runs %s (SHOW SERVERS), and spec.image.tag %s is below it: Neo4j does not support downgrades. Set the tag to %s or later",
			reported, tag, reported)
	}
	return nil
}

// refuseUpgradeStart records an upgrade that one of startRollingUpgrade's
// checks refused before anything was written to the server StatefulSet:
// Paused with autoPauseOnFailure, Failed otherwise, with the attempt's record
// (prepare) and the cause. The UpgradePaused or UpgradeFailed Warning, and the
// failure metric, follow only a write that changed the record's phase, target
// or error: the same refusal on the next attempt says nothing new. The cluster
// stays as it was; a Ready one has its message name the stopped upgrade.
func (r *Neo4jEnterpriseClusterReconciler) refuseUpgradeStart(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	cause error,
	prepare func(*neo4jv1beta1.UpgradeStatus),
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade")
	pause := autoPauseOnFailure(cluster)
	phase, step := upgradePhaseFailed, "Rolling upgrade refused before it started; it is tried again"
	if pause {
		phase, step = upgradePhasePaused, "Upgrade paused before it started - manual intervention required"
	}
	changed := false
	if err := r.patchUpgradeStatus(ctx, cluster, func(s *neo4jv1beta1.UpgradeStatus) {
		before := *s
		prepare(s)
		s.Phase, s.LastError, s.CurrentStep, s.Message = phase, cause.Error(), step, step
		changed = before.Phase != s.Phase || before.TargetVersion != s.TargetVersion || before.LastError != s.LastError
	}); err != nil {
		// Nothing changed on the servers; the next pass runs the checks again.
		logger.Error(err, "Failed to record the refused upgrade; retrying")
		return ctrl.Result{RequeueAfter: upgradeStepRequeue}, nil
	}
	logger.Info("Rolling upgrade refused before it started", "phase", phase,
		"target", cluster.Spec.Image.Tag, "error", cause.Error(), "new", changed)

	if changed {
		start := time.Now()
		if us := cluster.Status.UpgradeStatus; us != nil && us.StartTime != nil {
			start = us.StartTime.Time
		}
		NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace).upgradeMetrics.RecordUpgrade(ctx, false, time.Since(start))
		if pause {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonUpgradePaused, fmt.Sprintf("Upgrade paused: %v", cause))
		} else {
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonUpgradeFailed, "Rolling upgrade failed: %v", cause)
		}
	}
	// isUpgradeRequired started this attempt on a Ready cluster with every
	// server available; it still is. Its message names the stopped upgrade,
	// so the next normal pass writes no change and announces no ClusterReady.
	r.noteUpgradeInClusterMessage(ctx, cluster)

	if pause {
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}
	// A little past the interval, so the pass that runs then is due a retry
	// (startRefusedRecently).
	return ctrl.Result{RequeueAfter: upgradeStartRetry + upgradeTransitionRequeue}, nil
}

// noteUpgradeInClusterMessage writes the message of a cluster that is Ready
// with every server available for its upgrade as it now stands
// (clusterReadyMessage): in progress once the machine has started or
// retargeted one, paused or failed once a check refused it. Only the normal
// pass wrote the message, and it does not run while an upgrade is active, so
// a resumed upgrade said "is paused" for the whole of its walk (#553). A
// cluster that is not Ready, or short of a server, keeps the message that
// says so.
func (r *Neo4jEnterpriseClusterReconciler) noteUpgradeInClusterMessage(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	if cluster.Status.Phase != neo4jv1beta1.PhaseReady || serversUnavailableSince(cluster) != nil {
		return
	}
	_ = r.updateClusterStatus(ctx, cluster, neo4jv1beta1.PhaseReady, clusterReadyMessage(cluster))
}
