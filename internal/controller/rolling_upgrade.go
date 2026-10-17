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
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/metrics"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// RollingUpgradeOrchestrator handles intelligent rolling upgrades for Neo4j clusters
type RollingUpgradeOrchestrator struct {
	client.Client
	upgradeMetrics *metrics.UpgradeMetrics
}

// NewRollingUpgradeOrchestrator creates a new rolling upgrade orchestrator
func NewRollingUpgradeOrchestrator(c client.Client, clusterName, namespace string) *RollingUpgradeOrchestrator {
	return &RollingUpgradeOrchestrator{
		Client:         c,
		upgradeMetrics: metrics.NewUpgradeMetrics(clusterName, namespace),
	}
}

// The blocking upgrade loop (ExecuteRollingUpgrade / upgradeServers /
// updatePartitionAndWait) was replaced by the requeue-driven state machine in
// rolling_upgrade_statemachine.go (#174). This file keeps the shared
// orchestration helpers: status writes, version validation, StatefulSet
// access, version verification and timeout configuration.

func (r *RollingUpgradeOrchestrator) updateServerStatefulSet(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	mutate func(*appsv1.StatefulSet),
) (*appsv1.StatefulSet, error) {
	key := types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}

	sts := &appsv1.StatefulSet{}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if err := r.Get(ctx, key, sts); err != nil {
			return err
		}
		mutate(sts)
		return r.Update(ctx, sts)
	}); err != nil {
		return nil, err
	}

	if err := r.Get(ctx, key, sts); err != nil {
		return nil, err
	}

	return sts, nil
}

func (r *RollingUpgradeOrchestrator) getServerStatefulSet(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return nil, err
	}

	return sts, nil
}

// Utility methods
func (r *RollingUpgradeOrchestrator) updateUpgradeStatus(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	phase, currentStep, lastError string,
) error {
	if cluster.Status.UpgradeStatus == nil {
		return nil
	}

	// Refetch + RetryOnConflict on every status write. The upgrade runs across
	// minutes, so the reconcile-start `cluster` object's ResourceVersion goes
	// stale; a bare Status().Update then conflicts and was silently swallowed —
	// which could leave even a SUCCESSFUL upgrade stuck in "InProgress" (its
	// final "Completed" write lost), permanently disabling further orchestrated
	// upgrades for the CR. Mirrors updateClusterStatus.
	//
	// A phase change also ends the previous phase: its duration is observed
	// (upgrade_duration_seconds) after the write succeeds — see
	// rolling_upgrade_phase_metrics.go. Reset per attempt: the closure re-runs
	// on conflict.
	var ended *endedUpgradePhase
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ended = nil
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); getErr != nil {
			return getErr
		}
		if latest.Status.UpgradeStatus == nil {
			return nil
		}
		prevPhase := latest.Status.UpgradeStatus.Phase
		prevPhaseStart := snapshotPhaseStart(latest.Status.UpgradeStatus.PhaseStartTime)
		latest.Status.UpgradeStatus.Phase = phase
		latest.Status.UpgradeStatus.CurrentStep = currentStep
		latest.Status.UpgradeStatus.Message = currentStep
		if lastError != "" {
			latest.Status.UpgradeStatus.LastError = lastError
		}
		if phase == "Completed" {
			now := metav1.Now()
			latest.Status.UpgradeStatus.CompletionTime = &now
			latest.Status.LastUpgradeTime = &now
			latest.Status.Version = cluster.Spec.Image.Tag
		}
		ended = stampUpgradePhaseTransition(latest.Status.UpgradeStatus, prevPhase, prevPhaseStart, metav1.Now())
		// Keep the in-memory object consistent for callers that read it after.
		cluster.Status.UpgradeStatus = latest.Status.UpgradeStatus
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to update cluster status in updateUpgradeStatus")
		return err
	}
	recordEndedUpgradePhase(r.upgradeMetrics, ended)
	return nil
}

// validateVersionCompatibility asks the rule the reconciler's validation runs
// on every pass (validation.UpgradeValidator.ValidateVersionUpgrade, with the
// same status.version), so an edit that validation accepted is never refused
// here, and the two cannot drift apart. An empty current version (never
// verified, e.g. the first upgrade after the operator was installed against an
// existing cluster) is not checked.
func (r *RollingUpgradeOrchestrator) validateVersionCompatibility(currentVersion, targetVersion string) error {
	if errs := validation.NewUpgradeValidator().ValidateVersionUpgrade(currentVersion, targetVersion); len(errs) > 0 {
		return errs.ToAggregate()
	}
	return nil
}

// parseVersion reads a tag or a server-reported version for versionsMatch.
func (r *RollingUpgradeOrchestrator) parseVersion(version string) *VersionInfo {
	// Remove any prefix like "v" and suffixes like "-enterprise"
	version = strings.TrimPrefix(version, "v")
	if idx := strings.Index(version, "-"); idx != -1 {
		version = version[:idx]
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return nil
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil
	}

	info := &VersionInfo{Major: major, Minor: minor}
	if len(parts) > 2 {
		if p, err := strconv.Atoi(parts[2]); err == nil {
			info.Patch = p
			info.HasPatch = true
		}
	}
	return info
}

// VersionInfo represents parsed version information
type VersionInfo struct {
	Major int
	Minor int
	Patch int
	// HasPatch is false for a tag that names no patch ("5.26-enterprise",
	// "2026.08-enterprise"), which floats to the newest patch of its line.
	HasPatch bool
}

func (r *RollingUpgradeOrchestrator) validateStatefulSetsReady(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) error {
	serverSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, serverSts); err != nil {
		return fmt.Errorf("failed to get server StatefulSet: %w", err)
	}

	if serverSts.Spec.Replicas == nil {
		return fmt.Errorf("server StatefulSet has no replicas configured")
	}

	if serverSts.Status.ReadyReplicas != *serverSts.Spec.Replicas {
		return fmt.Errorf("server StatefulSet not ready: %d/%d replicas ready",
			serverSts.Status.ReadyReplicas, *serverSts.Spec.Replicas)
	}

	return nil
}

// verifyVersionUpgrade confirms every enabled server reports the target
// version. Source of truth is `SHOW SERVERS YIELD version` (via ListServers):
// it returns one row PER SERVER in a single query — unlike the previous
// dbms.components() approach, which went through the routing driver and
// actually sampled whichever server routing picked, once per member. SHOW
// SERVERS also reports the calendar version for ALL CalVer releases (2025.01
// included), whereas dbms.components() reports the pre-rebrand kernel
// "5.27.0" there; the versionsMatch kernel alias is kept as belt-and-braces.
func (r *RollingUpgradeOrchestrator) verifyVersionUpgrade(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	neo4jClient *neo4jclient.Client,
) error {
	logger := log.FromContext(ctx)

	// Get the target version from the cluster spec
	targetVersion := cluster.Spec.Image.Tag
	if targetVersion == "" {
		return fmt.Errorf("target version not specified in cluster spec")
	}

	logger.Info("Verifying cluster version after upgrade", "targetVersion", targetVersion)

	servers, err := neo4jClient.ListServers(ctx)
	if err != nil {
		return fmt.Errorf("failed to list servers for version verification: %w", err)
	}

	mismatches, verified := r.versionMismatchesFromServers(servers, targetVersion)
	if verified == 0 {
		return fmt.Errorf("no enabled servers found during version verification")
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("version verification failed for %d servers: %s",
			len(mismatches), strings.Join(mismatches, "; "))
	}

	// NOTE: Status.Version is intentionally NOT written here. The Completed
	// transition persists it via refetch+RetryOnConflict (#207) — a bare
	// Status().Update on the reconcile-start object would silently 409.

	logger.Info("Version verification completed successfully",
		"targetVersion", targetVersion, "verifiedServers", verified)
	return nil
}

// versionMismatchesFromServers compares each ENABLED server's self-reported
// SHOW SERVERS version against the target tag. Non-enabled servers (e.g. a
// leftover Cordoned entry from an aborted drain, or a Free server) are not
// the upgrade's concern — the Rolling phase already gated every cluster
// member on Enabled+Available — and must not wedge verification. Returns the
// mismatch descriptions and the number of enabled servers checked.
func (r *RollingUpgradeOrchestrator) versionMismatchesFromServers(
	servers []neo4jclient.ServerInfo,
	targetVersion string,
) (mismatches []string, verified int) {
	for _, s := range servers {
		if s.State != "Enabled" {
			continue
		}
		verified++
		label := s.Name
		if label == "" {
			label = s.Address
		}
		switch {
		case s.Version == "":
			mismatches = append(mismatches, fmt.Sprintf("%s: no version reported by SHOW SERVERS", label))
		case !r.versionsMatch(s.Version, targetVersion):
			mismatches = append(mismatches,
				fmt.Sprintf("%s: running %s, expected %s", label, s.Version, targetVersion))
		}
	}
	return mismatches, verified
}

// normalizeKernelAlias translates the one CalVer release that self-reports a
// SemVer kernel: Neo4j 2025.01.x (the CalVer rebrand release) identifies as
// kernel 5.27.x in dbms.components(). Later CalVers report the calendar
// version directly. Without this alias, post-upgrade version verification for
// a 2025.01.0-enterprise target can never match the servers' reported 5.27.0
// and a SUCCESSFUL upgrade is declared Failed (found live on Kind, #174).
func normalizeKernelAlias(v *VersionInfo) *VersionInfo {
	if v != nil && v.Major == 2025 && v.Minor == 1 {
		return &VersionInfo{Major: 5, Minor: 27, Patch: v.Patch, HasPatch: v.HasPatch}
	}
	return v
}

// versionsMatch reports whether a server's SHOW SERVERS version (actual) is
// the version the image tag (expected) names. Major and minor — year and
// month for CalVer — must be equal, after normalizeKernelAlias. The patch is
// compared only when the tag names one: "5.26-enterprise" and
// "2026.08-enterprise" float to the newest patch of their line, so a server on
// any patch of it runs what the tag names, the same reading the version rule
// gives a patch-less tag (validation.UpgradeValidator, operations.md ids 139
// and 149). Without that, an upgrade to a floating tag failed Verifying unless
// the servers happened to run .0. A server reporting no patch is read as .0.
func (r *RollingUpgradeOrchestrator) versionsMatch(actual, expected string) bool {
	// Normalize versions by removing quotes and whitespace
	actual = strings.TrimSpace(strings.Trim(actual, `"'`))
	expected = strings.TrimSpace(strings.Trim(expected, `"'`))

	// Direct string comparison first
	if actual == expected {
		return true
	}

	// parseVersion strips the "-enterprise" suffix; normalizeKernelAlias maps
	// 2025.01.x <-> 5.27.x (see above).
	a, e := r.parseVersion(actual), r.parseVersion(expected)
	if a == nil || e == nil {
		return false
	}
	na, ne := normalizeKernelAlias(a), normalizeKernelAlias(e)
	if na.Major != ne.Major || na.Minor != ne.Minor {
		return false
	}
	return !ne.HasPatch || na.Patch == ne.Patch
}

// Configuration helpers. Validation refuses a timeout that does not parse or
// is not above zero (validation.ValidateUpgradeStrategy), and it runs before
// every step; a value that reaches here anyway gets the default rather than a
// deadline at or before the step's start.
func upgradeTimeoutOrDefault(value string, def time.Duration) time.Duration {
	if timeout, err := time.ParseDuration(value); err == nil && timeout > 0 {
		return timeout
	}
	return def
}

func (r *RollingUpgradeOrchestrator) getUpgradeTimeout(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) time.Duration {
	if cluster.Spec.UpgradeStrategy != nil {
		return upgradeTimeoutOrDefault(cluster.Spec.UpgradeStrategy.UpgradeTimeout, 30*time.Minute)
	}
	return 30 * time.Minute // Default
}

func (r *RollingUpgradeOrchestrator) getHealthCheckTimeout(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) time.Duration {
	if cluster.Spec.UpgradeStrategy != nil {
		return upgradeTimeoutOrDefault(cluster.Spec.UpgradeStrategy.HealthCheckTimeout, 5*time.Minute)
	}
	return 5 * time.Minute // Default
}

func (r *RollingUpgradeOrchestrator) getStabilizationTimeout(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) time.Duration {
	if cluster.Spec.UpgradeStrategy != nil {
		return upgradeTimeoutOrDefault(cluster.Spec.UpgradeStrategy.StabilizationTimeout, 3*time.Minute)
	}
	return 3 * time.Minute // Default
}
