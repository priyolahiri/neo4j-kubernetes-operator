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

package validation

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// UpgradeValidator validates Neo4j upgrade configuration
type UpgradeValidator struct{}

// NewUpgradeValidator creates a new upgrade validator
func NewUpgradeValidator() *UpgradeValidator {
	return &UpgradeValidator{}
}

// VersionInfo represents parsed version information
type VersionInfo struct {
	Major int
	Minor int
	Patch int
	// HasPatch is false for a tag that names no patch ("5.26-enterprise",
	// "2025.10-enterprise"). Such a tag floats to the newest patch, so its
	// Patch of 0 cannot be ordered against one that names a patch.
	HasPatch bool
}

// ValidateVersionUpgrade checks moving a cluster's servers from the version
// they run to targetVersion (spec.image.tag). currentVersion is the cluster's
// status.version: the version the operator verified every server runs, stamped
// when the cluster first became Ready and after each verified upgrade. The
// reconciler asks this on every pass and the rolling-upgrade state machine asks
// it again before it starts, so the two never disagree about an edit.
//
// It refuses only what Neo4j does not support (Upgrade and Migration Guide,
// /current/: Introduction and "Neo4j 2025-2026"):
//   - a downgrade. "Neo4j does not support downgrades. If the upgrade or
//     migration is not successful, you have to do a full rollback, including
//     restoring a pre-upgrade or a pre-migration backup." CalVer to 5.x is one;
//   - CalVer from a 5.x other than 5.26: "All upgrades from earlier than 5.26
//     must first upgrade to Neo4j 5.26 LTS";
//   - Neo4j 4.x, which reaches 5 by migration, not by an upgrade.
//
// It decides nothing it cannot know. An empty current version (nothing
// verified yet) and one that does not parse are not checked, and a patch number
// is compared only when both tags name one.
func (v *UpgradeValidator) ValidateVersionUpgrade(currentVersion, targetVersion string) field.ErrorList {
	return v.validateVersionMove(currentVersion, targetVersion, "the servers run")
}

// ValidateTagAgainstRunning checks a cluster's spec.image.tag against what it
// runs, read from its status: status.version (ValidateVersionUpgrade), what
// the servers report for a status.version without a patch
// (status.runningVersion), and an unfinished upgrade that has restarted a
// server on its target (validateTagAgainstUpgrade, with
// status.upgradeStatus.resolvedTargetVersion). It is the whole rule the
// reconciler runs on every pass (ClusterValidator.validateAgainstRunning),
// and the rolling-upgrade state machine asks it again before it starts, so the
// two never disagree. The machine's check used to ask ValidateVersionUpgrade
// alone, which ignores the reported versions (#550; operations.md ids 139,
// 149 and 153).
func (v *UpgradeValidator) ValidateTagAgainstRunning(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	tag := cluster.Spec.Image.Tag
	if v.parseVersion(tag) == nil {
		// A tag the image rule already refuses.
		return nil
	}
	if cluster.Status.Version != "" {
		if errs := v.ValidateVersionUpgrade(cluster.Status.Version, tag); len(errs) > 0 {
			return errs
		}
	}
	if errs := v.validateTagAgainstRunningVersion(
		cluster.Status.RunningVersion, cluster.Status.Version, tag, "the servers run"); len(errs) > 0 {
		return errs
	}
	return v.validateTagAgainstUpgrade(cluster.Status.UpgradeStatus, tag)
}

// TagBelowReportedVersion reports whether tag names a version earlier than
// reported, a version a server reports in SHOW SERVERS, ordered as the
// version rule orders them (belowReported): a tag without a patch on the same
// line is not below it. The rolling-upgrade state machine asks it with what
// the servers report when it starts, before the operator may have recorded
// it (operations.md id 153).
func TagBelowReportedVersion(reported, tag string) bool {
	return NewUpgradeValidator().belowReported(reported, tag)
}

// validateVersionMove is ValidateVersionUpgrade for any deployment; runs names
// what runs currentVersion in a refusal ("the servers run", "the standalone
// runs"). Neo4j's upgrade rules are the same for a cluster and a standalone
// server.
func (v *UpgradeValidator) validateVersionMove(currentVersion, targetVersion, runs string) field.ErrorList {
	var allErrs field.ErrorList
	if currentVersion == "" || currentVersion == targetVersion {
		return allErrs
	}
	tagPath := field.NewPath("spec", "image", "tag")

	target := v.parseVersion(targetVersion)
	if target == nil {
		return append(allErrs, field.Invalid(tagPath, targetVersion, "invalid version format"))
	}
	current := v.parseVersion(currentVersion)
	if current == nil {
		return allErrs
	}

	if v.isDowngrade(current, target) {
		return append(allErrs, field.Invalid(tagPath, targetVersion, fmt.Sprintf(
			"downgrades are not supported: %s %s (status.version), and Neo4j cannot go back to an "+
				"earlier version. Set the tag to %s or later; running an earlier version means restoring a backup "+
				"taken before the upgrade", runs, currentVersion, currentVersion)))
	}

	var err error
	switch {
	case v.isCalVer(current) && v.isCalVer(target):
		// "You can upgrade from any version in the 2025–2026 series to any
		// subsequent version." Not a downgrade, so supported. Neo4j also makes
		// each LTS a checkpoint an upgrade must stop at; no CalVer LTS is
		// known here, so crossing one is not checked.
	case v.isSemVer(current) && v.isSemVer(target):
		err = v.validateSemVerUpgrade(current, target)
	case v.isSemVer(current) && v.isCalVer(target):
		err = v.validateSemVerToCalVerUpgrade(current, currentVersion, targetVersion)
	default:
		err = fmt.Errorf("unsupported upgrade path from %s to %s", currentVersion, targetVersion)
	}
	if err != nil {
		allErrs = append(allErrs, field.Invalid(tagPath, targetVersion, err.Error()))
	}
	return allErrs
}

// validateTagAgainstUpgrade checks spec.image.tag against an upgrade that has
// not completed — in flight, Paused or Failed — whose servers have started on
// a version above the one they ran before it. status.version stays at that
// version until the upgrade verifies the new one, but a server that has run a
// target has had its store opened by it, so a tag below would roll it back — a
// downgrade, which Neo4j does not support. The patch is ordered only when both
// tags name one, as in ValidateVersionUpgrade.
//
// What a server has run is status.upgradeStatus.startedTargetVersion, which
// the operator records when a restarted server's Neo4j container starts on a
// target (operations.md id 149). A server that never started on it — its image
// could not be pulled, its container could not be created — has not run it,
// so a Paused upgrade to an unpublished tag can be pointed at an earlier fixed
// one, or back to status.version. It was held to targetVersion as soon as the
// walk released a server (progress), which refused exactly that (#562 walk:
// "2026.09.9-enterprise", ErrImagePull, refused "2026.09.0-enterprise"). A
// record an earlier operator wrote has no startedTargetVersion and keeps that
// rule.
//
// A target without a patch ("5.26-enterprise") floats to the newest patch of
// its line, so the tag alone does not say what the restarted servers run, and
// a patch of the same line ("5.26.31-enterprise") is not ordered against it.
// What they report is recorded in status.upgradeStatus.resolvedTargetVersion
// (operations.md id 153), and the tag is held to that as well: the incident
// that found this set a floating target back to the patch the servers ran
// before, and the operator rolled the restarted server down from 5.26.32 to
// 5.26.31. Without a recorded version the patch is not checked.
func (v *UpgradeValidator) validateTagAgainstUpgrade(us *neo4jv1beta1.UpgradeStatus, tag string) field.ErrorList {
	if !upgradeUnfinished(us) {
		return nil
	}
	floor := upgradeFloor(us)
	if floor == "" || floor == us.PreviousVersion {
		// No server has started on anything newer than what they ran before
		// the upgrade, which the status.version rule already holds the tag to.
		return nil
	}
	tagPath := field.NewPath("spec", "image", "tag")
	if floor != tag {
		target, current := v.parseVersion(tag), v.parseVersion(floor)
		if target != nil && current != nil && v.isDowngrade(current, target) {
			if floor == us.TargetVersion {
				return field.ErrorList{field.Invalid(tagPath, tag, fmt.Sprintf(
					"downgrades are not supported: the upgrade to %s (status.upgradeStatus.targetVersion) has already restarted "+
						"servers on it, and Neo4j cannot go back to an earlier version. Set the tag to %s or later; running an "+
						"earlier version means restoring a backup taken before the upgrade", us.TargetVersion, us.TargetVersion))}
			}
			return field.ErrorList{field.Invalid(tagPath, tag, fmt.Sprintf(
				"downgrades are not supported: servers this upgrade restarted have run %s "+
					"(status.upgradeStatus.startedTargetVersion), and Neo4j cannot go back to an earlier version. Set the tag "+
					"to %s or later; running an earlier version means restoring a backup taken before the upgrade", floor, floor))}
		}
	}
	if v.belowReported(us.ResolvedTargetVersion, tag) {
		return field.ErrorList{field.Invalid(tagPath, tag, fmt.Sprintf(
			"downgrades are not supported: the upgrade to %s (status.upgradeStatus.targetVersion) has already restarted "+
				"servers on it, and they run %s (status.upgradeStatus.resolvedTargetVersion); Neo4j cannot go back to an "+
				"earlier version. Set the tag to %s or later; running an earlier version means restoring a backup taken "+
				"before the upgrade", us.TargetVersion, us.ResolvedTargetVersion, us.ResolvedTargetVersion))}
	}
	return nil
}

// upgradeFloor returns the newest version an unfinished upgrade's servers may
// have started on: status.upgradeStatus.startedTargetVersion, which the
// operator records from the servers' containers; for a record an earlier
// operator wrote, without it, targetVersion once the walk has released a
// server (progress); "" when nothing says a server has started on anything
// newer than previousVersion.
func upgradeFloor(us *neo4jv1beta1.UpgradeStatus) string {
	if us.StartedTargetVersion != "" {
		return us.StartedTargetVersion
	}
	if upgradeRestartedAServer(us) {
		return us.TargetVersion
	}
	return ""
}

// validateTagAgainstRunningVersion checks spec.image.tag against
// status.runningVersion: the Neo4j version the servers report for a
// status.version without a patch (operations.md id 153). Such a tag floats to
// the newest patch of its line, so after an upgrade to "5.26-enterprise"
// status.version names no patch, and "5.26.31-enterprise" is not ordered
// against it, while the servers run 5.26.32: moving the tag there rolls every
// server down. runs names what runs it ("the servers run", "the standalone
// runs"). An empty runningVersion (not read yet, or a status.version that
// names a patch) is not checked.
func (v *UpgradeValidator) validateTagAgainstRunningVersion(runningVersion, version, tag, runs string) field.ErrorList {
	if !v.belowReported(runningVersion, tag) {
		return nil
	}
	return field.ErrorList{field.Invalid(field.NewPath("spec", "image", "tag"), tag, fmt.Sprintf(
		"downgrades are not supported: %s %s (status.runningVersion, what %s in status.version resolved to), and "+
			"Neo4j cannot go back to an earlier version. Set the tag to %s or later; running an earlier version means "+
			"restoring a backup taken before the upgrade", runs, runningVersion, version, runningVersion))}
}

// belowReported reports whether tag names a version earlier than reported, a
// version a server reports (SHOW SERVERS), which always names a patch. A tag
// without a patch on the same line is not below it: it floats to the newest
// patch. An empty or unparseable reported version, or tag, is not judged.
func (v *UpgradeValidator) belowReported(reported, tag string) bool {
	if reported == "" {
		return false
	}
	current, target := v.parseVersion(reported), v.parseVersion(tag)
	return current != nil && target != nil && v.isDowngrade(current, target)
}

// upgradeUnfinished reports whether an upgrade record names a target and has
// not completed: in flight, Paused or Failed.
func upgradeUnfinished(us *neo4jv1beta1.UpgradeStatus) bool {
	if us == nil || us.TargetVersion == "" {
		return false
	}
	switch us.Phase {
	case "", "Completed", "Pending":
		return false
	}
	return true
}

// upgradeRestartedAServer reports whether an unfinished upgrade has released a
// server onto its target: the walk records it in status.upgradeStatus.progress
// when it lowers the partition for the first server (inProgress, then
// upgraded). It says the server was released, not that it started; it is the
// rule for a record without startedTargetVersion, which an earlier operator
// wrote.
func upgradeRestartedAServer(us *neo4jv1beta1.UpgradeStatus) bool {
	if !upgradeUnfinished(us) || us.Progress == nil {
		return false
	}
	return us.Progress.Upgraded > 0 || us.Progress.InProgress > 0
}

// ValidateUpgradeStrategy validates spec.upgradeStrategy. It is a spec rule,
// run for every cluster: the CRD's enum guards strategy, but the timeouts are
// free-form strings the schema cannot check, and the upgrade state machine
// would quietly use its default for one that does not parse. Recreate passes:
// the enum admits it, and NoEffectWarnings says what a cluster does with it.
func (v *UpgradeValidator) ValidateUpgradeStrategy(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	var allErrs field.ErrorList
	strategyPath := field.NewPath("spec", "upgradeStrategy")

	if cluster.Spec.UpgradeStrategy == nil {
		return allErrs
	}

	strategy := cluster.Spec.UpgradeStrategy

	// Validate strategy type
	validStrategies := []string{"RollingUpgrade", "Recreate"}
	if strategy.Strategy != "" {
		valid := false
		for _, validStrategy := range validStrategies {
			if strategy.Strategy == validStrategy {
				valid = true
				break
			}
		}
		if !valid {
			allErrs = append(allErrs, field.NotSupported(
				strategyPath.Child("strategy"),
				strategy.Strategy,
				validStrategies,
			))
		}
	}

	// The three timeouts each bound a step of the upgrade state machine.
	allErrs = append(allErrs, validateUpgradeTimeout(strategyPath.Child("upgradeTimeout"), strategy.UpgradeTimeout)...)
	allErrs = append(allErrs, validateUpgradeTimeout(strategyPath.Child("healthCheckTimeout"), strategy.HealthCheckTimeout)...)
	allErrs = append(allErrs, validateUpgradeTimeout(strategyPath.Child("stabilizationTimeout"), strategy.StabilizationTimeout)...)

	// Validate maxUnavailableDuringUpgrade
	if strategy.MaxUnavailableDuringUpgrade != nil {
		if *strategy.MaxUnavailableDuringUpgrade < 0 {
			allErrs = append(allErrs, field.Invalid(
				strategyPath.Child("maxUnavailableDuringUpgrade"),
				*strategy.MaxUnavailableDuringUpgrade,
				"must be non-negative",
			))
		}
	}

	return allErrs
}

// validateUpgradeTimeout checks one spec.upgradeStrategy timeout. Empty uses
// the default. The value is read with time.ParseDuration, as the state machine
// reads it, and must be above zero: a step's deadline is its start time plus
// the timeout, so "0s" or "-5m" put the deadline at or before the step's
// start. Staging checks its deadline before it does anything, so every
// upgrade failed there; the other steps failed at their first check that did
// not already pass.
func validateUpgradeTimeout(path *field.Path, value string) field.ErrorList {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	switch {
	case err != nil:
		return field.ErrorList{field.Invalid(path, value, "invalid duration format")}
	case d <= 0:
		return field.ErrorList{field.Invalid(path, value,
			"must be greater than zero, such as 30m or 1h30m: an upgrade step fails once this long has passed since it started, so zero or less fails it at its first check")}
	}
	return nil
}

// isDowngrade reports whether target is earlier than current. CalVer
// (2025+) is later than every 5.x. A tag without a patch floats to the newest
// one, so two tags that differ only there are not ordered.
func (v *UpgradeValidator) isDowngrade(current, target *VersionInfo) bool {
	if v.isCalVer(current) != v.isCalVer(target) {
		return v.isCalVer(current)
	}
	if target.Major != current.Major {
		return target.Major < current.Major
	}
	if target.Minor != current.Minor {
		return target.Minor < current.Minor
	}
	return current.HasPatch && target.HasPatch && target.Patch < current.Patch
}

// isCalVer checks if version follows CalVer format (2025+)
func (v *UpgradeValidator) isCalVer(version *VersionInfo) bool {
	return version.Major >= 2025
}

// isSemVer checks if version follows SemVer format (5.x)
func (v *UpgradeValidator) isSemVer(version *VersionInfo) bool {
	return version.Major >= 4 && version.Major <= 10 // Neo4j 4.x, 5.x
}

// validateSemVerUpgrade validates a SemVer to SemVer upgrade that is not a
// downgrade. Every 5.x upgrades to 5.26, the last 5 release and the one this
// operator runs; there is no 5.27. 4.x reaches 5 by migration, not upgrade.
func (v *UpgradeValidator) validateSemVerUpgrade(current, target *VersionInfo) error {
	if current.Major == 4 || target.Major == 4 {
		return fmt.Errorf("Neo4j 4.x versions are not supported - only 5.26.x (last semver LTS) or 2025.x.x (CalVer) versions are supported")
	}
	if current.Major == 5 && target.Major == 5 && target.Minor == 26 {
		return nil
	}
	return fmt.Errorf("only Neo4j 5.26.x (last semver LTS) or 2025.x.x (CalVer) versions are supported")
}

// validateSemVerToCalVerUpgrade validates upgrades from SemVer to CalVer
func (v *UpgradeValidator) validateSemVerToCalVerUpgrade(current *VersionInfo, currentStr, targetStr string) error {
	// Only 5.26.x (last semver LTS) may upgrade to CalVer 2025.x.x
	if current.Major == 5 && current.Minor == 26 {
		return nil // 5.26.x -> 2025.x.x is the only supported semver-to-calver path
	}

	return fmt.Errorf("upgrade from %s to CalVer %s requires Neo4j 5.26.x (last semver LTS) first", currentStr, targetStr)
}

// parseVersion parses a version string into components, handling both SemVer and CalVer
func (v *UpgradeValidator) parseVersion(version string) *VersionInfo {
	// Remove any prefix like "v" and suffixes like "-enterprise"
	version = strings.TrimPrefix(version, "v")
	// A digest the tag was pinned to says nothing about the version
	// ("5.26.31@sha256:…" is 5.26.31, not a tag without a patch).
	if idx := strings.Index(version, "@"); idx != -1 {
		version = version[:idx]
	}
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
