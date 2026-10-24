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

// The version a tag without a patch resolved to (operations.md id 153).
//
// status.version and status.upgradeStatus.targetVersion are image tags. A tag
// that names a patch ("5.26.31-enterprise") says the version the servers run;
// one that does not ("5.26-enterprise") floats to the newest patch of its line,
// so the version rule cannot order a patch of the same line against it. On the
// v1.22.0 journey an upgrade from 5.26.31-enterprise to 5.26-enterprise (then
// 5.26.32) paused after server-2 restarted, the tag was set back to
// 5.26.31-enterprise, the rule let it through, and the operator rolled server-2
// down to 5.26.31. After the upgrade completed the same hole stayed open,
// against every server: status.version read 5.26-enterprise.
//
// So for such a tag the operator records what the servers report in SHOW
// SERVERS (`version`, the "Neo4j version the server is running"; the source
// Verifying already trusts), and the version rule holds spec.image.tag to it:
//
//   - status.runningVersion, for status.version: written on a Ready pass from
//     the SHOW SERVERS the formation check runs on every pass anyway, so
//     reading it costs no query; written only when it changes, and at
//     Completed from the list Verifying read.
//   - status.upgradeStatus.resolvedTargetVersion, for the upgrade's target:
//     written when a restarted server passes the Rolling step's availability
//     check (the same SHOW SERVERS row), and, for a Paused or Failed upgrade,
//     on the next Ready pass from the servers at or above the partition it
//     kept, which run the target.

import (
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// tagFloats reports whether an image tag names a version without a patch
// ("5.26-enterprise", "2026.08-enterprise"), so the servers' own report is the
// only record of the patch they run. A tag that does not parse as a version is
// not one: the image rule refuses it, and there is nothing to order.
func tagFloats(tag string) bool {
	v := parseNeo4jVersion(tag)
	return v != nil && !v.HasPatch
}

// parseNeo4jVersion reads a tag ("5.26.31-enterprise", "5.26-enterprise") or a
// version a server reports ("5.26.32", "2026.08.1") the way versionsMatch
// does: major, minor and an optional patch, any "-suffix" dropped.
func parseNeo4jVersion(version string) *VersionInfo {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if i := strings.Index(version, "-"); i != -1 {
		version = version[:i]
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
			info.Patch, info.HasPatch = p, true
		}
	}
	return info
}

// laterReportedVersion returns the later of two versions servers reported, ""
// counting as earliest. A version that does not parse never wins over one
// that does. CalVer majors (2025+) are above every 5.x, so the numbers order
// the two schemes too.
func laterReportedVersion(a, b string) string {
	pa, pb := parseNeo4jVersion(a), parseNeo4jVersion(b)
	switch {
	case pb == nil:
		return a
	case pa == nil:
		return b
	}
	for _, d := range [][2]int{{pa.Major, pb.Major}, {pa.Minor, pb.Minor}, {pa.Patch, pb.Patch}} {
		if d[0] != d[1] {
			if d[1] > d[0] {
				return b
			}
			return a
		}
	}
	return a
}

// highestReportedVersion returns the highest version the Enabled servers
// among servers report, keeping only those include accepts (nil: all); "" when
// none reports one.
func highestReportedVersion(servers []neo4jclient.ServerInfo, include func(neo4jclient.ServerInfo) bool) string {
	highest := ""
	for _, s := range servers {
		if s.State != "Enabled" || s.Version == "" || (include != nil && !include(s)) {
			continue
		}
		highest = laterReportedVersion(highest, s.Version)
	}
	return highest
}

// runningVersionFor returns what status.runningVersion says for a
// status.version of tag, given the version the servers report: that version
// for a tag without a patch, "" for one that names a patch (the tag says the
// version itself). ok is false when it cannot say — a floating tag with no
// version reported — and status.runningVersion is then left as it is.
func runningVersionFor(tag, reported string) (string, bool) {
	switch {
	case tag == "":
		return "", false
	case !tagFloats(tag):
		return "", true
	case reported == "":
		return "", false
	}
	return reported, true
}

// resolvedTargetVersionFor returns what status.upgradeStatus.resolvedTargetVersion
// becomes when a server restarted on the upgrade's target reports reported:
// the later of it and what is recorded, for a target without a patch. ok is
// false when nothing changes. The recorded one is kept when a later one is
// lower: a server the upgrade restarted earlier may still run it.
func resolvedTargetVersionFor(us *neo4jv1beta1.UpgradeStatus, reported string) (string, bool) {
	if us == nil || reported == "" || !tagFloats(us.TargetVersion) {
		return "", false
	}
	want := laterReportedVersion(us.ResolvedTargetVersion, reported)
	return want, want != us.ResolvedTargetVersion
}

// reportedValue returns a reported version a status write carries, "" for
// none (statusRecord).
func reportedValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// heldTargetServerVersion returns the highest version reported by the servers
// a Paused or Failed upgrade has rolled onto its target: those at or above the
// partition it kept, once the server StatefulSet's template carries the
// target. "" when no upgrade is held, its target names a patch, the template
// does not carry it (the upgrade stopped before it staged), or no such server
// reports a version. The caller has checked that those servers are on the
// template and every server is ready (serverStatefulSetFullyRolled).
func heldTargetServerVersion(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, servers []neo4jclient.ServerInfo) string {
	if !upgradeHeld(cluster) || sts == nil || len(sts.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	us := cluster.Status.UpgradeStatus
	if !tagFloats(us.TargetVersion) || imageTag(sts.Spec.Template.Spec.Containers[0].Image) != us.TargetVersion {
		return ""
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	partition := int32(0)
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil && *ru.Partition > 0 {
		partition = *ru.Partition
	}
	if partition >= replicas {
		return ""
	}
	return highestReportedVersion(servers, func(s neo4jclient.ServerInfo) bool {
		for i := partition; i < replicas; i++ {
			if addressIsPod(s.Address, fmt.Sprintf("%s-server-%d", cluster.Name, i)) {
				return true
			}
		}
		return false
	})
}
