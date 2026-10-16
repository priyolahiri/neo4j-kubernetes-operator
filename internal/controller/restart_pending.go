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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Settings the operator applies at each server's next restart (#468).
//
// Some settings are worth having but not worth restarting every server for:
// the operator writes them into the startup script's restart-neutral section,
// and a server picks them up the next time it starts for any reason. Until it
// does, the RestartPending condition names it.
//
// Which servers are waiting is NOT read from SHOW SETTINGS: on 5.26.31,
// SHOW SETTINGS reports dbms.cluster.raft.async_channel_acquisition_enabled as
// false (isExplicitlySet false) on a server whose neo4j.conf sets it true and
// whose startup config dump shows it true — and the setting does take effect
// (Kind, 2026-10-07: restarting a follower stalled writes 32s with it off, not
// at all with it on). So a server is waiting when its Neo4j has the setting
// and its container started before the ConfigMap first carried it; the
// ConfigMap records that moment (deferredSettingsSinceAnnotation).

const (
	ConditionTypeRestartPending           = "RestartPending"
	ConditionReasonSettingsAwaitRestart   = "SettingsAwaitRestart"
	ConditionReasonNoSettingsAwaitRestart = "NoSettingsAwaitRestart"

	// deferredSettingsAnnotation names, on the cluster ConfigMap, the settings
	// it applies at the next restart; deferredSettingsSinceAnnotation is when
	// the ConfigMap first carried exactly those.
	deferredSettingsAnnotation      = "neo4j.com/deferred-settings"
	deferredSettingsSinceAnnotation = "neo4j.com/deferred-settings-since"

	// restartEffectiveSettingsAnnotation names, on the cluster ConfigMap, the
	// restart-effective settings whose value the operator changed on running
	// servers, each with the value it set ("name=value;…"; an empty value is
	// Neo4j's default); restartEffectiveSettingsSinceAnnotation is when it last
	// changed one. A record of its own, so a change here never moves the
	// startup-script record's since, nor the reverse.
	restartEffectiveSettingsAnnotation      = "neo4j.com/restart-effective-settings"
	restartEffectiveSettingsSinceAnnotation = "neo4j.com/restart-effective-settings-since"
)

// deferredSetting is a setting a server gets when it next starts.
type deferredSetting struct {
	Name, Want string
	// Supported reports whether a server running that Neo4j version has the
	// setting at all; one that does not is never waiting for it. Nil means
	// every version the operator runs has it, so no version is read.
	Supported func(version string) bool
}

// supports reports whether a server on that Neo4j version has the setting.
func (d deferredSetting) supports(version string) bool {
	return d.Supported == nil || d.Supported(version)
}

// Settings applied live that also wait for a restart.
//
// A few dynamic settings change at once with dbms.setConfigValue, and SHOW
// SETTINGS reports the new value, yet part of the server keeps the value it
// started with. The cluster connect timeout is one: the operator applies it
// to the running servers (knowledge rule 118), but Neo4j's Raft connections
// take it up only when the server restarts. In the first rolling restart after
// the operator lowered it to 5s live, every step whose surviving servers had
// started with 30s still stalled writes for 10s (4 of 4, Kind, 2026.08.1), and
// the v1.21.0 journey saw the same stall on an upgraded 5.26 cluster. The
// Operations Manual lists the setting as dynamic on both lines
// (configuration/dynamic-settings) and does not say which parts of the server
// take up a value changed at runtime.
//
// So what is recorded is a CHANGE: when the operator writes a neo4j.conf whose
// value for one of these differs from the running ConfigMap's (the line is
// added by an operator upgrade, or spec.config edits it), the setting and the
// moment are recorded, and the servers whose container started before it are
// named until each restarts. The change itself is applied as before. A value
// that does not change records nothing: a new cluster's servers start with it,
// and a cluster whose ConfigMap already had it (one created on v1.21.x) needs
// no restart. The cost is a gap — a cluster that got the line live from
// v1.21.0 or v1.21.1 and has not restarted since is not named — because the
// operator kept no record of that change.
var restartEffectiveSettings = []string{resources.ClusterConnectTimeoutSetting}

// restartEffectiveRecord parses the ConfigMap's record of restart-effective
// changes into the settings it names, keeping only settings still on the list.
// Every Neo4j the operator runs (5.26 and CalVer) has them, so none is gated
// on a version.
func restartEffectiveRecord(annotations map[string]string) []deferredSetting {
	values := map[string]string{}
	for _, entry := range strings.Split(annotations[restartEffectiveSettingsAnnotation], ";") {
		if name, value, ok := strings.Cut(entry, "="); ok {
			values[name] = value
		}
	}
	var out []deferredSetting
	for _, name := range restartEffectiveSettings {
		if value, ok := values[name]; ok {
			out = append(out, deferredSetting{Name: name, Want: value})
		}
	}
	return out
}

// stampRestartEffectiveSettings carries the record of restart-effective
// changes onto the desired ConfigMap, adding every setting whose value differs
// from the existing ConfigMap's neo4j.conf (an added or removed line counts)
// and moving the since to now when one does. A new ConfigMap records nothing:
// its servers start with what it carries. It reports whether the record
// differs from the existing ConfigMap's, so the caller writes it.
func stampRestartEffectiveSettings(desired, existing *corev1.ConfigMap, now time.Time) bool {
	if existing == nil {
		return false
	}
	record := restartEffectiveRecord(existing.Annotations)
	since := existing.Annotations[restartEffectiveSettingsSinceAnnotation]
	oldValues, newValues := confValues(existing.Data["neo4j.conf"]), confValues(desired.Data["neo4j.conf"])
	for _, name := range restartEffectiveSettings {
		ov, had := oldValues[name]
		nv, has := newValues[name]
		if had == has && slices.Equal(ov, nv) {
			continue
		}
		record = slices.DeleteFunc(record, func(d deferredSetting) bool { return d.Name == name })
		record = append(record, deferredSetting{Name: name, Want: strings.Join(nv, ",")})
		since = now.UTC().Format(time.RFC3339)
	}
	if len(record) > 0 {
		entries := make([]string, 0, len(record))
		for _, d := range record {
			entries = append(entries, d.Name+"="+d.Want)
		}
		sort.Strings(entries)
		if desired.Annotations == nil {
			desired.Annotations = map[string]string{}
		}
		desired.Annotations[restartEffectiveSettingsAnnotation] = strings.Join(entries, ";")
		desired.Annotations[restartEffectiveSettingsSinceAnnotation] = since
	}
	return existing.Annotations[restartEffectiveSettingsAnnotation] != desired.Annotations[restartEffectiveSettingsAnnotation] ||
		existing.Annotations[restartEffectiveSettingsSinceAnnotation] != desired.Annotations[restartEffectiveSettingsSinceAnnotation]
}

// supportsAsyncRaftChannels reports whether a Neo4j version is 5.26.29 or a
// later 5.26 patch. CalVer has the setting on by default, so it is not deferred.
func supportsAsyncRaftChannels(version string) bool {
	v, err := neo4jclient.ParseVersion(version)
	if err != nil || v.IsCalver {
		return false
	}
	return v.Major == 5 && v.Minor == 26 && v.Patch >= resources.AsyncRaftChannelsMinPatch
}

// clusterDeferredSettings returns the startup-script settings applied at the
// next restart for this cluster. A setting the user sets in spec.config is
// theirs: it is in neo4j.conf, so a change to it restarts as any static
// setting does. Restart-effective settings are recorded separately
// (stampRestartEffectiveSettings).
func clusterDeferredSettings(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) []deferredSetting {
	var out []deferredSetting
	if !resources.IsCalverImage(cluster.Spec.Image.Tag) {
		if _, own := cluster.Spec.Config[resources.AsyncRaftChannelsSetting]; !own {
			out = append(out, deferredSetting{Name: resources.AsyncRaftChannelsSetting, Want: "true", Supported: supportsAsyncRaftChannels})
		}
	}
	certManager := cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == resources.CertManagerMode
	if resources.TLSReloadApplies(cluster.Spec.Image.Tag, certManager) {
		if _, own := cluster.Spec.Config[resources.TLSReloadSetting]; !own {
			out = append(out, deferredSetting{Name: resources.TLSReloadSetting, Want: "true", Supported: supportsTLSReload})
		}
	}
	return out
}

// supportsTLSReload reports whether a Neo4j version has TLS reload (2025.03+).
func supportsTLSReload(version string) bool {
	v, err := neo4jclient.ParseVersion(version)
	return err == nil && v.SupportsTLSReload()
}

// deferredSettingsSignature identifies a set of deferred settings.
func deferredSettingsSignature(deferred []deferredSetting) string {
	parts := make([]string, 0, len(deferred))
	for _, d := range deferred {
		parts = append(parts, d.Name+"="+d.Want)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// stampDeferredSettings records the deferred settings on the desired ConfigMap,
// keeping the existing "since" while they are unchanged. It reports whether the
// stamp differs from the existing ConfigMap's, so the caller writes it.
func stampDeferredSettings(desired, existing *corev1.ConfigMap, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, now time.Time) bool {
	signature := deferredSettingsSignature(clusterDeferredSettings(cluster))
	var oldSignature, oldSince string
	if existing != nil {
		oldSignature = existing.Annotations[deferredSettingsAnnotation]
		oldSince = existing.Annotations[deferredSettingsSinceAnnotation]
	}
	if signature == "" {
		return oldSignature != ""
	}
	since := oldSince
	if oldSignature != signature || since == "" {
		since = now.UTC().Format(time.RFC3339)
	}
	if desired.Annotations == nil {
		desired.Annotations = map[string]string{}
	}
	desired.Annotations[deferredSettingsAnnotation] = signature
	desired.Annotations[deferredSettingsSinceAnnotation] = since
	return oldSignature != signature || oldSince != since
}

// versionReader is what the check needs from a connection to one server.
type versionReader interface {
	GetLoadedComponents(ctx context.Context) ([]neo4jclient.ComponentInfo, error)
	Close() error
}

// neo4jContainerStarted returns when the pod's neo4j container last started,
// or nil when it is not running.
func neo4jContainerStarted(pod *corev1.Pod) *time.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "neo4j" && cs.State.Running != nil {
			t := cs.State.Running.StartedAt.Time
			return &t
		}
	}
	return nil
}

// serverState is what decides whether one server is waiting.
type serverState struct {
	Version string
	Started time.Time
}

// pendingRestartServers returns, per server pod, the deferred settings it does
// not run with yet: its Neo4j has the setting and it started before the
// ConfigMap carried it.
func pendingRestartServers(servers map[string]serverState, deferred []deferredSetting, since time.Time) map[string][]string {
	pending := map[string][]string{}
	for pod, s := range servers {
		if !s.Started.Before(since) {
			continue
		}
		for _, d := range deferred {
			if d.supports(s.Version) {
				pending[pod] = append(pending[pod], d.Name)
			}
		}
	}
	return pending
}

// restartPendingCondition renders the condition for the pending servers.
func restartPendingCondition(pending map[string][]string) (metav1.ConditionStatus, string, string) {
	if len(pending) == 0 {
		return metav1.ConditionFalse, ConditionReasonNoSettingsAwaitRestart,
			"Every server runs the settings the operator applies at restart"
	}
	pods := make([]string, 0, len(pending))
	settings := map[string]struct{}{}
	for pod, names := range pending {
		pods = append(pods, pod)
		for _, n := range names {
			settings[n] = struct{}{}
		}
	}
	sort.Strings(pods)
	names := make([]string, 0, len(settings))
	for n := range settings {
		names = append(names, n)
	}
	sort.Strings(names)
	return metav1.ConditionTrue, ConditionReasonSettingsAwaitRestart,
		fmt.Sprintf("%s will apply %s at their next restart; the operator does not restart them for it",
			strings.Join(pods, ", "), strings.Join(names, ", "))
}

// restartPendingChecker reads the servers' versions only when something that
// decides the answer changed: the pods (a restart replaces the container) or
// the deferred settings.
type restartPendingChecker struct {
	mu   sync.Mutex
	seen map[string]string
	dial func(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (versionReader, error)
}

func (c *restartPendingChecker) unchanged(key, signature string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[key] == signature
}

func (c *restartPendingChecker) remember(key, signature string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = map[string]string{}
	}
	c.seen[key] = signature
}

// reconcileRestartPending maintains the RestartPending condition. It is
// best-effort and never fails the reconcile: what it cannot read is left for
// the next pass.
func (r *Neo4jEnterpriseClusterReconciler) reconcileRestartPending(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	logger := log.FromContext(ctx)
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-config", Namespace: cluster.Namespace}, cm); err != nil {
		return
	}
	startup := clusterDeferredSettings(cluster)
	changedLive := restartEffectiveRecord(cm.Annotations)
	if len(startup) == 0 && len(changedLive) == 0 {
		if meta.FindStatusCondition(cluster.Status.Conditions, ConditionTypeRestartPending) != nil {
			r.writeRestartPending(ctx, cluster, nil)
		}
		return
	}
	if cluster.Status.Phase != neo4jv1beta1.PhaseReady {
		return
	}

	// Each record has its own since: a server is waiting for a setting when it
	// started before the record of that setting.
	type deferredGroup struct {
		settings []deferredSetting
		since    time.Time
	}
	var groups []deferredGroup
	if len(startup) > 0 {
		if cm.Annotations[deferredSettingsAnnotation] != deferredSettingsSignature(startup) {
			return // the ConfigMap does not carry them yet
		}
		since, err := time.Parse(time.RFC3339, cm.Annotations[deferredSettingsSinceAnnotation])
		if err != nil {
			return
		}
		groups = append(groups, deferredGroup{startup, since})
	}
	if len(changedLive) > 0 {
		since, err := time.Parse(time.RFC3339, cm.Annotations[restartEffectiveSettingsSinceAnnotation])
		if err != nil {
			return
		}
		groups = append(groups, deferredGroup{changedLive, since})
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(resources.ServerPodSelector(cluster.Name))); err != nil {
		return
	}
	var parts []string
	for i := range pods.Items {
		started := neo4jContainerStarted(&pods.Items[i])
		if started == nil {
			return // a server is not running; look again when it is
		}
		parts = append(parts, pods.Items[i].Name+"@"+started.UTC().Format(time.RFC3339))
	}
	sort.Strings(parts)
	key := cluster.Namespace + "/" + cluster.Name
	signature := strings.Join(parts, ",") + "|" + cm.Annotations[deferredSettingsAnnotation] + "@" + cm.Annotations[deferredSettingsSinceAnnotation] +
		"|" + cm.Annotations[restartEffectiveSettingsAnnotation] + "@" + cm.Annotations[restartEffectiveSettingsSinceAnnotation]
	checker := r.restartPending()
	if checker.unchanged(key, signature) && meta.FindStatusCondition(cluster.Status.Conditions, ConditionTypeRestartPending) != nil {
		return
	}

	// Only a version-gated setting needs each server's version; the others
	// are decided from the pods alone, without a connection.
	needVersions := false
	for _, g := range groups {
		for _, d := range g.settings {
			needVersions = needVersions || d.Supported != nil
		}
	}
	servers := map[string]serverState{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if !needVersions {
			servers[p.Name] = serverState{Started: *neo4jContainerStarted(p)}
			continue
		}
		reader, err := checker.dial(ctx, cluster, p.Name)
		if err != nil {
			logger.V(1).Info("RestartPending: could not connect", "pod", p.Name, "error", err.Error())
			return
		}
		components, err := reader.GetLoadedComponents(ctx)
		_ = reader.Close()
		if err != nil || len(components) == 0 {
			logger.V(1).Info("RestartPending: could not read the version", "pod", p.Name, "error", fmt.Sprint(err))
			return
		}
		servers[p.Name] = serverState{Version: components[0].Version, Started: *neo4jContainerStarted(p)}
	}
	pending := map[string][]string{}
	for _, g := range groups {
		for pod, names := range pendingRestartServers(servers, g.settings, g.since) {
			pending[pod] = append(pending[pod], names...)
		}
	}
	if r.writeRestartPending(ctx, cluster, pending) {
		checker.remember(key, signature)
	}
}

// writeRestartPending writes the condition, or removes it when pending is nil.
func (r *Neo4jEnterpriseClusterReconciler) writeRestartPending(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, pending map[string][]string) bool {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		if pending == nil {
			if !meta.RemoveStatusCondition(&latest.Status.Conditions, ConditionTypeRestartPending) {
				return nil
			}
		} else {
			status, reason, message := restartPendingCondition(pending)
			existing := meta.FindStatusCondition(latest.Status.Conditions, ConditionTypeRestartPending)
			if existing != nil && existing.Status == status && existing.Reason == reason && existing.Message == message {
				return nil
			}
			SetNamedCondition(&latest.Status.Conditions, ConditionTypeRestartPending, latest.Generation, status, reason, message)
		}
		return r.Status().Update(ctx, latest)
	})
	return err == nil
}

// restartPending returns the reconciler's checker, creating the Bolt one.
func (r *Neo4jEnterpriseClusterReconciler) restartPending() *restartPendingChecker {
	r.restartPendingOnce.Do(func() {
		if r.RestartPendingChecker == nil {
			r.RestartPendingChecker = &restartPendingChecker{
				dial: func(_ context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (versionReader, error) {
					return dialClusterPod(r.Client, cluster, podName)
				},
			}
		}
	})
	return r.RestartPendingChecker
}
