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
)

// deferredSetting is a setting a server gets when it next starts.
type deferredSetting struct {
	Name, Want string
	// Supported reports whether a server running that Neo4j version has the
	// setting at all; one that does not is never waiting for it.
	Supported func(version string) bool
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

// clusterDeferredSettings returns the settings applied at the next restart for
// this cluster. A setting the user sets in spec.config is theirs: it is in
// neo4j.conf, so a change to it restarts as any static setting does.
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
			if d.Supported(s.Version) {
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
	deferred := clusterDeferredSettings(cluster)
	if len(deferred) == 0 {
		if meta.FindStatusCondition(cluster.Status.Conditions, ConditionTypeRestartPending) != nil {
			r.writeRestartPending(ctx, cluster, nil)
		}
		return
	}
	if cluster.Status.Phase != neo4jv1beta1.PhaseReady {
		return
	}

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-config", Namespace: cluster.Namespace}, cm); err != nil {
		return
	}
	if cm.Annotations[deferredSettingsAnnotation] != deferredSettingsSignature(deferred) {
		return // the ConfigMap does not carry them yet
	}
	since, err := time.Parse(time.RFC3339, cm.Annotations[deferredSettingsSinceAnnotation])
	if err != nil {
		return
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
	signature := strings.Join(parts, ",") + "|" + cm.Annotations[deferredSettingsAnnotation] + "@" + cm.Annotations[deferredSettingsSinceAnnotation]
	checker := r.restartPending()
	if checker.unchanged(key, signature) && meta.FindStatusCondition(cluster.Status.Conditions, ConditionTypeRestartPending) != nil {
		return
	}

	servers := map[string]serverState{}
	for i := range pods.Items {
		p := &pods.Items[i]
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
	if r.writeRestartPending(ctx, cluster, pendingRestartServers(servers, deferred, since)) {
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
