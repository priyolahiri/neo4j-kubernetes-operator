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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Applying a neo4j.conf change without a restart (#466).
//
// Neo4j can change some settings at runtime (SHOW SETTINGS … isDynamic, about
// fifty on 5.26 and on CalVer: query logging, transaction timeouts and memory
// limits, transaction-log retention, read-only flags, LDAP mappings).
// dbms.setConfigValue changes one server only and is not persisted — the
// Operations Manual (configuration/dynamic-settings) says to run it on every
// member AND update neo4j.conf. So when every changed setting is dynamic, the
// operator runs it on each server and then writes the ConfigMap that the next
// restart reads. In that order: written first, a crash before the apply would
// leave the servers on the old values with nothing left to apply.

// liveApplyTimeout bounds one live apply, connections included. A server that
// cannot be reached in that time is handled as a refusal: the change restarts
// the servers, as it did before.
const liveApplyTimeout = 60 * time.Second

// confValues parses neo4j.conf into each setting's values, in order: a setting
// such as server.jvm.additional may repeat. A line that is not key=value is
// kept as a key of its own, which no server knows, so a change to it is never
// applied live.
func confValues(conf string) map[string][]string {
	out := make(map[string][]string)
	for _, line := range strings.Split(conf, "\n") {
		if isCommentOrBlankLine(line) {
			continue
		}
		t := strings.TrimSpace(line)
		eq := strings.IndexByte(t, '=')
		if eq <= 0 {
			out[t] = append(out[t], "")
			continue
		}
		k := strings.TrimSpace(t[:eq])
		out[k] = append(out[k], strings.TrimSpace(t[eq+1:]))
	}
	return out
}

// neo4jConfChanges returns the settings whose value differs between two
// renderings of neo4j.conf, each with its new value — "" for a removed setting,
// which dbms.setConfigValue takes as "reset to the default".
func neo4jConfChanges(oldConf, newConf string) map[string]string {
	oldValues, newValues := confValues(oldConf), confValues(newConf)
	changes := make(map[string]string)
	for k, nv := range newValues {
		// Presence counts as well as the values: a line added with nothing
		// after "=" (or no "=") joins to "" just like an absent one.
		if ov, had := oldValues[k]; !had || !slices.Equal(ov, nv) {
			changes[k] = strings.Join(nv, ",")
		}
	}
	for k := range oldValues {
		if _, ok := newValues[k]; !ok {
			changes[k] = ""
		}
	}
	return changes
}

// sortedSettingNames returns the setting names in order, so events and applies are stable.
func sortedSettingNames(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// confSetter is what a live apply needs from a connection to one server.
type confSetter interface {
	SettingsDynamic(ctx context.Context, names []string) (map[string]bool, error)
	SetConfiguration(ctx context.Context, key, value string) error
	Close() error
}

// liveConfigBlocker returns why the changes cannot be applied without a
// restart, or "" when they can: every setting must be one the server knows and
// reports as dynamic, and none may be set by a NEO4J_* environment variable —
// the Neo4j image applies those over neo4j.conf at startup, so changing the
// conf alone would not be what a restart gives.
func liveConfigBlocker(changes map[string]string, dynamic map[string]bool, env []corev1.EnvVar) string {
	envNames := make(map[string]struct{}, len(env))
	for _, e := range env {
		envNames[e.Name] = struct{}{}
	}
	var static, unknown, fromEnv []string
	for _, k := range sortedSettingNames(changes) {
		if _, ok := envNames[resources.Neo4jSettingEnvVarName(k)]; ok {
			fromEnv = append(fromEnv, k)
			continue
		}
		isDynamic, known := dynamic[k]
		switch {
		case !known:
			unknown = append(unknown, k)
		case !isDynamic:
			static = append(static, k)
		}
	}
	var reasons []string
	if len(static) > 0 {
		reasons = append(reasons, "Neo4j reads "+strings.Join(static, ", ")+" only at startup")
	}
	if len(unknown) > 0 {
		reasons = append(reasons, strings.Join(unknown, ", ")+" is not a setting the server reports")
	}
	if len(fromEnv) > 0 {
		reasons = append(reasons, strings.Join(fromEnv, ", ")+" is also set by an environment variable")
	}
	return strings.Join(reasons, "; ")
}

// applyConfChanges runs dbms.setConfigValue for every change on one server.
func applyConfChanges(ctx context.Context, s confSetter, changes map[string]string) error {
	for _, k := range sortedSettingNames(changes) {
		if err := s.SetConfiguration(ctx, k, changes[k]); err != nil {
			return err
		}
	}
	return nil
}

// statefulSetSettled reports whether every replica is ready and on the current
// revision — nothing is rolling. A live apply needs every server up: a server
// it misses would keep the old value until its next restart.
func statefulSetSettled(sts *appsv1.StatefulSet, want int32) bool {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	return replicas == want &&
		sts.Status.ObservedGeneration >= sts.Generation &&
		sts.Status.ReadyReplicas == replicas &&
		sts.Status.UpdatedReplicas == replicas &&
		sts.Status.CurrentRevision == sts.Status.UpdateRevision
}

// containerEnv returns the env of the StatefulSet's neo4j container.
func containerEnv(sts *appsv1.StatefulSet) []corev1.EnvVar {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "neo4j" {
			return c.Env
		}
	}
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		return sts.Spec.Template.Spec.Containers[0].Env
	}
	return nil
}

// LiveConfigApplier applies neo4j.conf changes to a cluster's running servers.
// applied=false means the caller restarts the servers, as it would have before;
// reason says why, for the event.
type LiveConfigApplier interface {
	ApplyLive(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, changes map[string]string) (applied bool, reason string)
}

// boltLiveConfigApplier is the LiveConfigApplier used outside tests: it checks
// the cluster, asks server-0 which settings are dynamic, and runs
// dbms.setConfigValue on every server over Bolt.
type boltLiveConfigApplier struct {
	client.Client
	dial func(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (confSetter, error)
}

// newBoltLiveConfigApplier connects to each server pod through the headless
// Service, as the split-brain detector does.
func newBoltLiveConfigApplier(c client.Client) *boltLiveConfigApplier {
	return &boltLiveConfigApplier{
		Client: c,
		dial: func(_ context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (confSetter, error) {
			return dialClusterPod(c, cluster, podName)
		},
	}
}

// dialClusterPod opens a connection to one server pod of the cluster.
func dialClusterPod(c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (*neo4jclient.Client, error) {
	if cluster.Spec.Auth == nil || cluster.Spec.Auth.AdminSecret == "" {
		return nil, fmt.Errorf("cluster %s has no admin secret", cluster.Name)
	}
	scheme := "bolt"
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == resources.CertManagerMode {
		scheme = "bolt+s"
	}
	url := fmt.Sprintf("%s://%s.%s-headless.%s.svc.cluster.local:7687",
		scheme, podName, cluster.Name, cluster.Namespace)
	return neo4jclient.NewClientForPod(cluster, c, cluster.Spec.Auth.AdminSecret, url)
}

// ApplyLive implements LiveConfigApplier.
func (a *boltLiveConfigApplier) ApplyLive(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, changes map[string]string) (bool, string) {
	if len(changes) == 0 {
		return false, ""
	}
	if cluster.Status.Phase != neo4jv1beta1.PhaseReady || conditionIsTrue(cluster.Status.Conditions, ConditionTypeDegraded) {
		return false, "the cluster is not Ready with every server up"
	}
	sts := &appsv1.StatefulSet{}
	if err := a.Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, sts); err != nil {
		return false, fmt.Sprintf("could not read the server StatefulSet: %v", err)
	}
	servers := cluster.Spec.Topology.Servers
	if !statefulSetSettled(sts, servers) {
		return false, "the servers are not all ready on the current revision"
	}

	ctx, cancel := context.WithTimeout(ctx, liveApplyTimeout)
	defer cancel()

	setters := make([]confSetter, 0, servers)
	defer func() {
		for _, s := range setters {
			_ = s.Close()
		}
	}()
	for i := range servers {
		pod := fmt.Sprintf("%s-server-%d", cluster.Name, i)
		s, err := a.dial(ctx, cluster, pod)
		if err != nil {
			return false, fmt.Sprintf("could not connect to %s: %v", pod, err)
		}
		setters = append(setters, s)
	}

	// Every server runs the same image and configuration, so one answer holds.
	dynamic, err := setters[0].SettingsDynamic(ctx, sortedSettingNames(changes))
	if err != nil {
		return false, fmt.Sprintf("could not read the settings from %s-server-0: %v", cluster.Name, err)
	}
	if reason := liveConfigBlocker(changes, dynamic, containerEnv(sts)); reason != "" {
		return false, reason
	}
	for i, s := range setters {
		if err := applyConfChanges(ctx, s, changes); err != nil {
			// The servers already changed are put right by the restart that follows.
			return false, fmt.Sprintf("%s-server-%d refused the change: %v", cluster.Name, i, err)
		}
	}
	return true, ""
}

// StandaloneLiveConfigApplier applies neo4j.conf changes to a running
// standalone; applied=false means the pod restarts, as before.
type StandaloneLiveConfigApplier interface {
	ApplyLive(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone, changes map[string]string) (applied bool, reason string)
}

// boltStandaloneLiveConfigApplier is the StandaloneLiveConfigApplier used
// outside tests.
type boltStandaloneLiveConfigApplier struct {
	client.Client
	dial func(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) (confSetter, error)
}

func newBoltStandaloneLiveConfigApplier(c client.Client) *boltStandaloneLiveConfigApplier {
	return &boltStandaloneLiveConfigApplier{
		Client: c,
		dial: func(_ context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) (confSetter, error) {
			return neo4jclient.NewClientForEnterpriseStandalone(standalone, c, getStandaloneAdminSecretName(standalone))
		},
	}
}

// ApplyLive implements StandaloneLiveConfigApplier.
func (a *boltStandaloneLiveConfigApplier) ApplyLive(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone, changes map[string]string) (bool, string) {
	if len(changes) == 0 {
		return false, ""
	}
	if standalone.Status.Phase != neo4jv1beta1.PhaseReady {
		return false, "the standalone is not Ready"
	}
	sts := &appsv1.StatefulSet{}
	if err := a.Get(ctx, types.NamespacedName{Name: standalone.Name, Namespace: standalone.Namespace}, sts); err != nil {
		return false, fmt.Sprintf("could not read the StatefulSet: %v", err)
	}
	if !statefulSetSettled(sts, 1) {
		return false, "the pod is not ready on the current revision"
	}

	ctx, cancel := context.WithTimeout(ctx, liveApplyTimeout)
	defer cancel()
	s, err := a.dial(ctx, standalone)
	if err != nil {
		return false, fmt.Sprintf("could not connect: %v", err)
	}
	defer func() { _ = s.Close() }()

	dynamic, err := s.SettingsDynamic(ctx, sortedSettingNames(changes))
	if err != nil {
		return false, fmt.Sprintf("could not read the settings: %v", err)
	}
	if reason := liveConfigBlocker(changes, dynamic, containerEnv(sts)); reason != "" {
		return false, reason
	}
	if err := applyConfChanges(ctx, s, changes); err != nil {
		return false, fmt.Sprintf("the server refused the change: %v", err)
	}
	return true, ""
}

// conditionIsTrue reports whether the condition of that type is True.
func conditionIsTrue(conditions []metav1.Condition, conditionType string) bool {
	c := findCondition(conditions, conditionType)
	return c != nil && c.Status == metav1.ConditionTrue
}
