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

// A Neo4jPlugin whose procedures the target's effective allowlist hides says
// so (operations.md id 161; #553). dbms.security.procedures.allowlist is one
// server-wide list: once any plugin of the target sets it (spec.security
// allowedProcedures, its spec.config, or the target's own spec.config), Neo4j
// loads only the procedures and functions it matches, and every other plugin's
// are not loaded at all (calls fail as unknown, 42N48). The v1.22.0 re-walk
// installed the published GenAI example (genai.*, ai.*) beside an APOC plugin
// with no security block: apoc.version() failed while APOC stayed Ready, and
// nothing said why.
//
// The list is not widened to let the plugin's procedures in: it is a security
// boundary someone set, and loading more than they listed is not the
// operator's call. The plugin stays Ready — its JAR is on the servers — and
// carries the ProceduresHidden condition, with one Warning when it appears or
// its message changes.

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// allowlistSetting is the server-wide list of procedures and functions Neo4j
// loads (default *, all; empty, none).
const allowlistSetting = "dbms.security.procedures.allowlist"

// pluginProcedureNamespaces returns the namespaces the procedures and
// functions of the plugin named name live in; nil for a plugin the operator
// does not know (a custom JAR), which is not judged. GenAI's are genai.* on
// both lines and, from 2025.11, ai.* as well (Operations Manual,
// configuration/plugins.adoc, dev).
func pluginProcedureNamespaces(name string) []string {
	switch strings.ToLower(name) {
	case "apoc", "apoc-extended":
		return []string{"apoc."}
	case "graph-data-science", "gds":
		return []string{"gds."}
	case "bloom":
		return []string{"bloom."}
	case "genai":
		return []string{"genai.", "ai."}
	case "n10s", "neosemantics":
		return []string{"n10s."}
	case "fleet-management":
		return []string{"fleetManagement."}
	}
	return nil
}

// allowlistLoadsAny reports whether an entry of allowlist, a comma-separated
// list of procedure names and patterns with the wildcard *, can match a
// procedure or function in one of namespaces: apoc.coll.* loads part of APOC,
// genai.* none of it. An empty list loads nothing.
func allowlistLoadsAny(allowlist string, namespaces []string) bool {
	for _, entry := range strings.Split(allowlist, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		prefix, wildcard := entry, false
		if i := strings.IndexByte(entry, '*'); i >= 0 {
			prefix, wildcard = entry[:i], true
		}
		for _, ns := range namespaces {
			if strings.HasPrefix(prefix, ns) || (wildcard && strings.HasPrefix(ns, prefix)) {
				return true
			}
		}
	}
	return false
}

// effectiveAllowlist returns the allowlist the target's servers start with,
// as the Neo4j entrypoint builds it: the neo4j container's
// NEO4J_dbms_security_procedures_allowlist when there is one (it replaces the
// setting in neo4j.conf), else the setting in the target's rendered neo4j.conf
// ({target}-config) — on a standalone the union of its own spec.config and its
// plugins' settings. set is false when the list is not set (Neo4j loads every
// procedure) or cannot be read (a variable from a Secret or ConfigMap); where
// says where it was read.
func (r *Neo4jPluginReconciler) effectiveAllowlist(ctx context.Context, deployment *DeploymentInfo) (value, where string, set bool, err error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return "", "", false, client.IgnoreNotFound(err)
	}
	envName := resources.Neo4jSettingEnvVarName(allowlistSetting)
	for _, e := range neo4jContainerEnv(sts.Spec.Template.Spec) {
		if e.Name != envName {
			continue
		}
		if e.ValueFrom != nil {
			return "", "", false, nil
		}
		return e.Value, fmt.Sprintf("%s on StatefulSet %s", envName, sts.Name), true, nil
	}
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: deployment.Name + "-config", Namespace: deployment.Namespace}, cm); err != nil {
		if errors.IsNotFound(err) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	value, set = resources.Neo4jConfSettings(cm.Data["neo4j.conf"])[allowlistSetting]
	return value, fmt.Sprintf("neo4j.conf of ConfigMap %s", cm.Name), set, nil
}

// hiddenProceduresMessage says how the target's effective allowlist hides the
// plugin's procedures; "" when it does not, or when that cannot be told.
func (r *Neo4jPluginReconciler) hiddenProceduresMessage(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (string, error) {
	namespaces := pluginProcedureNamespaces(plugin.Spec.Name)
	if len(namespaces) == 0 {
		return "", nil
	}
	value, where, set, err := r.effectiveAllowlist(ctx, deployment)
	if err != nil || !set || allowlistLoadsAny(value, namespaces) {
		return "", err
	}
	patterns := make([]string, len(namespaces))
	for i, ns := range namespaces {
		patterns[i] = ns + "*"
	}
	// Who puts entries in the list: the target's enabled plugins that do.
	list := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, list, client.InNamespace(plugin.Namespace)); err != nil {
		return "", err
	}
	var setters []string
	for i := range list.Items {
		p := &list.Items[i]
		if p.Spec.ClusterRef == plugin.Spec.ClusterRef && p.Spec.Enabled && p.DeletionTimestamp == nil &&
			pluginListSettings(p)[allowlistSetting] != "" {
			setters = append(setters, "Neo4jPlugin "+p.Name)
		}
	}
	sort.Strings(setters)
	by := targetName(deployment) + "'s own configuration"
	if len(setters) > 0 {
		by = strings.Join(setters, ", ")
	}
	return fmt.Sprintf("dbms.security.procedures.allowlist is %q (%s, set by %s): it loads none of this plugin's procedures "+
		"(%s), so Neo4j does not load them and calls to them fail as unknown. The list is shared by every plugin of the target: "+
		"add %s to spec.security.allowedProcedures of this plugin or of the one that sets the list",
		value, where, by, strings.Join(patterns, ", "), strings.Join(patterns, ", ")), nil
}

// reportHiddenProcedures records in the plugin's ProceduresHidden condition
// whether its target's allowlist hides its procedures (message "" when it does
// not), and announces a new or changed message once, from the status write
// that records it (operations.md ids 136, 145). A pass that finds the same
// writes nothing — also a settled pass (id 135). A disabled plugin carries no
// such condition.
func (r *Neo4jPluginReconciler) reportHiddenProcedures(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) {
	message := ""
	if plugin.Spec.Enabled && deployment != nil {
		var err error
		if message, err = r.hiddenProceduresMessage(ctx, plugin, deployment); err != nil {
			log.FromContext(ctx).Error(err, "Cannot tell whether the target's allowlist hides the plugin's procedures")
			return
		}
	}
	if r.setProceduresHidden(ctx, plugin, message) {
		r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonPluginProceduresHidden, message)
	}
}

// setProceduresHidden writes message as the ProceduresHidden condition (True,
// reason NotInAllowlist), or removes the condition when message is "". It
// reports whether the write added the condition or changed its message, from
// the attempt whose write succeeded.
func (r *Neo4jPluginReconciler) setProceduresHidden(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, message string) bool {
	announce := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		announce = false
		latest := &neo4jv1beta1.Neo4jPlugin{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(plugin), latest); err != nil {
			return err
		}
		existing := findCondition(latest.Status.Conditions, ConditionTypeProceduresHidden)
		if message == "" {
			if !meta.RemoveStatusCondition(&latest.Status.Conditions, ConditionTypeProceduresHidden) {
				return nil
			}
			return r.Status().Update(ctx, latest)
		}
		if existing != nil && existing.Status == metav1.ConditionTrue && existing.Message == message {
			return nil
		}
		SetNamedCondition(&latest.Status.Conditions, ConditionTypeProceduresHidden, latest.Generation,
			metav1.ConditionTrue, ConditionReasonNotInAllowlist, message)
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		announce = true
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to record whether the plugin's procedures are hidden")
		return false
	}
	return announce
}
