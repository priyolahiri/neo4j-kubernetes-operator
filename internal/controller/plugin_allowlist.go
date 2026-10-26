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

// effectiveAllowlist returns the allowlist the target's servers will start
// with, as they build it from the neo4j.conf their controller writes
// ({target}-config) and the neo4j container's
// NEO4J_dbms_security_procedures_allowlist:
//   - on a cluster, the union of the two, with Aura Fleet Management's entry
//     when NEO4J_PLUGINS installs fleet-management and either sets the list:
//     the cluster's startup script gives the variable those before the
//     entrypoint writes it in place of the conf's line
//     (resources.ServerProcedureList, operations.md ids 179, 192);
//   - on a standalone, the variable when there is one someone else set (the
//     entrypoint writes it in place of the conf's line; its plugins and its
//     spec.env put their lists in the conf, id 193), else the conf's, which is
//     the union of its own spec.config, its spec.env and its plugins'.
//
// The conf is the one the target's controller renders from its spec now
// (resources.BuildConfigMapForEnterprise, desiredStandaloneConfigMap), not
// the ConfigMap as it is: between a spec change, or an operator upgrade, and
// the controller's write, the ConfigMap holds a list the servers will not run
// with, and judging it announced a hidden plugin that was not (id 194).
//
// set is false when the list is not set (Neo4j loads every procedure) or
// cannot be read (a variable from a Secret or ConfigMap); where says where it
// is set.
func (r *Neo4jPluginReconciler) effectiveAllowlist(ctx context.Context, deployment *DeploymentInfo) (value, where string, set bool, err error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		// NotFound included: a missing StatefulSet tells nothing (id 177).
		return "", "", false, err
	}
	envName := resources.Neo4jSettingEnvVarName(allowlistSetting)
	envValue, envSet := "", false
	for _, e := range neo4jContainerEnv(sts.Spec.Template.Spec) {
		if e.Name != envName {
			continue
		}
		if folded(deployment, e.Name) {
			// The standalone's own spec.env sets it: the conf holds it (id 193),
			// and the variable leaves the StatefulSet with the controller's next
			// write.
			break
		}
		if e.ValueFrom != nil {
			return "", "", false, nil
		}
		envValue, envSet = e.Value, true
		break
	}
	envWhere := fmt.Sprintf("%s on StatefulSet %s", envName, sts.Name)
	if envSet && deployment.Type == "standalone" {
		return envValue, envWhere, true, nil
	}
	conf, cmName, err := r.desiredConf(ctx, deployment)
	if err != nil {
		return "", "", false, err
	}
	confValue, confSet := resources.Neo4jConfSettings(conf)[allowlistSetting]
	confWhere := fmt.Sprintf("neo4j.conf of ConfigMap %s", cmName)
	if deployment.Type != "standalone" {
		value, set = resources.ServerProcedureList(allowlistSetting, confValue, confSet, envValue, envSet,
			neo4jPluginsList(neo4jContainerEnv(sts.Spec.Template.Spec)), nil)
		where = confWhere
		switch {
		case envSet && confSet:
			where = confWhere + " and " + envWhere
		case envSet:
			where = envWhere
		}
		return value, where, set, nil
	}
	if confSet {
		return confValue, confWhere, true, nil
	}
	return "", "", false, nil
}

// folded reports whether the variable name, on a standalone's StatefulSet, is
// a list of its own spec.env that its neo4j.conf holds instead
// (resources.SpecEnvListKey, operations.md ids 193, 209). A cluster's is not
// skipped: there the variable is the plugins' once its controller has written
// the StatefulSet, and before that holds what its conf holds now.
func folded(deployment *DeploymentInfo, name string) bool {
	standalone, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseStandalone)
	if !ok {
		return false
	}
	for _, e := range standalone.Spec.Env {
		if _, ok := resources.SpecEnvListKey(e); ok && e.Name == name {
			return true
		}
	}
	return false
}

// desiredConf returns the neo4j.conf the target's controller writes for its
// spec now, and the name of the ConfigMap that holds it: a cluster's is
// rendered from its spec alone; a standalone's adds its plugins' settings and
// keeps the keys someone else put in the ConfigMap (desiredStandaloneConfigMap,
// mergeStandaloneConf).
func (r *Neo4jPluginReconciler) desiredConf(ctx context.Context, deployment *DeploymentInfo) (conf, name string, err error) {
	switch target := deployment.Object.(type) {
	case *neo4jv1beta1.Neo4jEnterpriseCluster:
		cm := resources.BuildConfigMapForEnterprise(target)
		return cm.Data["neo4j.conf"], cm.Name, nil
	case *neo4jv1beta1.Neo4jEnterpriseStandalone:
		desired, err := desiredStandaloneConfigMap(ctx, r.Client, target)
		if err != nil {
			return "", "", err
		}
		live := &corev1.ConfigMap{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), live); err != nil {
			if !errors.IsNotFound(err) {
				return "", "", err
			}
			live = nil
		}
		return mergeStandaloneConf(desired.Data["neo4j.conf"], live), desired.Name, nil
	}
	return "", deployment.Name + "-config", nil
}

// targetSetsAllowlist reports whether the target's own spec sets its
// allowlist: the key in its spec.config. Aura Fleet Management only adds its
// entry to a list something else sets (operations.md id 192).
func targetSetsAllowlist(deployment *DeploymentInfo) bool {
	switch target := deployment.Object.(type) {
	case *neo4jv1beta1.Neo4jEnterpriseCluster:
		_, own := target.Spec.Config[allowlistSetting]
		return own
	case *neo4jv1beta1.Neo4jEnterpriseStandalone:
		_, own := target.Spec.Config[allowlistSetting]
		return own
	}
	return false
}

// hiddenProceduresMessage says how the target's effective allowlist hides the
// plugin's procedures; "" when it does not, or when that cannot be told.
func (r *Neo4jPluginReconciler) hiddenProceduresMessage(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (string, error) {
	namespaces := pluginProcedureNamespaces(plugin.Spec.Name)
	if len(namespaces) == 0 || fleetManagementBuiltInFor(plugin, deployment) {
		// A built-in Fleet Manager's procedures are the server's own: the
		// allowlist names extensions in the plugins directory (id 192).
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
		if p.Spec.ClusterRef == plugin.Spec.ClusterRef && pluginOnTarget(p, deployment) && p.DeletionTimestamp == nil &&
			pluginListSettings(p)[allowlistSetting] != "" {
			setters = append(setters, "Neo4jPlugin "+p.Name)
		}
	}
	sort.Strings(setters)
	if targetSetsAllowlist(deployment) || len(setters) == 0 {
		setters = append(setters, targetName(deployment)+"'s own configuration")
	}
	by := strings.Join(setters, ", ")
	return fmt.Sprintf("dbms.security.procedures.allowlist is %q (%s, set by %s): it loads none of this plugin's procedures "+
		"(%s), so Neo4j does not load them and calls to them fail as unknown. The list is shared by every plugin of the target: "+
		"add %s to spec.security.allowedProcedures of this plugin or of the one that sets the list",
		value, where, by, strings.Join(patterns, ", "), strings.Join(patterns, ", ")), nil
}

// reportHiddenProcedures records in the plugin's ProceduresHidden condition
// whether its target's allowlist hides its procedures (message "" when it does
// not), and announces a new or changed message once, from the status write
// that records it (operations.md ids 136, 145). A pass that finds the same
// writes nothing — also a settled pass (id 135). A plugin that is not on its
// target — disabled, or refused by its image (id 170) — carries no such
// condition.
func (r *Neo4jPluginReconciler) reportHiddenProcedures(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) {
	message := ""
	if deployment != nil && pluginOnTarget(plugin, deployment) {
		var err error
		if message, err = r.hiddenProceduresMessage(ctx, plugin, deployment); err != nil {
			// A StatefulSet that does not exist (not created yet, or deleted
			// for a storage expansion) leaves the condition as it is (id 177).
			if !errors.IsNotFound(err) {
				log.FromContext(ctx).Error(err, "Cannot tell whether the target's allowlist hides the plugin's procedures")
			}
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
