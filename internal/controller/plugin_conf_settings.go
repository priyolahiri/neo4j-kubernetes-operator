package controller

import (
	"sort"
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Plugin-derived neo4j.conf settings, factored into one place so the standalone
// controller (which OWNS the ConfigMap) and the plugin controller agree on the
// exact set. The standalone controller computes the UNION across all of a
// target's Neo4jPlugin CRs and renders it as part of neo4j.conf — making those
// keys operator-owned (tracked by the conf-keys ownership annotation), so they
// are pruned when a plugin is uninstalled, with no after-the-fact patching of
// the ConfigMap the standalone controller rebuilds (issue #146).

// automaticPluginSecuritySettings returns the neo4j.conf security settings a
// plugin requires to function (procedure allowlists, HTTP auth allowlist,
// unmanaged extension classes). Receiver-free so both controllers can call it.
func automaticPluginSecuritySettings(pluginName string) map[string]string {
	settings := make(map[string]string)
	switch pluginName {
	case "bloom":
		settings["dbms.security.procedures.unrestricted"] = "bloom.*"
		settings["dbms.security.http_auth_allowlist"] = "/,/browser.*,/bloom.*"
		settings["server.unmanaged_extension_classes"] = "com.neo4j.bloom.server=/bloom"
	case "graph-data-science", "gds":
		settings["dbms.security.procedures.unrestricted"] = "gds.*,apoc.load.*"
	case "fleet-management":
		settings["dbms.security.procedures.unrestricted"] = "fleetManagement.*"
		settings["dbms.security.procedures.allowlist"] = "fleetManagement.*"
	}
	return settings
}

// pluginConfKeyIsNonDynamic reports whether a setting must be present in
// neo4j.conf at startup (it can't be set dynamically).
func pluginConfKeyIsNonDynamic(key string) bool {
	switch key {
	case "gds.enterprise.license_file",
		"dbms.bloom.license_file",
		"dbms.security.procedures.allowlist",
		"dbms.security.procedures.denylist",
		"dbms.security.procedures.unrestricted":
		return true
	}
	return false
}

// pluginConfKeyIsSecurity reports whether a key is a security setting that must
// be applied via neo4j.conf at startup.
func pluginConfKeyIsSecurity(key string) bool {
	return strings.HasPrefix(key, "dbms.security.") ||
		strings.HasPrefix(key, "server.unmanaged_extension_classes") ||
		strings.HasPrefix(key, "dbms.bloom.")
}

// pluginConfKey reports whether a plugin setting belongs in neo4j.conf rather
// than the container env: security keys, and keys Neo4j reads only at startup.
func pluginConfKey(key string) bool {
	return pluginConfKeyIsNonDynamic(key) || pluginConfKeyIsSecurity(key)
}

// specSecuritySettings returns what a plugin's spec.security asks for:
// allowedProcedures as the allowlist and — outside the sandbox — as the
// unrestricted list too, deniedProcedures as the denylist. Each replaces the
// automatic value for its key.
func specSecuritySettings(plugin *neo4jv1beta1.Neo4jPlugin) map[string]string {
	settings := make(map[string]string)
	sec := plugin.Spec.Security
	if sec == nil {
		return settings
	}
	if len(sec.AllowedProcedures) > 0 {
		allowed := strings.Join(sec.AllowedProcedures, ",")
		settings["dbms.security.procedures.allowlist"] = allowed
		if !sec.Sandbox {
			settings["dbms.security.procedures.unrestricted"] = allowed
		}
	}
	if len(sec.DeniedProcedures) > 0 {
		settings["dbms.security.procedures.denylist"] = strings.Join(sec.DeniedProcedures, ",")
	}
	return settings
}

// pluginConfSettings returns the neo4j.conf settings derived from a single
// Neo4jPlugin: its automatic security settings, what its spec.security asks
// for, and any security or startup-only keys the user set in spec.config —
// later sources override earlier ones.
func pluginConfSettings(plugin *neo4jv1beta1.Neo4jPlugin) map[string]string {
	settings := make(map[string]string)
	for k, v := range automaticPluginSecuritySettings(plugin.Spec.Name) {
		settings[k] = v
	}
	for k, v := range specSecuritySettings(plugin) {
		settings[k] = v
	}
	for k, v := range plugin.Spec.Config {
		if pluginConfKey(k) {
			settings[k] = v
		}
	}
	return settings
}

// unionPluginConfSettings merges the conf settings of every enabled Neo4jPlugin
// targeting targetRef. Additive keys (procedure allowlists, http_auth_allowlist)
// are unioned across plugins (so GDS + APOC keep both token sets); other keys
// take the last value in plugin-name order (deterministic). Plugins are sorted
// by name so the result is stable between reconciles (no ConfigMap churn).
func unionPluginConfSettings(plugins []neo4jv1beta1.Neo4jPlugin, targetRef string) map[string]string {
	sorted := make([]*neo4jv1beta1.Neo4jPlugin, 0, len(plugins))
	for i := range plugins {
		p := &plugins[i]
		if p.Spec.ClusterRef == targetRef && p.Spec.Enabled {
			sorted = append(sorted, p)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	out := make(map[string]string)
	for _, p := range sorted {
		for k, v := range pluginConfSettings(p) {
			if existing, ok := out[k]; ok && resources.IsAdditiveConfKey(k) {
				out[k] = resources.MergeConfListValues(existing, v)
			} else {
				out[k] = v
			}
		}
	}
	return out
}
