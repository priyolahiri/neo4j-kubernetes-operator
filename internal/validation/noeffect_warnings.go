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

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// This file holds the warnings for spec fields the schema accepts but no code
// reads. A silently ignored field is worse than a rejected one: the user
// believes a setting is in force (a node selector, a registry TLS policy, a
// resource limit) and nothing is. We cannot reject them — manifests that set
// them are already deployed, and removing a field from the CRD is a breaking
// change — so the validator for each kind says so instead.
//
// Every message has the same shape:
//
//	spec.<path> is accepted but has no effect today: <what to do instead>
//
// Warnings are advisory. They are never added to an error list and never block
// a reconcile. Each reaches the user as a Warning event with reason
// ValidationWarning, through whichever channel the owning kind already has (see
// the NoEffectWarnings methods and the controllers that emit them).
//
// A field belongs here only while NOTHING reads it. When a controller starts
// acting on a field, delete its warning in the same change; the matching
// "Reserved" doc comment in api/v1beta1 and the api_reference row go with it.

// NoEffectWarning formats the warning for a schema field that nothing reads.
// instead says what to do to get the effect the user was after.
func NoEffectWarning(path *field.Path, instead string) string {
	return fmt.Sprintf("%s is accepted but has no effect today: %s", path.String(), instead)
}

// tlsNoEffectWarnings covers the TLS block shared by the cluster and the
// standalone. certificateSecret is warned about unconditionally: whatever the
// mode, the operator only ever uses the cert-manager Secret <name>-tls-secret.
func tlsNoEffectWarnings(tls *neo4jv1beta1.TLSSpec, resourceName string) []string {
	if tls == nil || tls.CertificateSecret == "" {
		return nil
	}
	return []string{NoEffectWarning(field.NewPath("spec", "tls", "certificateSecret"),
		fmt.Sprintf("the operator always uses the cert-manager Secret %s-tls-secret; to bring your own CA or "+
			"certificate chain, point spec.tls.issuerRef at an Issuer backed by it", resourceName))}
}

// NoEffectWarnings reports cluster spec fields that are set but not read.
func (v *ClusterValidator) NoEffectWarnings(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) []string {
	warnings := tlsNoEffectWarnings(cluster.Spec.TLS, cluster.Name)

	if p := cluster.Spec.Topology.Placement; p != nil {
		placementPath := field.NewPath("spec", "topology", "placement")
		if len(p.NodeSelector) > 0 {
			warnings = append(warnings, NoEffectWarning(placementPath.Child("nodeSelector"),
				"use the top-level spec.nodeSelector"))
		}
		if p.RequiredDuringScheduling {
			warnings = append(warnings, NoEffectWarning(placementPath.Child("requiredDuringScheduling"),
				"use spec.topology.placement.antiAffinity.type: required or spec.topology.enforceDistribution "+
					"for hard placement"))
		}
	}
	return warnings
}

// NoEffectWarnings reports standalone spec fields that are set but not read.
// The standalone has no warning channel of its own (ValidateCreate returns only
// errors), so this is a separate method the reconciler turns into events.
func (v *StandaloneValidator) NoEffectWarnings(standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) []string {
	return tlsNoEffectWarnings(standalone.Spec.TLS, standalone.Name)
}

// pluginNoEffectWarnings reports Neo4jPlugin spec fields that are set but not
// read. Called from PluginValidator.Validate, whose Warnings the plugin
// controller already emits as ValidationWarning events.
func pluginNoEffectWarnings(plugin *neo4jv1beta1.Neo4jPlugin) []string {
	var warnings []string
	specPath := field.NewPath("spec")

	if plugin.Spec.Resources != nil {
		warnings = append(warnings, NoEffectWarning(specPath.Child("resources"),
			"nothing is allocated or limited for a plugin; size the Neo4j pods through the target "+
				"cluster or standalone spec.resources"))
	}

	if src := plugin.Spec.Source; src != nil && src.Registry != nil {
		registryPath := specPath.Child("source", "registry")
		const registryInstead = "a custom source is fetched from spec.source.url; use spec.source.type url or " +
			"custom with spec.source.url, spec.source.checksum and spec.source.authSecret, and " +
			"spec.installMode VerifiedDownload"
		warnings = append(warnings, NoEffectWarning(registryPath, registryInstead))
		if src.Registry.TLS != nil {
			warnings = append(warnings, NoEffectWarning(registryPath.Child("tls"),
				"no registry is contacted, so no TLS policy is applied; the download in VerifiedDownload mode "+
					"trusts the cluster's spec.trustedCASecrets"))
		}
	}

	if sec := plugin.Spec.Security; sec != nil && sec.SecurityPolicy != "" {
		warnings = append(warnings, NoEffectWarning(specPath.Child("security", "securityPolicy"),
			"it does not change any Neo4j setting; restrict procedures with spec.security.allowedProcedures "+
				"and spec.security.deniedProcedures"))
	}
	return warnings
}

// databaseNoEffectWarnings reports Neo4jDatabase spec fields that are set but
// not read. Called from DatabaseValidator.Validate, whose Warnings the database
// controller already emits as ValidationWarning events.
func databaseNoEffectWarnings(database *neo4jv1beta1.Neo4jDatabase) []string {
	initial := database.Spec.InitialData
	if initial == nil {
		return nil
	}

	const instead = "only spec.initialData.cypherStatements is executed; supply the data as Cypher " +
		"statements, or seed from a backup with spec.seedURI"
	initialPath := field.NewPath("spec", "initialData")

	var warnings []string
	// source is never read, but "cypher" describes exactly what happens (every
	// shipped example sets it next to cypherStatements), so it is not worth a
	// warning. dump and csv promise an import that never runs.
	if initial.Source != "" && initial.Source != "cypher" {
		warnings = append(warnings, NoEffectWarning(initialPath.Child("source"),
			fmt.Sprintf("nothing is imported from a %q source; ", initial.Source)+instead))
	}
	if initial.ConfigMapRef != "" {
		warnings = append(warnings, NoEffectWarning(initialPath.Child("configMapRef"), instead))
	}
	if initial.SecretRef != "" {
		warnings = append(warnings, NoEffectWarning(initialPath.Child("secretRef"), instead))
	}
	if initial.Storage != nil {
		warnings = append(warnings, NoEffectWarning(initialPath.Child("storage"), instead))
	}
	return warnings
}

// CloudStorageNoEffectWarnings reports the cloud-identity fields of a storage
// location that are set but not read. storagePath is where the StorageLocation
// sits in the owning spec (for example spec.storage), and serviceAccountName is
// the ServiceAccount the operator actually runs the Job as, so the message can
// name it. One helper serves every kind that embeds a StorageLocation: backup
// and restore today.
func CloudStorageNoEffectWarnings(storage *neo4jv1beta1.StorageLocation, storagePath *field.Path,
	serviceAccountName string) []string {
	if storage == nil || storage.Cloud == nil || storage.Cloud.Identity == nil {
		return nil
	}
	identityPath := storagePath.Child("cloud", "identity")
	identity := storage.Cloud.Identity

	var warnings []string
	if identity.ServiceAccount != "" {
		warnings = append(warnings, NoEffectWarning(identityPath.Child("serviceAccount"),
			fmt.Sprintf("the Job always runs as the operator-managed ServiceAccount %s; bind your cloud "+
				"identity (IRSA, Workload Identity, Managed Identity) by setting %s", serviceAccountName,
				identityPath.Child("autoCreate", "annotations").String())))
	}
	// autoCreate.enabled defaults to true in the schema, so "true" cannot be told
	// apart from "not set"; only an explicit false is a statement of intent, and
	// it is the one the operator does not honour (the ServiceAccount is always
	// created).
	if identity.AutoCreate != nil && !identity.AutoCreate.Enabled {
		warnings = append(warnings, NoEffectWarning(identityPath.Child("autoCreate", "enabled"),
			fmt.Sprintf("the operator always ensures the ServiceAccount %s exists; only "+
				"autoCreate.annotations is honoured, so leave enabled at its default (true)", serviceAccountName)))
	}
	return warnings
}

// NoEffectWarnings reports Neo4jBackup spec fields that are set but not read.
// BackupValidator.Validate returns only errors, so this is a separate method the
// backup reconciler turns into events.
func (v *BackupValidator) NoEffectWarnings(backup *neo4jv1beta1.Neo4jBackup) []string {
	return CloudStorageNoEffectWarnings(&backup.Spec.Storage, field.NewPath("spec", "storage"),
		resources.BackupServiceAccountName)
}

// RestoreNoEffectWarnings reports Neo4jRestore spec fields that are set but not
// read. Restore has no validator type — its checks live in the reconciler — so
// this is a package function the reconciler turns into events.
func RestoreNoEffectWarnings(restore *neo4jv1beta1.Neo4jRestore) []string {
	sourcePath := field.NewPath("spec", "source")
	src := restore.Spec.Source

	warnings := CloudStorageNoEffectWarnings(src.Storage, sourcePath.Child("storage"), resources.RestoreServiceAccountName)
	if pitr := src.PITR; pitr != nil {
		warnings = append(warnings, CloudStorageNoEffectWarnings(pitr.LogStorage,
			sourcePath.Child("pitr", "logStorage"), resources.RestoreServiceAccountName)...)
		if base := pitr.BaseBackup; base != nil {
			warnings = append(warnings, CloudStorageNoEffectWarnings(base.Storage,
				sourcePath.Child("pitr", "baseBackup", "storage"), resources.RestoreServiceAccountName)...)
		}
	}
	return warnings
}

// AuraInstanceNoEffectWarnings reports AuraInstance spec fields that are set but
// not read. AuraInstance has no validator, so this is a package function the
// reconciler turns into events.
func AuraInstanceNoEffectWarnings(inst *neo4jv1beta1.AuraInstance) []string {
	if inst.Spec.ConnectionSecretFormat != "custom" {
		return nil
	}
	return []string{NoEffectWarning(field.NewPath("spec", "connectionSecretFormat"),
		"\"custom\" has no key template of its own and writes exactly the keys neo4j-driver does; "+
			"set neo4j-driver explicitly, or choose aura-dotenv, jdbc or servicebinding for a different layout")}
}
