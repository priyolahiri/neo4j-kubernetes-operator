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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// validateAuraFleetManagement validates the auraFleetManagement spec for a
// cluster or standalone whose image is tagged imageTag (spec.image.tag).
func validateAuraFleetManagement(spec *neo4jv1beta1.AuraFleetManagementSpec, imageTag string, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if spec == nil || !spec.Enabled {
		return allErrs
	}

	allErrs = append(allErrs, fleetManagementImageErrors(imageTag, path.Child("enabled"))...)

	// tokenSecretRef is optional: the plugin is installed even without it, but registration
	// is skipped until a token is provided. Validate the ref only when present.
	if spec.TokenSecretRef != nil {
		if spec.TokenSecretRef.Name == "" {
			allErrs = append(allErrs, field.Required(
				path.Child("tokenSecretRef", "name"),
				"tokenSecretRef.name must not be empty when tokenSecretRef is set",
			))
		}
	}

	allErrs = append(allErrs, validateAuraFleetProvision(spec, path)...)
	return allErrs
}

// validateAuraFleetProvision validates the operator-driven Fleet Manager
// onboarding block. CEL on the CRD already covers the single-object rules
// (provision XOR tokenSecretRef, providerConfigRef XOR credentialsSecretRef,
// the 30-character deployment-name cap, and the two enums); these checks cover
// what CEL cannot express — empty nested names, and the organization/project
// requirement that may be satisfied by a *different* object (the
// AuraProviderConfig defaults), which is why it is a warning-free runtime
// concern rather than a hard schema error.
func validateAuraFleetProvision(spec *neo4jv1beta1.AuraFleetManagementSpec, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	p := spec.Provision
	if p == nil {
		return allErrs
	}
	pp := path.Child("provision")

	// Belt-and-braces for the CEL XOR: a stale CRD (upgrade in flight) would let
	// both through, and silently preferring one would be worse than a clear error.
	if spec.TokenSecretRef != nil {
		allErrs = append(allErrs, field.Invalid(pp, "provision",
			"set at most one of provision or tokenSecretRef: either the operator mints the token (provision) or you supply it (tokenSecretRef)"))
	}

	hasProviderRef := p.ProviderConfigRef != nil && p.ProviderConfigRef.Name != ""
	hasInline := p.CredentialsSecretRef != nil && p.CredentialsSecretRef.Name != ""
	switch {
	case p.ProviderConfigRef != nil && p.ProviderConfigRef.Name == "":
		allErrs = append(allErrs, field.Required(pp.Child("providerConfigRef", "name"),
			"providerConfigRef.name must not be empty when providerConfigRef is set"))
	case p.CredentialsSecretRef != nil && p.CredentialsSecretRef.Name == "":
		allErrs = append(allErrs, field.Required(pp.Child("credentialsSecretRef", "name"),
			"credentialsSecretRef.name must not be empty when credentialsSecretRef is set"))
	case !hasProviderRef && !hasInline:
		allErrs = append(allErrs, field.Required(pp,
			"set exactly one of providerConfigRef or credentialsSecretRef to give the operator Aura API credentials"))
	}

	// The API caps a deployment name at 30 characters. CEL enforces this for an
	// explicit value; repeat it here so the error is identical whichever layer
	// catches it.
	if len(p.DeploymentName) > 30 {
		allErrs = append(allErrs, field.TooLong(pp.Child("deploymentName"), p.DeploymentName, 30))
	}

	// organizationId / projectId may legitimately be empty here and resolved from
	// the AuraProviderConfig's defaults, which this validator cannot read. Only
	// reject the case where neither the field nor any provider config could
	// possibly supply them.
	if !hasProviderRef {
		if p.OrganizationID == "" {
			allErrs = append(allErrs, field.Required(pp.Child("organizationId"),
				"organizationId is required when using credentialsSecretRef (no AuraProviderConfig to default from)"))
		}
		if p.ProjectID == "" {
			allErrs = append(allErrs, field.Required(pp.Child("projectId"),
				"projectId is required when using credentialsSecretRef (no AuraProviderConfig to default from)"))
		}
	}

	return allErrs
}

// fleetManagementPlugin is the name Aura Fleet Management installs its plugin
// by, in NEO4J_PLUGINS.
const fleetManagementPlugin = "fleet-management"

// fleetManagementImageErrors refuses Aura Fleet Management on an image whose
// entrypoint does not install fleet-management: 5.26.0 to 5.26.10 and 2025.01
// to 2025.07 (entrypointPluginLists). Enabling it puts the name in
// NEO4J_PLUGINS, the entrypoint exits on a name it does not list, and the
// servers do not start (operations.md id 191; #563, #573). Nothing is refused
// for a tag that names no supported version: the image rule speaks for it.
func fleetManagementImageErrors(imageTag string, path *field.Path) field.ErrorList {
	known, ok := entrypointPluginNames(imageTag)
	if !ok || slices.Contains(known, fleetManagementPlugin) {
		return nil
	}
	return field.ErrorList{field.Invalid(path, true, fmt.Sprintf(
		"Aura Fleet Management needs %s: it adds the fleet-management plugin to NEO4J_PLUGINS, and the entrypoint of the "+
			"neo4j:%s image does not know that plugin and exits on it, so the servers would not start; set spec.image.tag "+
			"to a release that installs it, or remove spec.auraFleetManagement",
		firstReleasesInstalling(fleetManagementPlugin), imageTag))}
}

// firstReleasesInstalling names, line by line, the first release whose
// entrypoint installs name: "5.26.11 or a later 5.26, or 2025.08.0 or later".
func firstReleasesInstalling(name string) string {
	var lts, calver string
	for _, list := range entrypointPluginLists {
		if !slices.Contains(list.names, name) {
			continue
		}
		if parseTagVersion(list.from).Major >= 2025 {
			if calver == "" {
				calver = list.from + " or later"
			}
		} else if lts == "" {
			lts = list.from + " or a later 5.26"
		}
	}
	var parts []string
	for _, p := range []string{lts, calver} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ", or ")
}
