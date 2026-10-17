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
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ValidateExternalSecretsConfig checks an enabled ExternalSecrets block, at
// esPath: spec.tls.externalSecrets and spec.auth.externalSecrets both build an
// ExternalSecret from it, and both dereference secretStoreRef, so both need
// it. Only the TLS block used to be checked; an auth block without
// secretStoreRef reached the builder and panicked on every pass.
func ValidateExternalSecretsConfig(cfg *neo4jv1beta1.ExternalSecretsConfig, esPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if cfg.SecretStoreRef == nil {
		allErrs = append(allErrs, field.Required(
			esPath.Child("secretStoreRef"),
			"secretStoreRef is required when external secrets is enabled",
		))
	} else {
		if cfg.SecretStoreRef.Name == "" {
			allErrs = append(allErrs, field.Required(
				esPath.Child("secretStoreRef", "name"),
				"secretStore name is required",
			))
		}

		validKinds := []string{"SecretStore", "ClusterSecretStore"}
		if cfg.SecretStoreRef.Kind != "" {
			valid := false
			for _, kind := range validKinds {
				if cfg.SecretStoreRef.Kind == kind {
					valid = true
					break
				}
			}
			if !valid {
				allErrs = append(allErrs, field.NotSupported(
					esPath.Child("secretStoreRef", "kind"),
					cfg.SecretStoreRef.Kind,
					validKinds,
				))
			}
		}
	}

	// Validate refresh interval.
	//
	// We intentionally validate using Go's time.ParseDuration because it
	// accepts the format ESO uses in practice ("15m", "1h"), and
	// time.ParseDuration("0") returns 0 — matching ESO's convention of
	// "0" meaning "disable polling". If ESO's duration semantics ever
	// diverge from Go's (it currently uses Kubernetes metav1.Duration,
	// which wraps time.ParseDuration), switch this to an ESO-specific
	// parser. Pure-numeric tokens like "0" are accepted here on purpose.
	if cfg.RefreshInterval != "" {
		if _, err := time.ParseDuration(cfg.RefreshInterval); err != nil {
			allErrs = append(allErrs, field.Invalid(
				esPath.Child("refreshInterval"),
				cfg.RefreshInterval,
				"invalid duration format",
			))
		}
	}

	// Validate data mappings
	if len(cfg.Data) == 0 {
		allErrs = append(allErrs, field.Required(
			esPath.Child("data"),
			"at least one data mapping is required when external secrets is enabled",
		))
	}

	for i, data := range cfg.Data {
		dataPath := esPath.Child("data").Index(i)
		if data.SecretKey == "" {
			allErrs = append(allErrs, field.Required(
				dataPath.Child("secretKey"),
				"secretKey is required",
			))
		}
		if data.RemoteRef != nil && data.RemoteRef.Key == "" {
			allErrs = append(allErrs, field.Required(
				dataPath.Child("remoteRef", "key"),
				"remoteRef key is required",
			))
		}
	}
	return allErrs
}
