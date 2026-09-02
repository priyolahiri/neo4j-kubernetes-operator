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

package resources

// The cloud-credential contract for backup and restore Jobs, in one place.
//
// The Job builder in internal/controller wires these keys into the pod as
// secretKeyRef env vars (aws, azure) or a projected volume item (gcp). A key
// the Secret does not carry does not fail politely: the pod never starts, and
// the kubelet reports CreateContainerConfigError with no mention of backups.
//
// These names are declared here so that a consumer OUTSIDE the reconciler —
// kubectl-neo4j's `preflight`, which checks the Secret's shape before the CR
// is applied — can check the same contract without restating it. The
// controller's builders are pinned to this list by a contract test in
// internal/controller, so the two cannot drift apart silently.

// ServiceAccount names for the Jobs the operator runs. Declared here for the
// same reason as the keys: `preflight` has to look up the ServiceAccount that
// will actually run the Job in order to check its cloud-identity annotation.
const (
	BackupServiceAccountName  = "neo4j-backup-sa"
	RestoreServiceAccountName = "neo4j-restore-sa"
)

// CloudCredentialKeys returns the keys that must exist in the Secret named by
// spec.storage.cloud.credentialsSecretRef for the given provider ("aws", "gcp"
// or "azure"). An unknown provider returns nil, which callers must read as
// "nothing to check" rather than "nothing is required".
//
// Azure is the one provider whose requirement is not a flat list: the account
// name is mandatory, and then EITHER a key or a SAS token. CloudCredentialKeys
// returns only what is unconditionally required; see CloudCredentialAlternates
// for the either/or part.
func CloudCredentialKeys(provider string) []string {
	switch provider {
	case "aws":
		return []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"}
	case "gcp":
		// Mounted as a file rather than an env var, but the Secret key is
		// still what has to be right.
		return []string{"GOOGLE_APPLICATION_CREDENTIALS_JSON"}
	case "azure":
		return []string{"AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY"}
	default:
		return nil
	}
}

// CloudIdentityAnnotation returns the ServiceAccount annotation that binds a
// Job to an ambient cloud identity for the given provider — the mechanism used
// when no credentialsSecretRef is set. Applied through
// spec.storage.cloud.identity.autoCreate.annotations, which the operator
// stamps onto the backup/restore ServiceAccount.
//
// Returns "" for an unknown provider.
func CloudIdentityAnnotation(provider string) string {
	switch provider {
	case "aws":
		return "eks.amazonaws.com/role-arn" // IRSA
	case "gcp":
		return "iam.gke.io/gcp-service-account" // Workload Identity
	case "azure":
		return "azure.workload.identity/client-id"
	default:
		return ""
	}
}
