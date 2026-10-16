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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// ClusterValidationResult holds validation results including warnings
type ClusterValidationResult struct {
	Errors field.ErrorList
	// Warnings are topology warnings (reason TopologyWarning).
	Warnings []string
	// NoEffectWarnings name spec fields that are set but not read by any code,
	// and auth provider names Neo4j does not document (AuthProviderWarnings)
	// (reason ValidationWarning). Advisory only: never added to Errors.
	NoEffectWarnings []string
}

// ClusterValidator provides validation for Neo4jEnterpriseCluster resources
type ClusterValidator struct {
	client            client.Client
	editionValidator  *EditionValidator
	topologyValidator *TopologyValidator
	imageValidator    *ImageValidator
	storageValidator  *StorageValidator
	tlsValidator      *TLSValidator
	authValidator     *AuthValidator
	upgradeValidator  *UpgradeValidator
	memoryValidator   *MemoryValidator
	configValidator   *ConfigValidator
}

// NewClusterValidator creates a new cluster validator
func NewClusterValidator(client client.Client) *ClusterValidator {
	return &ClusterValidator{
		client:            client,
		editionValidator:  NewEditionValidator(),
		topologyValidator: NewTopologyValidator(),
		imageValidator:    NewImageValidator(),
		storageValidator:  NewStorageValidator(),
		tlsValidator:      NewTLSValidator(),
		authValidator:     NewAuthValidator(),
		upgradeValidator:  NewUpgradeValidator(),
		memoryValidator:   NewMemoryValidator(),
		configValidator:   NewConfigValidator(),
	}
}

// ValidateCreate validates a Neo4jEnterpriseCluster for creation
func (v *ClusterValidator) ValidateCreate(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	allErrs := v.validateCluster(ctx, cluster)
	if len(allErrs) > 0 {
		return fmt.Errorf("validation failed: %s", allErrs.ToAggregate().Error())
	}
	return nil
}

// ApplyDefaults applies default values to the cluster
func (v *ClusterValidator) ApplyDefaults(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	// Edition field removed - operator only supports enterprise edition

	// Default image pull policy
	if cluster.Spec.Image.PullPolicy == "" {
		cluster.Spec.Image.PullPolicy = "IfNotPresent"
	}

	// Default TLS configuration - disable TLS by default for simplicity
	if cluster.Spec.TLS == nil {
		cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{
			Mode: "disabled",
		}
	} else if cluster.Spec.TLS.Mode == "" {
		cluster.Spec.TLS.Mode = "disabled"
	}

	// Default TLS issuer reference
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == "cert-manager" && cluster.Spec.TLS.IssuerRef == nil {
		cluster.Spec.TLS.IssuerRef = &neo4jv1beta1.IssuerRef{
			Kind: "ClusterIssuer",
		}
	}

	// Default auth configuration
	if cluster.Spec.Auth == nil {
		cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{
			AuthenticationProviders: []string{"native"},
			AuthorizationProviders:  []string{"native"},
		}
	} else if len(cluster.Spec.Auth.AuthenticationProviders) == 0 {
		cluster.Spec.Auth.AuthenticationProviders = []string{"native"}
		cluster.Spec.Auth.AuthorizationProviders = []string{"native"}
	}

	// Default service configuration
	if cluster.Spec.Service == nil {
		cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{
			Type: "ClusterIP",
		}
	}

	// Default storage retention policy to Delete
	if cluster.Spec.Storage.RetentionPolicy == "" {
		cluster.Spec.Storage.RetentionPolicy = "Delete"
	}

	// Note: We no longer auto-adjust topology values
	// Topology warnings will be generated during validation instead
}

// validateCluster performs comprehensive validation of the cluster
// maxClusterNameLength is the max name length for clusters.
// Generated resources append "-server" (7 chars), and DNS labels max at 63.
const maxClusterNameLength = 56

func (v *ClusterValidator) validateCluster(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// Preallocate slice with estimated capacity to reduce allocations
	allErrs = make(field.ErrorList, 0, 10)

	// Validate resource name length (generated services add suffixes like "-server")
	if len(cluster.Name) > maxClusterNameLength {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("metadata", "name"),
			cluster.Name,
			fmt.Sprintf("must be no more than %d characters (generated resources append suffixes up to 7 chars, and DNS labels max at 63)", maxClusterNameLength),
		))
	}

	// Edition validation removed - operator only supports enterprise edition

	// Require explicit Neo4j Enterprise license acceptance (fail closed) — the
	// operator never accepts the commercial license on the user's behalf.
	allErrs = append(allErrs, validateLicenseAcceptance(
		field.NewPath("spec", "acceptLicenseAgreement"), cluster.Spec.AcceptLicenseAgreement)...)

	// Validate topology (second most critical)
	allErrs = append(allErrs, v.topologyValidator.Validate(cluster)...)

	// Validate image.
	//
	// This used to `return` early when the image was invalid, on the reasoning
	// that "other validations are less meaningful" once it is. That is true of
	// the *deployment*, but not of the *feedback*: this operator has no
	// admission webhook, so a user's only channel is the errors this function
	// produces. Returning early meant a bad image hid every storage, TLS, auth
	// and spec.config problem until it was fixed, turning one apply into a
	// fix-one-rerun loop — and the same loop in `kubectl neo4j validate`, which
	// calls this code directly.
	//
	// Every validator below is independent of the image: none reads it except
	// validatePropertySharding, which is opt-in and returns an error rather
	// than assuming a parseable tag. TestValidateCluster_ReportsAllErrors pins
	// that this function keeps reporting the full set.
	allErrs = append(allErrs, v.imageValidator.Validate(cluster)...)

	allErrs = append(allErrs, v.storageValidator.Validate(cluster)...)
	allErrs = append(allErrs, v.tlsValidator.Validate(cluster)...)
	allErrs = append(allErrs, v.authValidator.Validate(cluster)...)

	// spec.upgradeStrategy needs only the spec, so it is checked here, where
	// `kubectl neo4j validate` checks it too. It used to sit with the
	// update-only rules, where the operator ran it on every pass and the CLI
	// never did: a timeout of "5 minutes" passed validate and was refused on
	// apply.
	allErrs = append(allErrs, v.upgradeValidator.ValidateUpgradeStrategy(cluster)...)

	// spec.config validation — rejects deprecated keys, operator-managed
	// discovery/SSL keys, per-pod runtime-managed keys (advertised
	// addresses, topology) that would collide with the operator's own
	// startup-appended values, and values of known type that Neo4j cannot
	// parse. Previously this validator was never wired in.
	allErrs = append(allErrs, v.configValidator.Validate(cluster)...)
	// propertySharding.config is written into neo4j.conf too, once sharding is
	// enabled, so its typed values are held to the same rules.
	if ps := cluster.Spec.PropertySharding; ps != nil && ps.Enabled {
		allErrs = append(allErrs, ValidateSettingValues(
			field.NewPath("spec", "propertySharding", "config"), ps.Config)...)
	}
	legacyLanguage := []ConfigAt{{Path: field.NewPath("spec", "config"), Config: cluster.Spec.Config}}
	if cluster.Spec.PropertySharding != nil {
		legacyLanguage = append(legacyLanguage, ConfigAt{
			Path: field.NewPath("spec", "propertySharding", "config"), Config: cluster.Spec.PropertySharding.Config})
	}
	allErrs = append(allErrs, ValidateServerCypherLanguage(
		cluster.Spec.ServerDefaultCypherLanguage, cluster.Spec.Image.Tag, legacyLanguage...)...)
	allErrs = append(allErrs, ValidateStdoutLogs(cluster.Spec.Monitoring, cluster.Spec.Config)...)

	// Property sharding validation (version requirements)
	allErrs = append(allErrs, v.validatePropertySharding(cluster)...)

	// Memory validation (critical for preventing runtime failures)
	allErrs = append(allErrs, v.memoryValidator.Validate(cluster)...)

	// MCP server validation
	allErrs = append(allErrs, validateMCPConfig(cluster.Spec.MCP, field.NewPath("spec", "mcp"))...)

	// Aura Fleet Management validation
	allErrs = append(allErrs, validateAuraFleetManagement(cluster.Spec.AuraFleetManagement, field.NewPath("spec", "auraFleetManagement"))...)

	// NetworkPolicy validation (allowReplicasFrom entry shape)
	allErrs = append(allErrs, validateNetworkPolicy(cluster.Spec.NetworkPolicy, field.NewPath("spec", "networkPolicy"))...)

	// Cross-cluster replication: the exposure proxy is refused without TLS,
	// and peer trust CAs are refused without TLS to read them.
	allErrs = append(allErrs, validateCrossClusterReplication(cluster,
		field.NewPath("spec", "crossClusterReplication"))...)
	allErrs = append(allErrs, validateClusterTrustCAsNeedTLS(cluster,
		field.NewPath("spec", "tls"))...)

	// Trusted CA Secrets + extra volume mounts (operator-managed path collision check)
	allErrs = append(allErrs, ValidateTrustedCASecrets(cluster.Spec.TrustedCASecrets, field.NewPath("spec", "trustedCASecrets"))...)
	allErrs = append(allErrs, ValidateExtraVolumes(cluster.Spec.ExtraVolumes, field.NewPath("spec", "extraVolumes"))...)
	allErrs = append(allErrs, ValidateExtraVolumeMounts(cluster.Spec.ExtraVolumeMounts, field.NewPath("spec", "extraVolumeMounts"))...)

	// Admin password shape: a leading "-" makes the entrypoint's
	// set-initial-password call fail to parse, crash-looping the pod with a
	// message that never mentions the password. See ValidateAdminSecretPassword.
	allErrs = append(allErrs, ValidateAdminSecretPassword(
		ctx, v.client, cluster.Namespace, resolveAdminSecretName(cluster.Spec.Auth),
		field.NewPath("spec", "auth", "adminSecret"))...)

	return allErrs
}

// resolveAdminSecretName mirrors the builder's choice of admin Secret: the
// explicit spec.auth.adminSecret when set, else the operator default. Kept here
// so validation judges the SAME Secret the StatefulSet will actually mount
// (internal/resources/cluster.go buildStatefulSetForEnterprise).
func resolveAdminSecretName(auth *neo4jv1beta1.AuthSpec) string {
	if auth != nil && auth.AdminSecret != "" {
		return auth.AdminSecret
	}
	// Reuse the builder's own constant rather than duplicating the literal, so
	// the two cannot drift apart.
	return resources.DefaultAdminSecret
}

// ValidateCreateWithWarnings runs the rules that need only the spec: what a
// cluster that does not run yet is held to, and a part of every later pass.
func (v *ClusterValidator) ValidateCreateWithWarnings(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) ClusterValidationResult {
	result := ClusterValidationResult{
		Errors:   v.validateCluster(ctx, cluster),
		Warnings: []string{},
	}

	// Get topology warnings
	topologyResult := v.topologyValidator.ValidateWithWarnings(cluster)
	result.Warnings = append(result.Warnings, topologyResult.Warnings...)
	result.NoEffectWarnings = v.NoEffectWarnings(cluster)

	return result
}

// ValidateWithWarnings is what the reconciler runs on every pass, and what
// `kubectl neo4j validate` runs: the spec rules (ValidateCreateWithWarnings)
// plus the rules that compare the spec with what the cluster already runs.
//
// There is no admission webhook, so there is no previous spec to compare with:
// the reconciler only ever sees the object as it is now. What the cluster runs
// is read from the status the operator writes, so a cluster with no status (a
// manifest not yet applied) is held to the spec rules alone.
func (v *ClusterValidator) ValidateWithWarnings(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) ClusterValidationResult {
	result := v.ValidateCreateWithWarnings(ctx, cluster)
	result.Errors = append(result.Errors, v.validateAgainstRunning(cluster)...)
	return result
}

// validateAgainstRunning holds the rules that need the cluster's running state.
//
// spec.image.tag is checked against status.version, the version the operator
// verified every server runs. It is set at the first Ready and after each
// verified upgrade, and it stays at the version the servers ran while an
// upgrade is in flight or after one failed, so moving the tag back to it (or
// onto a later target) is not a downgrade. The check also covers the paths
// where the upgrade machine checks nothing: a target changed mid-upgrade,
// which re-stages without a version check, and a cluster with no ready server,
// whose new image goes straight to the StatefulSet.
//
// The server count has no rule here. At least two servers is a spec rule, a
// scale-down is the drain's to carry out or refuse with its reason
// (internal/controller/scale_down.go), and whether added servers fit on the
// nodes is the scheduler's call: `kubectl neo4j preflight` estimates it before
// an apply, and a server that does not fit stays Pending.
func (v *ClusterValidator) validateAgainstRunning(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	if cluster.Status.Version == "" || v.upgradeValidator.parseVersion(cluster.Spec.Image.Tag) == nil {
		// Nothing verified yet, or a tag the image rule already refuses.
		return nil
	}
	return v.upgradeValidator.ValidateVersionUpgrade(cluster.Status.Version, cluster.Spec.Image.Tag)
}

// validatePropertySharding validates property sharding configuration and version requirements.
// Delegates the underlying static check to IsClusterShardingReady so backup/restore
// reconcilers can reuse the same precondition without duplicating the logic.
func (v *ClusterValidator) validatePropertySharding(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// Only run the gate when the user opted in. If sharding is disabled the
	// version requirement does not apply — the helper would reject any image
	// tag in that branch, which is wrong for the validator's purpose here.
	if cluster.Spec.PropertySharding == nil || !cluster.Spec.PropertySharding.Enabled {
		return allErrs
	}

	if err := IsClusterShardingReady(cluster); err != nil {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "image", "tag"),
			cluster.Spec.Image.Tag,
			err.Error()))
	}

	return allErrs
}
