package controller

// Cluster formation events
const (
	// EventReasonClusterFormationStarted — a cluster that has never formed
	// entered Forming. Announced by the status write that entered it, once
	// (operations.md id 136). There is no ClusterFormationFailed: formation
	// does not fail, it waits, and a cluster that cannot be reached for long
	// raises ConnectivityDegraded.
	EventReasonClusterFormationStarted = "ClusterFormationStarted"
	EventReasonClusterReady            = "ClusterReady"
	EventReasonTopologyWarning         = "TopologyWarning"
	EventReasonValidationFailed        = "ValidationFailed"
	EventReasonTopologyPlacementFailed = "TopologyPlacementFailed"
	// EventReasonTopologyPlacementCalc — the placement of a zoned cluster was
	// calculated. Announced when it is first calculated and when its zones
	// change, not on every pass (operations.md id 136).
	EventReasonTopologyPlacementCalc         = "TopologyPlacementCalculated"
	EventReasonTopologyZoneDiscoveryDegraded = "TopologyZoneDiscoveryDegraded"
	EventReasonPropertyShardingFailed        = "PropertyShardingValidationFailed"
	EventReasonServerRoleFailed              = "ServerRoleValidationFailed"
	EventReasonRouteAPINotFound              = "RouteAPINotFound"
	EventReasonMCPApocMissing                = "MCPApocMissing"
	EventReasonReconcileFailed               = "ReconcileFailed"
	EventReasonScaleDownDraining             = "ScaleDownDraining"
	EventReasonScaleDownBlocked              = "ScaleDownBlocked"
	// EventReasonConnectivityDegraded — the operator has failed to reach the
	// cluster's Bolt endpoint for a sustained streak of reconciles (#263).
	EventReasonConnectivityDegraded = "ConnectivityDegraded"
	// EventReasonClusterDegraded — a formed cluster has been short of one or
	// more servers for longer than the grace period and its phase is now
	// Degraded. Emitted once per transition (#444).
	EventReasonClusterDegraded = "ClusterDegraded"
	// EventReasonClusterQuorumLost — a formed cluster has fewer than a
	// majority of its servers available; dependents pause until a majority is
	// back. Emitted once per transition (#444).
	EventReasonClusterQuorumLost = "ClusterQuorumLost"
	// EventReasonConfigAppliedLive — a neo4j.conf change touched only settings
	// Neo4j can change at runtime, so the operator ran dbms.setConfigValue on
	// every server instead of restarting them (#466).
	EventReasonConfigAppliedLive = "ConfigAppliedLive"
	// EventReasonConfigNeedsRestart — a neo4j.conf change could not be applied
	// at runtime (a static setting, an env override, a server not up); the
	// message says why, and the servers restart as before (#466).
	EventReasonConfigNeedsRestart = "ConfigNeedsRestart"
	// EventReasonScaledWithoutRestart — a scale-up added only servers that join
	// as system secondaries, so the running servers were not restarted (#467).
	EventReasonScaledWithoutRestart = "ScaledWithoutRestart"
	// EventReasonTLSCertificateReloaded — a renewed TLS certificate was
	// reloaded on running servers with dbms.security.reloadTLS() (#469).
	EventReasonTLSCertificateReloaded = "TLSCertificateReloaded"
	// EventReasonTLSCertificateNeedsRestart — a server could not reload the
	// renewed certificate (TLS reload not enabled on it yet) and presents the
	// previous one until it restarts (#469).
	EventReasonTLSCertificateNeedsRestart = "TLSCertificateNeedsRestart"
)

// Rolling upgrade events
const (
	EventReasonUpgradeStarted   = "UpgradeStarted"
	EventReasonUpgradeCompleted = "UpgradeCompleted"
	EventReasonUpgradePaused    = "UpgradePaused"
	EventReasonUpgradeFailed    = "UpgradeFailed"
	// EventReasonUpgradeDeferred — an image change was held back: the cluster
	// is not Ready with every server available, or a scale-down drain is in
	// progress (#173/#174 mutual exclusion). The upgrade starts once it is.
	// Announced once per held target, not on every pass (operations.md id
	// 136).
	EventReasonUpgradeDeferred = "UpgradeDeferred"
)

// Backup and restore events
const (
	EventReasonBackupScheduled = "BackupScheduled"
	// EventReasonBackupRetentionCaveat — retention pruning configured on a CR
	// whose chain may contain differential artifacts (#217).
	EventReasonBackupRetentionCaveat = "BackupRetentionCaveat"
	// EventReasonBackupShardedExcluded — an all-databases backup wrote a
	// property-sharded family's shard databases to disk but did not catalogue
	// them, so an all-databases restore cannot recreate them; each must be
	// backed up with a ShardedDatabase-scoped Neo4jBackup and restored via its
	// Neo4jShardedDatabase CR. Makes the otherwise-silent exclusion explicit.
	EventReasonBackupShardedExcluded = "BackupShardedDatabasesExcluded"
	// EventReasonBackupShardedDifferential — a run wrote differential shard
	// backups to a PVC, which cannot seed a sharded database (rule 110).
	EventReasonBackupShardedDifferential = "BackupShardedDifferential"
	// EventReasonBackupValidateUnsupported — options.validate is set on a
	// backup of a 5.26 (non-CalVer) deployment, where `neo4j-admin backup
	// validate` does not exist; the run skips validation.
	EventReasonBackupValidateUnsupported = "BackupValidateUnsupported"
	// EventReasonServiceAccountAnnotationConflict — a backup/restore CR
	// overwrote DIFFERENT workload-identity annotations on the shared
	// namespace ServiceAccount; last writer wins and the others' cloud
	// access breaks (#227).
	EventReasonServiceAccountAnnotationConflict = "ServiceAccountAnnotationConflict"
	// EventReasonSeedEndpointNotProjected — an online restore from cloud
	// storage found the target's server pods lacking the credentials Secret
	// or the custom S3 endpoint (AWS_ENDPOINT_URL_S3, #252) the server-side
	// seed fetch needs, and the target not opted in to inject them. The
	// restore waits Pending and carries on once they are there.
	EventReasonSeedEndpointNotProjected = "SeedEndpointNotProjected"
	EventReasonBackupStarted            = "BackupStarted"
	EventReasonBackupCompleted          = "BackupCompleted"
	EventReasonBackupFailed             = "BackupFailed"
	EventReasonRestoreStarted           = "RestoreStarted"
	EventReasonRestoreCompleted         = "RestoreCompleted"
	EventReasonRestoreFailed            = "RestoreFailed"
	EventReasonRestoreFromChainParent   = "RestoreFromChainParent"
	EventReasonDatabaseCreateFailed     = "DatabaseCreateFailed"
	// EventReasonRestoreSeedProgress reports one poll of an all-databases
	// restore's per-database seed. It is emitted on EVERY non-terminal poll, not
	// only when the message changes, deliberately: the apiserver aggregates
	// identical events into a single object carrying a COUNT plus first/last
	// timestamps, which is exactly the "has this been stuck at 0/1 since the
	// first poll, or is it progressing?" signal that a status message alone
	// cannot give. Bounded by the number of distinct states, not by poll count.
	EventReasonRestoreSeedProgress = "RestoreSeedProgress"

	// EventReasonRestoreShardedNotCovered — an all-databases restore's source
	// backup recorded property-sharded databases it did not cover; they are not
	// recreated here and must be restored via their Neo4jShardedDatabase CR.
	EventReasonRestoreShardedNotCovered = "RestoreShardedDatabasesNotCovered"
)

// Database events
const (
	EventReasonClusterNotFound     = "ClusterNotFound"
	EventReasonDatabaseReady       = "DatabaseReady"
	EventReasonDatabaseDeleted     = "DatabaseDeleted"
	EventReasonDatabaseCreatedSeed = "DatabaseCreatedFromSeed"
	EventReasonCreationFailed      = "CreationFailed"
	EventReasonDeletionFailed      = "DeletionFailed"
	EventReasonDataImported        = "DataImported"
	EventReasonDataImportFailed    = "DataImportFailed"
	EventReasonDataSeeded          = "DataSeeded"
	EventReasonValidationWarning   = "ValidationWarning"
	EventReasonConnectionFailed    = "ConnectionFailed"
	// EventReasonSeedCredsMissing — a Neo4jDatabase or Neo4jShardedDatabase
	// seeds from cloud storage, and the hosting deployment's server pods lack
	// the credentials Secret (or, sharded, the custom S3 endpoint) the
	// server-side fetch needs, with the deployment not opted in to
	// neo4j.com/auto-inherit-seed-creds. The database waits Pending and
	// carries on once they are there. Emitted when the wait starts.
	EventReasonSeedCredsMissing = "SeedCredsMissing"
	// EventReasonSeedCredsAutoInherited — the operator projected the seed
	// credentials (and endpoint) onto the opted-in hosting deployment and
	// waits for its rolling restart.
	EventReasonSeedCredsAutoInherited = "SeedCredsAutoInherited"
)

// Plugin events
const (
	EventReasonPluginInstalled     = "PluginInstalled"
	EventReasonPluginInstallFailed = "PluginInstallFailed"
	// EventReasonPluginDuplicate is emitted on a Neo4jPlugin when another
	// CR in the same namespace targets the same clusterRef with the same
	// spec.name. The reconciler refuses to install when this would cause
	// two reconcilers to race on the same /plugins directory + the same
	// NEO4J_PLUGINS env value; the older CR (by creationTimestamp) wins
	// and the newer one is marked Failed.
	EventReasonPluginDuplicate = "PluginDuplicate"
	// EventReasonPluginMissing is emitted on a Ready Neo4jPlugin whose target
	// StatefulSet no longer carries what the plugin set (its NEO4J_PLUGINS
	// entry, a variable it sets, a VerifiedDownload init container or its
	// volumes); the plugin goes back to Installing and restores it, which
	// restarts the servers — once the target is Ready with every server
	// available (Waiting until then).
	EventReasonPluginMissing = "PluginMissing"
	// EventReasonPluginInstallDeferred is emitted once when a Neo4jPlugin's
	// install waits (phase Waiting) because its target is not Ready with every
	// server available — Degraded, still forming, rolling or upgrading: the
	// install restarts every server, which a cluster short of one could lose
	// its majority to (operations.md id 141).
	EventReasonPluginInstallDeferred = "PluginInstallDeferred"
	// EventReasonPluginInitContainerFailing is emitted on a VerifiedDownload
	// Neo4jPlugin whose own init container keeps server pods from starting:
	// a Warning, once, when the plugin writes the change that repairs it to a
	// target that is not Ready — every server that is down is failing in that
	// container, and the roll replaces those pods first (operations.md id
	// 152); and a Normal event when it deletes a standalone's pod stuck on an
	// older revision, which an OrderedReady StatefulSet does not replace.
	EventReasonPluginInitContainerFailing = "PluginInitContainerFailing"
	// EventReasonPluginSettingsFailing is emitted on a Neo4jPlugin whose write
	// changes the settings that every server that is down keeps failing with
	// (its neo4j container exits and restarts — a setting Neo4j refuses at
	// startup, typically): a Warning, once, when the plugin writes that change
	// to a target that is not Ready, and the roll replaces those pods first
	// (operations.md id 155); and a Normal event when it deletes a
	// standalone's pod stuck on an older revision, which an OrderedReady
	// StatefulSet does not replace.
	EventReasonPluginSettingsFailing = "PluginSettingsFailing"
)

// Split-brain events
const (
	EventReasonSplitBrainDetected     = "SplitBrainDetected"
	EventReasonSplitBrainRepaired     = "SplitBrainRepaired"
	EventReasonSplitBrainRepairFailed = "SplitBrainRepairFailed"
)

// Aura Fleet Management events
const (
	EventReasonAuraFleetFailed            = "AuraFleetManagementFailed"
	EventReasonAuraFleetPluginPatchFailed = "AuraFleetManagementPluginPatchFailed"
	EventReasonAuraFleetRegistered        = "AuraFleetManagementRegistered"

	// Operator-driven Fleet Manager provisioning (spec.auraFleetManagement.provision).
	EventReasonAuraFleetDeploymentCreated = "AuraFleetDeploymentCreated"
	EventReasonAuraFleetDeploymentAdopted = "AuraFleetDeploymentAdopted"
	EventReasonAuraFleetDeploymentDeleted = "AuraFleetDeploymentDeleted"
	EventReasonAuraFleetTokenProvisioned  = "AuraFleetTokenProvisioned"
	EventReasonAuraFleetTokenRotated      = "AuraFleetTokenRotated"
)

// Storage expansion events
const (
	EventReasonStorageExpansionStarted   = "StorageExpansionStarted"
	EventReasonStorageExpansionCompleted = "StorageExpansionCompleted"
	EventReasonStorageExpansionFailed    = "StorageExpansionFailed"
)

// Storage configuration events
const (
	EventReasonStorageClassNotFound = "StorageClassNotFound"
)

// Sharded database events
const (
	EventReasonShardedDatabaseReady = "ShardedDatabaseReady"
	EventReasonClusterNotReady      = "ClusterNotReady"
	EventReasonClientCreationFailed = "ClientCreationFailed"
	// EventReasonSeedBackupPending / EventReasonSeedProxyStarting — the
	// sharded database waits for its seedBackupRef's first Succeeded run, or
	// for the PVC seed proxy. Emitted when the wait starts, not on every pass.
	EventReasonSeedBackupPending = "SeedBackupPending"
	EventReasonSeedProxyStarting = "SeedProxyStarting"
	// EventReasonSeedBackupResolutionFailed — spec.seedBackupRef cannot be
	// turned into a seed (the Neo4jBackup is missing, or its storage or run
	// cannot seed a sharded database). Phase Failed; emitted when the failure
	// starts or its message changes.
	EventReasonSeedBackupResolutionFailed = "SeedBackupResolutionFailed"
	// EventReasonShardedDatabaseDropped — the destructive replaceExisting +
	// force path dropped the existing sharded database before recreating it
	// from the seed.
	EventReasonShardedDatabaseDropped = "ShardedDatabaseDropped"
)

// User and role management events
const (
	EventReasonUserCreated         = "UserCreated"
	EventReasonUserUpdated         = "UserUpdated"
	EventReasonUserDeleted         = "UserDeleted"
	EventReasonUserReady           = "UserReady"
	EventReasonUserDeletionFailed  = "UserDeletionFailed"
	EventReasonUserSyncFailed      = "UserSyncFailed"
	EventReasonPasswordRotated     = "PasswordRotated"
	EventReasonRolesGranted        = "RolesGranted"
	EventReasonRolesRevoked        = "RolesRevoked"
	EventReasonRolesResolved       = "RolesResolved"
	EventReasonRolePending         = "RolePending"
	EventReasonRoleCreated         = "RoleCreated"
	EventReasonRoleDeleted         = "RoleDeleted"
	EventReasonRoleReady           = "RoleReady"
	EventReasonRoleDeletionFailed  = "RoleDeletionFailed"
	EventReasonRoleSyncFailed      = "RoleSyncFailed"
	EventReasonPrivilegesApplied   = "PrivilegesApplied"
	EventReasonPrivilegesDriftKept = "PrivilegesDriftKept"
	// EventReasonPrivilegeUnknownDatabase — a privilege names a database that
	// does not exist on this cluster and is not an alias for one. Neo4j
	// refuses such a grant, so the operator skips it; without this the role
	// would reconcile to Ready with part of its spec simply absent. The
	// motivating case is a DR
	// cluster: the replica of `foo` is named `foo-replica`, privileges attach
	// to the database rather than to an alias, and a role copied verbatim
	// from the upstream is therefore inert exactly when it is needed.
	EventReasonPrivilegeUnknownDatabase = "PrivilegeNamesUnknownDatabase"
	// EventReasonPrivilegeTargetDropped — a Neo4jDatabase was just dropped while
	// a Neo4jRole on the same cluster still grants on it. Emitted at the moment
	// of the drop, on BOTH objects: on the role because it outlives the
	// deletion and is where the fix is made, on the database because that is
	// where someone watching the delete is looking. Neo4j removes the role's
	// privileges on a dropped database and refuses to re-grant them, so the
	// role's spec now asks for grants that cannot exist; without this the only
	// signal is PrivilegesResolve=False on the role's next reconcile.
	EventReasonPrivilegeTargetDropped = "PrivilegeTargetDropped"
)

// User and role-binding management events
const (
	EventReasonBindingCreated = "BindingCreated"
	EventReasonBindingUpdated = "BindingUpdated"
	EventReasonBindingDeleted = "BindingDeleted"
	EventReasonBindingFailed  = "BindingFailed"
	EventReasonUserNotFound   = "UserNotFound"
)

// Attribute-based access control (Neo4jAuthRule) events.
const (
	EventReasonAuthRuleCreated           = "AuthRuleCreated"
	EventReasonAuthRuleUpdated           = "AuthRuleUpdated"
	EventReasonAuthRuleDeleted           = "AuthRuleDeleted"
	EventReasonAuthRuleFailed            = "AuthRuleFailed"
	EventReasonAuthRuleVersionTooOld     = "AuthRuleVersionTooOld"
	EventReasonOIDCProviderNotConfigured = "OIDCProviderNotConfigured"
)

// Conditions used by the user/role/binding reconcilers.
const (
	ConditionTypeRolesSynced         = "RolesSynced"
	ConditionTypePasswordSynced      = "PasswordSynced"
	ConditionTypePendingDependencies = "PendingDependencies"
	ConditionTypeClusterNotReady     = "ClusterNotReady"
	ConditionTypePrivilegesSynced    = "PrivilegesSynced"
	ConditionTypeUserNotFound        = "UserNotFound"
	// ConditionTypePrivilegesResolve reports whether every database a
	// privilege names actually exists on this cluster. Separate from
	// PrivilegesSynced, which answers a different question: every privilege
	// that CAN be applied may be in sync while those naming a missing
	// database are skipped.
	ConditionTypePrivilegesResolve = "PrivilegesResolve"
	// ConditionTypeRoleNamesResolved records, on a Neo4jUser or
	// Neo4jRoleBinding whose spec.roles names a Neo4jRole by its CR name,
	// which Neo4j role each such name was granted as. Present only then. It
	// is what RolesResolved is announced from, so the event follows a change
	// of the mapping and not every pass.
	ConditionTypeRoleNamesResolved = "RoleNamesResolved"
)

const (
	ConditionReasonRolesSynced       = "RolesMatch"
	ConditionReasonRolesPending      = "RolesPending"
	ConditionReasonPasswordSynced    = "PasswordMatchesSecret"
	ConditionReasonClusterNotReady   = "ClusterNotReady"
	ConditionReasonUserReady         = "UserReady"
	ConditionReasonRoleReady         = "RoleReady"
	ConditionReasonPrivilegesSynced  = "PrivilegesMatch"
	ConditionReasonPrivilegesDrifted = "PrivilegesDrifted"
	// ConditionReasonPrivilegesUnattributed: learn mode holds rows it cannot
	// attribute to a spec statement and so will not revoke (Neo4jRole
	// status.unattributedPrivileges).
	ConditionReasonPrivilegesUnattributed = "UnattributedPrivileges"
	// ConditionReasonRoleCRNamesResolved is RoleNamesResolved's reason.
	ConditionReasonRoleCRNamesResolved = "RoleCRNamesResolved"
)

// Aura orchestration (control-plane) event reasons.
const (
	EventReasonAuraCredentialsValidated = "AuraCredentialsValidated"
	EventReasonAuraCredentialsInvalid   = "AuraCredentialsInvalid"
	EventReasonAuraInstanceCreated      = "AuraInstanceCreated"
	EventReasonAuraInstanceAdopted      = "AuraInstanceAdopted"
	EventReasonAuraInstanceUpdated      = "AuraInstanceUpdated"
	EventReasonAuraInstancePaused       = "AuraInstancePaused"
	EventReasonAuraInstanceResumed      = "AuraInstanceResumed"
	EventReasonAuraInstanceUpgraded     = "AuraInstanceUpgraded"
	EventReasonAuraInstanceDeleted      = "AuraInstanceDeleted"
	EventReasonAuraInstanceOrphaned     = "AuraInstanceOrphaned"
	EventReasonAuraInstanceFailed       = "AuraInstanceFailed"
	EventReasonAuraSnapshotCreated      = "AuraSnapshotCreated"
	EventReasonAuraRestoreStarted       = "AuraRestoreStarted"
	EventReasonAuraRestoreCompleted     = "AuraRestoreCompleted"
	EventReasonAuraRestoreFailed        = "AuraRestoreFailed"
	EventReasonAuraCMKCreated           = "AuraCustomerManagedKeyCreated"
	EventReasonAuraCMKAdopted           = "AuraCustomerManagedKeyAdopted"
	EventReasonAuraCMKReady             = "AuraCustomerManagedKeyReady"
	EventReasonAuraCMKDeleted           = "AuraCustomerManagedKeyDeleted"
	EventReasonAuraCMKOrphaned          = "AuraCustomerManagedKeyOrphaned"
	EventReasonAuraCMKDeleteBlocked     = "AuraCustomerManagedKeyDeleteBlocked"
	EventReasonAuraCMKFailed            = "AuraCustomerManagedKeyFailed"

	// Aura IP filter (v2beta1, beta) event reasons.
	EventReasonAuraIPFilterCreated  = "AuraIPFilterCreated"
	EventReasonAuraIPFilterAdopted  = "AuraIPFilterAdopted"
	EventReasonAuraIPFilterUpdated  = "AuraIPFilterUpdated"
	EventReasonAuraIPFilterReady    = "AuraIPFilterReady"
	EventReasonAuraIPFilterDeleted  = "AuraIPFilterDeleted"
	EventReasonAuraIPFilterOrphaned = "AuraIPFilterOrphaned"
	EventReasonAuraIPFilterFailed   = "AuraIPFilterFailed"

	// Aura database (v2beta1, beta) event reasons.
	EventReasonAuraDatabaseCreated  = "AuraDatabaseCreated"
	EventReasonAuraDatabaseAdopted  = "AuraDatabaseAdopted"
	EventReasonAuraDatabaseReady    = "AuraDatabaseReady"
	EventReasonAuraDatabaseDeleted  = "AuraDatabaseDeleted"
	EventReasonAuraDatabaseOrphaned = "AuraDatabaseOrphaned"
	EventReasonAuraDatabaseFailed   = "AuraDatabaseFailed"

	// Aura per-database backup/restore (v2beta1, beta) event reasons.
	EventReasonAuraDatabaseBackupCreated  = "AuraDatabaseBackupCreated"
	EventReasonAuraDatabaseBackupFailed   = "AuraDatabaseBackupFailed"
	EventReasonAuraDatabaseRestoreStarted = "AuraDatabaseRestoreStarted"
	EventReasonAuraDatabaseRestoreDone    = "AuraDatabaseRestoreCompleted"
	EventReasonAuraDatabaseRestoreFailed  = "AuraDatabaseRestoreFailed"

	// Aura console-RBAC (v2beta1, beta) event reasons.
	EventReasonAuraMemberUpdated  = "AuraMemberRoleUpdated"
	EventReasonAuraMemberReady    = "AuraMemberReady"
	EventReasonAuraMemberRemoved  = "AuraMemberRemoved"
	EventReasonAuraMemberNotFound = "AuraMemberNotFound"
	EventReasonAuraMemberFailed   = "AuraMemberFailed"
	EventReasonAuraInviteCreated  = "AuraInviteCreated"
	EventReasonAuraInviteAdopted  = "AuraInviteAdopted"
	EventReasonAuraInviteReady    = "AuraInviteReady"
	EventReasonAuraInviteDeleted  = "AuraInviteDeleted"
	EventReasonAuraInviteOrphaned = "AuraInviteOrphaned"
	EventReasonAuraInviteFailed   = "AuraInviteFailed"

	// Cross-cluster replication (CCDR) event reasons.
	EventReasonReplicationSourceCaveat = "ReplicationSourceCaveat"
	EventReasonReplicaCreated          = "ReplicaCreated"
	EventReasonReplicaReady            = "ReplicaReady"
	EventReasonReplicaFailed           = "ReplicaFailed"
	EventReasonReplicaDropped          = "ReplicaDropped"
	EventReasonReplicaRetainedPromoted = "ReplicaRetainedAfterPromotion"
	EventReasonReplicaPromotedDetected = "ReplicaPromotionDetected"
	EventReasonReplicaVersionTooOld    = "ReplicaVersionTooOld"
	// EventReasonReplicaObjectStoreEnvMissing — a backup-mode replica's
	// downstream servers have no AWS_REGION (or AWS_DEFAULT_REGION) in any
	// source of their environment, so Neo4j could not read the s3:// seed or
	// pulls (operations.md id 133). The replica waits Pending and retries on
	// its own once the servers have it. Emitted when the wait starts. It was
	// a ReplicaFailed with phase Failed before (id 145).
	EventReasonReplicaObjectStoreEnvMissing = "ReplicaObjectStoreEnvMissing"
	EventReasonPromotionStarted             = "ReplicaPromotionStarted"
	EventReasonPromotionCompleted           = "ReplicaPromotionCompleted"
	EventReasonPromotionFailed              = "ReplicaPromotionFailed"
	EventReasonUpstreamClusterNotFound      = "UpstreamClusterNotFound"
	EventReasonUpstreamClusterNotReady      = "UpstreamClusterNotReady"
	EventReasonUpstreamBackupNotFound       = "UpstreamBackupNotFound"
	EventReasonUpstreamBackupNotReady       = "UpstreamBackupNotReady"
	// EventReasonCCDRProxyUnauthenticated — the CCDR proxy is exposing the
	// tx-shipping port through a load balancer while spec.tls.strictPeerValidation
	// is false. The proxy is a TCP passthrough that authenticates nothing, so the
	// cluster SSL policy's client_auth=REQUIRE is the ONLY access control on that
	// port; with strictPeerValidation=false the policy is trust_all=true, so the
	// port is encrypted but any peer certificate is accepted and anyone who can
	// reach the load balancer can stream the database. A cluster with no spec.tls
	// at all never reaches this event: validateCrossClusterReplication refuses it
	// (phase Failed) before any proxy resource is built. Warned rather than
	// refused: strictPeerValidation=false is a deliberate (if debugging-only)
	// opt-out, and refusing would break clusters that already run this way. The
	// same state is reported on the CrossClusterProxySecure condition.
	EventReasonCCDRProxyUnauthenticated = "CrossClusterProxyUnauthenticated"

	// Composite database event reasons.
	EventReasonCompositeCreated            = "CompositeDatabaseCreated"
	EventReasonCompositeReady              = "CompositeDatabaseReady"
	EventReasonCompositeFailed             = "CompositeDatabaseFailed"
	EventReasonCompositeDropped            = "CompositeDatabaseDropped"
	EventReasonCompositeConstituentAdded   = "CompositeConstituentAdded"
	EventReasonCompositeConstituentMoved   = "CompositeConstituentRetargeted"
	EventReasonCompositeConstituentRemoved = "CompositeConstituentRemoved"
	// EventReasonCompositeNameBlocked — a dotted alias already occupies the
	// composite's namespace, so the composite cannot be created at all. Neo4j
	// accepts `CREATE ALIAS x.y` with no composite `x`, producing an ordinary
	// alias whose name contains a dot; `CREATE COMPOSITE DATABASE x` then
	// fails forever with 42N87 and an error that never mentions ordering.
	EventReasonCompositeNameBlocked = "CompositeDatabaseNameBlocked"
	// ReasonGraphPrivilegeOnComposite is a CONDITION reason, not an event
	// reason: it is set on Neo4jRole's PrivilegesResolve when a privilege
	// names a composite as a GRAPH. Neo4j accepts and shows back such a
	// grant while it does nothing, so the reason is the only signal. Named
	// here because the CLI's `explain` looks it up.
	ReasonGraphPrivilegeOnComposite = "GraphPrivilegeOnComposite"

	// Database alias event reasons.
	EventReasonAliasCreated    = "AliasCreated"
	EventReasonAliasRetargeted = "AliasRetargeted"
	EventReasonAliasReady      = "AliasReady"
	EventReasonAliasDropped    = "AliasDropped"
	EventReasonAliasFailed     = "AliasFailed"
)
