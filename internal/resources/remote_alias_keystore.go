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

import (
	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const (
	// RemoteAliasKeystoreVolume is the volume carrying the PKCS12 keystore.
	RemoteAliasKeystoreVolume = "remote-alias-keystore"
	// RemoteAliasKeystoreMountPath is where it is mounted, read-only.
	RemoteAliasKeystoreMountPath = "/keystore"
	// RemoteAliasKeystoreFileKey is the Secret key holding the keystore, and
	// also its filename inside the mount.
	RemoteAliasKeystoreFileKey = "keystore.p12"
	// RemoteAliasKeystorePasswordKey is the Secret key holding its password.
	RemoteAliasKeystorePasswordKey = "password"

	// The three neo4j.conf settings Neo4j reads for remote-alias credential
	// encryption. Named here so the env-var builder and the validator's error
	// message cannot drift apart.
	settingKeystorePath     = "dbms.security.keystore.path"
	settingKeystorePassword = "dbms.security.keystore.password"
	settingKeystoreKeyName  = "dbms.security.key.name"
)

// RemoteAliasKeystorePath is the absolute path Neo4j is told to read.
const RemoteAliasKeystorePath = RemoteAliasKeystoreMountPath + "/" + RemoteAliasKeystoreFileKey

// BuildRemoteAliasKeystoreEnvVars returns the Neo4j config env vars that point
// the server at the keystore.
//
// Delivered as environment variables rather than ConfigMap lines for one
// reason: the password. The operator's ConfigMap is not a Secret, and Neo4j's
// documented alternative — command expansion — would require starting the
// server with --expand-commands, which this operator does not do. The Docker
// image's NEO4J_<setting> convention gets the same result with machinery the
// operator already uses for the LDAP system-account credentials, and the
// server redacts the value in SHOW SETTINGS.
//
// All three settings go through Neo4jSettingEnvVarName so the escaping rule
// lives in exactly one place.
func BuildRemoteAliasKeystoreEnvVars(ks *neo4jv1beta1.RemoteAliasKeystoreSpec) []corev1.EnvVar {
	if ks == nil || ks.SecretRef == "" {
		return nil
	}
	return []corev1.EnvVar{
		{
			Name:  Neo4jSettingEnvVarName(settingKeystorePath),
			Value: RemoteAliasKeystorePath,
		},
		{
			Name:  Neo4jSettingEnvVarName(settingKeystoreKeyName),
			Value: ks.KeyName,
		},
		{
			Name: Neo4jSettingEnvVarName(settingKeystorePassword),
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ks.SecretRef},
					Key:                  RemoteAliasKeystorePasswordKey,
				},
			},
		},
	}
}

// BuildRemoteAliasKeystoreVolume returns the volume carrying the keystore file,
// or nil when none is configured.
//
// Every server mounts the SAME Secret. That is not incidental — Neo4j requires
// the keystore file to be identical across a cluster, because each server has
// to decrypt credentials any of them may have written. A keystore generated
// per pod would give each server its own key and leave every alias readable by
// exactly one server.
//
// Only the keystore file is projected. The password lives in the same Secret
// but reaches the server as an env var instead, so it is never written to disk
// in the container.
func BuildRemoteAliasKeystoreVolume(ks *neo4jv1beta1.RemoteAliasKeystoreSpec) *corev1.Volume {
	if ks == nil || ks.SecretRef == "" {
		return nil
	}
	mode := int32(0o440)
	return &corev1.Volume{
		Name: RemoteAliasKeystoreVolume,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: ks.SecretRef,
				Items: []corev1.KeyToPath{
					{Key: RemoteAliasKeystoreFileKey, Path: RemoteAliasKeystoreFileKey},
				},
				DefaultMode: &mode,
			},
		},
	}
}

// BuildRemoteAliasKeystoreVolumeMount returns the read-only mount for the
// keystore volume, or nil when none is configured.
func BuildRemoteAliasKeystoreVolumeMount(ks *neo4jv1beta1.RemoteAliasKeystoreSpec) *corev1.VolumeMount {
	if ks == nil || ks.SecretRef == "" {
		return nil
	}
	return &corev1.VolumeMount{
		Name:      RemoteAliasKeystoreVolume,
		MountPath: RemoteAliasKeystoreMountPath,
		ReadOnly:  true,
	}
}
