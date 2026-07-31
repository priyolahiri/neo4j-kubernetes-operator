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
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
)

// adminPasswordKey is the key the operator reads the admin password from.
const adminPasswordKey = "password"

// ValidateAdminSecretPassword rejects an admin password the Neo4j Docker
// entrypoint cannot consume, reading it from the referenced Secret.
//
// WHY THIS EXISTS — a password beginning with "-" bricks the container:
//
// Both the cluster and standalone paths hand the password to the image as
// NEO4J_AUTH="<user>/<password>". The upstream entrypoint then runs
// `neo4j-admin dbms set-initial-password <password>`, and neo4j-admin's picocli
// parser treats a leading "-" as an OPTION rather than the positional argument.
// The positional parameter never binds, so the container dies at startup with:
//
//	Missing required parameter: '<password>'
//	exit code 64            (picocli's usage-error code)
//
// and then CrashLoopBackOffs forever. Nothing in that output mentions the
// password's shape, and because the bad value is baked into the StatefulSet it
// never self-heals — it presents as "Neo4j won't start", which has twice been
// misdiagnosed as a Neo4j version regression.
//
// The mis-parse is in the UPSTREAM entrypoint, so the operator cannot fix it at
// runtime. Rejecting the CR up front turns an inscrutable crash-loop into an
// actionable validation error, which is the whole point of validating inline
// (project invariant 1 — no admission webhooks).
//
// A missing or unreadable Secret is deliberately NOT an error here: the Secret
// may legitimately be created after the CR, and the env var is projected with
// Optional:false so Kubernetes already blocks container creation with a clear
// message until it exists. This validator only judges a password it can see.
func ValidateAdminSecretPassword(
	ctx context.Context, c client.Client, namespace, secretName string, path *field.Path,
) field.ErrorList {
	var allErrs field.ErrorList
	if c == nil || secretName == "" {
		return allErrs
	}

	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, sec); err != nil {
		// Not found / not yet readable — see the doc comment.
		if apierrors.IsNotFound(err) {
			return allErrs
		}
		return allErrs
	}

	raw, ok := sec.Data[adminPasswordKey]
	if !ok || len(raw) == 0 {
		return allErrs
	}
	// The entrypoint splits NEO4J_AUTH on "/", so the password is used verbatim.
	// Trim nothing: a password with leading whitespace then a dash would still
	// reach neo4j-admin with the dash first once the shell has word-split it.
	pw := string(raw)

	if strings.HasPrefix(strings.TrimSpace(pw), "-") {
		allErrs = append(allErrs, field.Invalid(
			path, "<redacted>",
			"the admin password in Secret \""+secretName+"\" (key \""+adminPasswordKey+"\") must not begin with \"-\": "+
				"the Neo4j entrypoint passes it to `neo4j-admin dbms set-initial-password`, whose parser reads a "+
				"leading dash as a command-line option, so the container fails to start with "+
				"\"Missing required parameter: '<password>'\" (exit 64) and crash-loops. Choose a password that "+
				"starts with any other character",
		))
	}

	return allErrs
}
