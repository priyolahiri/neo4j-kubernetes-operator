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

// The server default Cypher language (spec.serverDefaultCypherLanguage) —
// design: docs/design/cypher-language-defaulting.md §5.
//
// Two facts shape everything here, both measured:
//
//   - db.query.default_language does not exist on the 5.26 LTS. With strict
//     config validation, writing it there stops the server from starting. On
//     the LTS this is a validation rule, never a value to write.
//   - Neo4j fixes a database's language when the database is created and never
//     re-reads the setting (2026.06.0, both directions). So the setting is
//     "the language new databases get", and changing it moves nothing that
//     exists.
//
// An unset spec is resolved ONCE and recorded in status.effectiveCypherLanguage
// (the stamp), so an operator upgrade can never change it under a running
// deployment: re-deriving it every reconcile is exactly what would.

const (
	// CypherLanguage5 and CypherLanguage25 are the values of
	// db.query.default_language and of spec.serverDefaultCypherLanguage.
	CypherLanguage5  = "CYPHER_5"
	CypherLanguage25 = "CYPHER_25"

	// ServerCypherLanguageKey is the neo4j.conf setting.
	ServerCypherLanguageKey = "db.query.default_language"
)

// ServerCypherLanguageInputs is what the stamp depends on.
type ServerCypherLanguageInputs struct {
	// Spec is spec.serverDefaultCypherLanguage.
	Spec string
	// Legacy is db.query.default_language set directly in spec.config (or, on
	// a cluster, propertySharding.config) — the way to set it before this
	// field existed. It is the user's explicit choice and is emitted by the
	// config path it came from, not by this one.
	Legacy string
	// Stamped is status.effectiveCypherLanguage.
	Stamped string
	// CalVer is whether the image is a CalVer (2025.x+) release.
	CalVer bool
	// Exists is whether the deployment's StatefulSet already exists — the
	// discriminator between a deployment this operator is creating now and
	// one that predates the field (sts.UID != "", never ResourceVersion).
	Exists bool
	// Running is db.query.default_language in the operator's current
	// ConfigMap, "" when absent or when there is no ConfigMap.
	Running string
}

// StampServerCypherLanguage resolves the value to record in
// status.effectiveCypherLanguage (design §5.4):
//
//	spec / legacy set          → that value
//	already stamped            → the stamp (never re-derived)
//	new deployment             → CYPHER_25 on CalVer, CYPHER_5 on the LTS
//	existing, never stamped    → what it already runs: the ConfigMap's value,
//	                             else the server default, CYPHER_5
//
// The last row is what makes an operator upgrade a no-op — including a
// sharding cluster, whose ConfigMap carries CYPHER_25 from when sharding set
// it — and it also covers a stamp lost to an etcd restore, or a reconcile that
// created the ConfigMap but crashed before its status write.
func StampServerCypherLanguage(in ServerCypherLanguageInputs) string {
	switch {
	case in.Spec != "":
		return in.Spec
	case in.Legacy != "":
		return in.Legacy
	case in.Stamped != "":
		return in.Stamped
	case !in.Exists && in.CalVer:
		return CypherLanguage25
	case !in.Exists:
		return CypherLanguage5
	case in.Running == CypherLanguage25 || in.Running == CypherLanguage5:
		return in.Running
	default:
		return CypherLanguage5
	}
}

// EmitServerCypherLanguage returns the db.query.default_language value the
// operator writes into neo4j.conf, or "" to write nothing. It reads the stamp
// the controller recorded, so config builders stay pure.
//
//   - Never on the LTS: the setting does not exist there.
//   - Never when the user set the key directly (legacy): their config path
//     writes it, and a second line would be a conflict to dedupe.
//   - An explicit spec value is written as given, CYPHER_5 included.
//   - An unset spec writes only CYPHER_25. A CYPHER_5 stamp is the CalVer
//     server's own default, and writing nothing keeps an existing deployment's
//     neo4j.conf byte-identical — a changed file would restart the servers
//     for no change in meaning.
func EmitServerCypherLanguage(spec, legacy, stamped string, calver bool) string {
	switch {
	case !calver, legacy != "":
		return ""
	case spec != "":
		return spec
	case stamped == CypherLanguage25:
		return CypherLanguage25
	default:
		return ""
	}
}

// LegacyServerCypherLanguage returns db.query.default_language from the given
// config maps (spec.config first), or "".
func LegacyServerCypherLanguage(configs ...map[string]string) string {
	for _, c := range configs {
		if v := c[ServerCypherLanguageKey]; v != "" {
			return v
		}
	}
	return ""
}

// ServerCypherLanguageForCluster is EmitServerCypherLanguage for a cluster CR.
func ServerCypherLanguageForCluster(spec, stamped, imageTag string, configs ...map[string]string) string {
	return EmitServerCypherLanguage(spec, LegacyServerCypherLanguage(configs...), stamped, isCalverImage(imageTag))
}

// IsCalverImage reports whether an image tag is a CalVer (2025.x+) release.
func IsCalverImage(tag string) bool { return isCalverImage(tag) }
