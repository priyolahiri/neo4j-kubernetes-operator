package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
)

// TestStandaloneCreateConfigMap_DedupesQueryLogKeys guards NEO3-16: the
// single-node example enables monitoring (which emits db.logs.query.enabled /
// db.logs.query.threshold) AND sets those same keys in spec.config. The rendered
// /conf/neo4j.conf must declare each key only once, or CalVer Neo4j (2025.x+)
// refuses to start with "<key> declared multiple times" → CrashLoopBackOff.
func TestStandaloneCreateConfigMap_DedupesQueryLogKeys(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-neo4j", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Image:      neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.04-enterprise"},
			Monitoring: &neo4jv1beta1.MonitoringSpec{Enabled: true, SlowQueryThreshold: "5s"},
			Config: map[string]string{
				"db.logs.query.enabled":   "true",
				"db.logs.query.threshold": "1s",
			},
		},
	}

	conf := r.createConfigMap(standalone).Data["neo4j.conf"]

	assert.Equal(t, 1, strings.Count(conf, "db.logs.query.threshold="),
		"db.logs.query.threshold must be declared exactly once")
	assert.Equal(t, 1, strings.Count(conf, "db.logs.query.enabled="),
		"db.logs.query.enabled must be declared exactly once")
	// User spec.config is appended last, so its value wins after de-duplication.
	assert.Contains(t, conf, "db.logs.query.threshold=1s")
}
