package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// restorePending reads the partition annotation as an int32: a value out of
// that range is no partition, as one that does not parse is not, and never
// wraps into one (CodeQL go/incorrect-integer-conversion).
func TestRestorePending_ReadsThePartitionAsAnInt32(t *testing.T) {
	for raw, want := range map[string]int64{"0": 0, "3": 3, "2147483647": 2147483647, "2147483648": -1, "4294967299": -1, "-1": -1, "x": -1} {
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{recreateRestorePartitionAnnotation: raw}}}
		p, pending := restorePending(sts)
		assert.True(t, pending, raw)
		if want < 0 {
			assert.Nil(t, p, raw)
			continue
		}
		if assert.NotNil(t, p, raw) {
			assert.Equal(t, int32(want), *p, raw)
		}
	}
	_, pending := restorePending(&appsv1.StatefulSet{})
	assert.False(t, pending)
}
