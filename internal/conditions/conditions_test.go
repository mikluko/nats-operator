package conditions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSet(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ready := func(s metav1.ConditionStatus, reason string, gen int64) metav1.Condition {
		return metav1.Condition{Type: "Ready", Status: s, Reason: reason, ObservedGeneration: gen}
	}
	was := func(s metav1.ConditionStatus, reason string, gen int64) []metav1.Condition {
		c := ready(s, reason, gen)
		c.LastTransitionTime = then
		return []metav1.Condition{c}
	}
	tests := []struct {
		name       string
		have       []metav1.Condition
		gen        int64
		set        metav1.Condition
		changed    bool
		transition bool
	}{
		{"added", nil, 3, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Up", ObservedGeneration: 9}, true, true},
		{"unchanged", was(metav1.ConditionTrue, "Up", 3), 3, ready(metav1.ConditionTrue, "Up", 0), false, false},
		{"new generation", was(metav1.ConditionTrue, "Up", 2), 3, ready(metav1.ConditionTrue, "Up", 0), true, false},
		{"new reason", was(metav1.ConditionTrue, "Up", 3), 3, ready(metav1.ConditionTrue, "Still", 0), true, false},
		{"new status", was(metav1.ConditionTrue, "Up", 3), 3, ready(metav1.ConditionFalse, "Down", 0), true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds := tt.have
			require.Equal(t, tt.changed, Set(&conds, tt.gen, tt.set))
			require.Len(t, conds, 1)
			got := conds[0]
			require.Equal(t, tt.gen, got.ObservedGeneration)
			require.Equal(t, tt.set.Status, got.Status)
			require.Equal(t, tt.set.Reason, got.Reason)
			require.Equal(t, tt.transition, !got.LastTransitionTime.Equal(&then))
		})
	}
}

func TestStatus(t *testing.T) {
	require.Equal(t, metav1.ConditionTrue, Status(true))
	require.Equal(t, metav1.ConditionFalse, Status(false))
}
