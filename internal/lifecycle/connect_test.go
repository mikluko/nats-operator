package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

func TestReleased(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status metav1.ConditionStatus
		reason string
		want   bool
	}{
		{"connection not found", metav1.ConditionFalse, ReasonConnectionNotFound, true},
		{"reference not permitted", metav1.ConditionFalse, grant.ReasonReferenceNotPermitted, true},
		{"connection failed", metav1.ConditionFalse, ReasonConnectionFailed, false},
		{"sync failed", metav1.ConditionFalse, ReasonSyncFailed, false},
		{"ready", metav1.ConditionTrue, ReasonSynced, false},
		{"no condition", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var st jetstreamv1beta1.SyncStatus
			if tc.reason != "" {
				st.Conditions = []metav1.Condition{{Type: ConditionReady, Status: tc.status, Reason: tc.reason}}
			}
			require.Equal(t, tc.want, Released(&st))
		})
	}
}
