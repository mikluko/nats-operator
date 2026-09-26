package natscluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestRecordGateBlocked pins that GateBlocked is recorded once, as the
// rollout's gate turns Progressing to GateBlocked.
func TestRecordGateBlocked(t *testing.T) {
	blocked := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonGateBlocked,
		Message: "restarting demo-2 (1 of 3); waiting for Settled for 10m0s: stream ORDERS lagging on demo-1"}
	rolling := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonRollingRestart}
	tests := []struct {
		name   string
		before []metav1.Condition
		now    metav1.Condition
		want   []string
	}{
		{name: "gate turns blocked", before: []metav1.Condition{rolling}, now: blocked,
			want: []string{"Warning GateBlocked " + blocked.Message}},
		{name: "first status is blocked", now: blocked, want: []string{"Warning GateBlocked " + blocked.Message}},
		{name: "still blocked", before: []metav1.Condition{blocked}, now: blocked},
		{name: "rolling", before: []metav1.Condition{rolling}, now: rolling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := events.NewFakeRecorder(10)
			nc := &clusterv1beta1.NatsCluster{Status: clusterv1beta1.NatsClusterStatus{Conditions: []metav1.Condition{tt.now}}}
			recordGateBlocked(rec, nc, tt.before)
			require.Equal(t, tt.want, recorded(rec))
		})
	}
}
