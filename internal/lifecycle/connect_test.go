package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/mikluko/nats-operator/internal/grant"
)

func TestNoConnReleased(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    *NoConn
		want bool
	}{
		{"connection not found", &NoConn{Reason: ReasonConnectionNotFound}, true},
		{"reference not permitted", &NoConn{Reason: grant.ReasonReferenceNotPermitted}, true},
		{"connection failed", &NoConn{Reason: ReasonConnectionFailed}, false},
		{"connected", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.n.Released())
		})
	}
}

func TestNoConnApply(t *testing.T) {
	denied := metav1.Condition{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionFalse, Reason: grant.ReasonReferenceNotPermitted, Message: "no grant"}
	for _, tc := range []struct {
		name string
		n    NoConn
		want []metav1.Condition
	}{
		{
			name: "connection not found",
			n:    NoConn{Reason: ReasonConnectionNotFound, Message: "gone"},
			want: []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonConnectionNotFound, Message: "gone", ObservedGeneration: 3}},
		},
		{
			name: "denied",
			n:    NoConn{Reason: grant.ReasonReferenceNotPermitted, Message: "no grant", Denied: &denied},
			want: []metav1.Condition{
				{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionFalse, Reason: grant.ReasonReferenceNotPermitted, Message: "no grant", ObservedGeneration: 3},
				{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: grant.ReasonReferenceNotPermitted, Message: "no grant", ObservedGeneration: 3},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var conds []metav1.Condition
			tc.n.Apply(&conds, 3)
			for i := range conds {
				conds[i].LastTransitionTime = metav1.Time{}
			}
			require.Equal(t, tc.want, conds)
			require.Zero(t, denied.ObservedGeneration)
		})
	}
}
