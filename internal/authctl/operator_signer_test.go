package authctl

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

func TestSetSignerCondition(t *testing.T) {
	const key = "OSIGNER"
	unreachable := fmt.Errorf("%w: no server answered STATSZ", ErrUnreachable)
	three := authv1beta1.Distribution{Servers: 3}
	tests := []struct {
		name       string
		d          Distributor
		wantStatus metav1.ConditionStatus
		wantReason string
		wantMsg    string
		wantAgain  time.Duration
	}{
		{"NoSystemConnection", nil, metav1.ConditionUnknown, ReasonNoSystemConnection,
			"the auth controller runs without --system-connection: no server is asked which keys it trusts", 0},
		{"Unreachable", &countingDistributor{distrustErr: unreachable}, metav1.ConditionUnknown, ReasonUnreachable,
			unreachable.Error(), distributionRecheck},
		{"Unobserved", &countingDistributor{distrustErr: fmt.Errorf("publish: closed")}, metav1.ConditionUnknown, ReasonUnobserved,
			"publish: closed", distributionRecheck},
		{"NoServerDistrusts", &countingDistributor{current: three}, metav1.ConditionFalse, ReasonSignerTrusted,
			"0 of 3 servers do not list signing key OSIGNER in their NATS operator JWT", 0},
		{"SomeServersDistrust", &countingDistributor{current: three, distrusting: 2, unknown: 1}, metav1.ConditionTrue, ReasonUntrustedSigner,
			"2 of 3 servers do not list signing key OSIGNER in their NATS operator JWT", distributionRecheck},
		{"TrustUnknown", &countingDistributor{current: three, unknown: 3}, metav1.ConditionUnknown, ReasonTrustUnknown,
			"3 of 3 servers report no NATS operator JWT on VARZ, or do not answer it, so whether they list signing key OSIGNER is unknown", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &authv1beta1.NatsOperator{}
			op.Namespace, op.Name, op.Generation = "ns", "op", 2
			require.Equal(t, tt.wantAgain, setSignerCondition(t.Context(), tt.d, op, key))
			cond := meta.FindStatusCondition(op.Status.Conditions, ConditionSigningKeyUntrusted)
			require.NotNil(t, cond)
			require.Equal(t, [3]string{string(tt.wantStatus), tt.wantReason, tt.wantMsg},
				[3]string{string(cond.Status), cond.Reason, cond.Message})
			require.EqualValues(t, 2, cond.ObservedGeneration)
		})
	}
}
