package authctl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// countingDistributor answers Current with current until a Push, and with
// after once pushed; Push answers pushErr.
type countingDistributor struct {
	current, after authv1beta1.Distribution
	currentErr     error
	pushErr        error
	pushes         int
}

func (d *countingDistributor) Push(context.Context, types.NamespacedName, string) error {
	d.pushes++
	return d.pushErr
}

func (d *countingDistributor) Current(context.Context, types.NamespacedName, string) (authv1beta1.Distribution, error) {
	if d.pushes > 0 {
		return d.after, nil
	}
	return d.current, d.currentErr
}

func (*countingDistributor) Lookup(context.Context, types.NamespacedName, string) (string, error) {
	return "", nil
}

func (*countingDistributor) Delete(context.Context, types.NamespacedName, string) error { return nil }

func TestDistribute(t *testing.T) {
	pushedAt := &metav1.Time{Time: time.Unix(1000, 0)}
	prev := &authv1beta1.Distribution{Servers: 3, Current: 1, LastPushTime: pushedAt}
	tests := []struct {
		name       string
		d          *countingDistributor
		token      string
		wantDist   *authv1beta1.Distribution
		wantStatus metav1.ConditionStatus
		wantReason string
		wantAgain  time.Duration
		wantErr    bool
		wantPushes int
	}{
		{name: "no token", d: &countingDistributor{}, wantDist: prev},
		{
			name:  "every server current",
			d:     &countingDistributor{current: authv1beta1.Distribution{Servers: 3, Current: 3}},
			token: "jwt", wantDist: &authv1beta1.Distribution{Servers: 3, Current: 3, LastPushTime: pushedAt},
			wantStatus: metav1.ConditionTrue, wantReason: ReasonAllServersCurrent,
		},
		{
			name: "a server behind is pushed to and counted again",
			d: &countingDistributor{current: authv1beta1.Distribution{Servers: 3, Current: 2},
				after: authv1beta1.Distribution{Servers: 3, Current: 2}},
			token: "jwt", wantDist: &authv1beta1.Distribution{Servers: 3, Current: 2, LastPushTime: pushedAt},
			wantStatus: metav1.ConditionFalse, wantReason: ReasonServersBehind, wantAgain: distributionRecheck, wantPushes: 1,
		},
		{
			name:  "unreachable keeps the distribution",
			d:     &countingDistributor{currentErr: fmt.Errorf("%w: down", ErrUnreachable)},
			token: "jwt", wantDist: prev,
			wantStatus: metav1.ConditionFalse, wantReason: ReasonUnreachable, wantAgain: distributionRecheck,
		},
		{
			name: "a refused push is an error",
			d: &countingDistributor{current: authv1beta1.Distribution{Servers: 3, Current: 2},
				pushErr: ErrStaleJWT},
			token: "jwt", wantDist: prev,
			wantStatus: metav1.ConditionUnknown, wantReason: ReasonUnobserved, wantErr: true, wantPushes: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dist, cond, again, err := distribute(t.Context(), tt.d, types.NamespacedName{Name: "op"}, tt.token, prev)
			require.Equal(t, tt.wantErr, err != nil, "err %v", err)
			require.Equal(t, tt.wantDist, dist)
			require.Equal(t, tt.wantAgain, again)
			require.Equal(t, tt.wantPushes, tt.d.pushes)
			if tt.wantReason == "" {
				require.Empty(t, cond.Type)
				return
			}
			require.Equal(t, ConditionDistributed, cond.Type)
			require.Equal(t, tt.wantStatus, cond.Status)
			require.Equal(t, tt.wantReason, cond.Reason)
		})
	}

	t.Run("nil distributor", func(t *testing.T) {
		dist, cond, again, err := distribute(t.Context(), nil, types.NamespacedName{Name: "op"}, "jwt", prev)
		require.NoError(t, err)
		require.Same(t, prev, dist)
		require.Equal(t, ConditionDistributed, cond.Type)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, ReasonNoSystemConnection, cond.Reason)
		require.Zero(t, again)

		_, cond, _, err = distribute(t.Context(), nil, types.NamespacedName{Name: "op"}, "", prev)
		require.NoError(t, err)
		require.Empty(t, cond.Type, "an account with no JWT has nothing to distribute")
	})
}

func TestUserReconciler_NoSystemConnection(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, authv1beta1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	u := &authv1beta1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "u"},
		Spec: authv1beta1.NatsUserSpec{AccountRef: authv1beta1.AccountReference{
			Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: "missing"},
		}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(u).WithStatusSubresource(u).Build()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(u)}
	distributed := func(r *UserReconciler) *metav1.Condition {
		_, err := r.Reconcile(t.Context(), req)
		require.NoError(t, err)
		var got authv1beta1.NatsUser
		require.NoError(t, c.Get(t.Context(), req.NamespacedName, &got))
		return meta.FindStatusCondition(got.Status.Conditions, ConditionDistributed)
	}

	cond := distributed(&UserReconciler{Client: c})
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, ReasonNoSystemConnection, cond.Reason)

	require.Nil(t, distributed(&UserReconciler{Client: c, Sessions: ConnSessions{}}))
}

func TestRecordDistribution(t *testing.T) {
	current := metav1.Condition{Type: ConditionDistributed, Status: metav1.ConditionTrue, Reason: ReasonAllServersCurrent}
	behind := metav1.Condition{Type: ConditionDistributed, Status: metav1.ConditionFalse, Reason: ReasonServersBehind}
	readyTrue := metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned}
	readyFalse := metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonPending}
	tests := []struct {
		name        string
		before      []metav1.Condition
		cond        metav1.Condition
		wantReady   string
		wantDistrib string
	}{
		{name: "no Type sets nothing", before: []metav1.Condition{readyTrue}, wantReady: ReasonSigned},
		{name: "every server current makes Ready Distributed", before: []metav1.Condition{readyTrue}, cond: current,
			wantReady: ReasonDistributed, wantDistrib: ReasonAllServersCurrent},
		{name: "a Ready False stays", before: []metav1.Condition{readyFalse}, cond: current,
			wantReady: ReasonPending, wantDistrib: ReasonAllServersCurrent},
		{name: "servers behind leave Ready", before: []metav1.Condition{readyTrue}, cond: behind,
			wantReady: ReasonSigned, wantDistrib: ReasonServersBehind},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds := append([]metav1.Condition(nil), tt.before...)
			recordDistribution(&conds, 7, tt.cond)
			var ready, distrib string
			for _, c := range conds {
				switch c.Type {
				case ConditionReady:
					ready = c.Reason
				case ConditionDistributed:
					distrib = c.Reason
					require.EqualValues(t, 7, c.ObservedGeneration)
				}
			}
			require.Equal(t, tt.wantReady, ready)
			require.Equal(t, tt.wantDistrib, distrib)
		})
	}
}
