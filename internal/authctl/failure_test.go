package authctl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func TestReconcileError(t *testing.T) {
	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	meta2 := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: "ns", Name: "x", Generation: 2, UID: types.UID("x")}
	}
	readyTrue := []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned, ObservedGeneration: 1, LastTransitionTime: metav1.Now()}}
	opRef := natsv1beta1.ObjectReference{Name: "op"}
	tests := []struct {
		name string
		obj  client.Object
		rec  func(client.Client) reconcile.Reconciler
		read func(client.Object) ([]metav1.Condition, int64)
	}{
		{
			"NatsOperator",
			&authv1beta1.NatsOperator{ObjectMeta: meta2(), Status: authv1beta1.NatsOperatorStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &OperatorReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*authv1beta1.NatsOperator).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
		{
			"NatsSystemAccount",
			&authv1beta1.NatsSystemAccount{ObjectMeta: meta2(), Spec: authv1beta1.NatsSystemAccountSpec{OperatorRef: opRef}, Status: authv1beta1.NatsSystemAccountStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &SystemAccountReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*authv1beta1.NatsSystemAccount).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
		{
			"NatsAccount",
			&authv1beta1.NatsAccount{ObjectMeta: meta2(), Spec: authv1beta1.NatsAccountSpec{OperatorRef: opRef}, Status: authv1beta1.NatsAccountStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &AccountReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*authv1beta1.NatsAccount).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
		{
			"NatsUser",
			&authv1beta1.NatsUser{ObjectMeta: meta2(), Spec: authv1beta1.NatsUserSpec{AccountRef: authv1beta1.AccountReference{Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: "acc"}}}, Status: authv1beta1.NatsUserStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &UserReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*authv1beta1.NatsUser).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
		{
			"NatsOperatorTrust",
			&natsv1beta1.NatsOperatorTrust{ObjectMeta: meta2(), Spec: natsv1beta1.NatsOperatorTrustSpec{OperatorRef: &natsv1beta1.ObjectReference{Name: "op"}}, Status: natsv1beta1.NatsOperatorTrustStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &OperatorTrustReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*natsv1beta1.NatsOperatorTrust).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
		{
			"NatsAccountTrust",
			&natsv1beta1.NatsAccountTrust{ObjectMeta: meta2(), Spec: natsv1beta1.NatsAccountTrustSpec{AccountRef: &natsv1beta1.ObjectReference{Name: "acc"}}, Status: natsv1beta1.NatsAccountTrustStatus{ObservedGeneration: 1, Conditions: readyTrue}},
			func(c client.Client) reconcile.Reconciler { return &AccountTrustReconciler{Client: c} },
			func(o client.Object) ([]metav1.Condition, int64) {
				st := o.(*natsv1beta1.NatsAccountTrust).Status
				return st.Conditions, st.ObservedGeneration
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			self := client.ObjectKeyFromObject(tt.obj)
			failing := errors.New("apiserver unavailable")
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tt.obj).WithStatusSubresource(tt.obj).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if key != self {
							return failing
						}
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						return failing
					},
				}).Build()
			_, err := tt.rec(c).Reconcile(t.Context(), reconcile.Request{NamespacedName: self})
			require.ErrorIs(t, err, failing)

			got, ok := tt.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(t.Context(), self, got))
			conds, observed := tt.read(got)
			require.EqualValues(t, 1, observed, "observedGeneration stays at the generation last reconciled in full")
			ready := meta.FindStatusCondition(conds, ConditionReady)
			require.NotNil(t, ready)
			require.Equal(t, metav1.ConditionFalse, ready.Status)
			require.Equal(t, ReasonReconcileError, ready.Reason)
			require.Contains(t, ready.Message, failing.Error())
			require.EqualValues(t, 2, ready.ObservedGeneration)
		})
	}
}

func TestObserve(t *testing.T) {
	failing := errors.New("boom")
	conflict := fmt.Errorf("update Secret: %w", apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, "x", failing))
	tests := []struct {
		name       string
		have       []metav1.Condition
		err        error
		wantReason string
		wantGen    int64
	}{
		{"success", nil, nil, "", 2},
		{"error over True", []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned, ObservedGeneration: 2}}, failing, ReasonReconcileError, 1},
		{"error keeps a False of this generation", []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonRecovering, ObservedGeneration: 2}}, failing, ReasonRecovering, 1},
		{"error over a False of an older generation", []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonPending, ObservedGeneration: 1}}, failing, ReasonReconcileError, 1},
		{"conflict", []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned, ObservedGeneration: 2}}, conflict, ReasonSigned, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds := tt.have
			observed := int64(1)
			observe(&conds, &observed, 2, tt.err)
			require.Equal(t, tt.wantGen, observed)
			ready := meta.FindStatusCondition(conds, ConditionReady)
			if tt.wantReason == "" {
				require.Nil(t, ready)
				return
			}
			require.Equal(t, tt.wantReason, ready.Reason)
		})
	}
}
