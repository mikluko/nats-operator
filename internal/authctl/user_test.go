package authctl

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func TestUserReconciler_CredsWithoutAccount(t *testing.T) {
	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	for _, tc := range []struct {
		name      string
		accountNS string
		deleted   bool
	}{
		{"refused by no grant", "nats-system", true},
		{"account missing", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := &authv1beta1.NatsUser{
				ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "api", UID: types.UID("api")},
				Spec: authv1beta1.NatsUserSpec{AccountRef: authv1beta1.AccountReference{
					Kind:            authv1beta1.AccountKindAccount,
					ObjectReference: natsv1beta1.ObjectReference{Namespace: tc.accountNS, Name: "payments"},
				}},
			}
			creds := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace: "orders", Name: "api-creds",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(u, authv1beta1.GroupVersion.WithKind("NatsUser"))},
			}}
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(u, creds).WithStatusSubresource(u).Build()
			_, err := (&UserReconciler{Client: c}).Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(u)})
			require.NoError(t, err)
			err = c.Get(t.Context(), client.ObjectKeyFromObject(creds), &corev1.Secret{})
			if tc.deleted {
				require.True(t, apierrors.IsNotFound(err), "got %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
