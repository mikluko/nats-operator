package authctl

import (
	"fmt"
	"testing"

	"github.com/nats-io/nkeys"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func TestSystemAccountReconciler_Push(t *testing.T) {
	pair := func(prefix nkeys.PrefixByte) nkeys.KeyPair {
		kp, err := nkeys.CreatePair(prefix)
		require.NoError(t, err)
		return kp
	}
	id, sk := pair(nkeys.PrefixByteAccount), pair(nkeys.PrefixByteAccount)
	opKeys := jwtplane.Keys{Identity: pair(nkeys.PrefixByteOperator), Signing: []jwtplane.SigningKey{{Name: "s", Pair: pair(nkeys.PrefixByteOperator)}}}
	sysPub, err := id.PublicKey()
	require.NoError(t, err)
	sysJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: jwtplane.Keys{Identity: id, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}}}, opKeys)
	require.NoError(t, err)
	idSeed, err := id.Seed()
	require.NoError(t, err)
	skSeed, err := sk.Seed()
	require.NoError(t, err)
	unreachable := fmt.Errorf("%w: down", ErrUnreachable)
	tests := []struct {
		name        string
		d           *countingDistributor
		unrecovered bool
		wantPushes  int
		wantTime    bool
		wantEvents  []string
	}{
		{"reached", &countingDistributor{after: authv1beta1.Distribution{Servers: 1, Current: 1}}, false, 1, true,
			[]string{"Normal JWTPushed system account JWT of " + sysPub + " pushed"}},
		{"unreachable", &countingDistributor{pushErr: unreachable, currentErr: unreachable}, false, 1, false, nil},
		{"revocations unrecovered", &countingDistributor{}, true, 0, false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme(t)
			require.NoError(t, natsv1beta1.AddToScheme(s))
			op := &authv1beta1.NatsOperator{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "op"},
				Spec:       authv1beta1.NatsOperatorSpec{SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"}},
				Status:     authv1beta1.NatsOperatorStatus{SystemAccount: &authv1beta1.SystemAccountStatus{Name: "sys", PublicKey: sysPub, JWT: sysJWT}},
			}
			if tt.unrecovered {
				op.Status.Conditions = []metav1.Condition{{Type: ConditionRevocationsUnrecovered, Status: metav1.ConditionTrue, Reason: ReasonRecovering, LastTransitionTime: metav1.Now()}}
			}
			sys := &authv1beta1.NatsSystemAccount{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys", UID: "sys"},
				Spec: authv1beta1.NatsSystemAccountSpec{
					OperatorRef: natsv1beta1.ObjectReference{Name: "op"},
					Keys: &authv1beta1.Keys{
						Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "sys-keys", Key: "identity"}},
						Signing:  []authv1beta1.SigningKey{{Name: "s", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "sys-keys", Key: "signing"}}},
					},
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys-keys"},
				Data:       map[string][]byte{"identity": idSeed, "signing": skSeed},
			}
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(op, sys, secret).WithStatusSubresource(sys).
				WithIndex(&authv1beta1.NatsUser{}, userAccountField, func(client.Object) []string { return nil }).
				Build()
			rec := events.NewFakeRecorder(10)
			r := &SystemAccountReconciler{Client: c, Distributor: tt.d, Recorder: rec}
			_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sys)})
			require.NoError(t, err)

			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sys), sys))
			require.Equal(t, JWTHash(sysJWT), sys.Status.JWTHash)
			require.Equal(t, tt.wantPushes, tt.d.pushes)
			require.NotNil(t, sys.Status.Distribution)
			require.Equal(t, tt.wantTime, sys.Status.Distribution.LastPushTime != nil, "LastPushTime %v", sys.Status.Distribution.LastPushTime)
			require.Equal(t, tt.wantEvents, drained(rec))
		})
	}
}
