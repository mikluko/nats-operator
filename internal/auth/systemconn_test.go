package auth_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/auth"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestSystemConnection_Conn pins which operators the one system connection
// serves: those whose system account issued the user its creds carry, and
// no other.
func TestSystemConnection_Conn(t *testing.T) {
	p := newPlane(t)
	srv := startServers(t, p, 1)[0]
	creds := func(keys jwtplane.Keys) []byte {
		kp, err := nkeys.CreateUser()
		require.NoError(t, err)
		pub, err := kp.PublicKey()
		require.NoError(t, err)
		seed, err := kp.Seed()
		require.NoError(t, err)
		token, err := jwtplane.SignUser(jwtplane.User{Name: "auth-controller", PublicKey: pub, SystemAccount: true, Preset: jwtplane.PresetAuthController}, keys)
		require.NoError(t, err)
		out, err := jwt.FormatUserConfig(token, seed)
		require.NoError(t, err)
		return out
	}

	tests := []struct {
		name     string
		operator *authv1beta1.NatsOperator
		creds    []byte
		wantErr  error
	}{
		{
			name:     "a user of the operator's system account",
			operator: operatorWithSystemAccount(p.sysPub),
			creds:    creds(p.sys),
		},
		{
			name:     "status.systemAccount lost: the operator JWT names the system account",
			operator: operatorWithJWT(p.opJWT),
			creds:    creds(p.sys),
		},
		{
			name:     "a user of another account",
			operator: operatorWithSystemAccount(p.sysPub),
			creds:    creds(p.acc),
			wantErr:  auth.ErrForeignConnection,
		},
		{
			name:    "no such operator",
			creds:   creds(p.sys),
			wantErr: auth.ErrOperatorGone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			require.NoError(t, authv1beta1.AddToScheme(s))
			require.NoError(t, natsv1beta1.AddToScheme(s))
			require.NoError(t, corev1.AddToScheme(s))
			objs := []client.Object{
				&natsv1beta1.NatsConnection{
					ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "system"},
					Spec: natsv1beta1.NatsConnectionSpec{
						Servers:     []string{srv.ClientURL()},
						Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "creds"}},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "creds"},
					Data:       map[string][]byte{natsconn.DefaultCredentialsKey: tt.creds},
				},
			}
			if tt.operator != nil {
				objs = append(objs, tt.operator)
			}
			pool := natsconn.NewPool()
			t.Cleanup(pool.Close)
			conn := &auth.SystemConnection{
				Reader: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
				Pool:   pool,
				Name:   key("nats-system", "system"),
			}
			nc, err := conn.Conn(t.Context(), demo)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.True(t, nc.IsConnected())
		})
	}
}

func operatorWithSystemAccount(pub string) *authv1beta1.NatsOperator {
	return &authv1beta1.NatsOperator{
		ObjectMeta: metav1.ObjectMeta{Namespace: demo.Namespace, Name: demo.Name},
		Status:     authv1beta1.NatsOperatorStatus{SystemAccount: &authv1beta1.SystemAccountStatus{Name: "sys", PublicKey: pub}},
	}
}

func operatorWithJWT(token string) *authv1beta1.NatsOperator {
	return &authv1beta1.NatsOperator{
		ObjectMeta: metav1.ObjectMeta{Namespace: demo.Namespace, Name: demo.Name},
		Status:     authv1beta1.NatsOperatorStatus{JWT: token},
	}
}
