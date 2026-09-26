package natsconn

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func TestReadEndpoint(t *testing.T) {
	const ns = "payments"
	tests := []struct {
		name    string
		spec    natsv1beta1.NatsConnectionSpec
		objs    []client.Object
		want    Endpoint
		wantErr error
	}{
		{
			name: "servers only",
			spec: natsv1beta1.NatsConnectionSpec{Servers: []string{"nats://a:4222"}},
			want: Endpoint{Servers: []string{"nats://a:4222"}},
		},
		{
			name: "default keys",
			spec: *connection(ns, "c", "nats://a:4222").Spec.DeepCopy(),
			objs: []client.Object{
				secret(ns, "ca", map[string][]byte{"ca.crt": []byte("CA")}),
				secret(ns, "creds", map[string][]byte{"user.creds": []byte("CREDS")}),
			},
			want: Endpoint{Servers: []string{"nats://a:4222"}, CA: []byte("CA"), Creds: []byte("CREDS")},
		},
		{
			name: "explicit keys",
			spec: natsv1beta1.NatsConnectionSpec{
				Servers:     []string{"nats://a:4222"},
				TLS:         &natsv1beta1.ConnectionTLS{CA: &natsv1beta1.CA{SecretKeyRef: natsv1beta1.CASecretKeySelector{Name: "s", Key: "root.pem"}}},
				Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "s", Key: "nats.creds"}},
			},
			objs: []client.Object{secret(ns, "s", map[string][]byte{"root.pem": []byte("CA"), "nats.creds": []byte("CREDS"), "user.creds": []byte("OTHER")})},
			want: Endpoint{Servers: []string{"nats://a:4222"}, CA: []byte("CA"), Creds: []byte("CREDS")},
		},
		{
			name:    "secret missing",
			spec:    *connection(ns, "c", "nats://a:4222").Spec.DeepCopy(),
			objs:    []client.Object{secret(ns, "ca", map[string][]byte{"ca.crt": []byte("CA")})},
			wantErr: ErrSecretNotFound,
		},
		{
			name:    "secret in another namespace is not read",
			spec:    *connection(ns, "c", "nats://a:4222").Spec.DeepCopy(),
			objs:    []client.Object{secret("other", "ca", map[string][]byte{"ca.crt": []byte("CA")}), secret(ns, "creds", map[string][]byte{"user.creds": []byte("C")})},
			wantErr: ErrSecretNotFound,
		},
		{
			name: "key missing",
			spec: *connection(ns, "c", "nats://a:4222").Spec.DeepCopy(),
			objs: []client.Object{
				secret(ns, "ca", map[string][]byte{"ca.crt": []byte("CA")}),
				secret(ns, "creds", map[string][]byte{"nats.creds": []byte("CREDS")}),
			},
			wantErr: ErrKeyNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadEndpoint(t.Context(), fakeClient(t, tt.objs...), ns, &tt.spec)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDial(t *testing.T) {
	n := startNATS(t)
	other := startNATS(t)
	tests := []struct {
		name    string
		ep      func(Endpoint) Endpoint
		wantErr error
		fails   bool
	}{
		{name: "CA and creds", ep: func(ep Endpoint) Endpoint { return ep }},
		{name: "CA from another root", ep: func(ep Endpoint) Endpoint { ep.CA = other.ca; return ep }, fails: true},
		{name: "no creds", ep: func(ep Endpoint) Endpoint { ep.Creds = nil; return ep }, fails: true},
		{name: "creds of another operator", ep: func(ep Endpoint) Endpoint { ep.Creds = other.creds; return ep }, fails: true},
		{name: "CA without a certificate", ep: func(ep Endpoint) Endpoint { ep.CA = []byte("not pem"); return ep }, wantErr: ErrInvalidCA},
		{name: "creds without a seed", ep: func(ep Endpoint) Endpoint { ep.Creds = []byte("not creds"); return ep }, wantErr: ErrInvalidCredentials},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc, err := Dial(tt.ep(n.endpoint()))
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.fails:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				t.Cleanup(nc.Close)
				require.True(t, nc.IsConnected())
				connz, err := n.srv.Connz(&server.ConnzOptions{Username: true})
				require.NoError(t, err)
				require.Len(t, connz.Conns, 1)
				require.Equal(t, n.account, connz.Conns[0].Account)
			}
		})
	}
}

func TestDialReconnectsWithoutLimit(t *testing.T) {
	n := startNATS(t)
	nc, err := Dial(n.endpoint())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	require.Equal(t, -1, nc.Opts.MaxReconnect)
}
