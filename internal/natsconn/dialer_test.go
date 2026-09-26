package natsconn

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

const jetstreamGroup = "jetstream.nats.mikluko.io"

func streamIn(namespace string) grant.Referrer {
	return grant.Referrer{Group: jetstreamGroup, Kind: "NatsStream", Namespace: namespace}
}

// paymentsGrant admits NatsStreams in payments to the NatsConnection name
// in nats-system, or to every one there when name is empty.
func paymentsGrant(name string) *natsv1beta1.NatsReferenceGrant {
	return &natsv1beta1.NatsReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "streams-from-payments"},
		Spec: natsv1beta1.NatsReferenceGrantSpec{
			From: []natsv1beta1.ReferenceGrantFrom{{Group: jetstreamGroup, Kind: "NatsStream", Namespace: "payments"}},
			To:   []natsv1beta1.ReferenceGrantTo{{Group: natsv1beta1.GroupVersion.Group, Kind: Kind, Name: name}},
		},
	}
}

// decoySecrets returns Secrets named as connection names them, holding
// nothing usable: a dial that read them instead of the connection's own
// would fail.
func decoySecrets(namespace string) []client.Object {
	return []client.Object{
		secret(namespace, "ca", map[string][]byte{DefaultCAKey: []byte("x")}),
		secret(namespace, "creds", map[string][]byte{DefaultCredentialsKey: []byte("x")}),
	}
}

func TestDialerReference(t *testing.T) {
	n := startNATS(t)
	tests := []struct {
		name     string
		from     grant.Referrer
		ref      natsv1beta1.ObjectReference
		grants   []client.Object
		wantDeny bool
	}{
		{name: "own namespace, implicit", from: streamIn("nats-system"), ref: natsv1beta1.ObjectReference{Name: "shared"}},
		{name: "own namespace, explicit", from: streamIn("nats-system"), ref: natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"}},
		{name: "cross namespace without grant", from: streamIn("payments"), ref: natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"}, wantDeny: true},
		{
			name:   "cross namespace with grant",
			from:   streamIn("payments"),
			ref:    natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"},
			grants: []client.Object{paymentsGrant("shared")},
		},
		{
			name:     "grant for another connection",
			from:     streamIn("payments"),
			ref:      natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"},
			grants:   []client.Object{paymentsGrant("admin")},
			wantDeny: true,
		},
		{
			name:     "grant for another namespace",
			from:     streamIn("orders"),
			ref:      natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"},
			grants:   []client.Object{paymentsGrant("shared")},
			wantDeny: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append(n.secrets("nats-system"), connection("nats-system", "shared", n.url))
			if tt.from.Namespace != "nats-system" {
				objs = append(objs, decoySecrets(tt.from.Namespace)...)
			}
			objs = append(objs, tt.grants...)
			p := NewPool()
			t.Cleanup(p.Close)
			d := &Dialer{Reader: fakeClient(t, objs...), Pool: p}

			nc, denied, err := d.Reference(t.Context(), tt.from, tt.ref)
			require.NoError(t, err)
			if tt.wantDeny {
				require.Nil(t, nc)
				require.NotNil(t, denied)
				require.Equal(t, grant.ConditionReferencesResolved, denied.Type)
				require.Equal(t, grant.ReasonNoGrant, denied.Reason)
				return
			}
			require.Nil(t, denied)
			require.True(t, nc.IsConnected())
		})
	}
}

func TestDialerReferenceShares(t *testing.T) {
	n := startNATS(t)
	objs := append(n.secrets("nats-system"), connection("nats-system", "shared", n.url), paymentsGrant(""))
	p := NewPool()
	t.Cleanup(p.Close)
	d := &Dialer{Reader: fakeClient(t, objs...), Pool: p}
	ref := natsv1beta1.ObjectReference{Name: "shared", Namespace: "nats-system"}

	a, _, err := d.Reference(t.Context(), streamIn("payments"), ref)
	require.NoError(t, err)
	b, _, err := d.Reference(t.Context(), streamIn("nats-system"), ref)
	require.NoError(t, err)
	require.Same(t, a, b)
}

func TestDialerReferenceMissingConnection(t *testing.T) {
	p := NewPool()
	t.Cleanup(p.Close)
	d := &Dialer{Reader: fakeClient(t), Pool: p}
	_, denied, err := d.Reference(t.Context(), streamIn("payments"), natsv1beta1.ObjectReference{Name: "absent"})
	require.Error(t, err)
	require.Nil(t, denied)
}
