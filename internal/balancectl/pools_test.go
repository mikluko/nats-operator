package balancectl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
)

func pool(name, key, value string) js.Pool {
	return js.Pool{Name: name, Selector: metav1.LabelSelector{MatchLabels: map[string]string{key: value}}}
}

func TestAssign(t *testing.T) {
	req := func(stream string) member {
		return member{Stream: stream, Labels: labels.Set{"traffic": "requests"}}
	}
	id := func(s string) balance.StreamID { return balance.StreamID{Account: "A", Stream: s} }
	pools := []js.Pool{pool("requests", "traffic", "requests"), pool("hot", "hot", "true"), pool("responses", "traffic", "responses")}
	tests := []struct {
		name     string
		members  []member
		want     []balance.Pool
		overlaps []string
	}{
		{
			name:    "no resources",
			want:    []balance.Pool{{Name: "requests"}, {Name: "hot"}, {Name: "responses"}},
			members: nil,
		},
		{
			name: "each in its own",
			members: []member{
				req("REQ_2"), req("REQ_1"),
				{Stream: "RES_1", Labels: labels.Set{"traffic": "responses"}},
				{Stream: "OTHER", Labels: labels.Set{"traffic": "none"}},
			},
			want: []balance.Pool{
				{Name: "requests", Streams: []balance.StreamID{id("REQ_1"), id("REQ_2")}},
				{Name: "hot"},
				{Name: "responses", Streams: []balance.StreamID{id("RES_1")}},
			},
		},
		{
			name: "first matching pool wins",
			members: []member{
				{Stream: "RES_1", Labels: labels.Set{"traffic": "responses", "hot": "true"}},
				{Stream: "REQ_12", Labels: labels.Set{"traffic": "requests", "hot": "true"}},
			},
			want: []balance.Pool{
				{Name: "requests", Streams: []balance.StreamID{id("REQ_12")}},
				{Name: "hot", Streams: []balance.StreamID{id("RES_1")}},
				{Name: "responses"},
			},
			overlaps: []string{
				"REQ_12 matches requests and hot; balanced in requests",
				"RES_1 matches hot and responses; balanced in hot",
			},
		},
		{
			name: "two resources for one stream",
			members: []member{
				{Stream: "KV_cfg", Labels: labels.Set{"traffic": "responses"}},
				{Stream: "KV_cfg", Labels: labels.Set{"traffic": "requests", "hot": "true"}},
			},
			want: []balance.Pool{
				{Name: "requests", Streams: []balance.StreamID{id("KV_cfg")}},
				{Name: "hot"},
				{Name: "responses"},
			},
			overlaps: []string{"KV_cfg matches requests, hot and responses; balanced in requests"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, overlaps, err := assign("A", pools, tt.members)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.overlaps, overlaps)
		})
	}

	t.Run("invalid selector", func(t *testing.T) {
		bad := js.Pool{Name: "bad", Selector: metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "k", Operator: "Near"}}}}
		_, _, err := assign("A", []js.Pool{bad}, nil)
		require.ErrorContains(t, err, "pool bad")
	})
}

func TestMembers(t *testing.T) {
	conn := natsv1beta1.ObjectReference{Name: "demo"}
	ready := []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "Synced"}}
	objs := []client.Object{
		&js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "req-07", Labels: map[string]string{"traffic": "requests"}},
			Spec:       js.NatsStreamSpec{ConnectionRef: conn, StreamConfig: js.StreamConfig{Name: "REQ_07", Placement: &js.Placement{Cluster: "C1"}}},
			Status:     js.NatsStreamStatus{SyncStatus: js.SyncStatus{Conditions: ready}},
		},
		&js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "unnamed"},
			Spec:       js.NatsStreamSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "demo", Namespace: "payments"}},
		},
		&js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "elsewhere"},
			Spec:       js.NatsStreamSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "demo", Namespace: "shared"}},
		},
		&js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "other-namespace"},
			Spec:       js.NatsStreamSpec{ConnectionRef: conn},
		},
		&js.NatsKeyValue{
			ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "cfg"},
			Spec:       js.NatsKeyValueSpec{ConnectionRef: conn},
		},
		&js.NatsObjectStore{
			ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "blobs"},
			Spec:       js.NatsObjectStoreSpec{ConnectionRef: conn, ObjectStoreConfig: js.ObjectStoreConfig{Name: "BLOBS"}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	got, err := members(t.Context(), c, "payments", conn)
	require.NoError(t, err)
	var streams []string
	for _, m := range got {
		streams = append(streams, m.Stream)
	}
	require.ElementsMatch(t, []string{"REQ_07", "unnamed", "KV_cfg", "OBJ_BLOBS"}, streams)
	require.Equal(t, []string{"REQ_07"}, expected(got, "C1"))
	require.Empty(t, expected(got, "C2"))
}

func TestPoolStatus(t *testing.T) {
	pools := []js.Pool{pool("requests", "traffic", "requests"), pool("responses", "traffic", "responses")}
	got := poolStatus(pools, []balance.PoolReport{
		{Name: "requests", Streams: 12, LeaderSkew: 2},
		{Name: balance.DefaultPool, Streams: 3, LeaderSkew: 1},
	})
	require.Equal(t, []js.PoolStatus{
		{Name: "requests", Streams: 12, LeaderSkew: 2},
		{Name: "responses"},
		{Name: "(default)", Streams: 3, LeaderSkew: 1},
	}, got)
}

func TestSystemPending(t *testing.T) {
	sys := func(name string, pending ...js.Move) client.Object {
		return &js.NatsSystemBalancer{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Status: js.NatsSystemBalancerStatus{Pending: pending}}
	}
	tests := []struct {
		name    string
		account string
		objs    []client.Object
		want    string
	}{
		{"none", "A", []client.Object{sys("demo")}, ""},
		{"another account", "A", []client.Object{sys("demo", js.Move{Kind: js.MovePlacement, Account: "B", Stream: "REQ_07", From: "s1"})}, ""},
		{
			"placement", "A",
			[]client.Object{sys("demo", js.Move{Kind: js.MovePlacement, Account: "A", Stream: "REQ_07", From: "s1"})},
			"REQ_07 has a placement move pending from NatsSystemBalancer demo",
		},
		{
			"consumer leader", "A",
			[]client.Object{sys("demo", js.Move{Kind: js.MoveLeader, Account: "A", Stream: "REQ_07", Consumer: "worker", From: "s1", To: "s2"})},
			"REQ_07 has a leader move pending from NatsSystemBalancer demo",
		},
		{"no account known", "", []client.Object{sys("demo", js.Move{Kind: js.MovePlacement, Stream: "REQ_07"})}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tt.objs...).WithStatusSubresource(&js.NatsSystemBalancer{}).Build()
			got, err := systemPending(t.Context(), c, tt.account)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAndList(t *testing.T) {
	for in, want := range map[string]string{"": "", "a": "a", "a b": "a and b", "a b c": "a, b and c"} {
		require.Equal(t, want, andList(strings.Fields(in)))
	}
}
