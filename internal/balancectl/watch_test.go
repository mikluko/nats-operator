package balancectl

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

func TestInCluster(t *testing.T) {
	items := []js.NatsBalancer{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "east"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "west"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "b", Name: "new"}},
	}
	clusters := map[types.NamespacedName]string{
		{Namespace: "a", Name: "east"}: "east",
		{Namespace: "a", Name: "west"}: "west",
	}
	req := func(ns, name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}
	for _, tc := range []struct {
		name    string
		cluster string
		want    []reconcile.Request
	}{
		{"same cluster and unplaced", "east", []reconcile.Request{req("a", "east"), req("b", "new")}},
		{"system balancer's cluster unknown", "", []reconcile.Request{req("a", "east"), req("a", "west"), req("b", "new")}},
		{"no balancer placed there", "south", []reconcile.Request{req("b", "new")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, inCluster(tc.cluster, clusters, items))
		})
	}
}

func TestPendingOrSpec(t *testing.T) {
	sb := func(gen int64, pending ...js.Move) *js.NatsSystemBalancer {
		return &js.NatsSystemBalancer{
			ObjectMeta: metav1.ObjectMeta{Name: "sys", Generation: gen},
			Status:     js.NatsSystemBalancerStatus{Pending: pending, Skew: &js.Skew{Leaders: int32(gen)}},
		}
	}
	move := js.Move{Kind: js.MoveLeader, Account: "A", Stream: "S", From: "s1", To: "s2"}
	for _, tc := range []struct {
		name     string
		old, cur *js.NatsSystemBalancer
		want     bool
	}{
		{"pending added", sb(1), sb(1, move), true},
		{"pending cleared", sb(1, move), sb(1), true},
		{"spec changed", sb(1), sb(2), true},
		{"other status", &js.NatsSystemBalancer{Status: js.NatsSystemBalancerStatus{Servers: []js.ServerLoad{{Name: "s1"}}}}, &js.NatsSystemBalancer{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, pendingOrSpec().Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.cur}))
		})
	}
	require.True(t, pendingOrSpec().Create(event.CreateEvent{Object: sb(1)}))
	require.True(t, pendingOrSpec().Delete(event.DeleteEvent{Object: sb(1)}))
}
