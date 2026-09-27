package balancectl

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

func TestBalancerSet(t *testing.T) {
	var s balancerSet
	b := &js.NatsBalancer{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "pay", UID: "1"}}
	key := types.NamespacedName{Namespace: "a", Name: "pay"}

	first := s.balancer(b)
	require.Same(t, first, s.balancer(b), "kept across reconciles")
	s.place(b, "east")
	require.Equal(t, map[types.NamespacedName]string{key: "east"}, s.clusters())

	b.UID = "2"
	require.NotSame(t, first, s.balancer(b), "a new resource of the same name starts afresh")
	require.Empty(t, s.clusters())

	s.place(b, "west")
	s.forget(key)
	require.Empty(t, s.clusters())
}
