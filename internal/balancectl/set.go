package balancectl

import (
	"maps"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mikluko/nats-operator/internal/balance"
)

// A balancerSet holds each balancer resource's [balance.Balancer], kept
// across reconciles for what it remembers of earlier passes and started
// afresh for a new resource of the same name, and the NATS cluster the
// resource's connection last reached. Its zero value is empty and ready, and
// it is safe for concurrent use.
type balancerSet struct {
	mu     sync.Mutex
	byName map[types.NamespacedName]*tracked
}

type tracked struct {
	uid      types.UID
	balancer *balance.Balancer
	cluster  string
}

// of is obj's entry; s.mu is held.
func (s *balancerSet) of(obj client.Object) *tracked {
	if s.byName == nil {
		s.byName = map[types.NamespacedName]*tracked{}
	}
	key := client.ObjectKeyFromObject(obj)
	t, ok := s.byName[key]
	if !ok || t.uid != obj.GetUID() {
		t = &tracked{uid: obj.GetUID(), balancer: &balance.Balancer{}}
		s.byName[key] = t
	}
	return t
}

func (s *balancerSet) balancer(obj client.Object) *balance.Balancer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.of(obj).balancer
}

// place records cluster as the NATS cluster obj's connection reaches.
func (s *balancerSet) place(obj client.Object, cluster string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.of(obj).cluster = cluster
}

// clusters is a copy of the NATS cluster each resource was last placed in.
func (s *balancerSet) clusters() map[types.NamespacedName]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[types.NamespacedName]string, len(s.byName))
	for key, t := range maps.All(s.byName) {
		if t.cluster != "" {
			out[key] = t.cluster
		}
	}
	return out
}

func (s *balancerSet) forget(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byName, key)
}
