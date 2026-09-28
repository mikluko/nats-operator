package balancectl

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// moveLeaseTTL is how long a move lease outlives its holder's last pass.
const moveLeaseTTL = time.Minute

// MoveLeases holds one move lease per NATS cluster: its holder is the only
// balancer that may move on that NATS cluster, from before its move until a
// pass of its own finds the move done. Exclusion holds within one process
// only; the zero value is ready and safe for concurrent use.
type MoveLeases struct {
	mu   sync.Mutex
	held map[string]moveLease
}

type moveLease struct {
	holder string
	until  time.Time
}

// processLeases is the MoveLeases of every reconciler whose Leases is nil.
var processLeases MoveLeases

// take grants holder cluster's lease, or renews it, until moveLeaseTTL after
// now, and returns "". Where another holds it, take grants nothing and
// returns that holder.
func (l *MoveLeases) take(cluster, holder string, now time.Time) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.held[cluster]; ok && cur.holder != holder && now.Before(cur.until) {
		return cur.holder
	}
	if l.held == nil {
		l.held = map[string]moveLease{}
	}
	l.held[cluster] = moveLease{holder: holder, until: now.Add(moveLeaseTTL)}
	return ""
}

// holds reports whether holder holds cluster's lease at now.
func (l *MoveLeases) holds(cluster, holder string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.held[cluster]
	return ok && cur.holder == holder && now.Before(cur.until)
}

// release frees cluster's lease where holder holds it.
func (l *MoveLeases) release(cluster, holder string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held[cluster].holder == holder {
		delete(l.held, cluster)
	}
}

// drop frees every lease holder holds.
func (l *MoveLeases) drop(holder string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for cluster, cur := range l.held {
		if cur.holder == holder {
			delete(l.held, cluster)
		}
	}
}

// holderName is the lease holder name of the balancer of kind named key.
func holderName(kind string, key types.NamespacedName) string {
	return kind + " " + key.String()
}

// leasesOr is l, or processLeases where l is nil.
func leasesOr(l *MoveLeases) *MoveLeases {
	if l == nil {
		return &processLeases
	}
	return l
}
