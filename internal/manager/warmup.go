package manager

import (
	"context"
	"errors"
	"net/http"
	"sync"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// warmer is a runnable controller-runtime warms up before its replica is
// elected, as its controllers are.
type warmer interface {
	manager.Runnable
	Warmup(ctx context.Context) error
}

// warming is a manager that records whether every controller added to it
// has finished its warmup.
type warming struct {
	ctrl.Manager
	mu      sync.Mutex
	pending []chan struct{}
}

// Add adds run, recording its warmup where it has one.
func (m *warming) Add(run manager.Runnable) error {
	w, ok := run.(warmer)
	if !ok {
		return m.Manager.Add(run)
	}
	done := make(chan struct{})
	m.mu.Lock()
	m.pending = append(m.pending, done)
	m.mu.Unlock()
	return m.Manager.Add(&warmed{warmer: w, done: done})
}

// warmedUp is a readiness check that passes once every controller added has
// finished its warmup, which ends only once its sources have synced.
func (m *warming) warmedUp(*http.Request) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, done := range m.pending {
		select {
		case <-done:
		default:
			return errors.New("controllers are warming up")
		}
	}
	return nil
}

// warmed closes done once its warmer's Warmup succeeds.
type warmed struct {
	warmer
	done chan struct{}
}

func (w *warmed) Warmup(ctx context.Context) error {
	if err := w.warmer.Warmup(ctx); err != nil {
		return err
	}
	close(w.done)
	return nil
}

// NeedLeaderElection is its warmer's, true for one that does not say.
func (w *warmed) NeedLeaderElection() bool {
	if l, ok := w.warmer.(manager.LeaderElectionRunnable); ok {
		return l.NeedLeaderElection()
	}
	return true
}
