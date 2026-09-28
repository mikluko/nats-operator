package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// adding is a manager that keeps what is added to it.
type adding struct {
	ctrl.Manager
	added []manager.Runnable
}

func (a *adding) Add(run manager.Runnable) error {
	a.added = append(a.added, run)
	return nil
}

// warmup is a controller's shape: a runnable with a warmup that returns err.
type warmup struct {
	manager.Runnable
	err     error
	elected bool
}

func (w *warmup) Warmup(context.Context) error { return w.err }
func (w *warmup) NeedLeaderElection() bool     { return w.elected }

// TestWarming pins that a warming manager is warmed up once every runnable
// with a warmup added to it has warmed up without error, keeping each one's
// leader election.
func TestWarming(t *testing.T) {
	inner := &adding{}
	m := &warming{Manager: inner}
	require.NoError(t, m.warmedUp(nil), "nothing added")

	plain := manager.RunnableFunc(func(context.Context) error { return nil })
	ok := &warmup{elected: true}
	failing := &warmup{err: errors.New("sync timeout")}
	for _, r := range []manager.Runnable{plain, ok, failing} {
		require.NoError(t, m.Add(r))
	}
	require.Len(t, inner.added, 3)
	_, isWarmed := inner.added[0].(*warmed)
	require.False(t, isWarmed, "a runnable without a warmup is added as it is")
	wOK := inner.added[1].(*warmed)
	wFailing := inner.added[2].(*warmed)
	require.True(t, wOK.NeedLeaderElection())
	require.False(t, wFailing.NeedLeaderElection())

	require.EqualError(t, m.warmedUp(nil), "controllers are warming up")
	require.NoError(t, wOK.Warmup(t.Context()))
	require.EqualError(t, m.warmedUp(nil), "controllers are warming up")
	require.EqualError(t, wFailing.Warmup(t.Context()), "sync timeout")
	require.EqualError(t, m.warmedUp(nil), "controllers are warming up")

	failing.err = nil
	require.NoError(t, wFailing.Warmup(t.Context()))
	require.NoError(t, m.warmedUp(nil))
}
