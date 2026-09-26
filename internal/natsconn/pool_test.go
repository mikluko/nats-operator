package natsconn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

var testKey = Key{Kind: Kind, NamespacedName: types.NamespacedName{Namespace: "payments", Name: "demo"}}

func TestPoolGet(t *testing.T) {
	n := startNATS(t)
	ep := n.endpoint()

	tests := []struct {
		name     string
		second   func(Endpoint) Endpoint
		wantSame bool
	}{
		{name: "same endpoint reuses", second: func(ep Endpoint) Endpoint { return ep }, wantSame: true},
		{name: "equal copy reuses", second: func(ep Endpoint) Endpoint {
			return Endpoint{Servers: append([]string(nil), ep.Servers...), CA: append([]byte(nil), ep.CA...), Creds: append([]byte(nil), ep.Creds...)}
		}, wantSame: true},
		{name: "rotated creds redial", second: func(ep Endpoint) Endpoint {
			ep.Creds = append(append([]byte(nil), ep.Creds...), '\n')
			return ep
		}},
		{name: "added server redials", second: func(ep Endpoint) Endpoint {
			ep.Servers = append(append([]string(nil), ep.Servers...), ep.Servers[0])
			return ep
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPool()
			t.Cleanup(p.Close)
			first, err := p.Get(testKey, ep)
			require.NoError(t, err)
			second, err := p.Get(testKey, tt.second(ep))
			require.NoError(t, err)
			if tt.wantSame {
				require.Same(t, first, second)
				return
			}
			require.NotSame(t, first, second)
			require.True(t, first.IsClosed())
			require.True(t, second.IsConnected())
		})
	}
}

func TestPoolKeysAreSeparate(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	t.Cleanup(p.Close)
	other := testKey
	other.Name = "other"
	a, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	b, err := p.Get(other, n.endpoint())
	require.NoError(t, err)
	require.NotSame(t, a, b)

	p.Forget(testKey)
	require.True(t, a.IsClosed())
	require.False(t, b.IsClosed())
}

func TestPoolRedialsClosed(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	t.Cleanup(p.Close)
	a, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	a.Close()
	b, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	require.NotSame(t, a, b)
	require.True(t, b.IsConnected())
}

func TestPoolFailedDialKeepsNothing(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	t.Cleanup(p.Close)
	good, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	bad := n.endpoint()
	bad.Creds = nil
	_, err = p.Get(testKey, bad)
	require.Error(t, err)
	require.True(t, good.IsClosed())
}

func TestPoolClose(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	a, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	p.Close()
	require.True(t, a.IsClosed())
	_, err = p.Get(testKey, n.endpoint())
	require.ErrorIs(t, err, ErrPoolClosed)
	p.Close()
}

func TestPoolStartClosesOnDone(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	a, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)
	go func() { done <- p.Start(ctx) }()
	cancel()
	require.NoError(t, <-done)
	require.True(t, a.IsClosed())
}

func TestPoolConcurrentGet(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	t.Cleanup(p.Close)
	conns := make([]*nats.Conn, 8)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Go(func() {
			nc, err := p.Get(testKey, n.endpoint())
			require.NoError(t, err)
			conns[i] = nc
		})
	}
	wg.Wait()
	for _, nc := range conns[1:] {
		require.Same(t, conns[0], nc)
	}
}

func TestPoolOnChange(t *testing.T) {
	n := startNATS(t)
	p := NewPool()
	t.Cleanup(p.Close)
	_, err := p.Get(testKey, n.endpoint())
	require.NoError(t, err)

	changed := make(chan Key, 16)
	p.OnChange(func(k Key) { changed <- k })
	n.srv.Shutdown()
	select {
	case k := <-changed:
		require.Equal(t, testKey, k)
	case <-time.After(5 * time.Second):
		t.Fatal("no change notified on disconnect")
	}
}
