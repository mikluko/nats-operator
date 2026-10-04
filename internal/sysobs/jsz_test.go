package sysobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// fakeServer answers STATSZ, routed to peer, and JSZ for the offsets in
// pages, as the server named name leading its own stream in one account
// per offset.
type fakeServer struct {
	name, peer string
	pages      []int
}

func (f fakeServer) serve(t *testing.T, nc *nats.Conn, total int) {
	t.Helper()
	reply := func(m *nats.Msg, v any) {
		data, err := json.Marshal(v)
		if err == nil {
			err = m.Respond(data)
		}
		if err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Errorf("respond as %s: %v", f.name, err)
		}
	}
	_, err := nc.Subscribe(subjPingStatsz, func(m *nats.Msg) {
		var sz wireStatsz
		sz.Server.Name = f.name
		sz.Stats.Routes = append(sz.Stats.Routes, struct {
			Name string `json:"name"`
		}{f.peer})
		reply(m, sz)
	})
	require.NoError(t, err)
	_, err = nc.Subscribe(subjPingJsz, func(m *nats.Msg) {
		var req wireJszRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("decode JSZ request: %v", err)
			return
		}
		if !slices.Contains(f.pages, req.Offset) {
			return
		}
		account := wireAccount{ID: fmt.Sprintf("A%d", req.Offset), Streams: []wireStream{{
			Name:    f.name,
			Cluster: &wireCluster{RaftGroup: f.name, Leader: f.name},
		}}}
		reply(m, wireJszResponse{Server: wireServerInfo{Name: f.name}, Data: &wireJSInfo{Accounts: []wireAccount{account}, Total: total}})
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
}

func TestObserve_ServerMissingAPageIsSilent(t *testing.T) {
	const total = 2*jszPageSize + 1
	all := []int{0, jszPageSize, 2 * jszPageSize}
	tests := []struct {
		name       string
		s2         []int
		wantSilent []string
	}{
		{"every page", all, nil},
		{"first page only", all[:1], []string{"s2"}},
		{"all but the last page", all[:2], []string{"s2"}},
		{"all but the first page", all[1:], []string{"s2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
			require.NoError(t, err)
			go s.Start()
			t.Cleanup(s.Shutdown)
			require.True(t, s.ReadyForConnections(5*time.Second))
			for _, f := range []fakeServer{{"s1", "s2", all}, {"s2", "s1", tt.s2}} {
				nc, err := nats.Connect(s.ClientURL())
				require.NoError(t, err)
				t.Cleanup(nc.Close)
				f.serve(t, nc, total)
			}
			nc, err := nats.Connect(s.ClientURL())
			require.NoError(t, err)
			t.Cleanup(nc.Close)

			snap, err := New(nc, "C1", WithWait(200*time.Millisecond)).Observe(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.wantSilent, snap.Silent)
			every := Load{StreamLeaders: len(all), StreamReplicas: len(all)}
			wantLoad := map[string]Load{"s1": every, "s2": {}}
			if tt.wantSilent == nil {
				wantLoad["s2"] = every
			}
			require.Equal(t, wantLoad, snap.Load())
		})
	}
}
