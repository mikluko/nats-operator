package natscluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestReloadServer drives reloadServer through sysobs against a server
// running story 1's rendered config with a system user added.
func TestReloadServer(t *testing.T) {
	nc := limitedStoryCluster(t)
	m := renderedMap(t, nc, writeRouteCert(t, nc))
	m["accounts"] = map[string]any{"SYS": map[string]any{"users": []any{map[string]any{"user": "sys", "password": "sys"}}}}
	m["system_account"] = "SYS"
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	conn, err := nats.Connect(fmt.Sprintf("nats://sys:sys@%s", s.Addr()))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	obs := sysobs.New(conn, "demo")
	snap := &sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0", ID: s.ID()}}}
	ctx := context.Background()

	rendered := func(mutate func(map[string]any)) (Server, []byte) {
		mutate(m)
		b := encode(t, m)
		return Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(b)}}}, b
	}
	tagged, taggedFile := rendered(func(m map[string]any) { m["server_tags"] = []any{"az:b"} })

	t.Run("file not yet updated", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged)
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("server unobserved", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, &sysobs.Snapshot{Silent: []string{"demo-0"}, Servers: snap.Servers}, tagged)
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("reloads", func(t *testing.T) {
		require.NoError(t, os.WriteFile(f, taggedFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, tagged)
		require.NoError(t, err)
		require.True(t, applied)
		require.Equal(t, []string{"az:b"}, varzTags(t, s))
	})
	t.Run("already loaded", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged)
		require.NoError(t, err)
		require.True(t, applied)
	})
	t.Run("restart-only change fails", func(t *testing.T) {
		domain, domainFile := rendered(func(m map[string]any) { m["jetstream"].(map[string]any)["domain"] = "hub" })
		require.NoError(t, os.WriteFile(f, domainFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, domain)
		require.ErrorIs(t, err, sysobs.ErrServer)
		require.False(t, applied)
	})
}

func varzTags(t *testing.T, s *server.Server) []string {
	t.Helper()
	v, err := s.Varz(nil)
	require.NoError(t, err)
	return v.Tags
}
