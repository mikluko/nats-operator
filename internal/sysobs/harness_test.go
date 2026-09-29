package sysobs

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/natstest"
)

// newTestCluster is a NATS cluster of size servers with account A, whose
// user a has JetStream, and the system account SYS, whose user is sys.
func newTestCluster(name string, size int) *natstest.Cluster {
	return &natstest.Cluster{Name: name, Size: size, Config: func(int) string {
		return `accounts {
  A: { jetstream: enabled, users: [{user: a, password: a}] }
  SYS: { users: [{user: sys, password: sys}] }
}
system_account: SYS
`
	}}
}

// startSupercluster is [natstest.StartSupercluster], its servers by name.
func startSupercluster(t *testing.T, clusters ...*natstest.Cluster) map[string]*natstest.Server {
	t.Helper()
	natstest.StartSupercluster(t, clusters...)
	out := map[string]*natstest.Server{}
	for _, c := range clusters {
		for i, s := range c.Servers {
			out[c.ServerName(i)] = s
		}
	}
	return out
}

func connect(t *testing.T, s *natstest.Server, user string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, user))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
