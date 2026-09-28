package balancectl

import (
	"github.com/nats-io/nats.go"

	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// NewPool returns the JetStream controller's connection pool, whose
// connections take replies under the jetstream-controller preset's inbox
// prefix, account users' connections among them.
func NewPool() *natsconn.Pool {
	return natsconn.NewPool(natsconn.WithNATSOptions(nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetJetStreamController))))
}
