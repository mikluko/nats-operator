package balancectl

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// jszSubject asks the server whose ID fills it for its JetStream state, which
// the jetstream-controller user preset may request.
const jszSubject = "$SYS.REQ.SERVER.%s.JSZ"

// answers reports whether the server whose ID is id answers sys, a connection
// of a system account: it does where both are in one NATS system, a cluster
// or a supercluster, and does not in another NATS system.
func answers(ctx context.Context, sys *nats.Conn, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	_, err := sys.RequestWithContext(ctx, fmt.Sprintf(jszSubject, id), nil)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, nats.ErrNoResponders):
		return false, nil
	}
	return false, fmt.Errorf("ask server %s for its JetStream state: %w", id, err)
}

// evacuationOf names a NatsClusterEvacuation, not yet Ready, that empties the
// NATS cluster nc is connected to, and is "" where none does. One empties it
// where its source is nc's cluster by name and the server nc is connected to
// answers the evacuation's own connection; one whose connection cannot be
// dialed or asked is taken to empty it. The error is one the Kubernetes API
// server returned.
func evacuationOf(ctx context.Context, c client.Reader, d *natsconn.Dialer, nc *nats.Conn) (string, error) {
	cluster := nc.ConnectedClusterName()
	var list js.NatsClusterEvacuationList
	if err := c.List(ctx, &list); err != nil {
		return "", fmt.Errorf("list NatsClusterEvacuations: %w", err)
	}
	for i := range list.Items {
		e := &list.Items[i]
		if e.Spec.From.Cluster != cluster || meta.IsStatusConditionTrue(e.Status.Conditions, ConditionReady) {
			continue
		}
		sys, why, err := dial(ctx, d, evacuationReferrer(e.Namespace), e.Spec.ConnectionRef)
		if err != nil {
			return "", err
		}
		if why == nil {
			if ok, err := answers(ctx, sys, nc.ConnectedServerId()); err == nil && !ok {
				continue
			}
		}
		return client.ObjectKeyFromObject(e).String(), nil
	}
	return "", nil
}

// evacuees is a [balance.Observer] that remembers which streams of its last
// observation the NatsClusterEvacuation evac moves, by [evacuates] over
// owners. With evac "" it remembers none.
type evacuees struct {
	balance.Observer
	evac   string
	owners map[types.UID]owner
	moving map[balance.StreamID]bool
}

// Observe implements [balance.Observer].
func (e *evacuees) Observe(ctx context.Context) (balance.Observation, error) {
	obs, err := e.Observer.Observe(ctx)
	e.moving = map[balance.StreamID]bool{}
	if err != nil || e.evac == "" {
		return obs, err
	}
	for _, g := range obs.Groups {
		if g.Consumer == "" && evacuates(g.Metadata, e.owners) {
			e.moving[g.ID()] = true
		}
	}
	return obs, nil
}

// Yield is a [balance.Keeper] Yield: why a stream of the last observation is
// the evacuation's rather than the balancer's, "" where it is not.
func (e *evacuees) Yield(id balance.StreamID) string {
	if e.moving[id] {
		return fmt.Sprintf("NatsClusterEvacuation %s moves it", e.evac)
	}
	return ""
}

// evacueesOf is an [evacuees] over o for the evacuation emptying the NATS
// cluster nc is connected to, if any. The error is one the Kubernetes API
// server returned.
func evacueesOf(ctx context.Context, c client.Reader, d *natsconn.Dialer, nc *nats.Conn, o balance.Observer) (*evacuees, error) {
	evac, err := evacuationOf(ctx, c, d, nc)
	if err != nil || evac == "" {
		return &evacuees{Observer: o}, err
	}
	owners, err := resources(ctx, c)
	if err != nil {
		return nil, err
	}
	return &evacuees{Observer: o, evac: evac, owners: owners}, nil
}
