package balancectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// jszSubject asks the server whose ID fills it for its JetStream state, which
// the jetstream-controller user preset may request.
const jszSubject = "$SYS.REQ.SERVER.%s.JSZ"

// answers reports whether the server whose ID is id answers sys, a connection
// of a system account: it does where both are in one NATS system, a NATS
// cluster or a supercluster, and does not in another NATS system.
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

// pingJszSubject asks every server a filter selects for its JetStream state.
const pingJszSubject = "$SYS.REQ.SERVER.PING.JSZ"

// serversDown says why snap, of NATS cluster cluster, may be missing streams,
// and is "" where it is not: a server of cluster did not answer, the meta
// group has no leader, or its leader reports a peer offline.
func serversDown(ctx context.Context, sys *nats.Conn, snap *sysobs.Snapshot, cluster string) (string, error) {
	if len(snap.Silent) > 0 {
		return fmt.Sprintf("%s of %s %s", servers(snap.Silent), cluster, plural(len(snap.Silent), "does not answer", "do not answer")), nil
	}
	leader := snap.MetaLeader()
	if leader == "" {
		return "the meta group has no leader", nil
	}
	offline, err := offlinePeers(ctx, sys, leader)
	if err != nil || len(offline) == 0 {
		return "", err
	}
	return fmt.Sprintf("%s %s offline", servers(offline), plural(len(offline), "is", "are")), nil
}

// offlinePeers names, sorted, the meta group's peers that leader, the meta
// leader, reports offline.
func offlinePeers(ctx context.Context, sys *nats.Conn, leader string) ([]string, error) {
	req, err := json.Marshal(map[string]any{"server_name": leader, "exact_match": true})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	msg, err := sys.RequestWithContext(ctx, pingJszSubject, req)
	if err != nil {
		return nil, fmt.Errorf("ask meta leader %s for its peers: %w", leader, err)
	}
	var resp struct {
		Data *struct {
			Meta *struct {
				Replicas []struct {
					Name    string `json:"name"`
					Offline bool   `json:"offline"`
				} `json:"replicas"`
			} `json:"meta_cluster"`
		} `json:"data"`
		Error *struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("ask meta leader %s for its peers: %w", leader, err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("ask meta leader %s for its peers: %s", leader, resp.Error.Description)
	}
	var out []string
	if resp.Data != nil && resp.Data.Meta != nil {
		for _, p := range resp.Data.Meta.Replicas {
			if p.Offline {
				out = append(out, p.Name)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

// servers is names after "server" or "servers".
func servers(names []string) string {
	return plural(len(names), "server", "servers") + " " + strings.Join(names, ", ")
}

// plural is one where n is 1, and many otherwise.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// evacuationOf names a NatsClusterEvacuation, not yet Ready, that empties the
// NATS cluster nc is connected to, and is "" where none does. One empties it
// where its source is nc's NATS cluster by name and the server nc is connected to
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
		sys, why, err := lifecycle.Dial(ctx, d, evacuationReferrer(e.Namespace), e.Spec.ConnectionRef)
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

// Yield is a [balance.Balancer] Yield: why a stream of the last observation is
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
