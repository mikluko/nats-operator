package balancectl

import (
	"context"
	"fmt"
	"strings"

	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// SystemObserver is a [balance.Observer] of one NATS cluster over every
// account, read as the system account through sysobs. It keeps the last
// observation it made.
//
// A SystemObserver is not safe for concurrent use.
type SystemObserver struct {
	Sys     *sysobs.Observer
	Cluster string

	last   *balance.Observation
	pinned map[balance.StreamID]string
}

// Observe implements [balance.Observer].
func (o *SystemObserver) Observe(ctx context.Context) (balance.Observation, error) {
	snap, err := o.Sys.Observe(ctx)
	if err != nil {
		return balance.Observation{}, err
	}
	obs := observation(o.Cluster, snap)
	o.last, o.pinned = &obs, pinned(obs)
	return obs, nil
}

// pinned is why each stream of obs whose declared placement names another
// NATS cluster is not to be moved.
func pinned(obs balance.Observation) map[balance.StreamID]string {
	out := map[balance.StreamID]string{}
	for _, g := range obs.Groups {
		if p := g.Placement; p != nil && p.Cluster != "" && p.Cluster != obs.Cluster {
			out[g.ID()] = "pinned to NATS cluster " + p.Cluster
		}
	}
	return out
}

// Last is what the last successful Observe returned, nil before one.
func (o *SystemObserver) Last() *balance.Observation { return o.last }

// Pinned is a [balance.Keeper] Yield: why a stream in the last observation
// is not the balancer's to move, "" where it is. A stream whose declared
// placement names another NATS cluster is never moved.
func (o *SystemObserver) Pinned(id balance.StreamID) string { return o.pinned[id] }

// observation is snap as a balancer reads it. The NATS cluster is unsettled
// while a roster server did not answer, the meta group has no leader, a group
// is unsettled by [balance.Unsettled], or a member lags too far to take
// leadership, as the copy a placement move is filling does.
func observation(cluster string, snap *sysobs.Snapshot) balance.Observation {
	obs := balance.Observation{Cluster: cluster}
	for _, s := range snap.Servers {
		obs.Servers = append(obs.Servers, balance.Server{Name: s.Name, Tags: s.Tags})
	}
	metaLeader := true
	for _, g := range snap.Groups {
		if g.Kind == sysobs.KindMeta {
			metaLeader = g.Leader != ""
			continue
		}
		obs.Groups = append(obs.Groups, group(g))
	}
	switch {
	case len(snap.Silent) > 0:
		obs.Unsettled = strings.Join(snap.Silent, ", ") + " did not answer"
	case !metaLeader:
		obs.Unsettled = "the meta group has no leader"
	default:
		obs.Unsettled = balance.Unsettled(obs.Groups)
	}
	if obs.Unsettled == "" {
		obs.Unsettled = lagging(obs.Groups)
	}
	return obs
}

func group(g sysobs.Group) balance.Group {
	out := balance.Group{Account: g.Account, Stream: g.Stream, Consumer: g.Consumer, Leader: g.Leader}
	if p := g.Placement; p != nil {
		out.Placement = &balance.Placement{Cluster: p.Cluster, Tags: p.Tags}
	}
	for _, m := range g.Members {
		if m.Server == g.Leader {
			continue
		}
		out.Members = append(out.Members, balance.Member{Name: m.Server, Current: m.Current, Offline: m.Offline, Lag: m.Lag})
	}
	return out
}

// lagging names the first member too far behind to take leadership, and is
// empty when there is none.
func lagging(groups []balance.Group) string {
	for _, g := range groups {
		for _, m := range g.Members {
			if !m.Takes() {
				return fmt.Sprintf("%s is %d behind for %s", m.Name, m.Lag, g)
			}
		}
	}
	return ""
}
