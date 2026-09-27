package balance

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/nats-io/nats.go/jetstream"
)

// AccountObserver is an [Observer] over one account's own JetStream API, for a
// balancer that holds no system credentials.
//
// What one account can see bounds it. The roster is the servers its groups
// name, so a server holding none of them is invisible and carries no tags;
// Settled is judged over its own groups alone; and the streams and consumers
// the server lists while their leaders change can come back short, which is
// read as unsettled only where the stream is named in Expect.
type AccountObserver struct {
	JS jetstream.JetStream
	// Account is the account's name as the server knows it: its public key
	// under a NATS operator.
	Account string
	// Cluster is the NATS cluster observed; groups placed in another are left
	// out.
	Cluster string
	// Expect is the streams whose absence from the listing holds the pass.
	Expect []string
}

// offlineCode is what the server answers about a stream it holds no working
// copy of. The client exports no name for it.
const offlineCode jetstream.ErrorCode = 10118

// Observe implements [Observer].
func (o AccountObserver) Observe(ctx context.Context) (Observation, error) {
	obs := Observation{Cluster: o.Cluster}
	var names []string
	lister := o.JS.ListStreams(ctx)
	for info := range lister.Info() {
		name := info.Config.Name
		if slices.Contains(names, name) || (info.Cluster != nil && info.Cluster.Name != o.Cluster) {
			continue
		}
		names = append(names, name)
		g := o.groupOf(name, "", info.Cluster, info.Config.Placement)
		g.Metadata = info.Config.Metadata
		obs.Groups = append(obs.Groups, g)
	}
	if err := lister.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return Observation{}, fmt.Errorf("list streams: %w", err)
	}

	for i, name := range names {
		placement, metadata := obs.Groups[i].Placement, obs.Groups[i].Metadata
		stream, err := o.JS.Stream(ctx, name)
		if offline(err) {
			obs.Groups[i].Offline, obs.Groups[i].Leader = true, ""
			continue
		}
		if err != nil {
			return Observation{}, fmt.Errorf("stream %s: %w", name, err)
		}
		consumers := stream.ListConsumers(ctx)
		for info := range consumers.Info() {
			g := o.groupOf(name, info.Name, info.Cluster, nil)
			g.Placement, g.Metadata = placement, metadata
			obs.Groups = append(obs.Groups, g)
		}
		switch err := consumers.Err(); {
		case offline(err):
			obs.Groups[i].Offline, obs.Groups[i].Leader = true, ""
		case err != nil && !errors.Is(err, jetstream.ErrEndOfData):
			return Observation{}, fmt.Errorf("list consumers of %s: %w", name, err)
		}
	}

	seen := map[string]bool{}
	for _, g := range obs.Groups {
		for _, h := range g.Holders() {
			seen[h] = true
		}
	}
	for _, name := range slices.Sorted(maps.Keys(seen)) {
		obs.Servers = append(obs.Servers, Server{Name: name})
	}

	for _, want := range o.Expect {
		if !slices.Contains(names, want) {
			obs.Unsettled = fmt.Sprintf("%s was not reported", StreamID{o.Account, want})
			return obs, nil
		}
	}
	obs.Unsettled = Unsettled(obs.Groups)
	return obs, nil
}

func offline(err error) bool {
	var api *jetstream.APIError
	return errors.As(err, &api) && api.ErrorCode == offlineCode
}

// groupOf is a group as its info reports it. A server not running clustered
// reports no cluster, and the group is then leaderless.
func (o AccountObserver) groupOf(stream, consumer string, c *jetstream.ClusterInfo, p *jetstream.Placement) Group {
	g := Group{Account: o.Account, Stream: stream, Consumer: consumer}
	if p != nil {
		g.Placement = &Placement{Cluster: p.Cluster, Tags: p.Tags}
	}
	if c == nil {
		return g
	}
	g.Leader = c.Leader
	for _, r := range c.Replicas {
		g.Members = append(g.Members, Member{Name: r.Name, Current: r.Current, Offline: r.Offline, Lag: r.Lag})
	}
	return g
}
