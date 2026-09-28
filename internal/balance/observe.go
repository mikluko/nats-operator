// Package balance evens JetStream leaders and stream copies across the servers
// of one NATS cluster, one move per pass and none while the NATS cluster is not
// Settled. It serves both the system balancer and the account balancer: what
// sets them apart is the [Observer], the pools and the movers each is given.
package balance

import (
	"context"
	"fmt"
	"slices"
)

// An Observer reads one NATS cluster: the balancer's scope and its boundary.
type Observer interface {
	Observe(ctx context.Context) (Observation, error)
}

// An Observation is one NATS cluster as a pass reads it.
type Observation struct {
	// Cluster is the NATS cluster's name.
	Cluster string
	// Servers is the NATS cluster's roster: every server in it, whether or not
	// it holds anything.
	Servers []Server
	// Groups is every stream and consumer Raft group placed in the NATS
	// cluster. The meta group is not among them.
	Groups []Group
	// Unsettled is why the NATS cluster is not Settled, empty when it is.
	Unsettled string
}

// A Server is one member of a NATS cluster's roster.
type Server struct {
	Name string
	Tags []string
}

// A StreamID names a stream across accounts.
type StreamID struct {
	Account string
	Stream  string
}

func (s StreamID) String() string { return s.Account + "/" + s.Stream }

// A Group is one Raft group: a stream's, or a consumer's when Consumer is set.
type Group struct {
	Account  string
	Stream   string
	Consumer string
	// Leader is empty while the group has none.
	Leader string
	// Offline says no server would describe the group.
	Offline bool
	// Members is every peer of the group but its leader.
	Members []Member
	// Placement is the stream's declared placement, nil where it declares none.
	// A consumer group carries its stream's.
	Placement *Placement
	// Metadata is the stream's config metadata; a consumer group carries
	// its stream's.
	Metadata map[string]string
}

// A Placement is what a stream's config declares about where it may sit.
type Placement struct {
	Cluster string
	Tags    []string
}

// A Member is one peer of a group as the group's leader reports it.
type Member struct {
	Name string
	// Current is the leader's word that the peer was seen recently and has
	// everything the leader has.
	Current bool
	Offline bool
	// Lag is how many entries behind the leader the peer is. It is read only
	// where Current is false.
	Lag uint64
}

// SettledLag is how far behind a peer may be and still take leadership: a
// follower under sustained writes is never Current for long, and a peer this
// far back catches up well inside one pass interval.
const SettledLag = 4096

// Takes reports whether leadership may move to m.
func (m Member) Takes() bool { return !m.Offline && (m.Current || m.Lag <= SettledLag) }

// ID is the stream the group belongs to.
func (g Group) ID() StreamID { return StreamID{Account: g.Account, Stream: g.Stream} }

func (g Group) String() string {
	s := g.ID().String()
	if g.Consumer != "" {
		s += " > " + g.Consumer
	}
	return s
}

// Holders is every server carrying a copy of g: its leader and its members.
func (g Group) Holders() []string {
	var out []string
	if g.Leader != "" {
		out = append(out, g.Leader)
	}
	for _, m := range g.Members {
		out = append(out, m.Name)
	}
	return out
}

// holds reports whether server carries a copy of g.
func (g Group) holds(server string) bool { return slices.Contains(g.Holders(), server) }

// Unsettled names the first group that keeps groups from being Settled, and is
// empty when there is none: every group has a leader and every member is
// online. A member that is merely behind is not a reason.
func Unsettled(groups []Group) string {
	for _, g := range groups {
		if g.Offline {
			return fmt.Sprintf("%s is offline", g)
		}
		if g.Leader == "" {
			return fmt.Sprintf("%s has no leader", g)
		}
		for _, m := range g.Members {
			if m.Offline {
				return fmt.Sprintf("%s is offline for %s", m.Name, g)
			}
		}
	}
	return ""
}
