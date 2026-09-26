package sysobs

import (
	"cmp"
	"slices"
)

// Kind is what a Raft group replicates.
type Kind string

// The kinds of Raft group.
const (
	KindMeta     Kind = "meta"
	KindStream   Kind = "stream"
	KindConsumer Kind = "consumer"
)

// Server is one member of a NATS cluster's roster. Metadata is the
// server's configured server_metadata.
type Server struct {
	Name      string
	ID        string
	Version   string
	Metadata  map[string]string
	JetStream bool
	Tags      []string
}

// Member is one server's place in a Raft group, as its leader sees it.
type Member struct {
	Server  string
	Current bool
	Offline bool
	// Lag is how many entries behind the leader the member is.
	Lag uint64
}

// Placement is where a stream's config declares it may sit.
type Placement struct {
	Cluster string
	Tags    []string
}

// Group is one Raft group placed in the NATS cluster. Account is the
// account's ID, its public key under a NATS operator; it is empty for the
// meta group, and Consumer is empty for a stream group.
type Group struct {
	Kind      Kind
	Account   string
	Stream    string
	Consumer  string
	RaftGroup string

	// Leader is the server that reports itself the group's leader, or ""
	// when none of the servers that answered does.
	Leader string

	// Members come from the leader's view and are sorted by server. A
	// group without a leader lists the servers that hold it, none current.
	Members []Member

	// NamedLeader is the leader a follower names when Leader is "": a
	// server that did not answer, or a stale view.
	NamedLeader string

	// Placement is the stream's declared placement, nil where it declares
	// none; a consumer group carries its stream's.
	Placement *Placement
}

// Snapshot is one observation of a NATS cluster.
type Snapshot struct {
	Servers []Server

	// Silent lists the roster's servers that did not answer the JSZ
	// request; their groups may be missing from Groups.
	Silent []string

	// Groups has the meta group first, when any server reports one, then
	// stream and consumer groups ordered by account, stream and consumer.
	Groups []Group
}

// Reason says why a group is not settled.
type Reason string

// The reasons a group is not settled.
const (
	ReasonNoLeader      Reason = "NoLeader"
	ReasonMemberOffline Reason = "MemberOffline"
	ReasonMemberBehind  Reason = "MemberBehind"
)

// Unsettled names a group that is not settled, why, and the servers the
// reason is about; Servers is empty for ReasonNoLeader.
type Unsettled struct {
	Group   Group
	Reason  Reason
	Servers []string
}

// Verdict judges a Snapshot against Settled.
type Verdict struct {
	Unsettled []Unsettled
	Silent    []string
}

// Settled reports whether every Raft group has a leader, every member of
// every group is online and current, and every server answered.
func (v Verdict) Settled() bool {
	return len(v.Unsettled) == 0 && len(v.Silent) == 0
}

// Verdict judges every group in s. An offline member outranks a member
// that is only behind, so each group appears at most once.
func (s *Snapshot) Verdict() Verdict {
	v := Verdict{Silent: slices.Clone(s.Silent)}
	for _, g := range s.Groups {
		if u, ok := judge(g); !ok {
			v.Unsettled = append(v.Unsettled, u)
		}
	}
	return v
}

func judge(g Group) (Unsettled, bool) {
	if g.Leader == "" {
		return Unsettled{Group: g, Reason: ReasonNoLeader}, false
	}
	var offline, behind []string
	for _, m := range g.Members {
		switch {
		case m.Offline:
			offline = append(offline, m.Server)
		case !m.Current:
			behind = append(behind, m.Server)
		}
	}
	switch {
	case len(offline) > 0:
		return Unsettled{Group: g, Reason: ReasonMemberOffline, Servers: offline}, false
	case len(behind) > 0:
		return Unsettled{Group: g, Reason: ReasonMemberBehind, Servers: behind}, false
	}
	return Unsettled{}, true
}

// Load is how many stream and consumer groups one server leads and holds a
// copy of; a copy it leads counts in both.
type Load struct {
	StreamLeaders    int
	StreamReplicas   int
	ConsumerLeaders  int
	ConsumerReplicas int
}

// Load returns the Load of every roster server and of any other server a
// group names as a member, keyed by server name. The meta group counts
// towards none.
func (s *Snapshot) Load() map[string]Load {
	out := make(map[string]Load, len(s.Servers))
	for _, srv := range s.Servers {
		out[srv.Name] = Load{}
	}
	for _, g := range s.Groups {
		if g.Kind == KindMeta || g.Leader == "" {
			continue
		}
		l := out[g.Leader]
		if g.Kind == KindStream {
			l.StreamLeaders++
		} else {
			l.ConsumerLeaders++
		}
		out[g.Leader] = l
		for _, m := range g.Members {
			l := out[m.Server]
			if g.Kind == KindStream {
				l.StreamReplicas++
			} else {
				l.ConsumerReplicas++
			}
			out[m.Server] = l
		}
	}
	return out
}

type groupKey struct {
	kind                      Kind
	account, stream, consumer string
}

type groupAcc struct {
	group   Group
	holders []string
	led     bool
}

// merge builds a Snapshot from the roster and each answering server's JSZ,
// keyed by server name. A group's members come from the server that
// reports itself its leader.
func merge(roster []Server, reports map[string]*wireJSInfo) *Snapshot {
	snap := &Snapshot{Servers: roster}
	inRoster := make(map[string]bool, len(roster))
	for _, s := range roster {
		inRoster[s.Name] = true
		if _, ok := reports[s.Name]; !ok {
			snap.Silent = append(snap.Silent, s.Name)
		}
	}

	groups := map[groupKey]*groupAcc{}
	see := func(k groupKey, server, raftGroup string, c *wireCluster, p *Placement) {
		a := groups[k]
		if a == nil {
			a = &groupAcc{group: Group{
				Kind: k.kind, Account: k.account, Stream: k.stream, Consumer: k.consumer,
				RaftGroup: raftGroup, Placement: p,
			}}
			groups[k] = a
		}
		a.holders = append(a.holders, server)
		if c.Leader != server {
			if !a.led && c.Leader != "" {
				a.group.NamedLeader = c.Leader
			}
			return
		}
		a.led = true
		a.group.Leader = server
		a.group.NamedLeader = ""
		a.group.Members = leaderView(server, c.Replicas, nil)
	}

	var metaLeader string
	metaNamed := map[string]string{}
	for server, info := range reports {
		if info == nil {
			continue
		}
		if info.Meta != nil {
			metaNamed[server] = info.Meta.Leader
			if info.Meta.Leader == server {
				metaLeader = server
				groups[groupKey{kind: KindMeta}] = &groupAcc{led: true, group: Group{
					Kind: KindMeta, Leader: server,
					Members: leaderView(server, info.Meta.Replicas, inRoster),
				}}
			}
		}
		for _, acc := range info.Accounts {
			for _, st := range acc.Streams {
				p := placementOf(st.Config)
				if st.Cluster != nil {
					see(groupKey{KindStream, acc.ID, st.Name, ""}, server, st.Cluster.RaftGroup, st.Cluster, p)
				}
				for _, co := range st.Consumers {
					if co.Cluster != nil {
						see(groupKey{KindConsumer, acc.ID, st.Name, co.Name}, server, co.Cluster.RaftGroup, co.Cluster, p)
					}
				}
			}
		}
	}
	if metaLeader == "" && len(metaNamed) > 0 {
		groups[groupKey{kind: KindMeta}] = &groupAcc{group: metaFromFollowers(metaNamed)}
	}

	for _, a := range groups {
		if !a.led {
			slices.Sort(a.holders)
			for _, h := range slices.Compact(a.holders) {
				a.group.Members = append(a.group.Members, Member{Server: h})
			}
		}
		snap.Groups = append(snap.Groups, a.group)
	}
	slices.SortFunc(snap.Groups, compareGroups)
	return snap
}

// leaderView lists the leader as current and online, then its replicas,
// keeping only those in keep when keep is not nil.
func leaderView(leader string, replicas []wirePeer, keep map[string]bool) []Member {
	ms := []Member{{Server: leader, Current: true}}
	for _, r := range replicas {
		if keep != nil && !keep[r.Name] {
			continue
		}
		ms = append(ms, Member{Server: r.Name, Current: r.Current, Offline: r.Offline, Lag: r.Lag})
	}
	slices.SortFunc(ms, func(a, b Member) int { return cmp.Compare(a.Server, b.Server) })
	return ms
}

func placementOf(c *wireStreamConfig) *Placement {
	if c == nil || c.Placement == nil {
		return nil
	}
	return &Placement{Cluster: c.Placement.Cluster, Tags: c.Placement.Tags}
}

// metaFromFollowers judges the meta group when its leader is not among
// the servers that answered, as happens when it sits in another NATS
// cluster of the supercluster: the group has that leader if every server
// that answered names the same one, and a server is current if it names
// it.
func metaFromFollowers(named map[string]string) Group {
	g := Group{Kind: KindMeta}
	leader, agreed := "", true
	for _, l := range named {
		switch {
		case l == "":
			agreed = false
		case leader == "":
			leader = l
		case l != leader:
			agreed = false
		}
	}
	if agreed {
		g.Leader = leader
	} else {
		g.NamedLeader = leader
	}
	for server, l := range named {
		g.Members = append(g.Members, Member{Server: server, Current: agreed && l == leader})
	}
	slices.SortFunc(g.Members, func(a, b Member) int { return cmp.Compare(a.Server, b.Server) })
	return g
}

func compareGroups(a, b Group) int {
	rank := func(k Kind) int {
		if k == KindMeta {
			return 0
		}
		return 1
	}
	return cmp.Or(
		cmp.Compare(rank(a.Kind), rank(b.Kind)),
		cmp.Compare(a.Account, b.Account),
		cmp.Compare(a.Stream, b.Stream),
		cmp.Compare(a.Consumer, b.Consumer),
	)
}
