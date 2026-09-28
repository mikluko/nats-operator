package balance

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

// A Balancer is a system or account balancer's passes over one NATS cluster.
// It makes at most one move a pass and none while the NATS cluster is not
// Settled, so every move is followed by a Settled reading before the next: a
// leader move starts an election, and a second one started into it is an
// outage.
//
// A Balancer is not safe for concurrent use.
type Balancer struct {
	Observer Observer
	// Pools partitions each observation; nil is [WholeCluster].
	Pools Pooler
	// Leaders makes leader moves; nil makes none.
	Leaders LeaderMover
	// Placement makes placement moves; nil makes none. A pass makes a
	// placement move ahead of any leader move.
	Placement PlacementMover
	// Yield returns why a stream is not this balancer's to move, and "" where it
	// is: a move pending on it from another balancer, or an evacuation. A
	// yielded stream is still counted.
	Yield func(StreamID) string
	// DryRun plans and reports, and moves nothing.
	DryRun bool

	// tried is, per pool, the streams spent on an unsure placement move since
	// the pool's unevenness last fell, and uneven the least unevenness each
	// pool has been read at: the least, because a stream mid-move reads more
	// uneven than it was.
	tried  map[string]map[StreamID]bool
	uneven map[string]int
}

// Passed is what one pass found and did.
type Passed struct {
	// Held is why the pass moved nothing because the NATS cluster was not
	// Settled; nothing else is reported with it.
	Held string
	// Moved is the leader move the pass made and Placed the placement move;
	// at most one is set.
	Moved  *LeaderMove
	Placed *PlacementMove
	// Servers is each roster server's load across every observed group.
	Servers []Load
	// LeaderSkew and CopySkew are the spread of Servers: the most leaders, or
	// copies, one server carries less the fewest another does.
	LeaderSkew, CopySkew int
	Pools                []PoolReport
	// Yielded names each pooled stream [Balancer.Yield] kept from moving, with why.
	Yielded []string
	// Unreachable is the accounts in the pools whose leaders the leader mover
	// cannot move, sorted.
	Unreachable []string
}

// A Load is what one server leads and holds.
type Load struct {
	Server string
	// Leaders counts stream and consumer groups alike.
	Leaders int
	// Copies counts stream copies.
	Copies int
}

// A PoolReport is one pool as a pass read it.
type PoolReport struct {
	Name    string
	Streams int
	// LeaderSkew is the spread of stream leaders over the roster.
	LeaderSkew int
	// PendingLeaders is how many leader moves would even the pool, and
	// Misplaced how many copies sit above an even share.
	PendingLeaders, Misplaced int
}

// passTimeout bounds one pass: a NATS cluster that has lost its meta leader
// answers nothing until it elects another.
const passTimeout = 10 * time.Second

// Pass observes the NATS cluster once and makes at most one move.
func (k *Balancer) Pass(ctx context.Context) (Passed, error) {
	ctx, cancel := context.WithTimeout(ctx, passTimeout)
	defer cancel()

	obs, err := k.Observer.Observe(ctx)
	if err != nil {
		return Passed{}, fmt.Errorf("observe: %w", err)
	}
	if obs.Unsettled != "" {
		return Passed{Held: obs.Unsettled}, nil
	}

	out := loads(obs)
	pools := k.Pools
	if pools == nil {
		pools = WholeCluster
	}
	yielded := map[StreamID]bool{}
	unreachable := map[string]bool{}
	var place *PlacementMove
	var placeTried map[StreamID]bool
	var lead *LeaderMove
	for _, p := range pools(obs.Groups) {
		streams, consumers := p.split(obs.Groups)
		for _, id := range p.Streams {
			if k.Yield == nil || yielded[id] {
				continue
			}
			if why := k.Yield(id); why != "" {
				yielded[id] = true
				out.Yielded = append(out.Yielded, fmt.Sprintf("%s: %s", id, why))
			}
		}
		report := PoolReport{Name: p.Name, Streams: len(streams), LeaderSkew: leaderSkew(streams, obs.Servers)}

		if k.Placement != nil {
			tried := k.triedIn(p.Name, streams)
			step, misplaced := PlanPlacement(streams, obs.Servers, obs.Cluster,
				func(g Group) bool { return yielded[g.ID()] },
				func(g Group) bool { return tried[g.ID()] })
			report.Misplaced = misplaced
			if place == nil && step != nil {
				place, placeTried = step, tried
			}
		}

		movable := func(g Group) bool {
			if k.Leaders == nil || yielded[g.ID()] {
				return false
			}
			if !k.Leaders.CanMove(ctx, g.Account) {
				unreachable[g.Account] = true
				return false
			}
			return true
		}
		for _, set := range [][]Group{streams, consumers} {
			moves := PlanLeaders(set, movable)
			report.PendingLeaders += len(moves)
			if lead == nil && len(moves) > 0 {
				lead = &moves[0]
			}
		}
		out.Pools = append(out.Pools, report)
	}
	out.Unreachable = slices.Sorted(maps.Keys(unreachable))

	switch {
	case k.DryRun:
	case place != nil:
		if err := k.Placement.MovePlacement(ctx, *place); err != nil {
			return out, err
		}
		if place.Unsure {
			placeTried[place.Group.ID()] = true
		}
		out.Placed = place
	case lead != nil:
		if err := k.Leaders.MoveLeader(ctx, *lead); err != nil {
			return out, err
		}
		out.Moved = lead
	}
	return out, nil
}

// triedIn is the pool's tried set, emptied when the pool reads more even than
// it ever has.
func (k *Balancer) triedIn(pool string, streams []Group) map[StreamID]bool {
	if k.tried == nil {
		k.tried, k.uneven = map[string]map[StreamID]bool{}, map[string]int{}
	}
	now := unevenness(streams)
	if least, seen := k.uneven[pool]; !seen || now < least {
		k.uneven[pool] = now
		k.tried[pool] = map[StreamID]bool{}
	}
	return k.tried[pool]
}

// unevenness is the sum over servers of the square of the copies each holds,
// which falls with every move that makes a pool more even and with no other.
func unevenness(streams []Group) int {
	holds := map[string]int{}
	for _, g := range streams {
		for _, h := range g.Holders() {
			holds[h]++
		}
	}
	var sum int
	for _, n := range holds {
		sum += n * n
	}
	return sum
}

// loads is each roster server's load over every group of obs, and its skew.
func loads(obs Observation) Passed {
	leaders, copies := map[string]int{}, map[string]int{}
	for _, g := range obs.Groups {
		leaders[g.Leader]++
		if g.Consumer == "" {
			for _, h := range g.Holders() {
				copies[h]++
			}
		}
	}
	var out Passed
	for _, s := range slices.SortedFunc(slices.Values(obs.Servers), func(a, b Server) int { return cmp.Compare(a.Name, b.Name) }) {
		out.Servers = append(out.Servers, Load{Server: s.Name, Leaders: leaders[s.Name], Copies: copies[s.Name]})
	}
	out.LeaderSkew = spread(out.Servers, func(l Load) int { return l.Leaders })
	out.CopySkew = spread(out.Servers, func(l Load) int { return l.Copies })
	return out
}

// leaderSkew is the spread of groups' leaders over roster.
func leaderSkew(groups []Group, roster []Server) int {
	leads := map[string]int{}
	for _, g := range groups {
		leads[g.Leader]++
	}
	var l []Load
	for _, s := range roster {
		l = append(l, Load{Leaders: leads[s.Name]})
	}
	return spread(l, func(l Load) int { return l.Leaders })
}

func spread(loads []Load, of func(Load) int) int {
	if len(loads) == 0 {
		return 0
	}
	lo, hi := of(loads[0]), of(loads[0])
	for _, l := range loads[1:] {
		lo, hi = min(lo, of(l)), max(hi, of(l))
	}
	return hi - lo
}
