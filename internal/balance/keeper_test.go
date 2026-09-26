package balance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// fake is a NATS cluster held in memory, and the three seams a Keeper acts
// through: a move is applied where the next observation reads it.
type fake struct {
	obs Observation
	err error
	// unreachable is the accounts whose leaders cannot be moved.
	unreachable []string
	// worst has a placement move land on the heaviest eligible server rather
	// than the lightest.
	worst bool

	leads  []LeaderMove
	places []PlacementMove
	log    []string
}

func (f *fake) Observe(context.Context) (Observation, error) {
	obs := f.obs
	obs.Groups = slices.Clone(f.obs.Groups)
	return obs, f.err
}

func (f *fake) CanMove(account string) bool { return !slices.Contains(f.unreachable, account) }

func (f *fake) MoveLeader(_ context.Context, m LeaderMove) error {
	f.leads, f.log = append(f.leads, m), append(f.log, "lead")
	for i, g := range f.obs.Groups {
		if g.String() == m.Group.String() {
			f.obs.Groups[i] = g.ledBy(m.To)
		}
	}
	return nil
}

func (f *fake) MovePlacement(_ context.Context, m PlacementMove) error {
	f.places, f.log = append(f.places, m), append(f.log, "place")
	holds := map[string]int{}
	for _, g := range f.obs.Groups {
		if g.Consumer == "" {
			for _, h := range g.Holders() {
				holds[h]++
			}
		}
	}
	landings := Landings(m.Group, f.obs.Servers, f.obs.Cluster)
	pick := slices.MinFunc[[]string]
	if f.worst {
		pick = slices.MaxFunc[[]string]
	}
	to := pick(landings, func(a, b string) int { return holds[a] - holds[b] })
	for i, g := range f.obs.Groups {
		if g.ID() != m.Group.ID() {
			continue
		}
		holders := append(slices.DeleteFunc(g.Holders(), func(h string) bool { return h == m.From }), to)
		f.obs.Groups[i] = group(g.Stream, holders[0], holders...)
		f.obs.Groups[i].Account, f.obs.Groups[i].Consumer = g.Account, g.Consumer
	}
	return nil
}

func keeper(f *fake) *Keeper { return &Keeper{Observer: f, Leaders: f, Placement: f} }

// replicated is n streams of account on the first three of servers, each with
// one consumer, every group led from servers[0].
func replicated(account string, n int, servers ...string) []Group {
	var out []Group
	for i := range n {
		g := group(fmt.Sprintf("s%d", i), servers[0], servers[:3]...)
		g.Account = account
		c := g
		c.Consumer = "c"
		out = append(out, g, c)
	}
	return out
}

// passUntilStill runs passes until one makes no move, and returns it.
func passUntilStill(t *testing.T, k *Keeper) (Passed, int) {
	t.Helper()
	for n := 1; n <= 100; n++ {
		got, err := k.Pass(context.Background())
		require.NoError(t, err)
		require.False(t, got.Moved != nil && got.Placed != nil, "a pass makes one move at most")
		if got.Moved == nil && got.Placed == nil {
			return got, n
		}
	}
	t.Fatal("the keeper never came to rest")
	return Passed{}, 0
}

func TestKeeper_MakesOneMoveAPassUntilEven(t *testing.T) {
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster("n0", "n1", "n2"), Groups: replicated("a", 6, "n0", "n1", "n2")}}
	got, passes := passUntilStill(t, keeper(f))
	require.Equal(t, 9, passes, "six streams and six consumers led from n0 are four moves each from 2/2/2")
	require.Len(t, f.leads, 8)
	require.Empty(t, f.places, "three copies on three servers leaves nothing to place")
	require.Equal(t, []Load{{"n0", 4, 6}, {"n1", 4, 6}, {"n2", 4, 6}}, got.Servers)
	require.Zero(t, got.LeaderSkew)
	require.Equal(t, []PoolReport{{Name: DefaultPool, Streams: 6}}, got.Pools)
}

func TestKeeper_PlacesAheadOfLeadersAndEndsEven(t *testing.T) {
	servers := []string{"n0", "n1", "n2", "n3", "n4"}
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster(servers...), Groups: replicated("a", 5, servers...)}}
	k := keeper(f)

	first, err := k.Pass(context.Background())
	require.NoError(t, err)
	require.NotNil(t, first.Placed, "a placement move goes first")
	require.Equal(t, []PoolReport{{Name: DefaultPool, Streams: 5, LeaderSkew: 5, PendingLeaders: 6, Misplaced: 6}}, first.Pools)

	got, _ := passUntilStill(t, k)
	require.Zero(t, got.Pools[0].Misplaced)
	require.Zero(t, got.Pools[0].PendingLeaders)
	require.Zero(t, got.CopySkew, "fifteen copies over five servers is three apiece")
	require.Equal(t, "place", f.log[0])
	require.False(t, slices.Contains(f.log[slices.Index(f.log, "lead"):], "place"), "no placement move follows a leader move: %v", f.log)
}

func TestKeeper_TriesAnUnsureMoveOnceAStreamUntilThePoolImproves(t *testing.T) {
	servers := roster("n0", "n1", "n2", "n3", "n4")
	f := &fake{worst: true, obs: Observation{Cluster: "c1", Servers: servers, Groups: []Group{
		group("a", "n0", "n0"), group("b", "n0", "n0"), group("c", "n0", "n0"),
		group("d", "n1", "n1"), group("e", "n1", "n1"),
		group("f", "n2", "n2"), group("g", "n2", "n2"),
		group("h", "n3", "n3"), group("i", "n3", "n3"),
	}}}
	k := &Keeper{Observer: f, Placement: f}
	got, _ := passUntilStill(t, k)
	require.Equal(t, 1, got.Pools[0].Misplaced, "a server that always picks badly leaves it one copy short")
	var moved []string
	for _, p := range f.places {
		require.True(t, p.Unsure)
		require.NotContains(t, moved, p.Group.Stream, "no stream is spent twice")
		moved = append(moved, p.Group.Stream)
	}
	require.NotEmpty(t, moved)
}

func TestKeeper_Holds(t *testing.T) {
	obs := Observation{Cluster: "c1", Servers: roster("n0", "n1", "n2"), Groups: replicated("a", 3, "n0", "n1", "n2")}
	for _, tc := range []struct {
		name   string
		mutate func(*fake, *Keeper)
		want   Passed
		err    bool
	}{
		{name: "unsettled holds everything", want: Passed{Held: "n2 is offline for a/s0"}, mutate: func(f *fake, _ *Keeper) {
			f.obs.Unsettled = "n2 is offline for a/s0"
		}},
		{name: "a failed observation fails the pass", err: true, mutate: func(f *fake, _ *Keeper) {
			f.err = errors.New("no meta leader")
		}},
		{name: "a dry run plans and moves nothing", mutate: func(_ *fake, k *Keeper) { k.DryRun = true }},
		{name: "no movers move nothing", mutate: func(_ *fake, k *Keeper) { k.Leaders, k.Placement = nil, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fake{obs: obs}
			f.obs.Groups = slices.Clone(obs.Groups)
			k := keeper(f)
			tc.mutate(f, k)
			got, err := k.Pass(context.Background())
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Nil(t, got.Moved)
			require.Nil(t, got.Placed)
			require.Empty(t, f.leads)
			require.Empty(t, f.places)
			if tc.want.Held != "" {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestKeeper_ReportsWhatItWouldMoveOnADryRun(t *testing.T) {
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster("n0", "n1", "n2"), Groups: replicated("a", 3, "n0", "n1", "n2")}}
	k := keeper(f)
	k.DryRun = true
	got, err := k.Pass(context.Background())
	require.NoError(t, err)
	require.Equal(t, []PoolReport{{Name: DefaultPool, Streams: 3, LeaderSkew: 3, PendingLeaders: 4}}, got.Pools)
}

func TestKeeper_YieldedStreamsAreCountedAndNeverMoved(t *testing.T) {
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster("n0", "n1", "n2"), Groups: replicated("a", 6, "n0", "n1", "n2")}}
	k := keeper(f)
	k.Yield = func(id StreamID) string {
		if id.Stream == "s0" {
			return "a placement move is pending from NatsSystemBalancer demo"
		}
		return ""
	}
	got, _ := passUntilStill(t, k)
	for _, m := range f.leads {
		require.NotEqual(t, "s0", m.Group.Stream)
	}
	require.Equal(t, []string{"a/s0: a placement move is pending from NatsSystemBalancer demo"}, got.Yielded)
	require.Equal(t, 4, got.Servers[0].Leaders, "n0 keeps s0 and its consumer and is still even")
}

func TestKeeper_LeavesAnAccountItCannotReach(t *testing.T) {
	servers := []string{"n0", "n1", "n2"}
	f := &fake{unreachable: []string{"b"}, obs: Observation{Cluster: "c1", Servers: roster(servers...),
		Groups: append(replicated("a", 3, servers...), replicated("b", 3, servers...)...)}}
	got, _ := passUntilStill(t, keeper(f))
	require.NotEmpty(t, f.leads)
	for _, m := range f.leads {
		require.Equal(t, "a", m.Group.Account)
	}
	require.Equal(t, []string{"b"}, got.Unreachable)
}

func TestKeeper_EvensEachDeclaredPoolApart(t *testing.T) {
	servers := []string{"n0", "n1", "n2"}
	groups := replicated("a", 6, servers...)
	for i := 0; i < 6; i += 2 {
		groups[i] = group(groups[i].Stream, "n1", servers...)
	}
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster(servers...), Groups: groups}}
	k := keeper(f)
	k.Pools = Declared("a", []Pool{{Name: "first", Streams: []StreamID{{"a", "s0"}, {"a", "s1"}, {"a", "s2"}}}})
	got, _ := passUntilStill(t, k)
	require.Len(t, got.Pools, 2)
	for _, p := range got.Pools {
		require.LessOrEqual(t, p.LeaderSkew, 1, p.Name)
		require.Equal(t, 3, p.Streams, p.Name)
	}
}

type failing struct{ *fake }

func (failing) MovePlacement(context.Context, PlacementMove) error { return errors.New("refused") }

func TestKeeper_AFailedMoveIsTheError(t *testing.T) {
	servers := []string{"n0", "n1", "n2", "n3", "n4"}
	f := &fake{obs: Observation{Cluster: "c1", Servers: roster(servers...), Groups: replicated("a", 5, servers...)}}
	k := &Keeper{Observer: f, Placement: failing{f}}
	got, err := k.Pass(context.Background())
	require.ErrorContains(t, err, "refused")
	require.Nil(t, got.Placed)
	require.Equal(t, 6, got.Pools[0].Misplaced)
}
