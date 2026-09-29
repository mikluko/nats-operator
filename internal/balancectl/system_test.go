package balancectl

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
)

func TestMoves(t *testing.T) {
	yes, no := true, false
	tests := []struct {
		name string
		in   *js.Moves
		want movesOn
	}{
		{"unset", nil, movesOn{leader: true}},
		{"empty", &js.Moves{}, movesOn{leader: true}},
		{"placement on", &js.Moves{Placement: &yes}, movesOn{leader: true, placement: true}},
		{"leader off", &js.Moves{Leader: &no}, movesOn{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, moves(tt.in)) })
	}
}

func TestRecord(t *testing.T) {
	now := time.Date(2026, 9, 26, 11, 31, 2, 0, time.UTC)
	at := metav1.NewTime(now)
	earlier := js.Move{Kind: js.MoveLeader, Account: "A", Stream: "X", From: "s1", To: "s2"}
	group := balance.Group{Account: "A", Stream: "S", Leader: "s1"}
	loads := []balance.Load{{Server: "s1", Leaders: 3, Copies: 4}, {Server: "s2", Leaders: 1, Copies: 2}}
	tests := []struct {
		name     string
		passed   balance.Passed
		want     js.NatsSystemBalancerStatus
		keepLoad bool
	}{
		{
			name:     "held",
			passed:   balance.Passed{Held: "s2 is offline for A/S"},
			keepLoad: true,
			want:     js.NatsSystemBalancerStatus{Pending: []js.Move{earlier}},
		},
		{
			name:   "no move",
			passed: balance.Passed{Servers: loads, LeaderSkew: 2, CopySkew: 2},
			want: js.NatsSystemBalancerStatus{
				Servers: []js.ServerLoad{{Name: "s1", Leaders: 3, Replicas: 4}, {Name: "s2", Leaders: 1, Replicas: 2}},
				Skew:    &js.Skew{Leaders: 2, Replicas: 2},
				Pending: []js.Move{earlier},
			},
		},
		{
			name:   "consumer leader move",
			passed: balance.Passed{Servers: loads, LeaderSkew: 2, CopySkew: 2, Moved: &balance.LeaderMove{Group: balance.Group{Account: "A", Stream: "S", Consumer: "D", Leader: "s1"}, To: "s2"}},
			want: js.NatsSystemBalancerStatus{
				Servers:  []js.ServerLoad{{Name: "s1", Leaders: 3, Replicas: 4}, {Name: "s2", Leaders: 1, Replicas: 2}},
				Skew:     &js.Skew{Leaders: 2, Replicas: 2},
				LastMove: &js.Move{Kind: js.MoveLeader, Account: "A", Stream: "S", Consumer: "D", From: "s1", To: "s2", Time: &at},
				Pending:  []js.Move{earlier, {Kind: js.MoveLeader, Account: "A", Stream: "S", Consumer: "D", From: "s1", To: "s2", Time: &at}},
			},
		},
		{
			name:   "placement move",
			passed: balance.Passed{Servers: loads, LeaderSkew: 2, CopySkew: 2, Placed: &balance.PlacementMove{Group: group, From: "s1", Cluster: "C1"}},
			want: js.NatsSystemBalancerStatus{
				Servers:  []js.ServerLoad{{Name: "s1", Leaders: 3, Replicas: 4}, {Name: "s2", Leaders: 1, Replicas: 2}},
				Skew:     &js.Skew{Leaders: 2, Replicas: 2},
				LastMove: &js.Move{Kind: js.MovePlacement, Account: "A", Stream: "S", From: "s1", Time: &at},
				Pending:  []js.Move{earlier, {Kind: js.MovePlacement, Account: "A", Stream: "S", From: "s1", Time: &at}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := js.NatsSystemBalancerStatus{Pending: []js.Move{earlier}}
			record(&st, tt.passed, now)
			require.Equal(t, tt.want, st)
		})
	}
}

func TestStillPending(t *testing.T) {
	leader := js.Move{Kind: js.MoveLeader, Account: "A", Stream: "S", From: "s1", To: "s2"}
	placement := js.Move{Kind: js.MovePlacement, Account: "A", Stream: "S", From: "s1"}
	on := func(unsettled string, holders ...string) balance.Observation {
		g := balance.Group{Account: "A", Stream: "S", Leader: holders[0]}
		for _, h := range holders[1:] {
			g.Members = append(g.Members, balance.Member{Name: h})
		}
		return balance.Observation{Unsettled: unsettled, Groups: []balance.Group{g, {Account: "A", Stream: "S", Consumer: "D", Leader: "s1"}}}
	}
	tests := []struct {
		name string
		obs  balance.Observation
		want []js.Move
	}{
		{"unsettled keeps a leader move", on("electing", "s2", "s1"), []js.Move{leader, placement}},
		{"settled ends a leader move", on("", "s2", "s1", "s3"), []js.Move{placement}},
		{"a placement move ends when its server lets go", on("s4 is 9000 behind for A/S", "s2", "s3", "s4"), []js.Move{leader}},
		{"a vanished stream ends its move", balance.Observation{Unsettled: "x"}, []js.Move{leader}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stillPending([]js.Move{leader, placement}, tt.obs))
		})
	}
}

func TestCapabilities(t *testing.T) {
	obs := balance.Observation{Groups: []balance.Group{
		{Account: "A", Stream: "S"}, {Account: "A", Stream: "T"}, {Account: "B", Stream: "S"}, {Account: "C", Stream: "S"},
	}}
	tests := []struct {
		name  string
		reach map[string]bool
		want  *js.Capabilities
	}{
		{"full", map[string]bool{"A": true, "B": true, "C": true}, &js.Capabilities{Placement: true, Leader: js.LeaderCapabilityFull}},
		{"partial", map[string]bool{"A": true}, &js.Capabilities{
			Placement: true, Leader: js.LeaderCapabilityPartial,
			LeaderReason: "2 of 3 accounts carry no jetstream-stepdown export; their leaders are not moved",
		}},
		{"none", map[string]bool{"A": false, "B": false, "C": false}, &js.Capabilities{
			Placement: true, Leader: js.LeaderCapabilityNone,
			LeaderReason: "3 of 3 accounts carry no jetstream-stepdown export; their leaders are not moved",
		}},
	}
	t.Run("leader moves off", func(t *testing.T) {
		require.Equal(t, &js.Capabilities{Placement: true}, capabilities(t.Context(), obs, nil))
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &stepdownReach{known: map[string]bool{}}
			for _, a := range []string{"A", "B", "C"} {
				r.known[a] = tt.reach[a]
			}
			require.Equal(t, tt.want, capabilities(t.Context(), obs, r))
		})
	}
}

func TestCompareAge(t *testing.T) {
	at := func(sec int64, ns, name string) js.NatsSystemBalancer {
		b := js.NatsSystemBalancer{}
		b.CreationTimestamp, b.Namespace, b.Name = metav1.Unix(sec, 0), ns, name
		return b
	}
	got := []js.NatsSystemBalancer{at(2, "a", "a"), at(1, "b", "b"), at(1, "b", "a"), at(1, "a", "z")}
	slices.SortFunc(got, compareAge)
	var names []string
	for _, b := range got {
		names = append(names, b.Namespace+"/"+b.Name)
	}
	require.Equal(t, []string{"a/z", "b/a", "b/b", "a/a"}, names)
}
