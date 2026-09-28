package streamctl

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

func TestStreamTransferFromClusterInfo(t *testing.T) {
	started := time.Date(2026, 9, 26, 12, 4, 10, 0, time.UTC)
	for _, tc := range []struct {
		name string
		info string
		want *js.StreamTransfer
	}{
		{
			name: "at its placement",
			info: `{"name":"east","leader":"east-0","replicas":[{"name":"east-1","current":true}]}`,
		},
		{
			name: "scaling within its cluster",
			info: `{"name":"east","leader":"east-0","desired":{"created":"2026-09-26T12:04:10Z","name":"east","replicas":[{"name":"east-0"},{"name":"east-1"},{"name":"east-2"}]}}`,
		},
		{
			name: "moving from the cluster its origin placement names",
			info: `{"name":"east","leader":"east-1","replicas":[
				{"name":"east-0","current":true},{"name":"east-2","current":true},
				{"name":"west-1","current":true},{"name":"west-0","current":true},{"name":"west-2","current":false,"lag":18231}],
				"desired":{"created":"2026-09-26T12:04:10Z","name":"west","replicas":[{"name":"west-2"},{"name":"west-0"},{"name":"west-1"}],
				"origin":{"replicas":3,"placement":{"cluster":"east"}}}}`,
			want: &js.StreamTransfer{From: "east", To: "west", Started: &metav1.Time{Time: started}, Replicas: []js.ReplicaStatus{
				{Name: "west-0", Current: true}, {Name: "west-1", Current: true}, {Name: "west-2", Lag: 18231},
			}},
		},
		{
			name: "moving with no origin placement, a new replica not yet in the group",
			info: `{"name":"east","leader":"east-0","replicas":[{"name":"east-1","current":true},{"name":"west-1","current":true}],
				"desired":{"created":"2026-09-26T12:04:10Z","name":"west","replicas":[{"name":"west-0"},{"name":"west-1"}]}}`,
			want: &js.StreamTransfer{From: "east", To: "west", Started: &metav1.Time{Time: started}, Replicas: []js.ReplicaStatus{
				{Name: "west-0"}, {Name: "west-1", Current: true},
			}},
		},
		{
			name: "moving with the leader already in the new cluster",
			info: `{"name":"west","leader":"west-0","replicas":[{"name":"east-1","current":true},{"name":"west-1","current":true}],
				"desired":{"created":"2026-09-26T12:04:10Z","name":"west","replicas":[{"name":"west-0"},{"name":"west-1"}],
				"origin":{"replicas":2,"placement":{"cluster":"east"}}}}`,
			want: &js.StreamTransfer{From: "east", To: "west", Started: &metav1.Time{Time: started}, Replicas: []js.ReplicaStatus{
				{Name: "west-0", Current: true}, {Name: "west-1", Current: true},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c clusterWire
			require.NoError(t, json.Unmarshal([]byte(tc.info), &c))
			require.Equal(t, tc.want, streamTransfer(&c))
		})
	}
	require.Nil(t, streamTransfer(nil))
}

func TestConsumersMoved(t *testing.T) {
	var consumers []clusterWire
	require.NoError(t, json.Unmarshal([]byte(`[
		{"name":"west"},
		{"name":"west","desired":{"name":"west"}},
		{"name":"east","desired":{"name":"west"}},
		{"name":"east"}]`), &consumers))
	require.Equal(t, &js.TransferConsumers{Moved: 1, Total: 4}, consumersMoved(consumers, "west"))
	require.Equal(t, &js.TransferConsumers{}, consumersMoved(nil, "west"))
}

// TestStreamMove edits a NatsStream's placement.cluster on a two-cluster
// supercluster and follows the move the server makes, held in flight by
// stopping one new replica: the transfer block and Synced False with
// ReasonMoving while it is, rechecked on MovingRecheck, then neither once
// the stream serves from the new cluster.
func TestStreamMove(t *testing.T) {
	sc := startSupercluster(t, "east", "west")
	f := newFixtureOn(t, sc["east"])
	f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) {
		s.Replicas = ptrTo(int32(3))
		s.Storage = ptrTo(js.StorageFile)
		s.Placement = &js.Placement{Cluster: "east"}
	}))
	require.Eventually(t, func() bool {
		res, err := f.streams.Reconcile(t.Context(), requestFor("orders"))
		return err == nil && res.RequeueAfter == testResync
	}, 30*time.Second, 200*time.Millisecond, "the stream never settled in east")
	s := f.stream("orders")
	condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	require.Nil(t, s.Status.Transfer)
	require.True(t, strings.HasPrefix(s.Status.Server.Leader, "east-"), s.Status.Server.Leader)

	for _, name := range []string{"audit", "billing"} {
		_, err := f.js.CreateConsumer(t.Context(), "ORDERS", jetstreamConsumer(name))
		require.NoError(t, err)
	}
	payload := make([]byte, 4<<10)
	for i := range 20000 {
		f.js.PublishAsync(fmt.Sprintf("orders.%d", i%16), payload) //nolint:errcheck // completion is awaited below.
	}
	select {
	case <-f.js.PublishAsyncComplete():
	case <-time.After(time.Minute):
		t.Fatal("publishing did not complete")
	}

	before, err := f.js.Stream(t.Context(), "ORDERS")
	require.NoError(t, err)
	stored := before.CachedInfo().State.Msgs
	require.NotZero(t, stored)
	f.editStream("orders", func(s *js.NatsStreamSpec) { s.Placement = &js.Placement{Cluster: "west"} })
	f.reconcileStream("orders")
	restart := stallMove(t, f.js, sc["west"], "ORDERS", "west")
	var seen *js.NatsStream
	var recheck time.Duration
	require.Eventually(t, func() bool {
		res, err := f.streams.Reconcile(t.Context(), requestFor("orders"))
		if err != nil {
			return false
		}
		s := f.stream("orders")
		if s.Status.Transfer == nil || s.Status.Transfer.Consumers == nil {
			return false
		}
		seen, recheck = s, res.RequeueAfter
		return true
	}, 30*time.Second, 50*time.Millisecond, "no transfer was reported")
	require.Equal(t, lifecycle.MovingRecheck, recheck)
	tr := seen.Status.Transfer
	require.Equal(t, "east", tr.From)
	require.Equal(t, "west", tr.To)
	require.NotNil(t, tr.Started)
	require.Equal(t, []string{"west-0", "west-1", "west-2"}, []string{tr.Replicas[0].Name, tr.Replicas[1].Name, tr.Replicas[2].Name})
	require.Equal(t, &js.TransferConsumers{Moved: 0, Total: 2}, tr.Consumers)
	current := 0
	for _, r := range tr.Replicas {
		if r.Current {
			current++
		}
	}
	require.Less(t, current, 3, "a stopped replica is never current")
	synced := condition(t, seen.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionFalse, ReasonMoving)
	require.Equal(t, fmt.Sprintf("moving to cluster west; %d of 3 new replicas current", current), synced.Message)
	condition(t, seen.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	require.True(t, strings.HasPrefix(seen.Status.Server.Leader, "east-"), "the east copies lead until the move ends")
	require.EqualValues(t, 2, seen.Status.ObservedGeneration)

	restart()
	require.Eventually(t, func() bool {
		_, err := f.streams.Reconcile(t.Context(), requestFor("orders"))
		s := f.stream("orders")
		return err == nil && s.Status.Transfer == nil && s.Status.Server != nil && strings.HasPrefix(s.Status.Server.Leader, "west-")
	}, time.Minute, 200*time.Millisecond, "the move never finished")
	s = f.stream("orders")
	condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	for _, r := range s.Status.Server.Replicas {
		require.True(t, strings.HasPrefix(r.Name, "west-"), r.Name)
	}
	info, err := f.js.Stream(t.Context(), "ORDERS")
	require.NoError(t, err)
	require.Equal(t, stored, info.CachedInfo().State.Msgs, "every message moved")
	for _, name := range []string{"audit", "billing"} {
		c, err := f.js.Consumer(t.Context(), "ORDERS", name)
		require.NoError(t, err)
		require.Equal(t, "west", c.CachedInfo().Cluster.Name, name)
	}
}

// stallMove waits until stream is moving to NATS cluster to, then stops one
// of its new replicas in n so the move cannot finish, and returns what
// starts that server again.
func stallMove(t *testing.T, j jetstream.JetStream, n *testNATS, stream, to string) (restart func()) {
	t.Helper()
	var victim string
	require.Eventually(t, func() bool {
		msg, err := j.Conn().Request("$JS.API.STREAM.INFO."+stream, nil, 5*time.Second)
		if err != nil {
			return false
		}
		var info struct {
			Cluster clusterWire `json:"cluster"`
		}
		if json.Unmarshal(msg.Data, &info) != nil {
			return false
		}
		d := info.Cluster.Desired
		if d == nil || d.Name != to || len(d.Replicas) == 0 {
			return false
		}
		names := make([]string, 0, len(d.Replicas))
		for _, r := range d.Replicas {
			names = append(names, r.Name)
		}
		slices.Sort(names)
		victim = names[len(names)-1]
		return true
	}, 30*time.Second, 5*time.Millisecond, "%s never began moving to %s", stream, to)
	return n.stop(t, victim)
}

func requestFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name}}
}
