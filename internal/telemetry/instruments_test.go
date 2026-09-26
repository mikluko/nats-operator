package telemetry

import (
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func cond(typ string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: typ, Status: status, Reason: reason, LastTransitionTime: metav1.Now()}
}

func om(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: "ns", Name: name} }

// fixtures are one or two resources of each kind whose status an
// instrument reads.
func fixtures() []client.Object {
	return []client.Object{
		&clusterv1beta1.NatsCluster{ObjectMeta: om("rolling"), Status: clusterv1beta1.NatsClusterStatus{
			Conditions: []metav1.Condition{
				cond("Ready", metav1.ConditionTrue, "AllServersReady"),
				cond("Progressing", metav1.ConditionTrue, "RollingRestart"),
			},
			Rollout: &clusterv1beta1.RolloutStatus{
				Current: "demo-2",
				Pending: []string{"demo-1", "demo-0"},
				Gate:    &clusterv1beta1.RolloutGate{WaitingFor: "Settled"},
			},
		}},
		&clusterv1beta1.NatsCluster{ObjectMeta: om("idle")},
		&js.NatsBalancer{ObjectMeta: om("orders"), Status: js.NatsBalancerStatus{
			Conditions: []metav1.Condition{cond("Holding", metav1.ConditionTrue, "YieldingToSystemBalancer")},
			Pools:      []js.PoolStatus{{Name: "fast", LeaderSkew: 3}, {Name: "(default)", LeaderSkew: 1}},
		}},
		&js.NatsSystemBalancer{ObjectMeta: om("c1"), Status: js.NatsSystemBalancerStatus{
			Skew:    &js.Skew{Leaders: 2, Replicas: 5},
			Pending: []js.Move{{Kind: js.MoveLeader}, {Kind: js.MovePlacement}, {Kind: js.MovePlacement}},
		}},
		&js.NatsClusterEvacuation{ObjectMeta: om("retire"), Status: js.NatsClusterEvacuationStatus{
			Conditions:     []metav1.Condition{cond("Ready", metav1.ConditionFalse, "Moving")},
			Remaining:      4,
			StalePlacement: []js.ServerStream{{Account: "A", Name: "S1"}, {Account: "A", Name: "S2"}},
		}},
		&js.NatsStream{ObjectMeta: om("orders"), Status: js.NatsStreamStatus{SyncStatus: js.SyncStatus{
			Conditions: []metav1.Condition{cond("Ready", metav1.ConditionTrue, "Synced")},
		}}},
		&authv1beta1.NatsUser{ObjectMeta: om("svc"), Status: authv1beta1.NatsUserStatus{
			Conditions: []metav1.Condition{cond("Ready", metav1.ConditionFalse, "Revoking")},
		}},
		&natsv1beta1.NatsConnection{ObjectMeta: om("c1"), Status: natsv1beta1.NatsConnectionStatus{
			Conditions: []metav1.Condition{cond("Ready", metav1.ConditionUnknown, "Connecting")},
		}},
	}
}

func fakeReader(t *testing.T) client.Reader {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme, authv1beta1.AddToScheme, js.AddToScheme,
	} {
		require.NoError(t, add(s))
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(fixtures()...).Build()
}

// collected is one controller's instruments as a collection reads them:
// by name, the unit and each point's value by its attributes.
type collected map[string]struct {
	unit   string
	points map[string]int64
}

func collect(t *testing.T, register func(metric.Meter) error, after func()) collected {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })
	require.NoError(t, register(mp.Meter("test")))
	if after != nil {
		after()
	}
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	out := collected{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			points := map[string]int64{}
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					points[key(p.Attributes.ToSlice())] = p.Value
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					points[key(p.Attributes.ToSlice())] = p.Value
				}
			default:
				t.Fatalf("%s is a %T", m.Name, m.Data)
			}
			out[m.Name] = struct {
				unit   string
				points map[string]int64
			}{m.Unit, points}
		}
	}
	return out
}

// key is attrs as k=v pairs, sorted and joined by commas.
func key(attrs []attribute.KeyValue) string {
	pairs := make([]string, len(attrs))
	for i, a := range attrs {
		pairs[i] = string(a.Key) + "=" + a.Value.AsString()
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// registered collects what each controller registers over the fixtures,
// with two held passes and one settled pass of the NatsBalancer recorded.
func registered(t *testing.T) map[string]collected {
	t.Helper()
	r := fakeReader(t)
	var j *JetStream
	return map[string]collected{
		ClusterController: collect(t, func(m metric.Meter) error { return RegisterCluster(m, r) }, nil),
		AuthController:    collect(t, func(m metric.Meter) error { return RegisterAuth(m, r) }, nil),
		JetStreamController: collect(t, func(m metric.Meter) error {
			var err error
			j, err = RegisterJetStream(m, r)
			return err
		}, func() {
			b := &js.NatsBalancer{ObjectMeta: om("orders")}
			holding := []metav1.Condition{cond("Holding", metav1.ConditionTrue, "YieldingToSystemBalancer")}
			j.BalancerPass(t.Context(), "NatsBalancer", b, holding)
			j.BalancerPass(t.Context(), "NatsBalancer", b, holding)
			j.BalancerPass(t.Context(), "NatsBalancer", b, []metav1.Condition{cond("Holding", metav1.ConditionFalse, "Settled")})
		}),
	}
}

// TestInstruments pins each instrument's value, read from the fixtures'
// status, through an in-memory reader.
func TestInstruments(t *testing.T) {
	got := registered(t)
	tests := []struct {
		controller string
		instrument Instrument
		attrs      map[string]string
		want       int64
	}{
		{ClusterController, RolloutPendingServers, map[string]string{"kind": "NatsCluster", "namespace": "ns", "name": "rolling"}, 2},
		{ClusterController, RolloutPendingServers, map[string]string{"kind": "NatsCluster", "namespace": "ns", "name": "idle"}, 0},
		{ClusterController, RolloutGate, map[string]string{"kind": "NatsCluster", "namespace": "ns", "name": "rolling", "waiting_for": "Settled"}, 1},
		{ClusterController, Condition, map[string]string{"kind": "NatsCluster", "namespace": "ns", "name": "rolling", "type": "Progressing", "reason": "RollingRestart"}, 1},
		{AuthController, Condition, map[string]string{"kind": "NatsUser", "namespace": "ns", "name": "svc", "type": "Ready", "reason": "Revoking"}, 0},
		{JetStreamController, BalancerLeaderSkew, map[string]string{"kind": "NatsBalancer", "namespace": "ns", "name": "orders", "pool": "fast"}, 3},
		{JetStreamController, BalancerLeaderSkew, map[string]string{"kind": "NatsBalancer", "namespace": "ns", "name": "orders", "pool": "(default)"}, 1},
		{JetStreamController, BalancerLeaderSkew, map[string]string{"kind": "NatsSystemBalancer", "namespace": "ns", "name": "c1"}, 2},
		{JetStreamController, BalancerPendingMoves, map[string]string{"kind": "NatsSystemBalancer", "namespace": "ns", "name": "c1", "move_kind": "Leader"}, 1},
		{JetStreamController, BalancerPendingMoves, map[string]string{"kind": "NatsSystemBalancer", "namespace": "ns", "name": "c1", "move_kind": "Placement"}, 2},
		{JetStreamController, BalancerHeldPasses, map[string]string{"kind": "NatsBalancer", "namespace": "ns", "name": "orders", "reason": "YieldingToSystemBalancer"}, 2},
		{JetStreamController, EvacuationRemaining, map[string]string{"kind": "NatsClusterEvacuation", "namespace": "ns", "name": "retire"}, 4},
		{JetStreamController, EvacuationStalePlacements, map[string]string{"kind": "NatsClusterEvacuation", "namespace": "ns", "name": "retire"}, 2},
		{JetStreamController, Condition, map[string]string{"kind": "NatsStream", "namespace": "ns", "name": "orders", "type": "Ready", "reason": "Synced"}, 1},
		{JetStreamController, Condition, map[string]string{"kind": "NatsConnection", "namespace": "ns", "name": "c1", "type": "Ready", "reason": "Connecting"}, 0},
		{JetStreamController, Condition, map[string]string{"kind": "NatsClusterEvacuation", "namespace": "ns", "name": "retire", "type": "Ready", "reason": "Moving"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.instrument.Name+" "+attrKey(tt.attrs), func(t *testing.T) {
			m, ok := got[tt.controller][tt.instrument.Name]
			require.True(t, ok, "%s registers no %s", tt.controller, tt.instrument.Name)
			v, ok := m.points[attrKey(tt.attrs)]
			require.True(t, ok, "no point at %s among %v", attrKey(tt.attrs), m.points)
			require.Equal(t, tt.want, v)
		})
	}
	require.Len(t, got[JetStreamController][BalancerHeldPasses.Name].points, 1, "a settled pass was counted")
	require.NotContains(t, got[ClusterController][RolloutGate.Name].points,
		attrKey(map[string]string{"kind": "NatsCluster", "namespace": "ns", "name": "idle", "waiting_for": ""}))
}

// TestInstrumentsListed pins Instruments, which the telemetry page lists,
// to what each controller registers: the same names and units, and no
// attribute an instrument's entry does not name.
func TestInstrumentsListed(t *testing.T) {
	got := registered(t)
	for _, controller := range []string{ClusterController, AuthController, JetStreamController} {
		var want []string
		for _, in := range Instruments {
			if !slices.Contains(in.Controllers, controller) {
				continue
			}
			want = append(want, in.Name)
			m, ok := got[controller][in.Name]
			require.True(t, ok, "%s does not register %s", controller, in.Name)
			require.Equal(t, in.Unit, m.unit, in.Name)
			for k := range m.points {
				for _, pair := range strings.Split(k, ",") {
					attr, _, _ := strings.Cut(pair, "=")
					require.Contains(t, in.Attributes, attr, "%s carries %s", in.Name, attr)
				}
			}
		}
		var names []string
		for name := range got[controller] {
			names = append(names, name)
		}
		require.ElementsMatch(t, want, names, controller)
	}
}

// attrKey is kvs as key renders a point's attributes.
func attrKey(kvs map[string]string) string {
	pairs := make([]string, 0, len(kvs))
	for k, v := range kvs {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}
