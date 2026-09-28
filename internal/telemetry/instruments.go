package telemetry

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/jwt/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Attribute keys the instruments and reconcile spans carry.
const (
	AttrKind       = "kind"
	AttrNamespace  = "namespace"
	AttrName       = "name"
	AttrType       = "type"
	AttrReason     = "reason"
	AttrPool       = "pool"
	AttrMoveKind   = "move_kind"
	AttrWaitingFor = "waiting_for"
)

// InstrumentType is what an Instrument is built as.
type InstrumentType int

const (
	// Gauge is an int64 observable gauge, read at each collection.
	Gauge InstrumentType = iota + 1
	// Counter is an int64 counter the reconcilers add to.
	Counter
)

// String is the type as the telemetry page names it.
func (t InstrumentType) String() string {
	switch t {
	case Gauge:
		return "gauge"
	case Counter:
		return "counter"
	default:
		return fmt.Sprintf("InstrumentType(%d)", int(t))
	}
}

// Instrument describes one instrument a controller registers: the metric
// it exports, the attributes on its points, and the status field it reads.
type Instrument struct {
	Name        string
	Unit        string
	Description string
	Type        InstrumentType
	Controllers []string
	Attributes  []string
	Reads       string
}

var resourceAttrs = []string{AttrKind, AttrNamespace, AttrName}

// The instruments, as Instruments lists them.
var (
	Condition = Instrument{
		Name:        "nats_operator.condition",
		Unit:        "1",
		Description: "1 while the condition is True, 0 while it is False or Unknown.",
		Type:        Gauge,
		Controllers: []string{ClusterController, AuthController, JetStreamController},
		Attributes:  append(slices.Clone(resourceAttrs), AttrType, AttrReason),
		Reads:       "status.conditions of every kind the controller reconciles",
	}
	AccountJWTExpiry = Instrument{
		Name:        "nats_operator.account.jwt_expiry",
		Unit:        "s",
		Description: "When the NatsAccount's current JWT expires, in seconds since the Unix epoch; a JWT that never expires has no point. Every account JWT expires once the auth controller has been down its jwtTTL, so the minimum over every account is the deadline for bringing it back.",
		Type:        Gauge,
		Controllers: []string{AuthController},
		Attributes:  resourceAttrs,
		Reads:       "NatsAccount status.jwt",
	}
	RolloutPendingServers = Instrument{
		Name:        "nats_operator.rollout.pending_servers",
		Unit:        "{server}",
		Description: "Servers still to update in the NatsCluster's rollout.",
		Type:        Gauge,
		Controllers: []string{ClusterController},
		Attributes:  resourceAttrs,
		Reads:       "NatsCluster status.rollout.pending",
	}
	RolloutGate = Instrument{
		Name:        "nats_operator.rollout.gate",
		Unit:        "1",
		Description: "1 while the rollout's gate is closed, by what it waits for.",
		Type:        Gauge,
		Controllers: []string{ClusterController},
		Attributes:  append(slices.Clone(resourceAttrs), AttrWaitingFor),
		Reads:       "NatsCluster status.rollout.gate.waitingFor",
	}
	BalancerLeaderSkew = Instrument{
		Name:        "nats_operator.balancer.leader_skew",
		Unit:        "{leader}",
		Description: "The most leaders one server carries less the fewest another does: per pool for a NatsBalancer, over the NATS cluster for a NatsSystemBalancer.",
		Type:        Gauge,
		Controllers: []string{JetStreamController},
		Attributes:  append(slices.Clone(resourceAttrs), AttrPool),
		Reads:       "NatsBalancer status.pools[].leaderSkew, NatsSystemBalancer status.skew.leaders",
	}
	BalancerPendingMoves = Instrument{
		Name:        "nats_operator.balancer.pending_moves",
		Unit:        "{move}",
		Description: "Moves the NatsSystemBalancer requested that are not yet complete, by kind of move.",
		Type:        Gauge,
		Controllers: []string{JetStreamController},
		Attributes:  append(slices.Clone(resourceAttrs), AttrMoveKind),
		Reads:       "NatsSystemBalancer status.pending",
	}
	BalancerHeldPasses = Instrument{
		Name:        "nats_operator.balancer.held_passes",
		Unit:        "{pass}",
		Description: "Balancer passes that ended with Holding True, by its reason.",
		Type:        Counter,
		Controllers: []string{JetStreamController},
		Attributes:  append(slices.Clone(resourceAttrs), AttrReason),
		Reads:       "NatsBalancer and NatsSystemBalancer status.conditions[Holding], after each pass",
	}
	EvacuationRemaining = Instrument{
		Name:        "nats_operator.evacuation.remaining",
		Unit:        "{stream}",
		Description: "Streams still to leave the evacuation's source cluster.",
		Type:        Gauge,
		Controllers: []string{JetStreamController},
		Attributes:  resourceAttrs,
		Reads:       "NatsClusterEvacuation status.remaining",
	}
	EvacuationStalePlacements = Instrument{
		Name:        "nats_operator.evacuation.stale_placements",
		Unit:        "{stream}",
		Description: "Moved streams no resource owns whose config still names the source cluster.",
		Type:        Gauge,
		Controllers: []string{JetStreamController},
		Attributes:  resourceAttrs,
		Reads:       "NatsClusterEvacuation status.stalePlacement",
	}
)

// Instruments are every instrument the controllers register.
var Instruments = []Instrument{
	Condition,
	AccountJWTExpiry,
	RolloutPendingServers,
	RolloutGate,
	BalancerLeaderSkew,
	BalancerPendingMoves,
	BalancerHeldPasses,
	EvacuationRemaining,
	EvacuationStalePlacements,
}

// Kind is a kind a controller reconciles, whose status.conditions the
// condition gauge reports.
type Kind struct {
	Name string
	List func() client.ObjectList
}

// The kinds each controller reconciles, one list per controller: its
// Register function reports the conditions of these, and it registers a
// reconciler for each of them and for no other kind.
var (
	ClusterKinds = []Kind{
		{"NatsCluster", func() client.ObjectList { return &clusterv1beta1.NatsClusterList{} }},
	}
	AuthKinds = []Kind{
		{"NatsOperator", func() client.ObjectList { return &authv1beta1.NatsOperatorList{} }},
		{"NatsSystemAccount", func() client.ObjectList { return &authv1beta1.NatsSystemAccountList{} }},
		{"NatsAccount", func() client.ObjectList { return &authv1beta1.NatsAccountList{} }},
		{"NatsUser", func() client.ObjectList { return &authv1beta1.NatsUserList{} }},
		{"NatsOperatorTrust", func() client.ObjectList { return &natsv1beta1.NatsOperatorTrustList{} }},
		{"NatsAccountTrust", func() client.ObjectList { return &natsv1beta1.NatsAccountTrustList{} }},
	}
	JetStreamKinds = []Kind{
		{"NatsConnection", func() client.ObjectList { return &natsv1beta1.NatsConnectionList{} }},
		{"NatsStream", func() client.ObjectList { return &js.NatsStreamList{} }},
		{"NatsConsumer", func() client.ObjectList { return &js.NatsConsumerList{} }},
		{"NatsKeyValue", func() client.ObjectList { return &js.NatsKeyValueList{} }},
		{"NatsObjectStore", func() client.ObjectList { return &js.NatsObjectStoreList{} }},
		{"NatsBalancer", func() client.ObjectList { return &js.NatsBalancerList{} }},
		{"NatsSystemBalancer", func() client.ObjectList { return &js.NatsSystemBalancerList{} }},
		{"NatsClusterEvacuation", func() client.ObjectList { return &js.NatsClusterEvacuationList{} }},
	}
)

// gauge builds in, which must be a Gauge.
func gauge(m metric.Meter, in Instrument) (metric.Int64ObservableGauge, error) {
	if in.Type != Gauge {
		return nil, fmt.Errorf("instrument %s is a %s, not a gauge", in.Name, in.Type)
	}
	g, err := m.Int64ObservableGauge(in.Name, metric.WithUnit(in.Unit), metric.WithDescription(in.Description))
	if err != nil {
		return nil, fmt.Errorf("instrument %s: %w", in.Name, err)
	}
	return g, nil
}

// counter builds in, which must be a Counter.
func counter(m metric.Meter, in Instrument) (metric.Int64Counter, error) {
	if in.Type != Counter {
		return nil, fmt.Errorf("instrument %s is a %s, not a counter", in.Name, in.Type)
	}
	c, err := m.Int64Counter(in.Name, metric.WithUnit(in.Unit), metric.WithDescription(in.Description))
	if err != nil {
		return nil, fmt.Errorf("instrument %s: %w", in.Name, err)
	}
	return c, nil
}

func attrs(kind, namespace, name string, more ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributes(append([]attribute.KeyValue{
		attribute.String(AttrKind, kind),
		attribute.String(AttrNamespace, namespace),
		attribute.String(AttrName, name),
	}, more...)...)
}

// RegisterCluster registers the cluster controller's instruments on m,
// reading NatsClusters through r at each collection.
func RegisterCluster(m metric.Meter, r client.Reader) error {
	pending, err := gauge(m, RolloutPendingServers)
	if err != nil {
		return err
	}
	gate, err := gauge(m, RolloutGate)
	if err != nil {
		return err
	}
	conds, err := gauge(m, Condition)
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var list clusterv1beta1.NatsClusterList
		if err := r.List(ctx, &list); err != nil {
			return fmt.Errorf("list NatsClusters: %w", err)
		}
		for _, nc := range list.Items {
			var n int64
			if ro := nc.Status.Rollout; ro != nil {
				n = int64(len(ro.Pending))
				if g := ro.Gate; g != nil && g.WaitingFor != "" {
					o.ObserveInt64(gate, 1, attrs("NatsCluster", nc.Namespace, nc.Name, attribute.String(AttrWaitingFor, g.WaitingFor)))
				}
			}
			o.ObserveInt64(pending, n, attrs("NatsCluster", nc.Namespace, nc.Name))
		}
		return observeConditions(ctx, o, conds, r, ClusterKinds)
	}, pending, gate, conds)
	return err
}

// RegisterAuth registers the auth controller's instruments on m, reading
// its kinds through r at each collection.
func RegisterAuth(m metric.Meter, r client.Reader) error {
	conds, err := gauge(m, Condition)
	if err != nil {
		return err
	}
	expiry, err := gauge(m, AccountJWTExpiry)
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var errs []error
		var accounts authv1beta1.NatsAccountList
		if err := r.List(ctx, &accounts); err != nil {
			errs = append(errs, fmt.Errorf("list NatsAccounts: %w", err))
		}
		for _, acc := range accounts.Items {
			if acc.Status.JWT == "" {
				continue
			}
			c, err := jwt.DecodeAccountClaims(acc.Status.JWT)
			if err != nil {
				errs = append(errs, fmt.Errorf("NatsAccount %s/%s: %w", acc.Namespace, acc.Name, err))
				continue
			}
			if c.Expires != 0 {
				o.ObserveInt64(expiry, c.Expires, attrs("NatsAccount", acc.Namespace, acc.Name))
			}
		}
		return errors.Join(append(errs, observeConditions(ctx, o, conds, r, AuthKinds))...)
	}, conds, expiry)
	return err
}

// JetStreamInstruments are the JetStream controller's instruments that its
// reconcilers record into, rather than read at collection.
type JetStreamInstruments struct {
	heldPasses metric.Int64Counter
}

// RegisterJetStream registers the JetStream controller's instruments on m,
// reading its kinds through r at each collection.
func RegisterJetStream(m metric.Meter, r client.Reader) (*JetStreamInstruments, error) {
	skew, err := gauge(m, BalancerLeaderSkew)
	if err != nil {
		return nil, err
	}
	pending, err := gauge(m, BalancerPendingMoves)
	if err != nil {
		return nil, err
	}
	remaining, err := gauge(m, EvacuationRemaining)
	if err != nil {
		return nil, err
	}
	stale, err := gauge(m, EvacuationStalePlacements)
	if err != nil {
		return nil, err
	}
	conds, err := gauge(m, Condition)
	if err != nil {
		return nil, err
	}
	held, err := counter(m, BalancerHeldPasses)
	if err != nil {
		return nil, err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var errs []error
		var balancers js.NatsBalancerList
		if err := r.List(ctx, &balancers); err != nil {
			errs = append(errs, fmt.Errorf("list NatsBalancers: %w", err))
		}
		for _, b := range balancers.Items {
			for _, p := range b.Status.Pools {
				o.ObserveInt64(skew, int64(p.LeaderSkew), attrs("NatsBalancer", b.Namespace, b.Name, attribute.String(AttrPool, p.Name)))
			}
		}
		var systems js.NatsSystemBalancerList
		if err := r.List(ctx, &systems); err != nil {
			errs = append(errs, fmt.Errorf("list NatsSystemBalancers: %w", err))
		}
		for _, b := range systems.Items {
			if s := b.Status.Skew; s != nil {
				o.ObserveInt64(skew, int64(s.Leaders), attrs("NatsSystemBalancer", b.Namespace, b.Name))
			}
			for _, k := range []js.MoveKind{js.MoveLeader, js.MovePlacement} {
				var n int64
				for _, m := range b.Status.Pending {
					if m.Kind == k {
						n++
					}
				}
				o.ObserveInt64(pending, n, attrs("NatsSystemBalancer", b.Namespace, b.Name, attribute.String(AttrMoveKind, string(k))))
			}
		}
		var evacuations js.NatsClusterEvacuationList
		if err := r.List(ctx, &evacuations); err != nil {
			errs = append(errs, fmt.Errorf("list NatsClusterEvacuations: %w", err))
		}
		for _, e := range evacuations.Items {
			at := attrs("NatsClusterEvacuation", e.Namespace, e.Name)
			o.ObserveInt64(remaining, int64(e.Status.Remaining), at)
			o.ObserveInt64(stale, int64(len(e.Status.StalePlacement)), at)
		}
		return errors.Join(append(errs, observeConditions(ctx, o, conds, r, JetStreamKinds))...)
	}, skew, pending, remaining, stale, conds)
	if err != nil {
		return nil, err
	}
	return &JetStreamInstruments{heldPasses: held}, nil
}

// BalancerPass records one balancer pass of obj, of kind, whose status
// holds conds: a held pass when Holding is True. A nil j records nothing.
func (j *JetStreamInstruments) BalancerPass(ctx context.Context, kind string, obj client.Object, conds []metav1.Condition) {
	if j == nil {
		return
	}
	c := meta.FindStatusCondition(conds, "Holding")
	if c == nil || c.Status != metav1.ConditionTrue {
		return
	}
	j.heldPasses.Add(ctx, 1, attrs(kind, obj.GetNamespace(), obj.GetName(), attribute.String(AttrReason, c.Reason)))
}

func observeConditions(ctx context.Context, o metric.Observer, g metric.Int64ObservableGauge, r client.Reader, kinds []Kind) error {
	var errs []error
	for _, k := range kinds {
		list := k.List()
		if err := r.List(ctx, list); err != nil {
			errs = append(errs, fmt.Errorf("list %s: %w", k.Name, err))
			continue
		}
		errs = append(errs, meta.EachListItem(list, func(item runtime.Object) error {
			obj := item.(client.Object)
			conds, err := conditions(item)
			if err != nil {
				return fmt.Errorf("%s %s/%s: %w", k.Name, obj.GetNamespace(), obj.GetName(), err)
			}
			for _, c := range conds {
				var v int64
				if c.Status == metav1.ConditionTrue {
					v = 1
				}
				o.ObserveInt64(g, v, attrs(k.Name, obj.GetNamespace(), obj.GetName(),
					attribute.String(AttrType, c.Type), attribute.String(AttrReason, c.Reason)))
			}
			return nil
		}))
	}
	return errors.Join(errs...)
}

func conditions(obj runtime.Object) ([]metav1.Condition, error) {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	status, ok := u["status"].(map[string]any)
	if !ok {
		return nil, nil
	}
	var out struct {
		Conditions []metav1.Condition `json:"conditions"`
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(status, &out); err != nil {
		return nil, err
	}
	return out.Conditions, nil
}
