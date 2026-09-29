// Package balancectl reconciles NatsSystemBalancer, NatsBalancer and
// NatsClusterEvacuation. A balancer leaves alone the streams an evacuation of
// its NATS cluster moves while that evacuation is not Ready.
package balancectl

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// SystemBalancerKind is the kind of a NatsSystemBalancer.
const SystemBalancerKind = "NatsSystemBalancer"

// Condition types a balancer reports.
const (
	ConditionReady   = lifecycle.ConditionReady
	ConditionHolding = "Holding"
)

// Condition reasons.
const (
	// ReasonBalancing is Ready's reason while passes run.
	ReasonBalancing = "Balancing"
	// ReasonNotClustered is Ready's reason while the connection reaches a
	// server that is in no NATS cluster.
	ReasonNotClustered = "NotClustered"
	// ReasonDuplicate is Ready's reason on a NatsSystemBalancer while
	// another whose connection reaches the same NATS cluster was created
	// before it, or in the same second and sorts first by namespace and name.
	ReasonDuplicate = "DuplicateBalancer"
	// ReasonPassFailed is Ready's reason after a pass that failed to observe
	// or to move.
	ReasonPassFailed = "PassFailed"

	// ReasonSettled is Holding's reason, False, while moves may be made.
	ReasonSettled = "Settled"
	// ReasonUnsettled is Holding's reason while the NATS cluster is not
	// Settled.
	ReasonUnsettled = "Unsettled"
	// ReasonMovePending is Holding's reason while a placement move the
	// balancer made has not left its server.
	ReasonMovePending = "MovePending"
	// ReasonMoveLeaseHeld is Holding's reason while another balancer holds
	// the move lease of the NATS cluster.
	ReasonMoveLeaseHeld = "MoveLeaseHeld"
)

const (
	// DefaultInterval is the least time between two moves where the spec
	// sets no interval, and how soon an evacuation with no move in flight is
	// reconciled again.
	DefaultInterval = time.Minute
	// DefaultPendingPoll is the PendingPoll default of every balancer and
	// evacuation reconciler.
	DefaultPendingPoll = 5 * time.Second
)

// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natssystembalancers,verbs=get;list;watch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natssystembalancers/status,verbs=patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsclusterevacuations,verbs=list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// SystemBalancerReconciler runs each NatsSystemBalancer's passes over the
// NATS cluster its NatsConnection reaches, which must be as a user of the
// system account.
type SystemBalancerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	// PendingPoll is how soon a balancer with a move pending, or held off by
	// another's move lease, is reconciled again; zero is DefaultPendingPoll.
	PendingPoll time.Duration
	// Recorder records moves started and done; nil records none.
	Recorder events.EventRecorder
	// Telemetry counts held passes; nil counts none.
	Telemetry *telemetry.JetStreamInstruments
	// Leases is shared with every other balancer reconciler that moves on
	// the same NATS clusters; nil is the process's own.
	Leases *MoveLeases

	balancers balancerSet
}

// Reconcile runs one balancing pass for the NatsSystemBalancer req names.
func (r *SystemBalancerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return reconcileBalancer(ctx, req, SystemBalancerKind, r.Client, r.Telemetry, r.Leases, &r.balancers, r.balance,
		func(b *js.NatsSystemBalancer) (any, []metav1.Condition) { return b.Status, b.Status.Conditions })
}

func (r *SystemBalancerReconciler) balance(ctx context.Context, b *js.NatsSystemBalancer) (reconcile.Result, error) {
	st := &b.Status
	st.ObservedGeneration = b.Generation
	interval := DefaultInterval
	if b.Spec.Interval != nil {
		interval = b.Spec.Interval.Duration
	}
	after := reconcile.Result{RequeueAfter: interval}

	nc, err := lifecycle.Resolve(ctx, r.Dialer, referrer(b.Namespace), b.Spec.ConnectionRef, &st.Conditions, b.Generation)
	if nc == nil {
		return retry(after, err)
	}
	cluster := nc.ConnectedClusterName()
	if cluster == "" {
		r.setReady(b, false, ReasonNotClustered, "the connection reaches a server in no NATS cluster")
		return after, nil
	}
	if other, err := r.duplicateOf(ctx, b, cluster); err != nil || other != "" {
		if other != "" {
			r.setReady(b, false, ReasonDuplicate, fmt.Sprintf("NatsSystemBalancer %s balances NATS cluster %s", other, cluster))
		}
		return retry(after, err)
	}
	obs := &SystemObserver{Sys: sysobs.New(nc, cluster), Cluster: cluster}
	ev, err := evacueesOf(ctx, r.Client, r.Dialer, nc, obs)
	if err != nil {
		return retry(after, err)
	}

	now := time.Now()
	var reach *stepdownReach
	k := r.balancers.balancer(b)
	k.Observer, k.Leaders, k.Placement = ev, nil, nil
	k.Yield = func(id balance.StreamID) string { return cmp.Or(obs.Pinned(id), ev.Yield(id)) }
	if moves(b.Spec.Moves).leader {
		reach = newStepdownReach(nc)
		k.Leaders = balance.Stepdown{Conn: nc, Prefix: reach.prefix}
	}
	if moves(b.Spec.Moves).placement {
		k.Placement = balance.StreamMove{Conn: nc}
	}
	due := st.LastMove == nil || st.LastMove.Time == nil || !now.Before(st.LastMove.Time.Add(interval))
	k.DryRun = !due || placementPending(st.Pending)
	leases, holder := leasesOr(r.Leases), holderName(SystemBalancerKind, client.ObjectKeyFromObject(b))
	var leasedTo string
	if !k.DryRun {
		leasedTo = leases.take(cluster, holder, now)
		k.DryRun = leasedTo != ""
	}

	passed, passErr := k.Pass(ctx)
	if last := obs.Last(); last != nil {
		before := st.Pending
		st.Pending = stillPending(st.Pending, *last)
		for _, m := range before {
			if !slices.ContainsFunc(st.Pending, sameMove(m)) {
				telemetry.Emit(r.Recorder, b, telemetry.MoveDone, "%s", describeMove(m))
			}
		}
		st.Capabilities = capabilities(ctx, *last, reach)
	}
	if passErr == nil {
		record(st, passed, now)
	}
	if len(st.Pending) > 0 {
		leases.take(cluster, holder, now)
	} else {
		leases.release(cluster, holder)
	}
	if passErr != nil {
		r.setReady(b, false, ReasonPassFailed, passErr.Error())
		return after, nil
	}
	if m := moveOf(passed, now); m != nil {
		telemetry.Emit(r.Recorder, b, telemetry.MoveStarted, "%s", describeMove(*m))
	}
	switch {
	case leasedTo != "":
		r.setHolding(b, true, ReasonMoveLeaseHeld, leaseMessage(leasedTo, cluster))
	case passed.Held != "":
		r.setHolding(b, true, ReasonUnsettled, passed.Held)
	case placementPending(st.Pending):
		r.setHolding(b, true, ReasonMovePending, fmt.Sprintf("%s is leaving %s", moveTarget(st.Pending[0]), st.Pending[0].From))
	default:
		r.setHolding(b, false, ReasonSettled, "")
	}
	if reach != nil && reach.err != nil {
		r.setReady(b, false, ReasonPassFailed, reach.err.Error())
	} else {
		r.setReady(b, true, ReasonBalancing, "")
	}

	if len(st.Pending) > 0 || leasedTo != "" {
		return reconcile.Result{RequeueAfter: r.pendingPoll()}, nil
	}
	return after, nil
}

// leaseMessage is Holding's message while holder holds cluster's move lease.
func leaseMessage(holder, cluster string) string {
	return fmt.Sprintf("%s holds the move lease of NATS cluster %s", holder, cluster)
}

// duplicateOf names the NatsSystemBalancer that balances cluster ahead of b:
// one created before it, or at the same second and first by namespace and
// name, whose connection reaches cluster. One whose connection fails is not
// balancing and does not count.
func (r *SystemBalancerReconciler) duplicateOf(ctx context.Context, b *js.NatsSystemBalancer, cluster string) (string, error) {
	var list js.NatsSystemBalancerList
	if err := r.Client.List(ctx, &list); err != nil {
		return "", fmt.Errorf("list NatsSystemBalancers: %w", err)
	}
	slices.SortFunc(list.Items, compareAge)
	for i := range list.Items {
		o := &list.Items[i]
		if o.UID == b.UID {
			return "", nil
		}
		nc, denied, err := r.Dialer.Reference(ctx, referrer(o.Namespace), o.Spec.ConnectionRef)
		if err != nil || denied != nil {
			continue
		}
		if nc.ConnectedClusterName() == cluster {
			return client.ObjectKeyFromObject(o).String(), nil
		}
	}
	return "", nil
}

func compareAge(a, b js.NatsSystemBalancer) int {
	return cmp.Or(
		a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
	)
}

// movesOn is a spec's moves with the API's defaults applied.
type movesOn struct{ leader, placement bool }

func moves(m *js.Moves) movesOn {
	out := movesOn{leader: true}
	if m == nil {
		return out
	}
	if m.Leader != nil {
		out.leader = *m.Leader
	}
	if m.Placement != nil {
		out.placement = *m.Placement
	}
	return out
}

// record writes what a pass that was not held found and did into st.
func record(st *js.NatsSystemBalancerStatus, p balance.Passed, now time.Time) {
	if p.Held != "" {
		return
	}
	st.Servers = nil
	for _, l := range p.Servers {
		st.Servers = append(st.Servers, js.ServerLoad{Name: l.Server, Leaders: int32(l.Leaders), Replicas: int32(l.Copies)})
	}
	st.Skew = &js.Skew{Leaders: int32(p.LeaderSkew), Replicas: int32(p.CopySkew)}
	if m := moveOf(p, now); m != nil {
		st.LastMove = m
		st.Pending = append(st.Pending, *m)
	}
}

// moveOf is the move pass p made at now, or nil.
func moveOf(p balance.Passed, now time.Time) *js.Move {
	at := metav1.NewTime(now)
	switch {
	case p.Moved != nil:
		g := p.Moved.Group
		return &js.Move{Kind: js.MoveLeader, Account: g.Account, Stream: g.Stream, Consumer: g.Consumer, From: g.Leader, To: p.Moved.To, Time: &at}
	case p.Placed != nil:
		g := p.Placed.Group
		return &js.Move{Kind: js.MovePlacement, Account: g.Account, Stream: g.Stream, From: p.Placed.From, Time: &at}
	}
	return nil
}

// sameMove matches a move of the same kind, group and servers as m.
func sameMove(m js.Move) func(js.Move) bool {
	return func(o js.Move) bool {
		return o.Kind == m.Kind && o.Account == m.Account && o.Stream == m.Stream && o.Consumer == m.Consumer && o.From == m.From && o.To == m.To
	}
}

// moveTarget is the group m moved, as [balance.Group] names it.
func moveTarget(m js.Move) string {
	return balance.Group{Account: m.Account, Stream: m.Stream, Consumer: m.Consumer}.String()
}

// describeMove is m as an event's note.
func describeMove(m js.Move) string {
	if m.Kind == js.MoveLeader {
		return fmt.Sprintf("leader of %s from %s to %s", moveTarget(m), m.From, m.To)
	}
	return fmt.Sprintf("%s off %s", moveTarget(m), m.From)
}

// stillPending is the moves of pending that obs does not show complete. A
// leader move is complete once the NATS cluster reads Settled after it, and a
// placement move once its server no longer holds the stream.
func stillPending(pending []js.Move, obs balance.Observation) []js.Move {
	var out []js.Move
	for _, m := range pending {
		switch m.Kind {
		case js.MoveLeader:
			if obs.Unsettled == "" {
				continue
			}
		case js.MovePlacement:
			id := balance.StreamID{Account: m.Account, Stream: m.Stream}
			i := slices.IndexFunc(obs.Groups, func(g balance.Group) bool { return g.Consumer == "" && g.ID() == id })
			if i < 0 || !slices.Contains(obs.Groups[i].Holders(), m.From) {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

func placementPending(pending []js.Move) bool {
	return slices.ContainsFunc(pending, func(m js.Move) bool { return m.Kind == js.MovePlacement })
}

// capabilities is what the balancer can move in obs: placement moves for
// every account, and leader moves for the accounts reach reaches. A nil
// reach, leader moves being off, probes nothing and reports no Leader.
func capabilities(ctx context.Context, obs balance.Observation, reach *stepdownReach) *js.Capabilities {
	if reach == nil {
		return &js.Capabilities{Placement: true}
	}
	accounts := map[string]bool{}
	for _, g := range obs.Groups {
		accounts[g.Account] = true
	}
	var unreachable int
	for _, a := range slices.Sorted(maps.Keys(accounts)) {
		if _, ok := reach.prefix(ctx, a); !ok {
			unreachable++
		}
	}
	c := &js.Capabilities{Placement: true, Leader: js.LeaderCapabilityFull}
	if unreachable == 0 {
		return c
	}
	c.Leader = js.LeaderCapabilityPartial
	if unreachable == len(accounts) {
		c.Leader = js.LeaderCapabilityNone
	}
	c.LeaderReason = fmt.Sprintf("%d of %d accounts carry no jetstream-stepdown export; their leaders are not moved", unreachable, len(accounts))
	return c
}

func (r *SystemBalancerReconciler) setReady(b *js.NatsSystemBalancer, ok bool, reason, message string) {
	conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(ok), Reason: reason, Message: message})
}

func (r *SystemBalancerReconciler) setHolding(b *js.NatsSystemBalancer, on bool, reason, message string) {
	conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionHolding, Status: conditions.Status(on), Reason: reason, Message: message})
}

func (r *SystemBalancerReconciler) pendingPoll() time.Duration {
	if r.PendingPoll > 0 {
		return r.PendingPoll
	}
	return DefaultPendingPoll
}

func referrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: SystemBalancerKind, Namespace: namespace}
}

// retry is res where err is nil, and no result beside err otherwise:
// controller-runtime ignores a result returned with an error.
func retry(res reconcile.Result, err error) (reconcile.Result, error) {
	if err != nil {
		return reconcile.Result{}, err
	}
	return res, nil
}

// SetupWithManager registers the reconciler with mgr.
func (r *SystemBalancerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsSystemBalancer).Spec.ConnectionRef}
	}
	if err := refindex.IndexConnections(ctx, idx, &js.NatsSystemBalancer{}, refs); err != nil {
		return fmt.Errorf("index NatsSystemBalancer connections: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsSystemBalancer{}, func(o client.Object) []string {
		return []string{refs(o)[0].Namespace}
	}); err != nil {
		return fmt.Errorf("index NatsSystemBalancer grant targets: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsSystemBalancer{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, refindex.EnqueueByField(mgr.GetClient(), &js.NatsSystemBalancerList{}, refindex.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(SystemBalancerKind).GroupKind(), &js.NatsSystemBalancerList{})).
		Watches(&js.NatsClusterEvacuation{}, refindex.EnqueueAll(mgr.GetClient(), &js.NatsSystemBalancerList{})).
		Complete(telemetry.Traced(SystemBalancerKind, r))
}
