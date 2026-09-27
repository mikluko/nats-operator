// Package balancectl reconciles balancers and evacuations: a
// NatsSystemBalancer is a [balance.Keeper] over the NATS cluster its
// NatsConnection reaches, and a NatsClusterEvacuation empties one NATS
// cluster, both on a system connection. A balancer leaves alone the streams
// an evacuation of its NATS cluster moves while it is not Ready.
package balancectl

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
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
	// ReasonDuplicate is Ready's reason on every NatsSystemBalancer of a
	// NATS cluster but the one created first.
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
)

const (
	// DefaultInterval is the least time between two moves where the spec
	// sets no interval.
	DefaultInterval = time.Minute
	// DefaultPendingPoll is SystemBalancerReconciler.PendingPoll's default.
	DefaultPendingPoll = 5 * time.Second
)

// SystemBalancerReconciler runs each NatsSystemBalancer's passes over the
// NATS cluster its NatsConnection reaches, which must be as a user of the
// system account: one move per interval, none while the NATS cluster is not
// Settled, none of a stream a NatsClusterEvacuation moves, and none by a
// second balancer of the same NATS cluster.
type SystemBalancerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	// PendingPoll is how soon a balancer with a move pending is reconciled
	// again; zero is DefaultPendingPoll.
	PendingPoll time.Duration
	// Recorder records moves started and done; nil records none.
	Recorder events.EventRecorder
	// Telemetry counts held passes; nil counts none.
	Telemetry *telemetry.JetStreamInstruments

	mu      sync.Mutex
	keepers map[types.NamespacedName]keeperOf
}

// Reconcile implements reconcile.Reconciler.
func (r *SystemBalancerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var b js.NatsSystemBalancer
	if err := r.Client.Get(ctx, req.NamespacedName, &b); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	base := b.DeepCopy()
	res, err := r.balance(ctx, &b)
	r.Telemetry.BalancerPass(ctx, SystemBalancerKind, &b, b.Status.Conditions)
	if perr := lifecycle.PatchStatus(ctx, r.Client, base, &b, base.Status, b.Status); perr != nil {
		return reconcile.Result{}, errors.Join(err, perr)
	}
	return res, err
}

func (r *SystemBalancerReconciler) balance(ctx context.Context, b *js.NatsSystemBalancer) (reconcile.Result, error) {
	st := &b.Status
	st.ObservedGeneration = b.Generation
	interval := DefaultInterval
	if b.Spec.Interval != nil {
		interval = b.Spec.Interval.Duration
	}
	after := reconcile.Result{RequeueAfter: interval}

	nc, err := r.connect(ctx, b)
	if nc == nil {
		return after, err
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
		return after, err
	}
	obs := &SystemObserver{Sys: sysobs.New(nc, cluster), Cluster: cluster}
	ev, err := evacueesOf(ctx, r.Client, r.Dialer, nc, obs)
	if err != nil {
		return after, err
	}

	now := time.Now()
	reach := newStepdownReach(ctx, nc)
	k := r.keeper(b)
	k.Observer, k.Leaders, k.Placement = ev, nil, nil
	k.Yield = func(id balance.StreamID) string { return cmp.Or(obs.Pinned(id), ev.Yield(id)) }
	if moves(b.Spec.Moves).leader {
		k.Leaders = balance.Stepdown{Conn: nc, Prefix: reach.prefix}
	}
	if moves(b.Spec.Moves).placement {
		k.Placement = balance.StreamMove{Conn: nc}
	}
	due := st.LastMove == nil || st.LastMove.Time == nil || !now.Before(st.LastMove.Time.Add(interval))
	k.DryRun = !due || placementPending(st.Pending)

	passed, passErr := k.Pass(ctx)
	if last := obs.Last(); last != nil {
		before := st.Pending
		st.Pending = stillPending(st.Pending, *last)
		for _, m := range before {
			if !slices.ContainsFunc(st.Pending, sameMove(m)) {
				telemetry.Emit(r.Recorder, b, telemetry.MoveDone, "%s", describeMove(m))
			}
		}
		st.Capabilities = capabilities(*last, reach)
	}
	if passErr == nil && reach.err != nil {
		passErr = reach.err
	}
	if passErr != nil {
		r.setReady(b, false, ReasonPassFailed, passErr.Error())
		return after, nil
	}
	record(st, passed, now)
	if m := moveOf(passed, now); m != nil {
		telemetry.Emit(r.Recorder, b, telemetry.MoveStarted, "%s", describeMove(*m))
	}
	switch {
	case passed.Held != "":
		r.setHolding(b, true, ReasonUnsettled, passed.Held)
	case placementPending(st.Pending):
		r.setHolding(b, true, ReasonMovePending, fmt.Sprintf("%s is leaving %s", moveTarget(st.Pending[0]), st.Pending[0].From))
	default:
		r.setHolding(b, false, ReasonSettled, "")
	}
	r.setReady(b, true, ReasonBalancing, "")

	if len(st.Pending) > 0 {
		return reconcile.Result{RequeueAfter: r.pendingPoll()}, nil
	}
	return after, nil
}

// connect returns the balancer's connection, or records on b why there is
// none; the error is one the Kubernetes API server returned.
func (r *SystemBalancerReconciler) connect(ctx context.Context, b *js.NatsSystemBalancer) (*nats.Conn, error) {
	nc, why, err := dial(ctx, r.Dialer, referrer(b.Namespace), b.Spec.ConnectionRef)
	if why != nil {
		why.apply(&b.Status.Conditions, b.Generation)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta.RemoveStatusCondition(&b.Status.Conditions, grant.ConditionReferencesResolved)
	return nc, nil
}

// A noConn is why a reference yields no connection: Ready's reason and
// message, and the condition a grant's refusal sets.
type noConn struct {
	reason, message string
	denied          *metav1.Condition
}

// apply records n in conds at generation.
func (n *noConn) apply(conds *[]metav1.Condition, generation int64) {
	if n.denied != nil {
		n.denied.ObservedGeneration = generation
		meta.SetStatusCondition(conds, *n.denied)
	}
	setConditionOn(conds, generation, ConditionReady, false, n.reason, n.message)
}

// dial returns the connection from's ref names, or why there is none; the
// error is one the Kubernetes API server returned.
func dial(ctx context.Context, d *natsconn.Dialer, from grant.Referrer, ref natsv1beta1.ObjectReference) (*nats.Conn, *noConn, error) {
	nc, denied, err := d.Reference(ctx, from, ref)
	switch {
	case denied != nil:
		return nil, &noConn{reason: grant.ReasonReferenceNotPermitted, message: denied.Message, denied: denied}, nil
	case apierrors.IsNotFound(err):
		return nil, &noConn{reason: lifecycle.ReasonConnectionNotFound, message: err.Error()}, nil
	case isAPIStatus(err):
		return nil, nil, err
	case err != nil:
		return nil, &noConn{reason: lifecycle.ReasonConnectionFailed, message: err.Error()}, nil
	}
	return nc, nil, nil
}

func isAPIStatus(err error) bool {
	var s apierrors.APIStatus
	return errors.As(err, &s)
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

// record writes what a pass found and did into st. A held pass leaves the
// load as last read, since a NATS cluster in the middle of an election
// counts leaders it is about to lose.
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
// every account, and leader moves for the accounts reach reaches.
func capabilities(obs balance.Observation, reach *stepdownReach) *js.Capabilities {
	accounts := map[string]bool{}
	for _, g := range obs.Groups {
		accounts[g.Account] = true
	}
	var unreachable int
	for _, a := range slices.Sorted(maps.Keys(accounts)) {
		if _, ok := reach.prefix(a); !ok {
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
	setCondition(b, ConditionReady, ok, reason, message)
}

func (r *SystemBalancerReconciler) setHolding(b *js.NatsSystemBalancer, on bool, reason, message string) {
	setCondition(b, ConditionHolding, on, reason, message)
}

func setCondition(b *js.NatsSystemBalancer, typ string, on bool, reason, message string) {
	setConditionOn(&b.Status.Conditions, b.Generation, typ, on, reason, message)
}

func setConditionOn(conds *[]metav1.Condition, generation int64, typ string, on bool, reason, message string) {
	s := metav1.ConditionFalse
	if on {
		s = metav1.ConditionTrue
	}
	meta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: s, Reason: reason, Message: message, ObservedGeneration: generation,
	})
}

type keeperOf struct {
	uid    types.UID
	keeper *balance.Keeper
}

// keeper is b's Keeper, kept across reconciles for what it remembers of
// earlier passes, and started afresh for a new balancer of the same name.
func (r *SystemBalancerReconciler) keeper(b *js.NatsSystemBalancer) *balance.Keeper {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.keepers == nil {
		r.keepers = map[types.NamespacedName]keeperOf{}
	}
	key := client.ObjectKeyFromObject(b)
	k, ok := r.keepers[key]
	if !ok || k.uid != b.UID {
		k = keeperOf{uid: b.UID, keeper: &balance.Keeper{}}
		r.keepers[key] = k
	}
	return k.keeper
}

func (r *SystemBalancerReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keepers, key)
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

// SetupWithManager registers the reconciler with mgr. It reconciles a
// balancer on its spec changing, its NatsConnection changing, a grant that
// admits it changing, and any NatsClusterEvacuation changing.
func (r *SystemBalancerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsSystemBalancer).Spec.ConnectionRef}
	}
	if err := lifecycle.IndexConnections(ctx, idx, &js.NatsSystemBalancer{}, refs); err != nil {
		return fmt.Errorf("index NatsSystemBalancer connections: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsSystemBalancer{}, func(o client.Object) []string {
		return []string{refs(o)[0].Namespace}
	}); err != nil {
		return fmt.Errorf("index NatsSystemBalancer grant targets: %w", err)
	}
	all := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		var list js.NatsSystemBalancerList
		if err := mgr.GetClient().List(ctx, &list); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list NatsSystemBalancers")
			return nil
		}
		out := make([]reconcile.Request, 0, len(list.Items))
		for i := range list.Items {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
		return out
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsSystemBalancer{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(mgr.GetClient(), &js.NatsSystemBalancerList{}, lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(SystemBalancerKind).GroupKind(), &js.NatsSystemBalancerList{})).
		Watches(&js.NatsClusterEvacuation{}, all).
		Complete(telemetry.Traced(SystemBalancerKind, r))
}
