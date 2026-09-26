package balancectl

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// EvacuationKind is the kind of a NatsClusterEvacuation.
const EvacuationKind = "NatsClusterEvacuation"

// ConditionProgressing is the condition an evacuation reports True while it
// has moves to make or in flight.
const ConditionProgressing = "Progressing"

// Evacuation condition reasons.
const (
	// ReasonEvacuated is Ready's reason once the source NATS cluster holds no
	// stream and no resource pins it.
	ReasonEvacuated = "Evacuated"
	// ReasonMoving is Ready's and Progressing's reason while streams are
	// leaving or wait to.
	ReasonMoving = "Moving"
	// ReasonPinnedObjects is Ready's reason while a resource's spec pins the
	// source NATS cluster and nothing else is left to move.
	ReasonPinnedObjects = "PinnedObjects"
	// ReasonAwaitingOwners is Ready's reason while the only streams left in
	// the source are ones whose resource declares another NATS cluster, which
	// their owners move.
	ReasonAwaitingOwners = "AwaitingOwners"
	// ReasonNothingMovable is Progressing's reason while nothing is left
	// that the evacuation may move.
	ReasonNothingMovable = "NothingMovable"
	// ReasonTargetTagsInSource is Ready's and Progressing's reason while a
	// server of the source carries every target tag; no move is made.
	ReasonTargetTagsInSource = "TargetTagsInSource"
	// ReasonMoveRefused is Ready's reason after a pass in which the server
	// refused a move.
	ReasonMoveRefused = "MoveRefused"
)

// DefaultMaxInFlight is EvacuationReconciler.MaxInFlight's default.
const DefaultMaxInFlight = 4

// requestGrace is how long a requested move is counted in flight before the
// source shows it leaving; past it the stream is requested again.
const requestGrace = time.Minute

// EvacuationReconciler empties each NatsClusterEvacuation's source NATS
// cluster, observed through a NatsConnection of the system account, one
// $JS.API.ACCOUNT.STREAM.MOVE per stream with the target tags. It moves no
// stream while a source server carries every target tag, never moves a
// stream whose owning resource declares a placement.cluster, and on the
// evacuation's deletion cancels the moves still in flight.
type EvacuationReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	// MaxInFlight is how many moves an evacuation keeps in flight at once;
	// zero is DefaultMaxInFlight.
	MaxInFlight int
	// PendingPoll is how soon an evacuation with moves in flight is
	// reconciled again; zero is DefaultPendingPoll.
	PendingPoll time.Duration

	mu   sync.Mutex
	runs map[types.UID]map[balance.StreamID]time.Time
}

// Reconcile implements reconcile.Reconciler.
func (r *EvacuationReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var e js.NatsClusterEvacuation
	if err := r.Client.Get(ctx, req.NamespacedName, &e); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !e.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, r.finalize(ctx, &e)
	}
	if err := lifecycle.AddFinalizer(ctx, r.Client, &e); err != nil {
		return reconcile.Result{}, err
	}
	base := e.DeepCopy()
	res, err := r.evacuate(ctx, &e)
	if perr := lifecycle.PatchStatus(ctx, r.Client, base, &e, base.Status, e.Status); perr != nil {
		return reconcile.Result{}, errors.Join(err, perr)
	}
	return res, err
}

func (r *EvacuationReconciler) evacuate(ctx context.Context, e *js.NatsClusterEvacuation) (reconcile.Result, error) {
	st := &e.Status
	st.ObservedGeneration = e.Generation
	from := e.Spec.From.Cluster
	after := reconcile.Result{RequeueAfter: DefaultInterval}

	owners, pinned, err := resources(ctx, r.Client, from)
	if err != nil {
		return reconcile.Result{}, err
	}
	st.Pinned = pinned

	nc, why, err := dial(ctx, r.Dialer, evacuationReferrer(e.Namespace), e.Spec.ConnectionRef)
	if why != nil {
		why.apply(&st.Conditions, e.Generation)
		return after, nil
	}
	if err != nil {
		return reconcile.Result{}, err
	}
	meta.RemoveStatusCondition(&st.Conditions, grant.ConditionReferencesResolved)

	snap, err := sysobs.New(nc, from).Observe(ctx)
	if err != nil {
		setEvacuation(e, ConditionReady, false, ReasonPassFailed, err.Error())
		return after, nil
	}
	if s := carrier(snap.Servers, e.Spec.To.ServerTags); s != "" {
		msg := fmt.Sprintf("server %s of %s carries %s", s, from, strings.Join(e.Spec.To.ServerTags, ", "))
		setEvacuation(e, ConditionReady, false, ReasonTargetTagsInSource, msg)
		setEvacuation(e, ConditionProgressing, false, ReasonTargetTagsInSource, msg)
		return after, nil
	}

	now := time.Now()
	requested := r.run(e.UID)
	p := planEvacuation(snap, owners, requested, now, r.maxInFlight())
	for _, id := range p.completed {
		delete(requested, id)
		st.Moved++
	}
	for _, id := range p.inFlight {
		if _, ok := requested[id]; !ok {
			requested[id] = now
		}
	}
	mover := balance.StreamMove{Conn: nc}
	var refused []error
	inFlight := len(p.inFlight)
	for _, g := range p.move {
		id := streamID(g)
		if err := mover.Evacuate(ctx, id, e.Spec.To.ServerTags); err != nil {
			refused = append(refused, err)
			continue
		}
		requested[id] = now
		inFlight++
		if stale(g, from, owners) {
			st.StalePlacement = addStale(st.StalePlacement, id)
		}
	}
	st.InFlight = int32(inFlight)

	leaving := inFlight + p.waiting + len(refused)
	switch {
	case len(refused) > 0:
		setEvacuation(e, ConditionReady, false, ReasonMoveRefused, fmt.Sprintf("%s refused: %v", count(len(refused), "move"), refused[0]))
	case leaving > 0:
		setEvacuation(e, ConditionReady, false, ReasonMoving, fmt.Sprintf("%s leaving %s", count(leaving, "stream"), from))
	case len(pinned) > 0:
		verb := "pin"
		if len(pinned) == 1 {
			verb = "pins"
		}
		setEvacuation(e, ConditionReady, false, ReasonPinnedObjects, fmt.Sprintf("%s %s placement.cluster %s", count(len(pinned), "resource"), verb, from))
	case p.owned > 0:
		setEvacuation(e, ConditionReady, false, ReasonAwaitingOwners, fmt.Sprintf("%s in %s left for their owners to move", count(p.owned, "stream"), from))
	default:
		setEvacuation(e, ConditionReady, true, ReasonEvacuated, "")
	}
	switch {
	case leaving > 0:
		setEvacuation(e, ConditionProgressing, true, ReasonMoving, "")
	case len(pinned) > 0 || p.owned > 0:
		setEvacuation(e, ConditionProgressing, false, ReasonNothingMovable, "")
	default:
		setEvacuation(e, ConditionProgressing, false, ReasonEvacuated, "")
	}
	if leaving > 0 {
		return reconcile.Result{RequeueAfter: r.pendingPoll()}, nil
	}
	return after, nil
}

// finalize cancels the moves e has in flight and releases e. A connection
// that cannot be resolved, or a source no server of which answers, leaves
// nothing to cancel; a Ready evacuation has nothing in flight.
func (r *EvacuationReconciler) finalize(ctx context.Context, e *js.NatsClusterEvacuation) error {
	if !controllerutil.ContainsFinalizer(e, lifecycle.Finalizer) {
		return nil
	}
	if !meta.IsStatusConditionTrue(e.Status.Conditions, ConditionReady) {
		if err := r.cancel(ctx, e); err != nil {
			return err
		}
	}
	if err := lifecycle.RemoveFinalizer(ctx, r.Client, e); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.runs, e.UID)
	r.mu.Unlock()
	return nil
}

func (r *EvacuationReconciler) cancel(ctx context.Context, e *js.NatsClusterEvacuation) error {
	from := e.Spec.From.Cluster
	nc, why, err := dial(ctx, r.Dialer, evacuationReferrer(e.Namespace), e.Spec.ConnectionRef)
	switch {
	case why != nil && why.reason != lifecycle.ReasonConnectionFailed:
		return nil
	case why != nil:
		return fmt.Errorf("cancel moves off %s: %s", from, why.message)
	case err != nil:
		return err
	}
	snap, err := sysobs.New(nc, from).Observe(ctx)
	if errors.Is(err, sysobs.ErrNoServers) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cancel moves off %s: %w", from, err)
	}
	owners, _, err := resources(ctx, r.Client, from)
	if err != nil {
		return err
	}
	requested := r.run(e.UID)
	p := planEvacuation(snap, owners, requested, time.Now(), 0)
	pending := p.inFlight
	for id := range requested {
		if !slices.Contains(pending, id) && slices.ContainsFunc(snap.Groups, func(g sysobs.Group) bool { return g.Kind == sysobs.KindStream && streamID(g) == id }) {
			pending = append(pending, id)
		}
	}
	mover := balance.StreamMove{Conn: nc}
	var errs []error
	for _, id := range pending {
		if err := mover.CancelMove(ctx, id); err != nil && !noMove(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// noMove reports whether err says there was no move to cancel.
func noMove(err error) bool {
	var apiErr *balance.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrCode == balance.ErrCodeNoMove || apiErr.ErrCode == errCodeStreamNotFound)
}

const errCodeStreamNotFound = 10059

// run is the moves requested for the evacuation uid and not yet seen to
// complete, with when each was requested.
func (r *EvacuationReconciler) run(uid types.UID) map[balance.StreamID]time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs == nil {
		r.runs = map[types.UID]map[balance.StreamID]time.Time{}
	}
	m, ok := r.runs[uid]
	if !ok {
		m = map[balance.StreamID]time.Time{}
		r.runs[uid] = m
	}
	return m
}

func (r *EvacuationReconciler) maxInFlight() int {
	if r.MaxInFlight > 0 {
		return r.MaxInFlight
	}
	return DefaultMaxInFlight
}

func (r *EvacuationReconciler) pendingPoll() time.Duration {
	if r.PendingPoll > 0 {
		return r.PendingPoll
	}
	return DefaultPendingPoll
}

// An owner is a resource that owns a stream on the server, and the NATS
// cluster its spec declares, "" where it declares none.
type owner struct {
	cluster string
}

// resources is every NatsStream, NatsKeyValue and NatsObjectStore by UID,
// and those whose spec declares placement.cluster from, sorted by namespace,
// name and kind.
func resources(ctx context.Context, c client.Reader, from string) (map[types.UID]owner, []js.PinnedObject, error) {
	owners := map[types.UID]owner{}
	var pinned []js.PinnedObject
	see := func(kind string, o metav1.Object, p *js.Placement) {
		var cluster string
		if p != nil {
			cluster = p.Cluster
		}
		owners[o.GetUID()] = owner{cluster: cluster}
		if cluster == from {
			pinned = append(pinned, js.PinnedObject{Kind: kind, Namespace: o.GetNamespace(), Name: o.GetName()})
		}
	}
	var streams js.NatsStreamList
	if err := c.List(ctx, &streams); err != nil {
		return nil, nil, fmt.Errorf("list NatsStreams: %w", err)
	}
	for i := range streams.Items {
		see("NatsStream", &streams.Items[i], streams.Items[i].Spec.Placement)
	}
	var kvs js.NatsKeyValueList
	if err := c.List(ctx, &kvs); err != nil {
		return nil, nil, fmt.Errorf("list NatsKeyValues: %w", err)
	}
	for i := range kvs.Items {
		see("NatsKeyValue", &kvs.Items[i], kvs.Items[i].Spec.Placement)
	}
	var objs js.NatsObjectStoreList
	if err := c.List(ctx, &objs); err != nil {
		return nil, nil, fmt.Errorf("list NatsObjectStores: %w", err)
	}
	for i := range objs.Items {
		see("NatsObjectStore", &objs.Items[i], objs.Items[i].Spec.Placement)
	}
	slices.SortFunc(pinned, func(a, b js.PinnedObject) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Kind, b.Kind))
	})
	return owners, pinned, nil
}

// An evacuationPlan is what one pass over the source finds and may do.
type evacuationPlan struct {
	// completed is the requested streams the source no longer holds.
	completed []balance.StreamID
	// inFlight is the streams leaving: a copy sits outside the source, or
	// a move was requested less than requestGrace ago.
	inFlight []balance.StreamID
	// move is the streams to request a move for now.
	move []sysobs.Group
	// waiting is how many more streams could move once others land.
	waiting int
	// owned is how many streams stay because their resource declares a
	// placement.cluster; their owners move them.
	owned int
}

// planEvacuation judges every stream group of snap, the source NATS cluster,
// against owners, the resources by UID, and requested, the moves asked for;
// move holds at most budget streams less those in flight.
func planEvacuation(snap *sysobs.Snapshot, owners map[types.UID]owner, requested map[balance.StreamID]time.Time, now time.Time, budget int) evacuationPlan {
	var p evacuationPlan
	roster := map[string]bool{}
	for _, s := range snap.Servers {
		roster[s.Name] = true
	}
	held := map[balance.StreamID]bool{}
	var movable []sysobs.Group
	for _, g := range snap.Groups {
		if g.Kind != sysobs.KindStream {
			continue
		}
		id := streamID(g)
		held[id] = true
		if o, ok := ownerOf(g, owners); ok && o.cluster != "" {
			p.owned++
			continue
		}
		at, asked := requested[id]
		switch {
		case leaving(g, roster):
			p.inFlight = append(p.inFlight, id)
		case asked && now.Sub(at) < requestGrace:
			p.inFlight = append(p.inFlight, id)
		default:
			movable = append(movable, g)
		}
	}
	for id := range requested {
		if !held[id] {
			p.completed = append(p.completed, id)
		}
	}
	slices.SortFunc(p.completed, compareStreamIDs)
	n := min(len(movable), max(0, budget-len(p.inFlight)))
	p.move, p.waiting = movable[:n], len(movable)-n
	return p
}

// leaving reports whether g has a copy, or a leader, outside roster, as a
// stream does while a move carries it off.
func leaving(g sysobs.Group, roster map[string]bool) bool {
	if g.Leader == "" && g.NamedLeader != "" && !roster[g.NamedLeader] {
		return true
	}
	return slices.ContainsFunc(g.Members, func(m sysobs.Member) bool { return !roster[m.Server] })
}

// ownerOf is the resource whose ownership marker g's metadata carries.
func ownerOf(g sysobs.Group, owners map[types.UID]owner) (owner, bool) {
	m, ok := lifecycle.ReadMarker(g.Metadata)
	if !ok {
		return owner{}, false
	}
	o, ok := owners[m.UID]
	return o, ok
}

// stale reports whether g, about to be moved off from, has no owning
// resource and a config that names from.
func stale(g sysobs.Group, from string, owners map[types.UID]owner) bool {
	if _, ok := ownerOf(g, owners); ok {
		return false
	}
	return g.Placement != nil && g.Placement.Cluster == from
}

func addStale(list []js.ServerStream, id balance.StreamID) []js.ServerStream {
	s := js.ServerStream{Account: id.Account, Name: id.Stream}
	if slices.Contains(list, s) {
		return list
	}
	list = append(list, s)
	slices.SortFunc(list, func(a, b js.ServerStream) int {
		return cmp.Or(cmp.Compare(a.Account, b.Account), cmp.Compare(a.Name, b.Name))
	})
	return list
}

// carrier names the first of servers carrying every one of tags, "" where
// none does. Tags compare case-insensitively, as the server compares them.
func carrier(servers []sysobs.Server, tags []string) string {
	for _, s := range servers {
		if !slices.ContainsFunc(tags, func(t string) bool {
			return !slices.ContainsFunc(s.Tags, func(h string) bool { return strings.EqualFold(h, t) })
		}) {
			return s.Name
		}
	}
	return ""
}

func streamID(g sysobs.Group) balance.StreamID {
	return balance.StreamID{Account: g.Account, Stream: g.Stream}
}

func compareStreamIDs(a, b balance.StreamID) int {
	return cmp.Or(cmp.Compare(a.Account, b.Account), cmp.Compare(a.Stream, b.Stream))
}

// count is n and noun, plural unless n is one.
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func setEvacuation(e *js.NatsClusterEvacuation, typ string, on bool, reason, message string) {
	setConditionOn(&e.Status.Conditions, e.Generation, typ, on, reason, message)
}

func evacuationReferrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: EvacuationKind, Namespace: namespace}
}

// SetupWithManager registers the reconciler with mgr. It reconciles an
// evacuation on its spec changing or its deletion, its NatsConnection
// changing, a grant that admits it changing, and any NatsStream,
// NatsKeyValue or NatsObjectStore changing.
func (r *EvacuationReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsClusterEvacuation).Spec.ConnectionRef}
	}
	if err := lifecycle.IndexConnections(ctx, idx, &js.NatsClusterEvacuation{}, refs); err != nil {
		return fmt.Errorf("index NatsClusterEvacuation connections: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsClusterEvacuation{}, func(o client.Object) []string {
		return []string{refs(o)[0].Namespace}
	}); err != nil {
		return fmt.Errorf("index NatsClusterEvacuation grant targets: %w", err)
	}
	all := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		var list js.NatsClusterEvacuationList
		if err := mgr.GetClient().List(ctx, &list); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "list NatsClusterEvacuations")
			return nil
		}
		out := make([]reconcile.Request, 0, len(list.Items))
		for i := range list.Items {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
		return out
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsClusterEvacuation{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(mgr.GetClient(), &js.NatsClusterEvacuationList{}, lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(EvacuationKind).GroupKind(), &js.NatsClusterEvacuationList{})).
		Watches(&js.NatsStream{}, all, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&js.NatsKeyValue{}, all, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&js.NatsObjectStore{}, all, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Complete(r)
}
