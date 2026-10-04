package balancectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// BalancerKind is the kind of a NatsBalancer.
const BalancerKind = "NatsBalancer"

// ConditionOverlapping is the condition a NatsBalancer reports on streams
// its pools select more than once.
const ConditionOverlapping = "Overlapping"

// NatsBalancer condition reasons.
const (
	// ReasonYielding is Holding's reason while a NatsSystemBalancer has a
	// move pending on one of the account's streams.
	ReasonYielding = "YieldingToSystemBalancer"
	// ReasonInvalidPool is Ready's reason while a pool's selector does not
	// parse.
	ReasonInvalidPool = "InvalidPool"
	// ReasonOverlapping is Overlapping's reason while a stream matches
	// several pools.
	ReasonOverlapping = "StreamsInSeveralPools"
	// ReasonDisjoint is Overlapping's reason, False, while no stream matches
	// several pools.
	ReasonDisjoint = "PoolsDisjoint"
)

// +kubebuilder:rbac:groups=jetstream.nats-operator.io,resources=natsbalancers,verbs=get;list;watch
// +kubebuilder:rbac:groups=jetstream.nats-operator.io,resources=natsbalancers/status,verbs=patch
// +kubebuilder:rbac:groups=jetstream.nats-operator.io,resources=natsstreams;natskeyvalues;natsobjectstores;natssystembalancers;natsclusterevacuations,verbs=list;watch
// +kubebuilder:rbac:groups=nats-operator.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// BalancerReconciler runs each NatsBalancer's passes over its account's
// streams, which its NatsConnection must reach as a user of that account.
type BalancerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	// PendingPoll is how soon a balancer is reconciled again while it yields
	// to a system balancer, another holds its lease, or a move is in flight;
	// zero is DefaultPendingPoll.
	PendingPoll time.Duration
	// Recorder records moves started; nil records none.
	Recorder events.EventRecorder
	// Telemetry counts held passes; nil counts none.
	Telemetry *telemetry.JetStreamInstruments
	// Leases is shared with every other balancer reconciler that moves on
	// the same NATS clusters; nil is the process's own.
	Leases *MoveLeases

	balancers balancerSet
}

// Reconcile runs one balancing pass for the NatsBalancer req names.
func (r *BalancerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return reconcileBalancer(ctx, req, BalancerKind, r.Client, r.Telemetry, r.Leases, &r.balancers, r.balance,
		func(b *js.NatsBalancer) (any, []metav1.Condition) { return b.Status, b.Status.Conditions })
}

// reconcileBalancer is Reconcile for a balancer of kind: it runs balance on
// the object req names and patches the status that status reads from it.
func reconcileBalancer[T any, P interface {
	*T
	client.Object
}](ctx context.Context, req reconcile.Request, kind string, c client.Client, tel *telemetry.JetStreamInstruments, leases *MoveLeases, set *balancerSet,
	balance func(context.Context, P) (reconcile.Result, error), status func(P) (any, []metav1.Condition),
) (reconcile.Result, error) {
	b := P(new(T))
	if err := c.Get(ctx, req.NamespacedName, b); err != nil {
		if apierrors.IsNotFound(err) {
			set.forget(req.NamespacedName)
			leasesOr(leases).drop(holderName(kind, req.NamespacedName))
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	base := b.DeepCopyObject().(P)
	res, err := balance(ctx, b)
	old, _ := status(base)
	cur, conds := status(b)
	tel.BalancerPass(ctx, kind, b, conds)
	if perr := lifecycle.PatchStatus(ctx, c, base, b, old, cur); perr != nil {
		return reconcile.Result{}, errors.Join(err, perr)
	}
	return res, err
}

func (r *BalancerReconciler) balance(ctx context.Context, b *js.NatsBalancer) (reconcile.Result, error) {
	st := &b.Status
	st.ObservedGeneration = b.Generation
	interval := DefaultInterval
	if b.Spec.Interval != nil {
		interval = b.Spec.Interval.Duration
	}
	after := reconcile.Result{RequeueAfter: interval}

	nc, err := lifecycle.Resolve(ctx, r.Dialer, balancerReferrer(b.Namespace), b.Spec.ConnectionRef, &st.Conditions, b.Generation)
	if nc == nil {
		return retry(after, err)
	}
	cluster := nc.ConnectedClusterName()
	if cluster == "" {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(false), Reason: ReasonNotClustered, Message: "the connection reaches a server in no NATS cluster"})
		return after, nil
	}
	r.balancers.place(b, cluster)
	account, err := accountOf(ctx, nc)
	if err != nil {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(false), Reason: ReasonPassFailed, Message: err.Error()})
		return after, nil
	}

	ms, err := members(ctx, r.Client, b.Namespace, b.Spec.ConnectionRef)
	if err != nil {
		return retry(after, err)
	}
	declared, overlaps, err := assign(account, b.Spec.Pools, ms)
	if err != nil {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(false), Reason: ReasonInvalidPool, Message: err.Error()})
		return after, nil
	}
	if len(overlaps) > 0 {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionOverlapping, Status: conditions.Status(true), Reason: ReasonOverlapping, Message: strings.Join(overlaps, ". ")})
	} else {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionOverlapping, Status: conditions.Status(false), Reason: ReasonDisjoint})
	}

	yield, err := systemPending(ctx, r.Client, account)
	if err != nil {
		return retry(after, err)
	}

	j, err := jetstream.New(nc)
	if err != nil {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(false), Reason: ReasonPassFailed, Message: err.Error()})
		return after, nil
	}
	ev, err := evacueesOf(ctx, r.Client, r.Dialer, nc, balance.AccountObserver{JS: j, Account: account, Cluster: cluster, Expect: expected(ms, cluster)})
	if err != nil {
		return retry(after, err)
	}
	k := r.balancers.balancer(b)
	k.Observer, k.Yield = ev, ev.Yield
	k.Pools = balance.Declared(account, declared)
	k.Leaders, k.Placement = nil, nil
	if moves(b.Spec.Moves).leader {
		k.Leaders = balance.Stepdown{Conn: nc}
	}
	if moves(b.Spec.Moves).placement {
		k.Placement = balance.StreamMove{Conn: nc}
	}
	now := time.Now()
	due := st.LastMove == nil || st.LastMove.Time == nil || !now.Before(st.LastMove.Time.Add(interval))
	k.DryRun = !due || yield != ""
	leases, holder := leasesOr(r.Leases), holderName(BalancerKind, client.ObjectKeyFromObject(b))
	var leasedTo string
	if !k.DryRun {
		leasedTo = leases.take(cluster, holder, now)
		k.DryRun = leasedTo != ""
	}

	passed, err := k.Pass(ctx)
	m := moveOf(passed, now)
	inFlight := err == nil && (m != nil || passed.Held != "" && leases.holds(cluster, holder, now))
	if inFlight {
		leases.take(cluster, holder, now)
	} else {
		leases.release(cluster, holder)
	}
	if err != nil {
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(false), Reason: ReasonPassFailed, Message: err.Error()})
		return after, nil
	}
	if passed.Held == "" {
		st.Pools = poolStatus(b.Spec.Pools, passed.Pools)
	}
	if m != nil {
		st.LastMove = m
		telemetry.Emit(r.Recorder, b, telemetry.MoveStarted, "%s", describeMove(*m))
	}
	switch {
	case yield != "":
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionHolding, Status: conditions.Status(true), Reason: ReasonYielding, Message: yield})
	case leasedTo != "":
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionHolding, Status: conditions.Status(true), Reason: ReasonMoveLeaseHeld, Message: leaseMessage(leasedTo, cluster)})
	case passed.Held != "":
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionHolding, Status: conditions.Status(true), Reason: ReasonUnsettled, Message: passed.Held})
	default:
		conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionHolding, Status: conditions.Status(false), Reason: ReasonSettled})
	}
	conditions.Set(&b.Status.Conditions, b.Generation, metav1.Condition{Type: ConditionReady, Status: conditions.Status(true), Reason: ReasonBalancing})
	if yield != "" || leasedTo != "" || inFlight {
		return reconcile.Result{RequeueAfter: r.pendingPoll()}, nil
	}
	return after, nil
}

// userInfoSubject answers any connection with its own user and account while
// the server runs a system account.
const userInfoSubject = "$SYS.REQ.USER.INFO"

// accountOf is the account nc is a user of, as the server names it, or ""
// where nc has no user JWT and the server runs no system account.
func accountOf(ctx context.Context, nc *nats.Conn) (string, error) {
	if account := jwtAccount(nc); account != "" {
		return account, nil
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, userInfoSubject, nil)
	if errors.Is(err, nats.ErrNoResponders) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the connection's account: %w", err)
	}
	var resp struct {
		Data struct {
			Account string `json:"account"`
		} `json:"data"`
		Error *jetstream.APIError `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return "", fmt.Errorf("read the connection's account: response %q: %w", msg.Data, err)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("read the connection's account: %w", resp.Error)
	}
	return resp.Data.Account, nil
}

// jwtAccount is the account nc's user JWT names, or "" where nc has none.
func jwtAccount(nc *nats.Conn) string {
	if nc.Opts.UserJWT == nil {
		return ""
	}
	token, err := nc.Opts.UserJWT()
	if err != nil {
		return ""
	}
	c, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return ""
	}
	if c.IssuerAccount != "" {
		return c.IssuerAccount
	}
	return c.Issuer
}

// systemPending says which of account's streams a NatsSystemBalancer has a
// move pending on, and is "" where none has.
func systemPending(ctx context.Context, c client.Reader, account string) (string, error) {
	if account == "" {
		return "", nil
	}
	var list js.NatsSystemBalancerList
	if err := c.List(ctx, &list); err != nil {
		return "", fmt.Errorf("list NatsSystemBalancers: %w", err)
	}
	for _, sb := range list.Items {
		for _, m := range sb.Status.Pending {
			if m.Account == account {
				return fmt.Sprintf("%s has a %s move pending from %s %s", m.Stream, strings.ToLower(string(m.Kind)), SystemBalancerKind, sb.Name), nil
			}
		}
	}
	return "", nil
}

// ofSystemBalancer is the NatsBalancers a change to the NatsSystemBalancer o
// concerns: those whose connection last reached the NATS cluster o's
// connection reaches, and every one whose NATS cluster is not known on either
// side.
func (r *BalancerReconciler) ofSystemBalancer(ctx context.Context, c client.Reader, o client.Object) []reconcile.Request {
	sb, ok := o.(*js.NatsSystemBalancer)
	if !ok {
		return nil
	}
	var cluster string
	if nc, denied, err := r.Dialer.Reference(ctx, referrer(sb.Namespace), sb.Spec.ConnectionRef); err == nil && denied == nil {
		cluster = nc.ConnectedClusterName()
	}
	var list js.NatsBalancerList
	if err := c.List(ctx, &list); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list NatsBalancers")
		return nil
	}
	return inCluster(cluster, r.balancers.clusters(), list.Items)
}

// inCluster is the requests for the balancers of items that clusters places
// in cluster, or does not place at all; with cluster "" it is all of them.
func inCluster(cluster string, clusters map[types.NamespacedName]string, items []js.NatsBalancer) []reconcile.Request {
	var out []reconcile.Request
	for i := range items {
		key := client.ObjectKeyFromObject(&items[i])
		if at := clusters[key]; cluster == "" || at == "" || at == cluster {
			out = append(out, reconcile.Request{NamespacedName: key})
		}
	}
	return out
}

// pendingOrSpec passes creates, deletes, spec changes, deletion marks, and
// status writes that change a NatsSystemBalancer's pending moves.
func pendingOrSpec() predicate.Predicate {
	return predicate.Or(lifecycle.SpecOrDeletion(), predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, ok1 := e.ObjectOld.(*js.NatsSystemBalancer)
			cur, ok2 := e.ObjectNew.(*js.NatsSystemBalancer)
			return ok1 && ok2 && !equality.Semantic.DeepEqual(old.Status.Pending, cur.Status.Pending)
		},
	})
}

func (r *BalancerReconciler) pendingPoll() time.Duration {
	if r.PendingPoll > 0 {
		return r.PendingPoll
	}
	return DefaultPendingPoll
}

func balancerReferrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: BalancerKind, Namespace: namespace}
}

// SetupWithManager registers the reconciler with mgr.
func (r *BalancerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsBalancer).Spec.ConnectionRef}
	}
	if err := refindex.IndexConnections(ctx, idx, &js.NatsBalancer{}, refs); err != nil {
		return fmt.Errorf("index NatsBalancer connections: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsBalancer{}, func(o client.Object) []string {
		return []string{refs(o)[0].Namespace}
	}); err != nil {
		return fmt.Errorf("index NatsBalancer grant targets: %w", err)
	}
	c := mgr.GetClient()
	members := refindex.EnqueueNamespace(c, &js.NatsBalancerList{})
	memberChanges := builder.WithPredicates(predicate.Or(lifecycle.SpecOrDeletion(), predicate.LabelChangedPredicate{}))
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsBalancer{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, refindex.EnqueueByField(c, &js.NatsBalancerList{}, refindex.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, js.GroupVersion.WithKind(BalancerKind).GroupKind(), &js.NatsBalancerList{})).
		Watches(&js.NatsStream{}, members, memberChanges).
		Watches(&js.NatsKeyValue{}, members, memberChanges).
		Watches(&js.NatsObjectStore{}, members, memberChanges).
		Watches(&js.NatsSystemBalancer{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.ofSystemBalancer(ctx, c, o)
		}), builder.WithPredicates(pendingOrSpec())).
		Watches(&js.NatsClusterEvacuation{}, refindex.EnqueueAll(c, &js.NatsBalancerList{})).
		Complete(telemetry.Traced(BalancerKind, r))
}
