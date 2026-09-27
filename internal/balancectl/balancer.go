package balancectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// BalancerReconciler runs each NatsBalancer's passes over the NATS cluster
// its NatsConnection reaches, as a user of the account it balances: its
// streams split into the declared pools and the default pool, one move per
// interval, none while the NATS cluster is not Settled, none of a stream a
// NatsClusterEvacuation moves, and none while any NatsSystemBalancer has a
// move pending on one of the account's streams.
type BalancerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	// PendingPoll is how soon a balancer yielding to a system balancer is
	// reconciled again; zero is DefaultPendingPoll.
	PendingPoll time.Duration
	// Recorder records moves started; nil records none.
	Recorder events.EventRecorder
	// Telemetry counts held passes; nil counts none.
	Telemetry *telemetry.JetStream

	mu      sync.Mutex
	keepers map[types.NamespacedName]keeperOf
}

// Reconcile implements reconcile.Reconciler.
func (r *BalancerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var b js.NatsBalancer
	if err := r.Client.Get(ctx, req.NamespacedName, &b); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	base := b.DeepCopy()
	res, err := r.balance(ctx, &b)
	r.Telemetry.BalancerPass(ctx, BalancerKind, &b, b.Status.Conditions)
	if perr := lifecycle.PatchStatus(ctx, r.Client, base, &b, base.Status, b.Status); perr != nil {
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

	nc, err := r.connect(ctx, b)
	if nc == nil {
		return after, err
	}
	cluster := nc.ConnectedClusterName()
	if cluster == "" {
		setBalancerCondition(b, ConditionReady, false, ReasonNotClustered, "the connection reaches a server in no NATS cluster")
		return after, nil
	}
	account, err := accountOf(ctx, nc)
	if err != nil {
		setBalancerCondition(b, ConditionReady, false, ReasonPassFailed, err.Error())
		return after, nil
	}

	ms, err := members(ctx, r.Client, b.Namespace, b.Spec.ConnectionRef)
	if err != nil {
		return after, err
	}
	declared, overlaps, err := assign(account, b.Spec.Pools, ms)
	if err != nil {
		setBalancerCondition(b, ConditionReady, false, ReasonInvalidPool, err.Error())
		return after, nil
	}
	if len(overlaps) > 0 {
		setBalancerCondition(b, ConditionOverlapping, true, ReasonOverlapping, strings.Join(overlaps, ". "))
	} else {
		setBalancerCondition(b, ConditionOverlapping, false, ReasonDisjoint, "")
	}

	yield, err := systemPending(ctx, r.Client, account)
	if err != nil {
		return after, err
	}

	j, err := jetstream.New(nc)
	if err != nil {
		setBalancerCondition(b, ConditionReady, false, ReasonPassFailed, err.Error())
		return after, nil
	}
	ev, err := evacueesOf(ctx, r.Client, r.Dialer, nc, balance.AccountObserver{JS: j, Account: account, Cluster: cluster, Expect: expected(ms, cluster)})
	if err != nil {
		return after, err
	}
	k := r.keeper(b)
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

	passed, err := k.Pass(ctx)
	if err != nil {
		setBalancerCondition(b, ConditionReady, false, ReasonPassFailed, err.Error())
		return after, nil
	}
	if passed.Held == "" {
		st.Pools = poolStatus(b.Spec.Pools, passed.Pools)
	}
	if m := moveOf(passed, now); m != nil {
		st.LastMove = m
		telemetry.Emit(r.Recorder, b, telemetry.MoveStarted, "%s", describeMove(*m))
	}
	switch {
	case yield != "":
		setBalancerCondition(b, ConditionHolding, true, ReasonYielding, yield)
	case passed.Held != "":
		setBalancerCondition(b, ConditionHolding, true, ReasonUnsettled, passed.Held)
	default:
		setBalancerCondition(b, ConditionHolding, false, ReasonSettled, "")
	}
	setBalancerCondition(b, ConditionReady, true, ReasonBalancing, "")
	if yield != "" {
		return reconcile.Result{RequeueAfter: r.pendingPoll()}, nil
	}
	return after, nil
}

// connect returns the balancer's connection, or records on b why there is
// none; the error is one the Kubernetes API server returned.
func (r *BalancerReconciler) connect(ctx context.Context, b *js.NatsBalancer) (*nats.Conn, error) {
	nc, denied, err := r.Dialer.Reference(ctx, balancerReferrer(b.Namespace), b.Spec.ConnectionRef)
	switch {
	case denied != nil:
		denied.ObservedGeneration = b.Generation
		meta.SetStatusCondition(&b.Status.Conditions, *denied)
		setBalancerCondition(b, ConditionReady, false, grant.ReasonReferenceNotPermitted, denied.Message)
		return nil, nil
	case apierrors.IsNotFound(err):
		setBalancerCondition(b, ConditionReady, false, lifecycle.ReasonConnectionNotFound, err.Error())
		return nil, nil
	case isAPIStatus(err):
		return nil, err
	case err != nil:
		setBalancerCondition(b, ConditionReady, false, lifecycle.ReasonConnectionFailed, err.Error())
		return nil, nil
	}
	meta.RemoveStatusCondition(&b.Status.Conditions, grant.ConditionReferencesResolved)
	return nc, nil
}

// userInfoSubject answers any connection with its own user and account while
// the server runs a system account.
const userInfoSubject = "$SYS.REQ.USER.INFO"

// accountOf is the account nc is a user of, as the server names it: the
// account's public key under a NATS operator. It is "" where the server runs
// no system account, which leaves no system balancer to yield to.
func accountOf(ctx context.Context, nc *nats.Conn) (string, error) {
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
		Error *balance.APIError `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return "", fmt.Errorf("read the connection's account: response %q: %w", msg.Data, err)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("read the connection's account: %w", resp.Error)
	}
	return resp.Data.Account, nil
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

func setBalancerCondition(b *js.NatsBalancer, typ string, on bool, reason, message string) {
	s := metav1.ConditionFalse
	if on {
		s = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{
		Type: typ, Status: s, Reason: reason, Message: message, ObservedGeneration: b.Generation,
	})
}

// keeper is b's pass state, kept across reconciles and started afresh for a
// new balancer of the same name.
func (r *BalancerReconciler) keeper(b *js.NatsBalancer) *balance.Keeper {
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

func (r *BalancerReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keepers, key)
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

// SetupWithManager registers the reconciler with mgr. It reconciles a
// balancer on its spec changing, its NatsConnection changing, a grant that
// admits it changing, any NatsStream, NatsKeyValue or NatsObjectStore in its
// namespace changing, and any NatsSystemBalancer or NatsClusterEvacuation
// changing.
func (r *BalancerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsBalancer).Spec.ConnectionRef}
	}
	if err := lifecycle.IndexConnections(ctx, idx, &js.NatsBalancer{}, refs); err != nil {
		return fmt.Errorf("index NatsBalancer connections: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsBalancer{}, func(o client.Object) []string {
		return []string{refs(o)[0].Namespace}
	}); err != nil {
		return fmt.Errorf("index NatsBalancer grant targets: %w", err)
	}
	c := mgr.GetClient()
	balancers := func(sameNamespace bool) handler.EventHandler {
		return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			var opts []client.ListOption
			if sameNamespace {
				opts = append(opts, client.InNamespace(o.GetNamespace()))
			}
			var list js.NatsBalancerList
			if err := c.List(ctx, &list, opts...); err != nil {
				ctrl.LoggerFrom(ctx).Error(err, "list NatsBalancers")
				return nil
			}
			out := make([]reconcile.Request, 0, len(list.Items))
			for i := range list.Items {
				out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
			}
			return out
		})
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsBalancer{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(c, &js.NatsBalancerList{}, lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, js.GroupVersion.WithKind(BalancerKind).GroupKind(), &js.NatsBalancerList{})).
		Watches(&js.NatsStream{}, balancers(true)).
		Watches(&js.NatsKeyValue{}, balancers(true)).
		Watches(&js.NatsObjectStore{}, balancers(true)).
		Watches(&js.NatsSystemBalancer{}, balancers(false)).
		Watches(&js.NatsClusterEvacuation{}, balancers(false)).
		Complete(telemetry.Traced(BalancerKind, r))
}
