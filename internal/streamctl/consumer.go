package streamctl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// ConsumerKind is the kind a NatsConsumer is referred to by.
const ConsumerKind = "NatsConsumer"

// NatsConsumer condition reasons beside the lifecycle package's.
const (
	// ReasonStreamNotFound is Ready's reason while the stream, or the
	// NatsStream streamRef names, does not exist.
	ReasonStreamNotFound = "StreamNotFound"
	// ReasonStreamNotReady is Ready's reason while the NatsStream streamRef
	// names is not Ready at its generation.
	ReasonStreamNotReady = "StreamNotReady"
	// ReasonImmutableField is Terminal's reason when the server consumer
	// differs from spec in a field nats-server cannot change and
	// recreateOnImmutableChange is not set.
	ReasonImmutableField = "ImmutableField"
)

// StreamRefField is the field index holding "namespace/name" of the
// NatsStream a NatsConsumer's streamRef names.
const StreamRefField = "jetstream.nats.mikluko.io/stream-ref"

// immutableConsumerKeys are the ConsumerConfig keys nats-server refuses to
// update (consumer.go checkNewConsumerConfig); a switch between push and
// pull is refused too.
var immutableConsumerKeys = []string{
	"deliver_policy", "mem_storage", "opt_start_seq", "opt_start_time",
	"ack_policy", "replay_policy", "idle_heartbeat", "flow_control", "max_waiting",
}

// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsconsumers,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsconsumers/status,verbs=patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsstreams,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// ConsumerReconciler keeps NatsConsumers' consumers at their specs under
// their lifecycle policies. A consumer with streamRef waits for that
// NatsStream to be Ready and, without connectionRef, uses its connection.
// Deleting a NatsConsumer whose deletionPolicy is Delete waits until its
// connection can delete the consumer, except where its streamRef names a
// NatsStream that no longer exists or lifecycle.Released lets it go: those
// leave the server alone.
type ConsumerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

// Reconcile implements reconcile.Reconciler.
func (r *ConsumerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var c js.NatsConsumer
	if err := r.Client.Get(ctx, req.NamespacedName, &c); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !c.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &c)
	}
	if err := lifecycle.AddFinalizer(ctx, r.Client, &c); err != nil {
		return reconcile.Result{}, err
	}
	base := c.DeepCopy()
	res, syncErr := r.sync(ctx, &c)
	if err := lifecycle.PatchStatus(ctx, r.Client, base, &c, base.Status, c.Status); err != nil {
		return reconcile.Result{}, errors.Join(syncErr, err)
	}
	return res, syncErr
}

func (r *ConsumerReconciler) sync(ctx context.Context, c *js.NatsConsumer) (reconcile.Result, error) {
	o, err := r.object(ctx, c, true)
	if err != nil || o == nil {
		return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, err
	}
	res, info, err := r.Syncer.Sync(ctx, lifecycle.Resource{
		Object:      c,
		Policies:    c.Spec.Policies,
		Status:      &c.Status.SyncStatus,
		OwnerExists: lifecycle.OwnerExists(r.Client, &js.NatsConsumerList{}),
	}, o)
	if info != nil {
		s := &js.ConsumerServerStatus{}
		if !info.Created.IsZero() {
			s.Created = ptrTo(metav1.NewTime(info.Created))
		}
		s.Leader, s.Replicas = cluster(info.Cluster)
		c.Status.Server = s
	}
	return res, err
}

func (r *ConsumerReconciler) finalize(ctx context.Context, c *js.NatsConsumer) (reconcile.Result, error) {
	if c.Spec.DeletionPolicy == js.DeletionDelete {
		base := c.DeepCopy()
		o, err := r.object(ctx, c, false)
		if err != nil {
			return reconcile.Result{}, err
		}
		var wait *lifecycle.WaitError
		switch {
		case o == nil && !streamGone(c) && !lifecycle.Released(&c.Status.SyncStatus):
			return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, lifecycle.PatchStatus(ctx, r.Client, base, c, base.Status, c.Status)
		case o == nil:
		default:
			if err := lifecycle.Finalize(ctx, c.UID, c.Spec.DeletionPolicy, o); err != nil && !errors.As(err, &wait) {
				return reconcile.Result{}, err
			}
		}
	}
	return reconcile.Result{}, lifecycle.RemoveFinalizer(ctx, r.Client, c)
}

// streamGone reports whether c's status says the NatsStream its streamRef
// names does not exist.
func streamGone(c *js.NatsConsumer) bool {
	cond := meta.FindStatusCondition(c.Status.Conditions, lifecycle.ConditionReady)
	return c.Spec.StreamRef != nil && cond != nil && cond.Reason == ReasonStreamNotFound
}

// object resolves c's stream and connection, waiting for a referenced
// NatsStream to be Ready where ready is set. Where they cannot be resolved
// it records why on c's status and returns nil and no error.
func (r *ConsumerReconciler) object(ctx context.Context, c *js.NatsConsumer, ready bool) (*consumerObject, error) {
	st, gen := &c.Status.SyncStatus, c.Generation
	from := consumerReferrer(c.Namespace)
	stream := c.Spec.Stream
	var connRef natsv1beta1.ObjectReference
	if c.Spec.ConnectionRef != nil {
		connRef = *c.Spec.ConnectionRef
	}
	if ref := c.Spec.StreamRef; ref != nil {
		s, err := r.streamRef(ctx, c, *ref, ready)
		if err != nil || s == nil {
			return nil, err
		}
		stream = streamName(s)
		if c.Spec.ConnectionRef == nil {
			connRef, from = s.Spec.ConnectionRef, streamReferrer(s.Namespace)
			if connRef.Namespace == "" {
				connRef.Namespace = s.Namespace
			}
		}
	}
	api, err := lifecycle.Connect(ctx, r.Dialer, from, connRef, st, gen)
	if err != nil || api == nil {
		return nil, err
	}
	return &consumerObject{api: api, client: r.Client, obj: c, stream: stream}, nil
}

// streamRef returns the NatsStream ref names, or nil after recording on c's
// status why it cannot be used.
func (r *ConsumerReconciler) streamRef(ctx context.Context, c *js.NatsConsumer, ref natsv1beta1.ObjectReference, ready bool) (*js.NatsStream, error) {
	st, gen := &c.Status.SyncStatus, c.Generation
	ns := ref.Namespace
	if ns == "" {
		ns = c.Namespace
	}
	denied, err := grant.Admit(ctx, r.Client, consumerReferrer(c.Namespace), grant.Target{
		Group: js.GroupVersion.Group, Kind: StreamKind, Namespace: ns, Name: ref.Name,
	})
	if err != nil {
		return nil, err
	}
	if denied != nil {
		conditions.Set(&st.Conditions, gen, *denied)
		lifecycle.NotReady(st, gen, grant.ReasonReferenceNotPermitted, denied.Message)
		return nil, nil
	}
	var s js.NatsStream
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: ref.Name}, &s); err != nil {
		if apierrors.IsNotFound(err) {
			lifecycle.NotReady(st, gen, ReasonStreamNotFound, fmt.Sprintf("NatsStream %s/%s does not exist", ns, ref.Name))
			return nil, nil
		}
		return nil, fmt.Errorf("get NatsStream %s/%s: %w", ns, ref.Name, err)
	}
	if cond := meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionReady); ready &&
		(cond == nil || cond.Status != metav1.ConditionTrue || cond.ObservedGeneration != s.Generation) {
		lifecycle.NotReady(st, gen, ReasonStreamNotReady, fmt.Sprintf("NatsStream %s/%s is not Ready", ns, ref.Name))
		return nil, nil
	}
	return &s, nil
}

// SetupWithManager registers the field indexes the reconciler reads and
// builds its controller, watching NatsConsumers, the NatsStreams and
// NatsConnections they name and the NatsReferenceGrants that admit them.
func (r *ConsumerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	conns := func(o client.Object) []natsv1beta1.ObjectReference {
		if ref := o.(*js.NatsConsumer).Spec.ConnectionRef; ref != nil {
			return []natsv1beta1.ObjectReference{*ref}
		}
		return nil
	}
	all := func(o client.Object) []natsv1beta1.ObjectReference {
		out := conns(o)
		if ref := o.(*js.NatsConsumer).Spec.StreamRef; ref != nil {
			out = append(out, *ref)
		}
		return out
	}
	if err := lifecycle.IndexConnections(ctx, idx, &js.NatsConsumer{}, conns); err != nil {
		return fmt.Errorf("index NatsConsumer connections: %w", err)
	}
	if err := lifecycle.IndexUID(ctx, idx, &js.NatsConsumer{}); err != nil {
		return fmt.Errorf("index NatsConsumer UIDs: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsConsumer{}, refNamespaces(all)); err != nil {
		return fmt.Errorf("index NatsConsumer grant targets: %w", err)
	}
	if err := idx.IndexField(ctx, &js.NatsConsumer{}, StreamRefField, func(o client.Object) []string {
		ref := o.(*js.NatsConsumer).Spec.StreamRef
		if ref == nil {
			return nil
		}
		ns := ref.Namespace
		if ns == "" {
			ns = o.GetNamespace()
		}
		return []string{types.NamespacedName{Namespace: ns, Name: ref.Name}.String()}
	}); err != nil {
		return fmt.Errorf("index NatsConsumer stream refs: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsConsumer{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&js.NatsStream{}, lifecycle.EnqueueByField(mgr.GetClient(), &js.NatsConsumerList{}, StreamRefField)).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(mgr.GetClient(), &js.NatsConsumerList{}, lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(ConsumerKind).GroupKind(), &js.NatsConsumerList{})).
		Complete(telemetry.Traced("NatsConsumer", r))
}

func consumerReferrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: ConsumerKind, Namespace: namespace}
}

// consumerName is the server-side durable name of c.
func consumerName(c *js.NatsConsumer) string {
	if c.Spec.Name != "" {
		return c.Spec.Name
	}
	return c.Name
}

// consumerObject is a NatsConsumer's consumer on stream.
type consumerObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsConsumer
	stream string
}

// Describe implements lifecycle.Object.
func (o *consumerObject) Describe() string {
	return fmt.Sprintf("consumer %s on stream %s", consumerName(o.obj), o.stream)
}

// Fetch implements lifecycle.Object.
func (o *consumerObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, err := o.api.ConsumerInfo(ctx, o.stream, consumerName(o.obj))
	var apiErr *jetstream.APIError
	switch {
	case errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamNotFound:
		return nil, &lifecycle.WaitError{Reason: ReasonStreamNotFound, Message: fmt.Sprintf("stream %s does not exist", o.stream)}
	case errors.Is(err, lifecycle.ErrNotFound):
		return nil, nil
	}
	return info, err
}

// Desired implements lifecycle.Object.
func (o *consumerObject) Desired() (lifecycle.Config, error) {
	cfg, err := lifecycle.ToConfig(consumerToWire(&o.obj.Spec.ConsumerConfig, consumerName(o.obj)))
	if err != nil {
		return nil, err
	}
	if o.obj.Spec.DeliverSubject == "" {
		cfg["deliver_subject"] = nil
	}
	return cfg, nil
}

// Create implements lifecycle.Object.
func (o *consumerObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.api.PutConsumer(ctx, o.stream, lifecycle.ActionCreate, cfg)
}

// Update updates the consumer, or where cfg changes a field nats-server
// cannot, recreates it under recreateOnImmutableChange and otherwise
// returns a TerminalError.
func (o *consumerObject) Update(ctx context.Context, cur *lifecycle.Info, cfg lifecycle.Config) (*lifecycle.Info, error) {
	changed := lifecycle.Changed(cur.Config, cfg, immutableConsumerKeys...)
	if isPush(cur.Config) != isPush(cfg) {
		changed = append(changed, "deliver_subject")
	}
	if len(changed) == 0 {
		return o.api.PutConsumer(ctx, o.stream, lifecycle.ActionUpdate, cfg)
	}
	if o.obj.Spec.RecreateOnImmutableChange == nil || !*o.obj.Spec.RecreateOnImmutableChange {
		return nil, &lifecycle.TerminalError{
			Reason: ReasonImmutableField,
			Message: fmt.Sprintf("%s differs from spec in %s, which nats-server cannot change; set spec.recreateOnImmutableChange to recreate it",
				o.Describe(), strings.Join(changed, ", ")),
		}
	}
	if err := o.Delete(ctx); err != nil && !errors.Is(err, lifecycle.ErrNotFound) {
		return nil, err
	}
	return o.Create(ctx, cfg)
}

func isPush(cfg lifecycle.Config) bool {
	s, _ := cfg["deliver_subject"].(string)
	return s != ""
}

// Delete implements lifecycle.Object.
func (o *consumerObject) Delete(ctx context.Context) error {
	return o.api.DeleteConsumer(ctx, o.stream, consumerName(o.obj))
}

// WriteSpec implements lifecycle.Object.
func (o *consumerObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w consumerWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := consumerFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	want := o.obj.DeepCopy()
	if replace {
		want.Spec.ConsumerConfig = server
	} else if _, err := lifecycle.FillOmitted(&want.Spec.ConsumerConfig, &server); err != nil {
		return err
	}
	return lifecycle.PatchSpec(ctx, o.client, o.obj, want, &o.obj.Spec, want.Spec)
}
