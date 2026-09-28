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
	"github.com/mikluko/nats-operator/internal/jsapi"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
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
// NatsStream to be Ready.
type ConsumerReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

var consumerKind = lifecycle.Kind[*js.NatsConsumer]{
	Name: ConsumerKind,
	New:  func() *js.NatsConsumer { return &js.NatsConsumer{} },
	List: func() client.ObjectList { return &js.NatsConsumerList{} },
	Fields: func(c *js.NatsConsumer) lifecycle.Fields {
		return lifecycle.Fields{Policies: c.Spec.Policies, Deletion: c.Spec.DeletionPolicy, Sync: &c.Status.SyncStatus, Status: c.Status}
	},
	Resolve: func(ctx context.Context, c client.Client, d *natsconn.Dialer, obj *js.NatsConsumer, ready bool) (lifecycle.Object, bool, error) {
		o, gone, err := object(ctx, c, d, obj, ready)
		if o == nil {
			return nil, gone, err
		}
		return o, false, nil
	},
	Record: func(c *js.NatsConsumer, info *lifecycle.Info) {
		s := &js.ConsumerServerStatus{}
		if !info.Created.IsZero() {
			s.Created = ptrTo(metav1.NewTime(info.Created))
		}
		s.Leader, s.Replicas = cluster(info.Cluster)
		c.Status.Server = s
	},
	Conns: func(c *js.NatsConsumer) []natsv1beta1.ObjectReference {
		if ref := c.Spec.ConnectionRef; ref != nil {
			return []natsv1beta1.ObjectReference{*ref}
		}
		return nil
	},
	Refs: func(c *js.NatsConsumer) []natsv1beta1.ObjectReference {
		var out []natsv1beta1.ObjectReference
		if ref := c.Spec.ConnectionRef; ref != nil {
			out = append(out, *ref)
		}
		if ref := c.Spec.StreamRef; ref != nil {
			out = append(out, *ref)
		}
		return out
	},
	Watches: watchStreamRefs,
}

// Reconcile implements reconcile.Reconciler.
func (r *ConsumerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return consumerKind.Reconcile(ctx, r.Client, r.Dialer, r.Syncer, req)
}

// object resolves c's stream and connection, or returns nil after recording
// why on c's status, with gone reporting that nothing will reach the
// consumer.
func object(ctx context.Context, cl client.Client, d *natsconn.Dialer, c *js.NatsConsumer, ready bool) (o *consumerObject, gone bool, err error) {
	st, gen := &c.Status.SyncStatus, c.Generation
	from := consumerReferrer(c.Namespace)
	stream := c.Spec.Stream
	var connRef natsv1beta1.ObjectReference
	if c.Spec.ConnectionRef != nil {
		connRef = *c.Spec.ConnectionRef
	}
	if ref := c.Spec.StreamRef; ref != nil {
		s, gone, err := streamRef(ctx, cl, c, *ref, ready)
		if err != nil || s == nil {
			return nil, gone, err
		}
		stream = streamName(s)
		if c.Spec.ConnectionRef == nil {
			connRef, from = s.Spec.ConnectionRef, streamKind.Referrer(s.Namespace)
			if connRef.Namespace == "" {
				connRef.Namespace = s.Namespace
			}
		}
	}
	api, why, err := lifecycle.Connect(ctx, d, from, connRef, st, gen)
	if err != nil || api == nil {
		return nil, why.Released(), err
	}
	return &consumerObject{api: api, client: cl, obj: c, stream: stream}, false, nil
}

// streamRef returns the NatsStream ref names, or nil after recording on c's
// status why it cannot be used, with gone reporting that it does not exist or
// no grant admits it.
func streamRef(ctx context.Context, cl client.Client, c *js.NatsConsumer, ref natsv1beta1.ObjectReference, ready bool) (s *js.NatsStream, gone bool, err error) {
	st, gen := &c.Status.SyncStatus, c.Generation
	ns := ref.Namespace
	if ns == "" {
		ns = c.Namespace
	}
	denied, err := grant.Admit(ctx, cl, consumerReferrer(c.Namespace), grant.Target{
		Group: js.GroupVersion.Group, Kind: StreamKind, Namespace: ns, Name: ref.Name,
	})
	if err != nil {
		return nil, false, err
	}
	if denied != nil {
		conditions.Set(&st.Conditions, gen, *denied)
		lifecycle.NotReady(st, gen, grant.ReasonReferenceNotPermitted, denied.Message)
		return nil, true, nil
	}
	var got js.NatsStream
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: ref.Name}, &got); err != nil {
		if apierrors.IsNotFound(err) {
			lifecycle.NotReady(st, gen, ReasonStreamNotFound, fmt.Sprintf("NatsStream %s/%s does not exist", ns, ref.Name))
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("get NatsStream %s/%s: %w", ns, ref.Name, err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, lifecycle.ConditionReady); ready &&
		(cond == nil || cond.Status != metav1.ConditionTrue || cond.ObservedGeneration != got.Generation) {
		lifecycle.NotReady(st, gen, ReasonStreamNotReady, fmt.Sprintf("NatsStream %s/%s is not Ready", ns, ref.Name))
		return nil, false, nil
	}
	return &got, false, nil
}

// SetupWithManager registers the reconciler with mgr.
func (r *ConsumerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return consumerKind.SetupWithManager(ctx, mgr, r)
}

// watchStreamRefs registers StreamRefField and enqueues the NatsConsumers
// whose streamRef names a NatsStream that changes.
func watchStreamRefs(ctx context.Context, mgr ctrl.Manager, b *builder.Builder) (*builder.Builder, error) {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &js.NatsConsumer{}, StreamRefField, func(o client.Object) []string {
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
		return nil, fmt.Errorf("index NatsConsumer stream refs: %w", err)
	}
	return b.Watches(&js.NatsStream{}, refindex.EnqueueByField(mgr.GetClient(), &js.NatsConsumerList{}, StreamRefField)), nil
}

// consumerReferrer is consumerKind.Referrer, which consumerKind's own
// initializer cannot reference.
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
	case errors.Is(err, jsapi.ErrNotFound):
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
	if err := o.Delete(ctx); err != nil && !errors.Is(err, jsapi.ErrNotFound) {
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
	return lifecycle.WriteSpec(ctx, o.client, o.obj, func(c *js.NatsConsumer) *js.ConsumerConfig { return &c.Spec.ConsumerConfig }, server, replace)
}
