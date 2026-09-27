package streamctl

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// KeyValueKind is the kind a NatsKeyValue is referred to by.
const KeyValueKind = "NatsKeyValue"

// ReasonNotABucket is Terminal's reason when the stream a bucket would be
// kept in exists and is not a key-value bucket.
const ReasonNotABucket = "NotABucket"

// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natskeyvalues,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natskeyvalues/status,verbs=patch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// KeyValueReconciler keeps NatsKeyValues' buckets at their specs through
// nats.go's key-value manager, under their lifecycle policies. The marker
// is in the bucket's stream, KV_<bucket>, and drift is judged on the
// fields KeyValueConfig carries.
type KeyValueReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

var keyValueKind = lifecycle.Kind[*js.NatsKeyValue]{
	Name: KeyValueKind,
	New:  func() *js.NatsKeyValue { return &js.NatsKeyValue{} },
	List: func() client.ObjectList { return &js.NatsKeyValueList{} },
	Fields: func(b *js.NatsKeyValue) lifecycle.Fields {
		return lifecycle.Fields{Policies: b.Spec.Policies, Deletion: b.Spec.DeletionPolicy, Sync: &b.Status.SyncStatus, Status: b.Status}
	},
	Connection: func(b *js.NatsKeyValue) natsv1beta1.ObjectReference { return b.Spec.ConnectionRef },
	Bind: func(api *lifecycle.API, c client.Client, b *js.NatsKeyValue) lifecycle.Object {
		return &keyValueObject{api: api, client: c, obj: b}
	},
	Record: func(b *js.NatsKeyValue, info *lifecycle.Info) { b.Status.Server = streamServerStatus(info) },
}

// Reconcile implements reconcile.Reconciler.
func (r *KeyValueReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return keyValueKind.Reconcile(ctx, r.Client, r.Dialer, r.Syncer, req)
}

// SetupWithManager registers the field indexes the reconciler reads and
// builds its controller.
func (r *KeyValueReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return keyValueKind.SetupWithManager(ctx, mgr, r)
}

// keyValueObject is a NatsKeyValue's bucket. Its configs are nats.go
// KeyValueConfigs in JSON.
type keyValueObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsKeyValue
}

func (o *keyValueObject) bucket() string { return bucketName(o.obj.Spec.Name, o.obj) }

func (o *keyValueObject) manager() (jetstream.KeyValueManager, error) {
	return jetstream.New(o.api.Conn)
}

// Describe implements lifecycle.Object.
func (o *keyValueObject) Describe() string { return "key-value bucket " + o.bucket() }

// Fetch implements lifecycle.Object. A stream KV_<bucket> that keeps no
// history per subject is not a bucket, and is Terminal.
func (o *keyValueObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, s, err := bucketStream(ctx, o.api, kvStreamPrefix, o.bucket())
	if err != nil || info == nil {
		return nil, err
	}
	if deref(s.MaxMsgsPerSubject) < 1 {
		return nil, &lifecycle.TerminalError{
			Reason:  ReasonNotABucket,
			Message: fmt.Sprintf("stream %s exists and is not a key-value bucket", s.Name),
		}
	}
	return withConfig(info, kvFromStream(s))
}

// Desired implements lifecycle.Object.
func (o *keyValueObject) Desired() (lifecycle.Config, error) {
	if p := o.obj.Spec.Placement; p != nil && p.Preferred != "" {
		return nil, errPreferred
	}
	return lifecycle.ToConfig(kvToWire(&o.obj.Spec.KeyValueConfig, o.bucket()))
}

// Create implements lifecycle.Object.
func (o *keyValueObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.put(ctx, cfg, jetstream.KeyValueManager.CreateKeyValue)
}

// Update implements lifecycle.Object.
func (o *keyValueObject) Update(ctx context.Context, _ *lifecycle.Info, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.put(ctx, cfg, jetstream.KeyValueManager.UpdateKeyValue)
}

func (o *keyValueObject) put(ctx context.Context, cfg lifecycle.Config, put func(jetstream.KeyValueManager, context.Context, jetstream.KeyValueConfig) (jetstream.KeyValue, error)) (*lifecycle.Info, error) {
	var kc jetstream.KeyValueConfig
	if err := decodeBucketConfig(cfg, &kc); err != nil {
		return nil, err
	}
	m, err := o.manager()
	if err != nil {
		return nil, err
	}
	if _, err := put(m, ctx, kc); err != nil {
		return nil, bucketError(err)
	}
	return refetch(ctx, o.Fetch, o.Describe())
}

// Delete implements lifecycle.Object.
func (o *keyValueObject) Delete(ctx context.Context) error {
	m, err := o.manager()
	if err != nil {
		return err
	}
	if err := m.DeleteKeyValue(ctx, o.bucket()); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		return err
	}
	return nil
}

// WriteSpec implements lifecycle.Object.
func (o *keyValueObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w kvWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := kvFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	return lifecycle.WriteSpec(ctx, o.client, o.obj, func(b *js.NatsKeyValue) *js.KeyValueConfig { return &b.Spec.KeyValueConfig }, server, replace)
}
