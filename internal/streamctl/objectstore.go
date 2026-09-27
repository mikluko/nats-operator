package streamctl

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// ObjectStoreKind is the kind a NatsObjectStore is referred to by.
const ObjectStoreKind = "NatsObjectStore"

// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsobjectstores,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsobjectstores/status,verbs=patch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// ObjectStoreReconciler keeps NatsObjectStores' object stores at their
// specs through nats.go's object store manager, under their lifecycle
// policies. The marker is in the store's stream, OBJ_<bucket>, and drift is
// judged on the fields ObjectStoreConfig carries.
type ObjectStoreReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

var objectStoreKind = bucketKind[*js.NatsObjectStore]{
	kind: ObjectStoreKind,
	new:  func() *js.NatsObjectStore { return &js.NatsObjectStore{} },
	list: func() client.ObjectList { return &js.NatsObjectStoreList{} },
	fields: func(os *js.NatsObjectStore) bucketFields {
		return bucketFields{
			conn:     os.Spec.ConnectionRef,
			policies: os.Spec.Policies,
			deletion: os.Spec.DeletionPolicy,
			sync:     &os.Status.SyncStatus,
			server:   &os.Status.Server,
		}
	},
	object: func(api *lifecycle.API, c client.Client, os *js.NatsObjectStore) lifecycle.Object {
		return &objectStoreObject{api: api, client: c, obj: os}
	},
}

// Reconcile implements reconcile.Reconciler.
func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return objectStoreKind.reconcile(ctx, r.Client, r.Dialer, r.Syncer, req)
}

// SetupWithManager registers the field indexes the reconciler reads and
// builds its controller.
func (r *ObjectStoreReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return objectStoreKind.setup(ctx, mgr, r)
}

// objectStoreObject is a NatsObjectStore's object store. Its configs are
// nats.go ObjectStoreConfigs in JSON.
type objectStoreObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsObjectStore
}

func (o *objectStoreObject) bucket() string { return bucketName(o.obj.Spec.Name, o.obj) }

func (o *objectStoreObject) manager() (jetstream.ObjectStoreManager, error) {
	return jetstream.New(o.api.Conn)
}

// Describe implements lifecycle.Object.
func (o *objectStoreObject) Describe() string { return "object store " + o.bucket() }

// Fetch implements lifecycle.Object.
func (o *objectStoreObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, s, err := bucketStream(ctx, o.api, objStreamPrefix, o.bucket())
	if err != nil || info == nil {
		return nil, err
	}
	return withConfig(info, objFromStream(s))
}

// Desired implements lifecycle.Object.
func (o *objectStoreObject) Desired() (lifecycle.Config, error) {
	if p := o.obj.Spec.Placement; p != nil && p.Preferred != "" {
		return nil, errPreferred
	}
	return lifecycle.ToConfig(objToWire(&o.obj.Spec.ObjectStoreConfig, o.bucket()))
}

// Create implements lifecycle.Object.
func (o *objectStoreObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.put(ctx, cfg, jetstream.ObjectStoreManager.CreateObjectStore)
}

// Update implements lifecycle.Object.
func (o *objectStoreObject) Update(ctx context.Context, _ *lifecycle.Info, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.put(ctx, cfg, jetstream.ObjectStoreManager.UpdateObjectStore)
}

func (o *objectStoreObject) put(ctx context.Context, cfg lifecycle.Config, put func(jetstream.ObjectStoreManager, context.Context, jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error)) (*lifecycle.Info, error) {
	var oc jetstream.ObjectStoreConfig
	if err := decodeBucketConfig(cfg, &oc); err != nil {
		return nil, err
	}
	m, err := o.manager()
	if err != nil {
		return nil, err
	}
	if _, err := put(m, ctx, oc); err != nil {
		return nil, bucketError(err)
	}
	return refetch(ctx, o.Fetch, o.Describe())
}

// Delete implements lifecycle.Object.
func (o *objectStoreObject) Delete(ctx context.Context) error {
	m, err := o.manager()
	if err != nil {
		return err
	}
	if err := m.DeleteObjectStore(ctx, o.bucket()); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		return err
	}
	return nil
}

// WriteSpec implements lifecycle.Object.
func (o *objectStoreObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w objWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := objFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	want := o.obj.DeepCopy()
	if replace {
		want.Spec.ObjectStoreConfig = server
	} else if _, err := lifecycle.FillOmitted(&want.Spec.ObjectStoreConfig, &server); err != nil {
		return err
	}
	return lifecycle.PatchSpec(ctx, o.client, o.obj, want, &o.obj.Spec, want.Spec)
}
