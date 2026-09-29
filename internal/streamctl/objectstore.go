package streamctl

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
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
// policies. Drift is judged on the fields ObjectStoreConfig carries.
type ObjectStoreReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

var _ reconcile.Reconciler = (*ObjectStoreReconciler)(nil)

var objectStoreKind = lifecycle.Kind[*js.NatsObjectStore]{
	Name: ObjectStoreKind,
	New:  func() *js.NatsObjectStore { return &js.NatsObjectStore{} },
	List: func() client.ObjectList { return &js.NatsObjectStoreList{} },
	Fields: func(b *js.NatsObjectStore) lifecycle.Fields {
		return lifecycle.Fields{Policies: b.Spec.Policies, Deletion: b.Spec.DeletionPolicy, Sync: &b.Status.SyncStatus, Status: b.Status}
	},
	Connection: func(b *js.NatsObjectStore) natsv1beta1.ObjectReference { return b.Spec.ConnectionRef },
	Bind: func(api *lifecycle.API, c client.Client, b *js.NatsObjectStore) lifecycle.Object {
		return &objectStoreObject{api: api, client: c, obj: b}
	},
	Record: func(b *js.NatsObjectStore, info *lifecycle.Info) { b.Status.Server = streamServerStatus(info) },
}

// Reconcile brings the object store of the NatsObjectStore req names to its spec,
// or runs its deletion policy where the NatsObjectStore is being deleted.
func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return objectStoreKind.Reconcile(ctx, r.Client, r.Dialer, r.Syncer, req)
}

// SetupWithManager registers the reconciler with mgr.
func (r *ObjectStoreReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return objectStoreKind.SetupWithManager(ctx, mgr, r)
}

// objectStoreObject is a NatsObjectStore's object store. Its configs are
// nats.go ObjectStoreConfigs in JSON.
type objectStoreObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsObjectStore
}

var _ lifecycle.Object = (*objectStoreObject)(nil)

func (o *objectStoreObject) bucket() string { return bucketName(o.obj.Spec.Name, o.obj) }

func (o *objectStoreObject) manager() (jetstream.ObjectStoreManager, error) {
	return jetstream.New(o.api.Conn)
}

func (o *objectStoreObject) Describe() string { return "object store " + o.bucket() }

// Fetch returns a TerminalError, reason NotABucket, where stream
// OBJ_<bucket> is not one isObjectStore accepts.
func (o *objectStoreObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, s, err := bucketStream(ctx, o.api, objStreamPrefix, o.bucket())
	if err != nil || info == nil {
		return nil, err
	}
	if !isObjectStore(s, o.bucket()) {
		return nil, &lifecycle.TerminalError{
			Reason:  ReasonNotABucket,
			Message: fmt.Sprintf("stream %s exists and is not an object store", s.Name),
		}
	}
	return withConfig(info, objFromStream(s))
}

// isObjectStore reports whether s takes the chunk and meta subjects of
// bucket and allows rollups, as nats.go's object store needs.
func isObjectStore(s *streamWire, bucket string) bool {
	return deref(s.AllowRollup) &&
		slices.Contains(s.Subjects, "$O."+bucket+".C.>") &&
		slices.Contains(s.Subjects, "$O."+bucket+".M.>")
}

func (o *objectStoreObject) Desired() (lifecycle.Config, error) {
	if p := o.obj.Spec.Placement; p != nil && p.Preferred != "" {
		return nil, errPreferred
	}
	return lifecycle.ToConfig(objToWire(&o.obj.Spec.ObjectStoreConfig, o.bucket()))
}

func (o *objectStoreObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.put(ctx, cfg, jetstream.ObjectStoreManager.CreateObjectStore)
}

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

func (o *objectStoreObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w objWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := objFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	return lifecycle.WriteSpec(ctx, o.client, o.obj, func(b *js.NatsObjectStore) *js.ObjectStoreConfig { return &b.Spec.ObjectStoreConfig }, server, replace)
}
