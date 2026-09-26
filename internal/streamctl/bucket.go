package streamctl

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// bucketKind adapts NatsKeyValue or NatsObjectStore, P, to the reconcile
// loop the two share.
type bucketKind[P client.Object] struct {
	kind   string
	new    func() P
	list   func() client.ObjectList
	fields func(P) bucketFields
	object func(api *lifecycle.API, c client.Client, obj P) lifecycle.Object
}

// bucketFields are the parts of a bucket resource the reconcile loop reads
// and writes, pointing into the resource.
type bucketFields struct {
	conn     natsv1beta1.ObjectReference
	policies js.Policies
	deletion js.DeletionPolicy
	sync     *js.SyncStatus
	server   **js.StreamServerStatus
}

// bucketStatus is a bucket resource's status, compared to decide whether
// to write it.
// Its fields are exported for equality.Semantic.
type bucketStatus struct {
	Sync   js.SyncStatus
	Server *js.StreamServerStatus
}

func (f bucketFields) status() bucketStatus { return bucketStatus{Sync: *f.sync, Server: *f.server} }

func (k bucketKind[P]) reconcile(ctx context.Context, c client.Client, d *natsconn.Dialer, s lifecycle.Syncer, req reconcile.Request) (reconcile.Result, error) {
	obj := k.new()
	if err := c.Get(ctx, req.NamespacedName, obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !obj.GetDeletionTimestamp().IsZero() {
		return k.finalize(ctx, c, d, obj)
	}
	if err := lifecycle.AddFinalizer(ctx, c, obj); err != nil {
		return reconcile.Result{}, err
	}
	base, ok := obj.DeepCopyObject().(P)
	if !ok {
		return reconcile.Result{}, fmt.Errorf("%s deep copy is %T", k.kind, obj.DeepCopyObject())
	}
	res, syncErr := k.sync(ctx, c, d, s, obj)
	if err := lifecycle.PatchStatus(ctx, c, base, obj, k.fields(base).status(), k.fields(obj).status()); err != nil {
		return reconcile.Result{}, errors.Join(syncErr, err)
	}
	return res, syncErr
}

func (k bucketKind[P]) sync(ctx context.Context, c client.Client, d *natsconn.Dialer, s lifecycle.Syncer, obj P) (reconcile.Result, error) {
	f := k.fields(obj)
	api, err := lifecycle.Connect(ctx, d, k.referrer(obj.GetNamespace()), f.conn, f.sync, obj.GetGeneration())
	if err != nil || api == nil {
		return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, err
	}
	res, info, err := s.Sync(ctx, lifecycle.Resource{
		Object:      obj,
		Policies:    f.policies,
		Status:      f.sync,
		OwnerExists: lifecycle.OwnerExists(c, k.list()),
	}, k.object(api, c, obj))
	if info != nil {
		*f.server = streamServerStatus(info)
	}
	return res, err
}

func (k bucketKind[P]) finalize(ctx context.Context, c client.Client, d *natsconn.Dialer, obj P) (reconcile.Result, error) {
	f := k.fields(obj)
	if f.deletion == js.DeletionDelete {
		base, ok := obj.DeepCopyObject().(P)
		if !ok {
			return reconcile.Result{}, fmt.Errorf("%s deep copy is %T", k.kind, obj.DeepCopyObject())
		}
		api, err := lifecycle.Connect(ctx, d, k.referrer(obj.GetNamespace()), f.conn, f.sync, obj.GetGeneration())
		if err != nil {
			return reconcile.Result{}, err
		}
		switch {
		case api == nil && !lifecycle.Released(f.sync):
			return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter},
				lifecycle.PatchStatus(ctx, c, base, obj, k.fields(base).status(), f.status())
		case api == nil:
		default:
			if err := lifecycle.Finalize(ctx, obj.GetUID(), f.deletion, k.object(api, c, obj)); err != nil {
				return reconcile.Result{}, err
			}
		}
	}
	return reconcile.Result{}, lifecycle.RemoveFinalizer(ctx, c, obj)
}

// setup registers the field indexes the reconcile loop reads and builds r's
// controller, watching the kind, the NatsConnections it names and the
// NatsReferenceGrants that admit it.
func (k bucketKind[P]) setup(ctx context.Context, mgr ctrl.Manager, r reconcile.Reconciler) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		p, ok := o.(P)
		if !ok {
			return nil
		}
		return []natsv1beta1.ObjectReference{k.fields(p).conn}
	}
	if err := lifecycle.IndexConnections(ctx, idx, k.new(), refs); err != nil {
		return fmt.Errorf("index %s connections: %w", k.kind, err)
	}
	if err := lifecycle.IndexUID(ctx, idx, k.new()); err != nil {
		return fmt.Errorf("index %s UIDs: %w", k.kind, err)
	}
	if err := grant.IndexReferrers(ctx, idx, k.new(), refNamespaces(refs)); err != nil {
		return fmt.Errorf("index %s grant targets: %w", k.kind, err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(k.new(), builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(mgr.GetClient(), k.list(), lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(k.kind).GroupKind(), k.list())).
		Complete(telemetry.Traced(k.kind, r))
}

func (k bucketKind[P]) referrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: k.kind, Namespace: namespace}
}

// bucketName is a bucket resource's server-side bucket name.
func bucketName(spec string, obj client.Object) string {
	if spec != "" {
		return spec
	}
	return obj.GetName()
}

// validBucket is the bucket names nats.go's bucket managers take.
var validBucket = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// bucketStream fetches the stream prefix+bucket, the one a bucket is kept
// in, returning nil when it does not exist. A bucket name nats.go refuses is
// a TerminalError, and nothing is read.
func bucketStream(ctx context.Context, api *lifecycle.API, prefix, bucket string) (*lifecycle.Info, *streamWire, error) {
	if !validBucket.MatchString(bucket) {
		return nil, nil, &lifecycle.TerminalError{
			Reason:  lifecycle.ReasonRejected,
			Message: fmt.Sprintf("bucket name %q is not letters, digits, '-' and '_'", bucket),
		}
	}
	info, err := api.StreamInfo(ctx, prefix+bucket)
	if errors.Is(err, lifecycle.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var w streamWire
	if err := lifecycle.FromConfig(info.Config, &w); err != nil {
		return nil, nil, err
	}
	return info, &w, nil
}

// withConfig returns info carrying cfg, a bucket config read from info's
// stream config, in its place.
func withConfig(info *lifecycle.Info, v any) (*lifecycle.Info, error) {
	cfg, err := lifecycle.ToConfig(v)
	if err != nil {
		return nil, err
	}
	out := *info
	out.Config = cfg
	return &out, nil
}

// decodeBucketConfig decodes cfg into v, nats.go's KeyValueConfig or
// ObjectStoreConfig; a value v cannot hold is a spec the server will
// never take.
func decodeBucketConfig(cfg lifecycle.Config, v any) error {
	if err := lifecycle.FromConfig(cfg, v); err != nil {
		return &lifecycle.TerminalError{Reason: lifecycle.ReasonRejected, Message: err.Error()}
	}
	return nil
}

// bucketError returns err from a nats.go bucket manager call, a
// TerminalError where nats.go refused the config before sending it.
func bucketError(err error) error {
	for _, invalid := range []error{
		jetstream.ErrInvalidBucketName,
		jetstream.ErrInvalidStoreName,
		jetstream.ErrHistoryTooLarge,
		jetstream.ErrLimitMarkerTTLNotSupported,
	} {
		if errors.Is(err, invalid) {
			return &lifecycle.TerminalError{Reason: lifecycle.ReasonRejected, Message: err.Error()}
		}
	}
	return err
}

// errPreferred is the TerminalError for a bucket spec naming a preferred
// leader, which nats.go's Placement cannot carry.
var errPreferred = &lifecycle.TerminalError{
	Reason:  lifecycle.ReasonRejected,
	Message: "placement.preferred cannot be set on a bucket: nats.go's bucket managers do not carry it",
}

// refetch returns the bucket's info after a create or update through fetch.
func refetch(ctx context.Context, fetch func(context.Context) (*lifecycle.Info, error), describe string) (*lifecycle.Info, error) {
	info, err := fetch(ctx)
	if err == nil && info == nil {
		return nil, fmt.Errorf("%s is gone after it was written", describe)
	}
	return info, err
}
