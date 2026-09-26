package streamctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// StreamKind is the kind a NatsStream is referred to by.
const StreamKind = "NatsStream"

// StreamReconciler keeps NatsStreams' streams at their specs through their
// connections, under their lifecycle policies. Deleting a NatsStream whose
// deletionPolicy is Delete waits until its connection can delete the stream.
type StreamReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

// Reconcile implements reconcile.Reconciler.
func (r *StreamReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var s js.NatsStream
	if err := r.Client.Get(ctx, req.NamespacedName, &s); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !s.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &s)
	}
	if err := lifecycle.AddFinalizer(ctx, r.Client, &s); err != nil {
		return reconcile.Result{}, err
	}
	base := s.DeepCopy()
	res, syncErr := r.sync(ctx, &s)
	if err := lifecycle.PatchStatus(ctx, r.Client, base, &s, base.Status, s.Status); err != nil {
		return reconcile.Result{}, errors.Join(syncErr, err)
	}
	return res, syncErr
}

func (r *StreamReconciler) sync(ctx context.Context, s *js.NatsStream) (reconcile.Result, error) {
	api, err := lifecycle.Connect(ctx, r.Dialer, streamReferrer(s.Namespace), s.Spec.ConnectionRef, &s.Status.SyncStatus, s.Generation)
	if err != nil || api == nil {
		return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, err
	}
	o := &streamObject{api: api, client: r.Client, obj: s}
	res, info, err := r.Syncer.Sync(ctx, lifecycle.Resource{
		Object:      s,
		Policies:    s.Spec.Policies,
		Status:      &s.Status.SyncStatus,
		OwnerExists: lifecycle.OwnerExists(r.Client, &js.NatsStreamList{}),
	}, o)
	if info != nil {
		s.Status.Server = streamServerStatus(info)
	}
	return res, err
}

func (r *StreamReconciler) finalize(ctx context.Context, s *js.NatsStream) (reconcile.Result, error) {
	if s.Spec.DeletionPolicy == js.DeletionDelete {
		base := s.DeepCopy()
		api, err := lifecycle.Connect(ctx, r.Dialer, streamReferrer(s.Namespace), s.Spec.ConnectionRef, &s.Status.SyncStatus, s.Generation)
		if err != nil {
			return reconcile.Result{}, err
		}
		if api == nil {
			return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, lifecycle.PatchStatus(ctx, r.Client, base, s, base.Status, s.Status)
		}
		if err := lifecycle.Finalize(ctx, s.UID, s.Spec.DeletionPolicy, &streamObject{api: api, obj: s}); err != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, lifecycle.RemoveFinalizer(ctx, r.Client, s)
}

// SetupWithManager registers the field indexes the reconciler reads and
// builds its controller, watching NatsStreams, the NatsConnections they
// name and the NatsReferenceGrants that admit them.
func (r *StreamReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	refs := func(o client.Object) []natsv1beta1.ObjectReference {
		return []natsv1beta1.ObjectReference{o.(*js.NatsStream).Spec.ConnectionRef}
	}
	if err := lifecycle.IndexConnections(ctx, idx, &js.NatsStream{}, refs); err != nil {
		return fmt.Errorf("index NatsStream connections: %w", err)
	}
	if err := lifecycle.IndexUID(ctx, idx, &js.NatsStream{}); err != nil {
		return fmt.Errorf("index NatsStream UIDs: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &js.NatsStream{}, refNamespaces(refs)); err != nil {
		return fmt.Errorf("index NatsStream grant targets: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&js.NatsStream{}, builder.WithPredicates(lifecycle.SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, lifecycle.EnqueueByField(mgr.GetClient(), &js.NatsStreamList{}, lifecycle.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), js.GroupVersion.WithKind(StreamKind).GroupKind(), &js.NatsStreamList{})).
		Complete(telemetry.Traced("NatsStream", r))
}

func streamReferrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: js.GroupVersion.Group, Kind: StreamKind, Namespace: namespace}
}

// streamName is the server-side name of s.
func streamName(s *js.NatsStream) string {
	if s.Spec.Name != "" {
		return s.Spec.Name
	}
	return s.Name
}

// streamObject is a NatsStream's stream.
type streamObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsStream
}

// Describe implements lifecycle.Object.
func (o *streamObject) Describe() string { return "stream " + streamName(o.obj) }

// Fetch implements lifecycle.Object.
func (o *streamObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, err := o.api.StreamInfo(ctx, streamName(o.obj))
	if errors.Is(err, lifecycle.ErrNotFound) {
		return nil, nil
	}
	return info, err
}

// Desired implements lifecycle.Object.
func (o *streamObject) Desired() (lifecycle.Config, error) {
	w := streamToWire(&o.obj.Spec.StreamConfig)
	w.Name = streamName(o.obj)
	return lifecycle.ToConfig(w)
}

// Create implements lifecycle.Object.
func (o *streamObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.api.CreateStream(ctx, cfg)
}

// Update implements lifecycle.Object.
func (o *streamObject) Update(ctx context.Context, _ *lifecycle.Info, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.api.UpdateStream(ctx, cfg)
}

// Delete implements lifecycle.Object.
func (o *streamObject) Delete(ctx context.Context) error {
	return o.api.DeleteStream(ctx, streamName(o.obj))
}

// WriteSpec implements lifecycle.Object.
func (o *streamObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w streamWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := streamFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	want := o.obj.DeepCopy()
	if replace {
		want.Spec.StreamConfig = server
	} else if _, err := lifecycle.FillOmitted(&want.Spec.StreamConfig, &server); err != nil {
		return err
	}
	return lifecycle.PatchSpec(ctx, o.client, o.obj, want, &o.obj.Spec, want.Spec)
}

// streamServerStatus reads the stream's state from info.
func streamServerStatus(info *lifecycle.Info) *js.StreamServerStatus {
	var si jetstream.StreamInfo
	if err := json.Unmarshal(info.Raw, &si); err != nil {
		return nil
	}
	out := &js.StreamServerStatus{
		Messages: int64(si.State.Msgs),                                           //nolint:gosec // message counts fit.
		Bytes:    resource.NewQuantity(int64(si.State.Bytes), resource.BinarySI), //nolint:gosec // byte counts fit.
	}
	if !info.Created.IsZero() {
		out.Created = ptrTo(metav1.NewTime(info.Created))
	}
	out.Leader, out.Replicas = cluster(info.Cluster)
	return out
}

func cluster(c *jetstream.ClusterInfo) (string, []js.ReplicaStatus) {
	if c == nil {
		return "", nil
	}
	var reps []js.ReplicaStatus
	for _, p := range c.Replicas {
		reps = append(reps, js.ReplicaStatus{Name: p.Name, Current: p.Current, Lag: int64(p.Lag)}) //nolint:gosec // lag fits.
	}
	return c.Leader, reps
}
