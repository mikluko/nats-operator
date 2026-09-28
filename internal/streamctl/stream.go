package streamctl

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jsapi"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// StreamKind is the kind a NatsStream is referred to by.
const StreamKind = "NatsStream"

// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsstreams,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=jetstream.nats.mikluko.io,resources=natsstreams/status,verbs=patch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsreferencegrants,verbs=list;watch

// StreamReconciler keeps NatsStreams' streams at their specs through their
// connections, under their lifecycle policies.
type StreamReconciler struct {
	Client client.Client
	Dialer *natsconn.Dialer
	Syncer lifecycle.Syncer
}

var _ reconcile.Reconciler = (*StreamReconciler)(nil)

var streamKind = lifecycle.Kind[*js.NatsStream]{
	Name: StreamKind,
	New:  func() *js.NatsStream { return &js.NatsStream{} },
	List: func() client.ObjectList { return &js.NatsStreamList{} },
	Fields: func(s *js.NatsStream) lifecycle.Fields {
		return lifecycle.Fields{Policies: s.Spec.Policies, Deletion: s.Spec.DeletionPolicy, Sync: &s.Status.SyncStatus, Status: s.Status}
	},
	Connection: func(s *js.NatsStream) natsv1beta1.ObjectReference { return s.Spec.ConnectionRef },
	Bind: func(api *lifecycle.API, c client.Client, s *js.NatsStream) lifecycle.Object {
		return &streamObject{api: api, client: c, obj: s}
	},
	Record:  func(s *js.NatsStream, info *lifecycle.Info) { s.Status.Server = streamServerStatus(info) },
	Observe: observeTransfer,
}

// Reconcile brings the stream of the NatsStream req names to its spec,
// or runs its deletion policy where the NatsStream is being deleted.
func (r *StreamReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return streamKind.Reconcile(ctx, r.Client, r.Dialer, r.Syncer, req)
}

// SetupWithManager registers the reconciler with mgr.
func (r *StreamReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return streamKind.SetupWithManager(ctx, mgr, r)
}

// streamName is the server-side name of s.
func streamName(s *js.NatsStream) string {
	if s.Spec.Name != "" {
		return s.Spec.Name
	}
	return s.Name
}

type streamObject struct {
	api    *lifecycle.API
	client client.Client
	obj    *js.NatsStream
}

var _ lifecycle.Object = (*streamObject)(nil)

func (o *streamObject) Describe() string { return "stream " + streamName(o.obj) }

func (o *streamObject) Fetch(ctx context.Context) (*lifecycle.Info, error) {
	info, err := o.api.StreamInfo(ctx, streamName(o.obj))
	if errors.Is(err, jsapi.ErrNotFound) {
		return nil, nil
	}
	return info, err
}

func (o *streamObject) Desired() (lifecycle.Config, error) {
	w := streamToWire(&o.obj.Spec.StreamConfig)
	w.Name = streamName(o.obj)
	return lifecycle.ToConfig(w)
}

func (o *streamObject) Create(ctx context.Context, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.api.CreateStream(ctx, cfg)
}

func (o *streamObject) Update(ctx context.Context, _ *lifecycle.Info, cfg lifecycle.Config) (*lifecycle.Info, error) {
	return o.api.UpdateStream(ctx, cfg)
}

func (o *streamObject) Delete(ctx context.Context) error {
	return o.api.DeleteStream(ctx, streamName(o.obj))
}

func (o *streamObject) WriteSpec(ctx context.Context, cfg lifecycle.Config, replace bool) error {
	var w streamWire
	if err := lifecycle.FromConfig(cfg, &w); err != nil {
		return err
	}
	server := streamFromWire(&w)
	server.Name = o.obj.Spec.Name
	server.Metadata = cfg.UserMetadata()
	return lifecycle.WriteSpec(ctx, o.client, o.obj, func(s *js.NatsStream) *js.StreamConfig { return &s.Spec.StreamConfig }, server, replace)
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
