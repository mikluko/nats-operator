// Command jetstream-controller owns jetstream.nats-operator.io and reads
// nats-operator.io.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balancectl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/streamctl"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	resync := flag.Duration("resync-period", lifecycle.DefaultResync, "how often a JetStream resource is compared to its server object")
	if err := manager.Run(manager.Controller{
		Name:        telemetry.JetStreamController,
		Group:       jetstreamv1beta1.GroupVersion.Group,
		AddToScheme: schemes,
		Setup: func(ctx context.Context, mgr ctrl.Manager) error {
			return setup(ctx, mgr, *resync)
		},
	}); err != nil {
		os.Exit(1)
	}
}

var schemes = []func(*runtime.Scheme) error{natsv1beta1.AddToScheme, jetstreamv1beta1.AddToScheme}

// +kubebuilder:rbac:groups=nats-operator.io,resources=natsconnections,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats-operator.io,resources=natsconnections/status,verbs=patch

// setup adds the JetStream controller to mgr, comparing each JetStream
// resource to its server object every resync.
func setup(ctx context.Context, mgr ctrl.Manager, resync time.Duration) error {
	metrics, err := telemetry.RegisterJetStream(otel.Meter(telemetry.JetStreamController), mgr.GetClient())
	if err != nil {
		return fmt.Errorf("register instruments: %w", err)
	}
	rec := mgr.GetEventRecorder(telemetry.JetStreamController)
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetJetStreamController))
	if err := mgr.Add(pool); err != nil {
		return fmt.Errorf("add connection pool: %w", err)
	}
	conns := &natsconn.Reconciler{Client: mgr.GetClient(), Pool: pool}
	if err := conns.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsConnection reconciler: %w", err)
	}
	dialer := &natsconn.Dialer{Reader: mgr.GetClient(), Pool: pool}
	syncer := lifecycle.Syncer{Resync: resync}
	streams := &streamctl.StreamReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}
	if err := streams.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsStream reconciler: %w", err)
	}
	consumers := &streamctl.ConsumerReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}
	if err := consumers.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsConsumer reconciler: %w", err)
	}
	kvs := &streamctl.KeyValueReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}
	if err := kvs.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsKeyValue reconciler: %w", err)
	}
	stores := &streamctl.ObjectStoreReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}
	if err := stores.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsObjectStore reconciler: %w", err)
	}
	balancers := &balancectl.SystemBalancerReconciler{Client: mgr.GetClient(), Dialer: dialer, Recorder: rec, Telemetry: metrics}
	if err := balancers.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsSystemBalancer reconciler: %w", err)
	}
	accounts := &balancectl.BalancerReconciler{Client: mgr.GetClient(), Dialer: dialer, Recorder: rec, Telemetry: metrics}
	if err := accounts.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsBalancer reconciler: %w", err)
	}
	evacuations := &balancectl.EvacuationReconciler{Client: mgr.GetClient(), Dialer: dialer, Recorder: rec}
	if err := evacuations.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsClusterEvacuation reconciler: %w", err)
	}
	return nil
}
