// The JetStream controller owns jetstream.nats.mikluko.io and reads
// nats.mikluko.io.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balancectl"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/streamctl"
)

func main() {
	opts := manager.Flags(flag.CommandLine, jetstreamv1beta1.GroupVersion.Group)
	resync := flag.Duration("resync-period", lifecycle.DefaultResync, "how often a JetStream resource is compared to its server object")
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("jetstream-controller")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		natsv1beta1.AddToScheme,
		jetstreamv1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			log.Error(err, "build scheme")
			os.Exit(1)
		}
	}

	mgr, err := manager.New(opts, scheme)
	if err != nil {
		log.Error(err, "start")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := setup(ctx, mgr, *resync); err != nil {
		log.Error(err, "set up controllers")
		os.Exit(1)
	}
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "run")
		os.Exit(1)
	}
}

// setup adds the connection pool, the NatsConnection reconciler, the
// stream and consumer reconcilers, the system and account balancer
// reconcilers and the evacuation reconciler to mgr.
func setup(ctx context.Context, mgr ctrl.Manager, resync time.Duration) error {
	pool := natsconn.NewPool()
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
	balancers := &balancectl.SystemBalancerReconciler{Client: mgr.GetClient(), Dialer: dialer}
	if err := balancers.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsSystemBalancer reconciler: %w", err)
	}
	accounts := &balancectl.BalancerReconciler{Client: mgr.GetClient(), Dialer: dialer}
	if err := accounts.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsBalancer reconciler: %w", err)
	}
	evacuations := &balancectl.EvacuationReconciler{Client: mgr.GetClient(), Dialer: dialer}
	if err := evacuations.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsClusterEvacuation reconciler: %w", err)
	}
	return nil
}
