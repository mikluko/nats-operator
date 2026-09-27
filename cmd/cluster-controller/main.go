// The cluster controller owns cluster.nats.mikluko.io and reads
// nats.mikluko.io.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natscluster"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	opts := manager.Flags(flag.CommandLine, clusterv1beta1.GroupVersion.Group)
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("cluster-controller")

	scheme, err := newScheme()
	if err != nil {
		log.Error(err, "build scheme")
		os.Exit(1)
	}

	mgr, err := manager.New(ctrl.GetConfigOrDie(), opts, scheme)
	if err != nil {
		log.Error(err, "start")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := telemetry.Install(ctx, mgr, telemetry.ClusterController); err != nil {
		log.Error(err, "set up telemetry")
		os.Exit(1)
	}
	if err := setup(ctx, mgr); err != nil {
		log.Error(err, "set up controllers")
		os.Exit(1)
	}
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "run")
		os.Exit(1)
	}
}

// newScheme is the cluster controller's scheme.
func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		natsv1beta1.AddToScheme,
		clusterv1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, err
		}
	}
	return scheme, nil
}

// setup registers the cluster controller's instruments and adds the
// connection pool and the NatsCluster reconciler to mgr.
func setup(ctx context.Context, mgr ctrl.Manager) error {
	if err := telemetry.RegisterCluster(otel.Meter(telemetry.ClusterController), mgr.GetClient()); err != nil {
		return fmt.Errorf("register instruments: %w", err)
	}
	pool := natsconn.NewPool()
	if err := mgr.Add(pool); err != nil {
		return fmt.Errorf("add connection pool: %w", err)
	}
	sys := &natscluster.SystemConnections{
		Client:   mgr.GetClient(),
		Pool:     pool,
		Fallback: natscluster.PodMonitor{Monitor: sysobs.NewMonitor(&http.Client{Timeout: 5 * time.Second}, 0)},
	}
	r := &natscluster.Reconciler{
		Client:   mgr.GetClient(),
		Observer: sys,
		Reloader: sys.Reloader,
		Admin:    sys.Admin,
		Forget:   sys.Forget,
		Recorder: mgr.GetEventRecorder(telemetry.ClusterController),
	}
	if err := r.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("set up NatsCluster reconciler: %w", err)
	}
	return nil
}
