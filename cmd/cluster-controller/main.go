// The cluster controller owns cluster.nats.mikluko.io and reads
// nats.mikluko.io.
package main

import (
	"flag"
	"net/http"
	"os"
	"time"

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
)

func main() {
	opts := manager.Flags(flag.CommandLine, clusterv1beta1.GroupVersion.Group)
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("cluster-controller")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		natsv1beta1.AddToScheme,
		clusterv1beta1.AddToScheme,
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
	pool := natsconn.NewPool()
	if err := mgr.Add(pool); err != nil {
		log.Error(err, "add connection pool")
		os.Exit(1)
	}
	sys := &natscluster.SystemConnections{
		Client:   mgr.GetClient(),
		Pool:     pool,
		Fallback: natscluster.MonitorObserver{Monitor: sysobs.NewMonitor(&http.Client{Timeout: 5 * time.Second}, 0)},
	}
	r := &natscluster.Reconciler{
		Client:   mgr.GetClient(),
		Observer: sys,
		Reloader: sys.Reloader,
		Forget:   sys.Forget,
	}
	ctx := ctrl.SetupSignalHandler()
	if err := r.SetupWithManager(ctx, mgr); err != nil {
		log.Error(err, "set up natscluster reconciler")
		os.Exit(1)
	}
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "run")
		os.Exit(1)
	}
}
