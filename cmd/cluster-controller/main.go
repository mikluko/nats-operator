// The cluster controller owns cluster.nats.mikluko.io and reads
// nats.mikluko.io.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natscluster"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	if err := manager.Run(manager.Controller{
		Name:        telemetry.ClusterController,
		Group:       clusterv1beta1.GroupVersion.Group,
		AddToScheme: schemes,
		Owned:       owned,
		Setup:       setup,
	}); err != nil {
		os.Exit(1)
	}
}

// owned is what the NatsCluster reconciler renders, the data volume claims
// its StatefulSets' templates stamp out included.
var owned = manager.Owned{
	Label: natscluster.LabelCluster,
	Kinds: []client.Object{
		&appsv1.StatefulSet{},
		&corev1.ConfigMap{},
		&corev1.Service{},
		&corev1.PersistentVolumeClaim{},
		&policyv1.PodDisruptionBudget{},
		&networkingv1.NetworkPolicy{},
	},
}

var schemes = []func(*runtime.Scheme) error{natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme}

func setup(ctx context.Context, mgr ctrl.Manager) error {
	if err := telemetry.RegisterCluster(otel.Meter(telemetry.ClusterController), mgr.GetClient()); err != nil {
		return fmt.Errorf("register instruments: %w", err)
	}
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetClusterController))
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
