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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natscluster"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	if err := manager.Run(manager.Controller{
		Name:      telemetry.ClusterController,
		Group:     clusterv1beta1.GroupVersion.Group,
		NewScheme: newScheme,
		Owned:     owned,
		Setup:     setup,
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

// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// setup registers the cluster controller's instruments and adds the
// connection pool and the NatsCluster reconciler to mgr.
func setup(ctx context.Context, mgr ctrl.Manager) error {
	if err := telemetry.RegisterCluster(otel.Meter(telemetry.ClusterController), mgr.GetClient()); err != nil {
		return fmt.Errorf("register instruments: %w", err)
	}
	pool := natscluster.NewPool()
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
