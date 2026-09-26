// Package manager builds the controller-runtime manager the three controllers
// share: the same flags, probes and leader election, with each controller's
// own scheme.
package manager

import (
	"flag"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options is what the command line sets on a manager.
type Options struct {
	MetricsAddr      string
	ProbeAddr        string
	LeaderElection   bool
	LeaderElectionID string
}

// Flags registers the manager's flags on fs and returns the Options they
// fill in once fs is parsed. The leader election ID defaults to id, which is
// the controller's own API group so that two controllers never contend.
func Flags(fs *flag.FlagSet, id string) *Options {
	o := &Options{}
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", ":8080", "address the metrics endpoint binds to; 0 disables it")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "address the health and readiness probes bind to")
	fs.BoolVar(&o.LeaderElection, "leader-elect", false, "enable leader election so only one replica reconciles")
	fs.StringVar(&o.LeaderElectionID, "leader-election-id", id, "lease name used for leader election")
	return o
}

// New builds a manager for scheme with health and readiness probes
// registered. The caller starts it.
func New(o *Options, scheme *runtime.Scheme) (ctrl.Manager, error) {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: o.MetricsAddr},
		HealthProbeBindAddress: o.ProbeAddr,
		LeaderElection:         o.LeaderElection,
		LeaderElectionID:       o.LeaderElectionID,
	})
	if err != nil {
		return nil, fmt.Errorf("new manager: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("add healthz: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("add readyz: %w", err)
	}
	return mgr, nil
}
