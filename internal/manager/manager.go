// Package manager builds and runs the controller-runtime manager the three
// controllers share: the same flags, logging, telemetry, probes and leader
// election, with each controller's own scheme and reconcilers.
package manager

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/mikluko/nats-operator/internal/telemetry"
)

// readyWait is how long a readiness probe waits for the cache to sync.
const readyWait = time.Second

// Controller is one controller binary, as Run starts it.
type Controller struct {
	// Name names the controller's logger, telemetry service and event
	// source, one of the telemetry package's controller names.
	Name string
	// Group is the API group the controller owns, the default of
	// --leader-election-id.
	Group string
	// AddToScheme registers the API groups the controller reads, beyond
	// client-go's.
	AddToScheme []func(*runtime.Scheme) error
	Owned       Owned
	// Setup adds the controller's runnables and reconcilers to mgr.
	Setup func(ctx context.Context, mgr ctrl.Manager) error
}

// Run registers the manager's flags and zap's on flag.CommandLine, parses
// it along with any flag the caller registered there first, and runs c until
// SIGINT or SIGTERM. An error it returns has already been logged.
func Run(c Controller) error {
	opts := Flags(flag.CommandLine, c.Group)
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	cfg, err := ctrl.GetConfig()
	if err == nil {
		err = start(ctrl.SetupSignalHandler(), cfg, opts, c)
	} else {
		err = fmt.Errorf("load kubeconfig: %w", err)
	}
	if err != nil {
		ctrl.Log.WithName(c.Name).Error(err, "exit")
	}
	return err
}

// start runs c's manager against cfg until ctx ends.
func start(ctx context.Context, cfg *rest.Config, o *Options, c Controller) error {
	scheme, err := NewScheme(c.AddToScheme...)
	if err != nil {
		return fmt.Errorf("build scheme: %w", err)
	}
	mgr, err := New(cfg, o, scheme, c.Owned)
	if err != nil {
		return err
	}
	if err := telemetry.Install(ctx, mgr, c.Name); err != nil {
		return fmt.Errorf("set up telemetry: %w", err)
	}
	if err := c.Setup(ctx, mgr); err != nil {
		return fmt.Errorf("set up controllers: %w", err)
	}
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	return nil
}

// NewScheme returns a scheme of client-go's kinds and those add registers.
func NewScheme(add ...func(*runtime.Scheme) error) (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, f := range append([]func(*runtime.Scheme) error{clientgoscheme.AddToScheme}, add...) {
		if err := f(scheme); err != nil {
			return nil, err
		}
	}
	return scheme, nil
}

// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Options is what the command line sets on a manager.
type Options struct {
	MetricsAddr      string
	ProbeAddr        string
	LeaderElection   bool
	LeaderElectionID string
	// WatchNamespaces confines the cache, and so every reconcile, to these
	// namespaces; empty, it watches all.
	WatchNamespaces []string
}

// Flags registers the manager's flags on fs and returns the Options they
// fill in once fs is parsed, the leader election ID defaulting to id.
func Flags(fs *flag.FlagSet, id string) *Options {
	o := &Options{}
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", ":8080", "address the metrics endpoint serves HTTPS on, to a bearer token allowed to get /metrics; 0 disables it")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "address the health and readiness probes bind to")
	fs.BoolVar(&o.LeaderElection, "leader-elect", false, "enable leader election so only one replica reconciles")
	fs.StringVar(&o.LeaderElectionID, "leader-election-id", id, "lease name used for leader election")
	fs.Func("watch-namespaces", "comma-separated namespaces the controller watches and reconciles in, and no other; unset, it watches all", func(v string) error {
		o.WatchNamespaces = nil
		for ns := range strings.SplitSeq(v, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				o.WatchNamespaces = append(o.WatchNamespaces, ns)
			}
		}
		if len(o.WatchNamespaces) == 0 {
			return errors.New("no namespace named")
		}
		return nil
	})
	return o
}

// New builds a manager for scheme against the API server cfg reaches, with
// health and readiness probes registered and its cache scoped to owned and
// to o.WatchNamespaces, for the caller to start. It fails while the API
// server is unreachable, since scoping the cache reads its discovery.
func New(cfg *rest.Config, o *Options, scheme *runtime.Scheme, owned Owned) (ctrl.Manager, error) {
	cacheOpts, err := cacheOptions(owned)
	if err != nil {
		return nil, err
	}
	if len(o.WatchNamespaces) > 0 {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{}
		for _, ns := range o.WatchNamespaces {
			cacheOpts.DefaultNamespaces[ns] = cache.Config{}
		}
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cacheOpts,
		Client:                 clientOptions(),
		Metrics:                metricsOptions(o.MetricsAddr),
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
	if err := mgr.AddReadyzCheck("readyz", cacheSynced(mgr.GetCache().WaitForCacheSync)); err != nil {
		return nil, fmt.Errorf("add readyz: %w", err)
	}
	return mgr, nil
}

// cacheSynced is a readiness check that passes once wait, a cache's
// WaitForCacheSync, reports every informer it started synced, waiting for
// that at most readyWait.
func cacheSynced(wait func(context.Context) bool) healthz.Checker {
	return func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(req.Context(), readyWait)
		defer cancel()
		if !wait(ctx) {
			return errors.New("cache is not synced")
		}
		return nil
	}
}
