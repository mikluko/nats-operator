// Package managertest reads what a controller's setup registers with a
// controller-runtime manager, without an API server.
package managertest

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// ReconciledKinds runs setup against a manager for scheme and returns the
// kind each controller it registers reconciles, in registration order.
// Field indexes setup registers are accepted and dropped, and nothing is
// started. Controller names are unique per process, so setup runs once per
// test binary.
func ReconciledKinds(t *testing.T, scheme *runtime.Scheme, setup func(context.Context, manager.Manager) error) []string {
	t.Helper()
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:1"}, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)
	rec := &recording{Manager: mgr}
	require.NoError(t, setup(t.Context(), rec))
	return rec.kinds
}

// recording is a manager that records the kind of each controller added to
// it, rather than running it.
type recording struct {
	manager.Manager
	kinds []string
}

// Add records the kind a controller reconciles, read off the logger its
// builder names it in, and adds any other runnable to the manager.
func (r *recording) Add(run manager.Runnable) error {
	c, ok := run.(interface{ GetLogger() logr.Logger })
	if !ok {
		return r.Manager.Add(run)
	}
	s, ok := c.GetLogger().GetSink().(*valuesSink)
	if !ok {
		return fmt.Errorf("controller logger %T was not built from the manager's", c.GetLogger().GetSink())
	}
	kind, ok := s.values["controllerKind"].(string)
	if !ok {
		return fmt.Errorf("controller logger carries no controllerKind: %v", s.values)
	}
	r.kinds = append(r.kinds, kind)
	return nil
}

// GetLogger is a logger that keeps the values it is built with.
func (r *recording) GetLogger() logr.Logger { return logr.New(&valuesSink{}) }

// GetFieldIndexer drops every index.
func (r *recording) GetFieldIndexer() client.FieldIndexer { return noIndexer{} }

type noIndexer struct{}

// IndexField implements client.FieldIndexer.
func (noIndexer) IndexField(context.Context, client.Object, string, client.IndexerFunc) error {
	return nil
}

// valuesSink keeps the values of WithValues and discards every line.
type valuesSink struct {
	values map[string]any
}

// Init implements logr.LogSink.
func (*valuesSink) Init(logr.RuntimeInfo) {}

// Enabled implements logr.LogSink.
func (*valuesSink) Enabled(int) bool { return false }

// Info implements logr.LogSink.
func (*valuesSink) Info(int, string, ...any) {}

// Error implements logr.LogSink.
func (*valuesSink) Error(error, string, ...any) {}

// WithName implements logr.LogSink.
func (s *valuesSink) WithName(string) logr.LogSink { return s }

// WithValues implements logr.LogSink.
func (s *valuesSink) WithValues(kv ...any) logr.LogSink {
	values := maps.Clone(s.values)
	if values == nil {
		values = map[string]any{}
	}
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			values[k] = kv[i+1]
		}
	}
	return &valuesSink{values: values}
}
