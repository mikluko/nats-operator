// Command auth-controller owns auth.nats.mikluko.io and reads nats.mikluko.io.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	crmanager "sigs.k8s.io/controller-runtime/pkg/manager"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	systemConnection := flag.String("system-connection", "",
		"namespace/name of the NatsConnection the auth controller reaches NATS through, whose creds are a user of a NatsOperator's "+
			"system account holding the auth-controller preset; unset, JWTs are signed but neither pushed nor deleted, and no connection is kicked")
	if err := manager.Run(manager.Controller{
		Name:        telemetry.AuthController,
		Group:       authv1beta1.GroupVersion.Group,
		AddToScheme: schemes,
		Setup: func(ctx context.Context, mgr ctrl.Manager) error {
			return setup(ctx, mgr, *systemConnection)
		},
	}); err != nil {
		os.Exit(1)
	}
}

var schemes = []func(*runtime.Scheme) error{natsv1beta1.AddToScheme, authv1beta1.AddToScheme}

// setup registers the auth controller's instruments and adds its
// reconcilers to mgr; with systemConnection set, as namespace/name, it adds
// the connection pool and resolvers that reach NATS through it.
func setup(ctx context.Context, mgr ctrl.Manager, systemConnection string) error {
	if err := telemetry.RegisterAuth(otel.Meter(telemetry.AuthController), mgr.GetClient()); err != nil {
		return fmt.Errorf("register instruments: %w", err)
	}
	var d authctl.Distributor
	var s authctl.Sessions
	if systemConnection != "" {
		name, err := namespacedName(systemConnection)
		if err != nil {
			return fmt.Errorf("parse --system-connection: %w", err)
		}
		pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetAuthController))
		conn := &authctl.SystemConnection{Reader: mgr.GetClient(), Pool: pool, Name: name}
		resolvers := &authctl.Resolvers{Conn: conn.Conn, Log: ctrl.Log.WithName("resolvers")}
		for _, r := range []crmanager.Runnable{pool, resolvers} {
			if err := mgr.Add(r); err != nil {
				return fmt.Errorf("add runnable: %w", err)
			}
		}
		d, s = resolvers, authctl.ConnSessions{Resolvers: resolvers}
	}
	if err := authctl.Setup(ctx, mgr, d, s, mgr.GetEventRecorder(telemetry.AuthController)); err != nil {
		return fmt.Errorf("set up reconcilers: %w", err)
	}
	return nil
}

// namespacedName parses namespace/name.
func namespacedName(s string) (types.NamespacedName, error) {
	ns, name, ok := strings.Cut(s, "/")
	if !ok || ns == "" || name == "" {
		return types.NamespacedName{}, fmt.Errorf("%q is not namespace/name", s)
	}
	return types.NamespacedName{Namespace: ns, Name: name}, nil
}
