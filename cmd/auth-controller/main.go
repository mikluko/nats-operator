// The auth controller owns auth.nats.mikluko.io and reads nats.mikluko.io.
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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/auth"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

func main() {
	opts := manager.Flags(flag.CommandLine, authv1beta1.GroupVersion.Group)
	systemConnection := flag.String("system-connection", "",
		"namespace/name of the NatsConnection the auth controller reaches NATS through, whose creds are a user of a NatsOperator's "+
			"system account holding the auth-controller preset; unset, JWTs are signed but neither pushed nor deleted, and no connection is kicked")
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("auth-controller")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		natsv1beta1.AddToScheme,
		authv1beta1.AddToScheme,
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
	if err := telemetry.Install(ctx, mgr, telemetry.AuthController); err != nil {
		log.Error(err, "set up telemetry")
		os.Exit(1)
	}
	if err := telemetry.RegisterAuth(otel.Meter(telemetry.AuthController), mgr.GetClient()); err != nil {
		log.Error(err, "register instruments")
		os.Exit(1)
	}
	var d auth.Distributor
	var s auth.Sessions
	if *systemConnection != "" {
		name, err := namespacedName(*systemConnection)
		if err != nil {
			log.Error(err, "parse --system-connection")
			os.Exit(1)
		}
		pool := natsconn.NewPool()
		conn := &auth.SystemConnection{Reader: mgr.GetClient(), Pool: pool, Name: name}
		resolvers := &auth.Resolvers{Conn: conn.Conn, Log: ctrl.Log.WithName("resolvers")}
		for _, r := range []interface {
			Start(ctx context.Context) error
		}{pool, resolvers} {
			if err := mgr.Add(r); err != nil {
				log.Error(err, "add runnable")
				os.Exit(1)
			}
		}
		d, s = resolvers, auth.ConnSessions{Conn: conn.Conn}
	}
	if err := auth.Setup(ctx, mgr, d, s, mgr.GetEventRecorder(telemetry.AuthController)); err != nil {
		log.Error(err, "set up reconcilers")
		os.Exit(1)
	}
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "run")
		os.Exit(1)
	}
}

// namespacedName parses namespace/name.
func namespacedName(s string) (types.NamespacedName, error) {
	ns, name, ok := strings.Cut(s, "/")
	if !ok || ns == "" || name == "" {
		return types.NamespacedName{}, fmt.Errorf("%q is not namespace/name", s)
	}
	return types.NamespacedName{Namespace: ns, Name: name}, nil
}
