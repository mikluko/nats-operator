// The JetStream controller owns jetstream.nats.mikluko.io and reads
// nats.mikluko.io.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
)

func main() {
	opts := manager.Flags(flag.CommandLine, jetstreamv1beta1.GroupVersion.Group)
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
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "run")
		os.Exit(1)
	}
}
