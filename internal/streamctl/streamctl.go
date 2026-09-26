// Package streamctl reconciles NatsStreams and NatsConsumers into streams
// and consumers on the NATS cluster their NatsConnection reaches, under the
// lifecycle package's policies.
package streamctl

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// refNamespaces adapts refs to grant.IndexReferrers.
func refNamespaces(refs func(client.Object) []natsv1beta1.ObjectReference) func(client.Object) []string {
	return func(o client.Object) []string {
		var out []string
		for _, r := range refs(o) {
			out = append(out, r.Namespace)
		}
		return out
	}
}
