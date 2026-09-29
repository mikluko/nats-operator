// Package streamctl reconciles NatsStreams, NatsConsumers, NatsKeyValues
// and NatsObjectStores into streams, consumers, key-value buckets and
// object stores on the NATS system their NatsConnection reaches, under the
// lifecycle package's policies.
package streamctl

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

// ServerStream is the server-side stream obj stands for: a NatsStream's own,
// KV_<bucket> for a NatsKeyValue and OBJ_<bucket> for a NatsObjectStore. It
// is "" for any other object.
func ServerStream(obj client.Object) string {
	switch o := obj.(type) {
	case *js.NatsStream:
		return streamName(o)
	case *js.NatsKeyValue:
		return kvStreamPrefix + bucketName(o.Spec.Name, o)
	case *js.NatsObjectStore:
		return objStreamPrefix + bucketName(o.Spec.Name, o)
	}
	return ""
}
