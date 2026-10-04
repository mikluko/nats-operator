package lifecycle

import (
	"strings"

	"k8s.io/apimachinery/pkg/types"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

// Metadata keys of the ownership marker.
const (
	OwnerKey  = "jetstream.nats-operator.io/owner"
	OriginKey = "jetstream.nats-operator.io/origin"
)

// reservedPrefix marks the metadata keys nats-server writes itself.
const reservedPrefix = "_nats."

// Marker is the ownership marker: the owning resource's UID and how it came
// to own the object.
type Marker struct {
	UID    types.UID
	Origin jetstreamv1beta1.OwnershipOrigin
}

// ReadMarker returns the marker in metadata; ok is false when metadata
// names no owner. An absent or unknown origin reads as Created.
func ReadMarker(metadata map[string]string) (m Marker, ok bool) {
	uid := metadata[OwnerKey]
	if uid == "" {
		return Marker{}, false
	}
	origin := jetstreamv1beta1.OwnershipOrigin(metadata[OriginKey])
	if origin != jetstreamv1beta1.OwnershipAdopted {
		origin = jetstreamv1beta1.OwnershipCreated
	}
	return Marker{UID: types.UID(uid), Origin: origin}, true
}

// userMetadata returns metadata without the marker and the keys nats-server
// reserves, or nil when nothing is left.
func userMetadata(metadata map[string]string) map[string]string {
	var out map[string]string
	for k, v := range metadata {
		if k == OwnerKey || k == OriginKey || strings.HasPrefix(k, reservedPrefix) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

// withMarker returns user plus m's keys.
func withMarker(user map[string]string, m Marker) map[string]string {
	out := make(map[string]string, len(user)+2)
	for k, v := range user {
		out[k] = v
	}
	out[OwnerKey] = string(m.UID)
	out[OriginKey] = string(m.Origin)
	return out
}
