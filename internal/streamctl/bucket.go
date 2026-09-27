package streamctl

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/nats-io/nats.go/jetstream"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mikluko/nats-operator/internal/jsapi"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

// bucketName is a bucket resource's server-side bucket name.
func bucketName(spec string, obj client.Object) string {
	if spec != "" {
		return spec
	}
	return obj.GetName()
}

// validBucket is the bucket names nats.go's bucket managers take.
var validBucket = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// bucketStream fetches the stream prefix+bucket, the one a bucket is kept
// in, returning nil when it does not exist. A bucket name nats.go refuses is
// a TerminalError, and nothing is read.
func bucketStream(ctx context.Context, api *lifecycle.API, prefix, bucket string) (*lifecycle.Info, *streamWire, error) {
	if !validBucket.MatchString(bucket) {
		return nil, nil, &lifecycle.TerminalError{
			Reason:  lifecycle.ReasonRejected,
			Message: fmt.Sprintf("bucket name %q is not letters, digits, '-' and '_'", bucket),
		}
	}
	info, err := api.StreamInfo(ctx, prefix+bucket)
	if errors.Is(err, jsapi.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var w streamWire
	if err := lifecycle.FromConfig(info.Config, &w); err != nil {
		return nil, nil, err
	}
	return info, &w, nil
}

// withConfig returns info carrying cfg, a bucket config read from info's
// stream config, in its place.
func withConfig(info *lifecycle.Info, v any) (*lifecycle.Info, error) {
	cfg, err := lifecycle.ToConfig(v)
	if err != nil {
		return nil, err
	}
	out := *info
	out.Config = cfg
	return &out, nil
}

// decodeBucketConfig decodes cfg into v, nats.go's KeyValueConfig or
// ObjectStoreConfig; a value v cannot hold is a spec the server will
// never take.
func decodeBucketConfig(cfg lifecycle.Config, v any) error {
	if err := lifecycle.FromConfig(cfg, v); err != nil {
		return &lifecycle.TerminalError{Reason: lifecycle.ReasonRejected, Message: err.Error()}
	}
	return nil
}

// bucketError returns err from a nats.go bucket manager call, a
// TerminalError where nats.go refused the config before sending it.
func bucketError(err error) error {
	for _, invalid := range []error{
		jetstream.ErrInvalidBucketName,
		jetstream.ErrInvalidStoreName,
		jetstream.ErrHistoryTooLarge,
		jetstream.ErrLimitMarkerTTLNotSupported,
	} {
		if errors.Is(err, invalid) {
			return &lifecycle.TerminalError{Reason: lifecycle.ReasonRejected, Message: err.Error()}
		}
	}
	return err
}

// errPreferred is the TerminalError for a bucket spec naming a preferred
// leader, which nats.go's Placement cannot carry.
var errPreferred = &lifecycle.TerminalError{
	Reason:  lifecycle.ReasonRejected,
	Message: "placement.preferred cannot be set on a bucket: nats.go's bucket managers do not carry it",
}

// refetch returns the bucket's info after a create or update through fetch.
func refetch(ctx context.Context, fetch func(context.Context) (*lifecycle.Info, error), describe string) (*lifecycle.Info, error) {
	info, err := fetch(ctx)
	if err == nil && info == nil {
		return nil, fmt.Errorf("%s is gone after it was written", describe)
	}
	return info, err
}
