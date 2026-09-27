// Package jsapi sends JetStream API requests over a NATS connection and
// returns the error a reply carries as a *jetstream.APIError.
package jsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultTimeout bounds a request whose context carries no deadline.
const DefaultTimeout = 5 * time.Second

// ErrNotFound is what a request for a stream or consumer that does not
// exist wraps beside its *jetstream.APIError.
var ErrNotFound = errors.New("not found")

// Request sends body, JSON-encoded unless nil, to subject on nc and returns
// the reply. A reply carrying an error returns it as a *jetstream.APIError,
// wrapped with ErrNotFound for a missing stream or consumer.
func Request(ctx context.Context, nc *nats.Conn, subject string, body any) ([]byte, error) {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return nil, fmt.Errorf("encode %s request: %w", subject, err)
		}
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}
	msg, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", subject, err)
	}
	var reply struct {
		Error *jetstream.APIError `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, fmt.Errorf("decode %s reply: %w", subject, err)
	}
	if e := reply.Error; e != nil {
		if e.ErrorCode == jetstream.JSErrCodeStreamNotFound || e.ErrorCode == jetstream.JSErrCodeConsumerNotFound {
			return nil, fmt.Errorf("%w: %w", ErrNotFound, e)
		}
		return nil, e
	}
	return msg.Data, nil
}
