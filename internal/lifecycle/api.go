package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultAPITimeout bounds a JetStream API request whose context carries no
// deadline.
const DefaultAPITimeout = 5 * time.Second

// API makes JetStream API requests in the account the connection signs
// into, sending and returning configs whole so that keys this module does
// not model survive an update.
type API struct {
	Conn *nats.Conn
}

// Info is the reply to an info, create or update request.
type Info struct {
	Config  Config
	Created time.Time
	Cluster *jetstream.ClusterInfo
	// Raw is the whole reply, for the kind-specific parts.
	Raw []byte
}

// ErrNotFound is what a request for a stream or consumer that does not
// exist wraps.
var ErrNotFound = errors.New("not found")

// StreamInfo returns the stream name's info, or an error wrapping
// ErrNotFound.
func (a API) StreamInfo(ctx context.Context, name string) (*Info, error) {
	return a.request(ctx, "$JS.API.STREAM.INFO."+name, nil)
}

// CreateStream creates a stream from cfg, whose "name" key names it.
func (a API) CreateStream(ctx context.Context, cfg Config) (*Info, error) {
	return a.request(ctx, "$JS.API.STREAM.CREATE."+nameOf(cfg), cfg)
}

// UpdateStream replaces the config of the stream cfg names.
func (a API) UpdateStream(ctx context.Context, cfg Config) (*Info, error) {
	return a.request(ctx, "$JS.API.STREAM.UPDATE."+nameOf(cfg), cfg)
}

// DeleteStream deletes stream name with its messages and consumers; a
// stream that does not exist is an error wrapping ErrNotFound.
func (a API) DeleteStream(ctx context.Context, name string) error {
	_, err := a.do(ctx, "$JS.API.STREAM.DELETE."+name, nil)
	return err
}

// ConsumerInfo returns the info of consumer name on stream, or an error
// wrapping ErrNotFound when either does not exist.
func (a API) ConsumerInfo(ctx context.Context, stream, name string) (*Info, error) {
	return a.request(ctx, "$JS.API.CONSUMER.INFO."+stream+"."+name, nil)
}

// Consumer actions a create request carries.
const (
	ActionCreate = "create"
	ActionUpdate = "update"
)

// PutConsumer creates or updates, as action says, the consumer cfg's "name"
// key names on stream.
func (a API) PutConsumer(ctx context.Context, stream, action string, cfg Config) (*Info, error) {
	req := map[string]any{"stream_name": stream, "config": cfg, "action": action}
	return a.request(ctx, "$JS.API.CONSUMER.CREATE."+stream+"."+nameOf(cfg), req)
}

// DeleteConsumer deletes consumer name on stream; one that does not exist is
// an error wrapping ErrNotFound.
func (a API) DeleteConsumer(ctx context.Context, stream, name string) error {
	_, err := a.do(ctx, "$JS.API.CONSUMER.DELETE."+stream+"."+name, nil)
	return err
}

func nameOf(cfg Config) string {
	s, _ := cfg["name"].(string)
	return s
}

func (a API) request(ctx context.Context, subject string, body any) (*Info, error) {
	raw, err := a.do(ctx, subject, body)
	if err != nil {
		return nil, err
	}
	var reply struct {
		Config  json.RawMessage        `json:"config"`
		Created time.Time              `json:"created"`
		Cluster *jetstream.ClusterInfo `json:"cluster"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return nil, fmt.Errorf("decode %s reply: %w", subject, err)
	}
	cfg, err := DecodeConfig(reply.Config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", subject, err)
	}
	return &Info{Config: cfg, Created: reply.Created, Cluster: reply.Cluster, Raw: raw}, nil
}

// do sends body to subject and returns the reply, or the API error it
// carries: an *jetstream.APIError, wrapped with ErrNotFound for a missing
// stream or consumer.
func (a API) do(ctx context.Context, subject string, body any) ([]byte, error) {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return nil, fmt.Errorf("encode %s request: %w", subject, err)
		}
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultAPITimeout)
		defer cancel()
	}
	msg, err := a.Conn.RequestWithContext(ctx, subject, data)
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
