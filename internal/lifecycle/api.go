package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mikluko/nats-operator/internal/jsapi"
)

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
	// Moving reports that the server holds a placement for the object's
	// Raft group that the group has not reached yet.
	Moving bool
	// Raw is the whole reply, for the kind-specific parts.
	Raw []byte
}

// StreamInfo returns the stream name's info, or an error wrapping
// jsapi.ErrNotFound.
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
// stream that does not exist is an error wrapping jsapi.ErrNotFound.
func (a API) DeleteStream(ctx context.Context, name string) error {
	_, err := jsapi.Request(ctx, a.Conn, "$JS.API.STREAM.DELETE."+name, nil)
	return err
}

// ConsumerInfo returns the info of consumer name on stream, or an error
// wrapping jsapi.ErrNotFound when either does not exist.
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
// an error wrapping jsapi.ErrNotFound.
func (a API) DeleteConsumer(ctx context.Context, stream, name string) error {
	_, err := jsapi.Request(ctx, a.Conn, "$JS.API.CONSUMER.DELETE."+stream+"."+name, nil)
	return err
}

func nameOf(cfg Config) string {
	s, _ := cfg["name"].(string)
	return s
}

func (a API) request(ctx context.Context, subject string, body any) (*Info, error) {
	raw, err := jsapi.Request(ctx, a.Conn, subject, body)
	if err != nil {
		return nil, err
	}
	var reply struct {
		Config  json.RawMessage `json:"config"`
		Created time.Time       `json:"created"`
		Cluster *struct {
			jetstream.ClusterInfo
			Desired json.RawMessage `json:"desired"`
		} `json:"cluster"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return nil, fmt.Errorf("decode %s reply: %w", subject, err)
	}
	cfg, err := DecodeConfig(reply.Config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", subject, err)
	}
	info := &Info{Config: cfg, Created: reply.Created, Raw: raw}
	if c := reply.Cluster; c != nil {
		info.Cluster = &c.ClusterInfo
		info.Moving = len(c.Desired) > 0 && string(c.Desired) != "null"
	}
	return info, nil
}
