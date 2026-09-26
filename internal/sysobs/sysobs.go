// Package sysobs observes one NATS cluster through a connection authenticated
// as a user of the system account: its roster, its Raft groups across every
// account, whether it is Settled, and each server's leader and replica
// counts. It also reloads a server's configuration.
package sysobs

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	subjPingStatsz = "$SYS.REQ.SERVER.PING.STATSZ"
	subjPingJsz    = "$SYS.REQ.SERVER.PING.JSZ"
	subjVarz       = "$SYS.REQ.SERVER.%s.VARZ"
	subjReload     = "$SYS.REQ.SERVER.%s.RELOAD"

	jszPageSize = 1024
)

// ErrNoServers is returned when no server of the NATS cluster answers.
var ErrNoServers = errors.New("no server of the NATS cluster answered")

// ErrServer wraps an error a server returned in its API response.
var ErrServer = errors.New("server returned an error")

// Observer observes one NATS cluster. It is safe for concurrent use.
type Observer struct {
	nc      *nats.Conn
	cluster string
	wait    time.Duration
}

// Option configures an Observer.
type Option func(*Observer)

// WithWait sets how long a request waits for servers it expects to answer
// and have not; it also bounds a single-server request whose context has no
// deadline. The default is two seconds.
func WithWait(d time.Duration) Option {
	return func(o *Observer) { o.wait = d }
}

// New returns an Observer of the NATS cluster named cluster, over nc, which
// must be authenticated as a user of the system account. Only servers whose
// cluster name equals cluster exactly answer its requests.
func New(nc *nats.Conn, cluster string, opts ...Option) *Observer {
	o := &Observer{nc: nc, cluster: cluster, wait: 2 * time.Second}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *Observer) filter() wireFilter {
	return wireFilter{Cluster: o.cluster, ExactMatch: true}
}

// Roster returns the servers of the NATS cluster that answer STATSZ, sorted
// by name. It waits until every server named by another's routes has
// answered, or for the Observer's wait; a server that is down and routed to
// by nobody is absent.
func (o *Observer) Roster(ctx context.Context) ([]Server, error) {
	seen := map[string]Server{}
	routed := map[string]bool{}
	complete := func() bool {
		for name := range routed {
			if _, ok := seen[name]; !ok {
				return false
			}
		}
		return len(seen) > 0
	}
	err := o.gather(ctx, subjPingStatsz, o.filter(), func(data []byte) (bool, error) {
		var m wireStatsz
		if err := json.Unmarshal(data, &m); err != nil {
			return false, fmt.Errorf("decode STATSZ: %w", err)
		}
		seen[m.Server.Name] = Server{Name: m.Server.Name, ID: m.Server.ID, Version: m.Server.Version, JetStream: m.Server.JetStream}
		for _, r := range m.Stats.Routes {
			if r.Name != "" {
				routed[r.Name] = true
			}
		}
		return complete(), nil
	})
	if err != nil {
		return nil, err
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoServers, o.cluster)
	}
	out := make([]Server, 0, len(seen))
	for _, s := range seen {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Server) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

// Observe reads the roster and then every roster server's JSZ with its
// accounts, streams and consumers, and merges them into a Snapshot.
func (o *Observer) Observe(ctx context.Context) (*Snapshot, error) {
	roster, err := o.Roster(ctx)
	if err != nil {
		return nil, err
	}
	reports := map[string]*wireJSInfo{}
	for offset := 0; ; offset += jszPageSize {
		total, err := o.jszPage(ctx, roster, offset, reports)
		if err != nil {
			return nil, err
		}
		if total <= offset+jszPageSize {
			break
		}
	}
	return merge(roster, reports), nil
}

// jszPage requests one page of accounts from every server, appends each
// answer's accounts to reports, and returns the largest account total any
// server reported.
func (o *Observer) jszPage(ctx context.Context, roster []Server, offset int, reports map[string]*wireJSInfo) (int, error) {
	want := make(map[string]bool, len(roster))
	for _, s := range roster {
		want[s.Name] = true
	}
	req := wireJszRequest{wireFilter: o.filter(), Accounts: true, Streams: true, Consumer: true, Offset: offset, Limit: jszPageSize}
	total := 0
	err := o.gather(ctx, subjPingJsz, req, func(data []byte) (bool, error) {
		var r wireJszResponse
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("decode JSZ: %w", err)
		}
		if r.Error != nil {
			return false, fmt.Errorf("%w: JSZ from %s: %d %s", ErrServer, r.Server.Name, r.Error.Code, r.Error.Description)
		}
		if r.Data == nil {
			return false, nil
		}
		total = max(total, r.Data.Total)
		if prev := reports[r.Server.Name]; prev != nil {
			prev.Accounts = append(prev.Accounts, r.Data.Accounts...)
		} else {
			reports[r.Server.Name] = r.Data
		}
		delete(want, r.Server.Name)
		return len(want) == 0, nil
	})
	return total, err
}

// gather publishes req to subject and hands each reply to handle until
// handle reports it has all it expects, the Observer's wait passes, or ctx
// ends; only the last is an error.
func (o *Observer) gather(ctx context.Context, subject string, req any, handle func([]byte) (bool, error)) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	inbox := o.nc.NewRespInbox()
	sub, err := o.nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", inbox, err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := o.nc.PublishRequest(subject, inbox, body); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	wctx, cancel := context.WithTimeout(ctx, o.wait)
	defer cancel()
	for {
		msg, err := sub.NextMsgWithContext(wctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if wctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive %s: %w", subject, err)
		}
		done, err := handle(msg.Data)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// request sends req to one server's subject and decodes its one reply
// into resp, bounded by the Observer's wait when ctx has no deadline.
func (o *Observer) request(ctx context.Context, subject string, req, resp any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.wait)
		defer cancel()
	}
	var body []byte
	if req != nil {
		var err error
		if body, err = json.Marshal(req); err != nil {
			return err
		}
	}
	msg, err := o.nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return fmt.Errorf("request %s: %w", subject, err)
	}
	if err := json.Unmarshal(msg.Data, resp); err != nil {
		return fmt.Errorf("decode %s: %w", subject, err)
	}
	return nil
}
