// Package sysobs observes and administers one NATS cluster as a user of the
// system account, or observes it read-only over its HTTP monitoring ports.
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
	subjPingGwz    = "$SYS.REQ.SERVER.PING.GATEWAYZ"
	subjVarz       = "$SYS.REQ.SERVER.%s.VARZ"
	subjReload     = "$SYS.REQ.SERVER.%s.RELOAD"

	jszPageSize = 1024
)

// ErrNoServers is returned when no server of the NATS cluster answers.
var ErrNoServers = errors.New("no server of the NATS cluster answered")

// ErrServer wraps an error a server returned in its API response.
var ErrServer = errors.New("server returned an error")

// SystemClient reads and administers one NATS cluster over its system
// account. It is safe for concurrent use.
type SystemClient struct {
	nc       *nats.Conn
	cluster  string
	wait     time.Duration
	gateways bool
}

// Option configures a SystemClient.
type Option func(*SystemClient)

// WithWait sets how long a request waits for servers it expects to answer
// and have not; it also bounds a single-server request whose context has no
// deadline. The default is two seconds.
func WithWait(d time.Duration) Option {
	return func(o *SystemClient) { o.wait = d }
}

// WithGateways makes Observe read every server's gateways over GATEWAYZ,
// which the observing user must be allowed to publish.
func WithGateways() Option {
	return func(o *SystemClient) { o.gateways = true }
}

// New returns a SystemClient of the NATS cluster named cluster over nc, which
// must be authenticated as a user of the system account. Its requests reach
// only servers whose cluster name is exactly cluster, every server where
// cluster is empty, save the one to the meta leader, which reaches it in
// whichever NATS cluster it runs.
func New(nc *nats.Conn, cluster string, opts ...Option) *SystemClient {
	o := &SystemClient{nc: nc, cluster: cluster, wait: 2 * time.Second}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *SystemClient) filter() wireFilter {
	return wireFilter{Cluster: o.cluster, ExactMatch: true}
}

// Roster returns the servers of the NATS cluster that answer STATSZ, sorted
// by name. It waits until every server named by another's routes has
// answered, or for the SystemClient's wait; a server that has not answered
// by then is absent, and none answering is ErrNoServers.
func (o *SystemClient) Roster(ctx context.Context) ([]Server, error) {
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
		seen[m.Server.Name] = Server{Name: m.Server.Name, ID: m.Server.ID, Version: m.Server.Version, Metadata: m.Server.Metadata, JetStream: m.Server.JetStream, Tags: m.Server.Tags}
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

// Observe merges the roster and each roster server's JSZ, and its gateways
// under WithGateways, into a Snapshot. A meta group led from another NATS
// cluster has the roster servers among its leader's peers as members, or is
// FromFollowers when that leader does not answer.
func (o *SystemClient) Observe(ctx context.Context) (*Snapshot, error) {
	roster, err := o.Roster(ctx)
	if err != nil {
		return nil, err
	}
	if o.gateways {
		if err := o.readGateways(ctx, roster); err != nil {
			return nil, err
		}
	}
	reports, err := o.jsz(ctx, roster)
	if err != nil {
		return nil, err
	}
	snap := merge(roster, reports)
	if err := o.readRemoteMeta(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// readRemoteMeta replaces a FromFollowers meta group of snap that agrees on
// a leader with that leader's own view, when the leader answers JSZ within
// the SystemClient's wait.
func (o *SystemClient) readRemoteMeta(ctx context.Context, snap *Snapshot) error {
	i := slices.IndexFunc(snap.Groups, func(g Group) bool { return g.Kind == KindMeta && g.FromFollowers && g.Leader != "" })
	if i < 0 {
		return nil
	}
	leader := snap.Groups[i].Leader
	var meta *wireMeta
	err := o.gather(ctx, subjPingJsz, wireJszRequest{wireFilter: wireFilter{Name: leader, ExactMatch: true}}, func(data []byte) (bool, error) {
		var r wireJszResponse
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("decode JSZ: %w", err)
		}
		if r.Error != nil {
			return false, fmt.Errorf("%w: JSZ from %s: %d %s", ErrServer, r.Server.Name, r.Error.Code, r.Error.Description)
		}
		if r.Server.Name != leader || r.Data == nil || r.Data.Meta == nil || r.Data.Meta.Leader != leader {
			return false, nil
		}
		meta = r.Data.Meta
		return true, nil
	})
	if err != nil || meta == nil {
		return err
	}
	snap.Groups[i] = remoteLeaderView(snap.Groups[i], meta, snap.Servers)
	return nil
}

// readGateways sets the Gateways of every roster server that answers GATEWAYZ.
func (o *SystemClient) readGateways(ctx context.Context, roster []Server) error {
	idx := make(map[string]int, len(roster))
	for i, s := range roster {
		idx[s.Name] = i
	}
	pending := len(roster)
	return o.gather(ctx, subjPingGwz, o.filter(), func(data []byte) (bool, error) {
		var r wireGatewayzResponse
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("decode GATEWAYZ: %w", err)
		}
		if r.Error != nil {
			return false, fmt.Errorf("%w: GATEWAYZ from %s: %d %s", ErrServer, r.Server.Name, r.Error.Code, r.Error.Description)
		}
		i, ok := idx[r.Server.Name]
		if !ok || r.Data == nil || roster[i].Gateways != nil {
			return false, nil
		}
		roster[i].Gateways = r.Data.gateways()
		pending--
		return pending == 0, nil
	})
}

// jsz reads every page of accounts from the roster's servers, keyed by
// server name; a server that misses any page is absent.
func (o *SystemClient) jsz(ctx context.Context, roster []Server) (map[string]*wireJSInfo, error) {
	want := make(map[string]bool, len(roster))
	for _, s := range roster {
		want[s.Name] = true
	}
	reports := map[string]*wireJSInfo{}
	for offset := 0; len(want) > 0; offset += jszPageSize {
		page, total, err := o.jszPage(ctx, want, offset)
		if err != nil {
			return nil, err
		}
		for name := range want {
			r, ok := page[name]
			switch {
			case !ok:
				delete(want, name)
				delete(reports, name)
			case reports[name] == nil:
				reports[name] = r
			default:
				reports[name].Accounts = append(reports[name].Accounts, r.Accounts...)
			}
		}
		if total <= offset+jszPageSize {
			break
		}
	}
	return reports, nil
}

// jszPage requests one page of accounts from the servers in want and
// returns each answer keyed by server name, and the largest account total
// any of them reported.
func (o *SystemClient) jszPage(ctx context.Context, want map[string]bool, offset int) (map[string]*wireJSInfo, int, error) {
	page := make(map[string]*wireJSInfo, len(want))
	req := wireJszRequest{wireFilter: o.filter(), Accounts: true, Streams: true, Consumer: true, Config: true, Offset: offset, Limit: jszPageSize}
	total := 0
	err := o.gather(ctx, subjPingJsz, req, func(data []byte) (bool, error) {
		var r wireJszResponse
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("decode JSZ: %w", err)
		}
		if r.Error != nil {
			return false, fmt.Errorf("%w: JSZ from %s: %d %s", ErrServer, r.Server.Name, r.Error.Code, r.Error.Description)
		}
		if r.Data == nil || !want[r.Server.Name] || page[r.Server.Name] != nil {
			return false, nil
		}
		total = max(total, r.Data.Total)
		page[r.Server.Name] = r.Data
		return len(page) == len(want), nil
	})
	return page, total, err
}

// gather publishes req to subject and hands each reply to handle until
// handle reports it has all it expects, the SystemClient's wait passes, or ctx
// ends; only the last is an error.
func (o *SystemClient) gather(ctx context.Context, subject string, req any, handle func([]byte) (bool, error)) error {
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
// into resp, bounded by the SystemClient's wait when ctx has no deadline.
func (o *SystemClient) request(ctx context.Context, subject string, req, resp any) error {
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
