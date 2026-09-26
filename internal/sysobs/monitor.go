package sysobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Endpoint is one server's HTTP monitoring endpoint: Name is the server_name
// the server is expected to report, URL the base URL its /varz and /jsz are
// under.
type Endpoint struct {
	Name string
	URL  string
}

// MonitorObserver observes a NATS cluster through each server's HTTP
// monitoring endpoint, for a NATS cluster whose system account nobody can
// connect to. It is safe for concurrent use.
type MonitorObserver struct {
	hc   *http.Client
	wait time.Duration
}

// NewMonitor returns a MonitorObserver over hc. Each request waits at most
// wait, two seconds when wait is zero.
func NewMonitor(hc *http.Client, wait time.Duration) *MonitorObserver {
	if wait == 0 {
		wait = 2 * time.Second
	}
	return &MonitorObserver{hc: hc, wait: wait}
}

type wireMonitorVarz struct {
	Name      string            `json:"server_name"`
	ID        string            `json:"server_id"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
	JetStream struct {
		Config *json.RawMessage `json:"config"`
	} `json:"jetstream"`
}

// Observe reads every endpoint's /varz, /gatewayz and /jsz with accounts,
// streams and consumers, and merges them into a Snapshot. Every endpoint is
// on the roster, by its Name when it did not answer; one that fails any
// request, or reports a server_name other than its Name, is Silent. It
// returns ErrNoServers when none answers.
func (m *MonitorObserver) Observe(ctx context.Context, endpoints []Endpoint) (*Snapshot, error) {
	type answer struct {
		server Server
		jsz    *wireJSInfo
		ok     bool
	}
	answers := make([]answer, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Go(func() {
			answers[i].server = Server{Name: ep.Name}
			srv, jsz, err := m.observeOne(ctx, ep)
			if err != nil {
				return
			}
			answers[i] = answer{server: srv, jsz: jsz, ok: true}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	roster := make([]Server, 0, len(endpoints))
	reports := map[string]*wireJSInfo{}
	for _, a := range answers {
		roster = append(roster, a.server)
		if a.ok {
			reports[a.server.Name] = a.jsz
		}
	}
	if len(reports) == 0 {
		return nil, ErrNoServers
	}
	return merge(roster, reports), nil
}

func (m *MonitorObserver) observeOne(ctx context.Context, ep Endpoint) (Server, *wireJSInfo, error) {
	var v wireMonitorVarz
	if err := m.get(ctx, ep.URL, "/varz", nil, &v); err != nil {
		return Server{}, nil, err
	}
	if v.Name != ep.Name {
		return Server{}, nil, fmt.Errorf("%s reports server_name %q", ep.URL, v.Name)
	}
	srv := Server{Name: v.Name, ID: v.ID, Version: v.Version, Metadata: v.Metadata, JetStream: v.JetStream.Config != nil}
	var gwz wireGatewayz
	if err := m.get(ctx, ep.URL, "/gatewayz", nil, &gwz); err != nil {
		return Server{}, nil, err
	}
	srv.Gateways = gwz.gateways()

	jsz := &wireJSInfo{}
	for offset := 0; ; offset += jszPageSize {
		q := url.Values{
			"accounts":  {"true"},
			"streams":   {"true"},
			"consumers": {"true"},
			"offset":    {strconv.Itoa(offset)},
			"limit":     {strconv.Itoa(jszPageSize)},
		}
		var page wireJSInfo
		if err := m.get(ctx, ep.URL, "/jsz", q, &page); err != nil {
			return Server{}, nil, err
		}
		if offset == 0 {
			jsz.Meta = page.Meta
		}
		jsz.Accounts = append(jsz.Accounts, page.Accounts...)
		jsz.Total = page.Total
		if page.Total <= offset+jszPageSize {
			break
		}
	}
	return srv, jsz, nil
}

func (m *MonitorObserver) get(ctx context.Context, base, path string, q url.Values, out any) error {
	ctx, cancel := context.WithTimeout(ctx, m.wait)
	defer cancel()
	u, err := url.JoinPath(base, path)
	if err != nil {
		return err
	}
	if q != nil {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := m.hc.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: get %s: %s", ErrServer, u, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", u, err)
	}
	return nil
}
