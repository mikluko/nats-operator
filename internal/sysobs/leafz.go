package sysobs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

const subjPingLeafz = "$SYS.REQ.SERVER.PING.LEAFZ"

// Leaf is one leafnode connection a server holds.
type Leaf struct {
	// Account is the local account the connection is bound to: an account
	// public key under a NATS operator, $G without one.
	Account string
	// Spoke is whether this server dialed the connection, as a leaf does
	// its remotes, rather than accepted it as a hub does.
	Spoke bool
	// Remote is the server_name at the other end.
	Remote string
}

type wireLeafz struct {
	Leafs []struct {
		Name    string `json:"name"`
		IsSpoke bool   `json:"is_spoke"`
		Account string `json:"account"`
	} `json:"leafs"`
}

func (w *wireLeafz) leafs() []Leaf {
	out := make([]Leaf, 0, len(w.Leafs))
	for _, l := range w.Leafs {
		out = append(out, Leaf{Account: l.Account, Spoke: l.IsSpoke, Remote: l.Name})
	}
	return out
}

type wireLeafzResponse struct {
	Server wireServerInfo `json:"server"`
	Data   *wireLeafz     `json:"data"`
	Error  *wireError     `json:"error"`
}

// Leafz returns the leafnode connections of every server of the NATS
// cluster that answers LEAFZ, by server name, waiting until every server in
// servers has answered or for the SystemClient's wait. A server that does not
// answer is absent.
func (o *SystemClient) Leafz(ctx context.Context, servers []string) (map[string][]Leaf, error) {
	want := make(map[string]bool, len(servers))
	for _, s := range servers {
		want[s] = true
	}
	out := map[string][]Leaf{}
	err := o.gather(ctx, subjPingLeafz, o.filter(), func(data []byte) (bool, error) {
		var r wireLeafzResponse
		if err := json.Unmarshal(data, &r); err != nil {
			return false, fmt.Errorf("decode LEAFZ: %w", err)
		}
		if r.Error != nil {
			return false, fmt.Errorf("%w: LEAFZ from %s: %d %s", ErrServer, r.Server.Name, r.Error.Code, r.Error.Description)
		}
		if r.Data != nil {
			out[r.Server.Name] = r.Data.leafs()
		}
		delete(want, r.Server.Name)
		return len(want) == 0, nil
	})
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoServers, o.cluster)
	}
	return out, nil
}

// Leafz reads every endpoint's /leafz and returns each answering server's
// leafnode connections by its Name. An endpoint that fails is absent; it
// returns ErrNoServers when none answers.
func (m *MonitorObserver) Leafz(ctx context.Context, endpoints []Endpoint) (map[string][]Leaf, error) {
	var mu sync.Mutex
	out := map[string][]Leaf{}
	var wg sync.WaitGroup
	for _, ep := range endpoints {
		wg.Go(func() {
			var w wireLeafz
			if err := m.get(ctx, ep.URL, "/leafz", nil, &w); err != nil {
				return
			}
			mu.Lock()
			out[ep.Name] = w.leafs()
			mu.Unlock()
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNoServers
	}
	return out, nil
}
