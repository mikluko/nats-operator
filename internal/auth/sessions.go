package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/types"
)

// Sessions closes a user's live connections on the NATS clusters that trust
// a NATS operator.
type Sessions interface {
	// Kick closes every connection the user key user of the account key
	// account holds on any server trusting operator, and returns how many
	// it found. Zero means every server answered with none.
	Kick(ctx context.Context, operator types.NamespacedName, account, user string) (int, error)
}

const (
	subjPingStatsz = "$SYS.REQ.SERVER.PING.STATSZ"
	subjPingConnz  = "$SYS.REQ.SERVER.PING.CONNZ"
	subjKick       = "$SYS.REQ.SERVER.%s.KICK"
)

// ErrServerSilent is returned by ConnSessions.Kick when a server that
// answered STATSZ did not answer CONNZ.
var ErrServerSilent = errors.New("server did not answer CONNZ")

// ConnSessions is Sessions over a connection authenticated as a system user
// holding the auth-controller preset.
type ConnSessions struct {
	// Conn returns the system connection to the NATS clusters trusting
	// operator.
	Conn func(ctx context.Context, operator types.NamespacedName) (*nats.Conn, error)
	// Wait is how long a ping gathers replies; zero is two seconds.
	Wait time.Duration
}

// Kick implements Sessions. The roster is the servers answering STATSZ; each
// is asked for the user's connections by a CONNZ ping filtered on user and
// account server-side, and every connection found is kicked by its server
// ID and client ID. A connection gone before its kick counts as kicked.
// An empty roster is ErrUnreachable.
func (s ConnSessions) Kick(ctx context.Context, operator types.NamespacedName, account, user string) (int, error) {
	nc, err := s.Conn(ctx, operator)
	if err != nil {
		return 0, fmt.Errorf("system connection for NatsOperator %s: %w", operator, err)
	}
	wait := s.Wait
	if wait == 0 {
		wait = 2 * time.Second
	}
	roster := map[string]bool{}
	answered := map[string][]uint64{}
	err = gatherPings(ctx, nc, wait, map[string]any{
		subjPingStatsz: struct{}{},
		subjPingConnz:  connzRequest{User: user, Account: account},
	}, func(subject string, data []byte) error {
		switch subject {
		case subjPingStatsz:
			var m struct {
				Server struct {
					ID string `json:"id"`
				} `json:"server"`
			}
			if err := json.Unmarshal(data, &m); err != nil {
				return fmt.Errorf("decode STATSZ: %w", err)
			}
			roster[m.Server.ID] = true
		default:
			var m connzResponse
			if err := json.Unmarshal(data, &m); err != nil {
				return fmt.Errorf("decode CONNZ: %w", err)
			}
			if m.Error != nil {
				return fmt.Errorf("CONNZ from %s: %d %s", m.Server.ID, m.Error.Code, m.Error.Description)
			}
			var cids []uint64
			if m.Data != nil {
				for _, c := range m.Data.Connections {
					cids = append(cids, c.CID)
				}
			}
			answered[m.Server.ID] = cids
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(roster) == 0 {
		return 0, fmt.Errorf("%w: no server answered STATSZ", ErrUnreachable)
	}
	var silent []string
	for id := range roster {
		if _, ok := answered[id]; !ok {
			silent = append(silent, id)
		}
	}
	if len(silent) > 0 {
		slices.Sort(silent)
		return 0, fmt.Errorf("%w: %s", ErrServerSilent, strings.Join(silent, ", "))
	}
	var n int
	for id, cids := range answered {
		for _, cid := range cids {
			if err := kick(ctx, nc, id, cid, wait); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

type connzRequest struct {
	User    string `json:"user"`
	Account string `json:"acc"`
}

type apiError struct {
	Code        int    `json:"code"`
	Description string `json:"description"`
}

type connzResponse struct {
	Server struct {
		ID string `json:"id"`
	} `json:"server"`
	Data *struct {
		Connections []struct {
			CID uint64 `json:"cid"`
		} `json:"connections"`
	} `json:"data"`
	Error *apiError `json:"error"`
}

// kick asks server id to close client cid. A client already gone is not an
// error: nats-server answers "no such client or leafnode id".
func kick(ctx context.Context, nc *nats.Conn, id string, cid uint64, wait time.Duration) error {
	body, err := json.Marshal(struct {
		CID uint64 `json:"cid"`
	}{cid})
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, fmt.Sprintf(subjKick, id), body)
	if err != nil {
		return fmt.Errorf("kick %d on %s: %w", cid, id, err)
	}
	var resp struct {
		Error *apiError `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("decode KICK: %w", err)
	}
	if resp.Error != nil && !strings.Contains(resp.Error.Description, "no such client") {
		return fmt.Errorf("kick %d on %s: %d %s", cid, id, resp.Error.Code, resp.Error.Description)
	}
	return nil
}

// gatherPings publishes each request to its subject and hands every reply
// to handle, with the subject it answers, until wait passes. It returns
// ctx's error if ctx ends first.
func gatherPings(ctx context.Context, nc *nats.Conn, wait time.Duration, requests map[string]any, handle func(subject string, data []byte) error) error {
	type reply struct {
		subject string
		data    []byte
	}
	replies := make(chan reply, 256)
	done := make(chan struct{})
	defer close(done)
	for subject, req := range requests {
		body, err := json.Marshal(req)
		if err != nil {
			return err
		}
		inbox := nc.NewRespInbox()
		sub, err := nc.Subscribe(inbox, func(m *nats.Msg) {
			select {
			case replies <- reply{subject, m.Data}:
			case <-done:
			}
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", inbox, err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := nc.PublishRequest(subject, inbox, body); err != nil {
			return fmt.Errorf("publish %s: %w", subject, err)
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case r := <-replies:
			if err := handle(r.subject, r.data); err != nil {
				return err
			}
		}
	}
}
