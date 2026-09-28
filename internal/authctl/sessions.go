package authctl

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

// ErrServerSilent is returned by ConnSessions.Kick when a server in the
// roster did not answer CONNZ.
var ErrServerSilent = errors.New("server did not answer CONNZ")

// ConnSessions is Sessions over the system connections of Resolvers, to
// the servers in their rosters.
type ConnSessions struct {
	Resolvers *Resolvers
}

// Kick implements Sessions over each server in the roster.
func (s ConnSessions) Kick(ctx context.Context, operator types.NamespacedName, account, user string) (int, error) {
	r := s.Resolvers
	st := r.state(operator)
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return 0, err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return 0, err
	}
	body, err := json.Marshal(connzRequest{User: user, Account: account})
	if err != nil {
		return 0, err
	}
	answered := map[string][]uint64{}
	var errs []error
	err = collect(ctx, nc, subjPingConnz, body, r.wait(), 0, func(data []byte) {
		var m connzResponse
		if err := json.Unmarshal(data, &m); err != nil {
			errs = append(errs, fmt.Errorf("decode CONNZ: %w", err))
			return
		}
		if m.Error != nil {
			errs = append(errs, fmt.Errorf("CONNZ from %s: %d %s", m.Server.ID, m.Error.Code, m.Error.Description))
			return
		}
		var cids []uint64
		if m.Data != nil {
			for _, c := range m.Data.Connections {
				cids = append(cids, c.CID)
			}
		}
		answered[m.Server.ID] = cids
	})
	if err := errors.Join(append(errs, err)...); err != nil {
		return 0, err
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
			if err := kick(ctx, nc, id, cid, r.wait()); err != nil {
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
