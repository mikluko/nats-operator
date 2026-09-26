package balance

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

// A LeaderMover makes leader moves.
type LeaderMover interface {
	// CanMove reports whether the mover reaches account's leaders at all.
	CanMove(account string) bool
	// MoveLeader asks m.Group's leader to step down in favour of m.To. It
	// returns once the server accepted the request, before the election ends.
	MoveLeader(ctx context.Context, m LeaderMove) error
}

// A PlacementMover makes placement moves.
type PlacementMover interface {
	// MovePlacement asks the meta leader to move m.Group's stream off m.From.
	// It returns once the request is accepted, before any data has moved.
	MovePlacement(ctx context.Context, m PlacementMove) error
}

// Stepdown is a [LeaderMover] over the JetStream stepdown API.
type Stepdown struct {
	Conn *nats.Conn
	// Prefix maps an account to the subject prefix its stepdown API is reached
	// under from Conn, and reports false for an account Conn cannot reach. Nil
	// is an account connection, reaching its own account's API unprefixed.
	Prefix func(account string) (string, bool)
}

// CanMove implements [LeaderMover].
func (s Stepdown) CanMove(account string) bool {
	if s.Prefix == nil {
		return true
	}
	_, ok := s.Prefix(account)
	return ok
}

// MoveLeader implements [LeaderMover]. The server refuses a preferred server
// that is not a member of the group or already leads it, and that refusal is
// the error.
func (s Stepdown) MoveLeader(ctx context.Context, m LeaderMove) error {
	var prefix string
	if s.Prefix != nil {
		var ok bool
		if prefix, ok = s.Prefix(m.Group.Account); !ok {
			return fmt.Errorf("step %s down: account %s grants no leader moves", m.Group, m.Group.Account)
		}
	}
	subject := prefix + "$JS.API.STREAM.LEADER.STEPDOWN." + m.Group.Stream
	if m.Group.Consumer != "" {
		subject = prefix + "$JS.API.CONSUMER.LEADER.STEPDOWN." + m.Group.Stream + "." + m.Group.Consumer
	}
	var req struct {
		Placement struct {
			Preferred string `json:"preferred"`
		} `json:"placement"`
	}
	req.Placement.Preferred = m.To
	if err := request(ctx, s.Conn, subject, req); err != nil {
		return fmt.Errorf("step %s down for %s: %w", m.Group, m.To, err)
	}
	return nil
}

// StreamMove is a [PlacementMover] over `$JS.API.ACCOUNT.STREAM.MOVE`, which
// the server accepts from the system account for any account and from any
// other account for its own streams.
type StreamMove struct {
	Conn *nats.Conn
}

// MovePlacement implements [PlacementMover].
func (s StreamMove) MovePlacement(ctx context.Context, m PlacementMove) error {
	subject := fmt.Sprintf("$JS.API.ACCOUNT.STREAM.MOVE.%s.%s", m.Group.Account, m.Group.Stream)
	req := struct {
		Server  string `json:"server"`
		Cluster string `json:"cluster,omitempty"`
	}{m.From, m.Cluster}
	if err := request(ctx, s.Conn, subject, req); err != nil {
		return fmt.Errorf("move %s off %s: %w", m.Group.ID(), m.From, err)
	}
	return nil
}

// An APIError is the error a JetStream API response carries.
type APIError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s (%d, %d)", e.Description, e.Code, e.ErrCode)
}

// request sends body to subject and fails on a response carrying an error.
func request(ctx context.Context, nc *nats.Conn, subject string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	msg, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return err
	}
	var resp struct {
		Error *APIError `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("response %q: %w", msg.Data, err)
	}
	if resp.Error != nil {
		return resp.Error
	}
	return nil
}
