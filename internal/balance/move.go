package balance

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mikluko/nats-operator/internal/jsapi"
)

// A LeaderMover makes leader moves.
type LeaderMover interface {
	// CanMove reports whether the mover reaches account's leaders at all.
	CanMove(ctx context.Context, account string) bool
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
	Prefix func(ctx context.Context, account string) (string, bool)
}

// CanMove implements [LeaderMover].
func (s Stepdown) CanMove(ctx context.Context, account string) bool {
	if s.Prefix == nil {
		return true
	}
	_, ok := s.Prefix(ctx, account)
	return ok
}

// MoveLeader implements [LeaderMover]. The server refuses a preferred server
// that is not a member of the group or already leads it, and that refusal is
// the error.
func (s Stepdown) MoveLeader(ctx context.Context, m LeaderMove) error {
	var prefix string
	if s.Prefix != nil {
		var ok bool
		if prefix, ok = s.Prefix(ctx, m.Group.Account); !ok {
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
	if _, err := jsapi.Request(ctx, s.Conn, subject, req); err != nil {
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
	if _, err := jsapi.Request(ctx, s.Conn, subject, req); err != nil {
		return fmt.Errorf("move %s off %s: %w", m.Group.ID(), m.From, err)
	}
	return nil
}

// Evacuate asks the meta leader to move every copy of id to servers carrying
// tags on top of the stream's own placement tags. Where no server of the
// stream's NATS cluster carries them, the server picks another NATS cluster
// that has enough. The tags are not written to the stream's config.
func (s StreamMove) Evacuate(ctx context.Context, id StreamID, tags []string) error {
	subject := fmt.Sprintf("$JS.API.ACCOUNT.STREAM.MOVE.%s.%s", id.Account, id.Stream)
	req := struct {
		Tags []string `json:"tags"`
	}{tags}
	if _, err := jsapi.Request(ctx, s.Conn, subject, req); err != nil {
		return fmt.Errorf("move %s to %v: %w", id, tags, err)
	}
	return nil
}

// ErrCodeNoMove is the *jetstream.APIError code CancelMove fails with where
// id has no move in progress.
const ErrCodeNoMove jetstream.ErrorCode = 10129

// CancelMove asks the meta leader to roll back the move of id in progress,
// back to the servers it started from.
func (s StreamMove) CancelMove(ctx context.Context, id StreamID) error {
	subject := fmt.Sprintf("$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.%s.%s", id.Account, id.Stream)
	if _, err := jsapi.Request(ctx, s.Conn, subject, struct{}{}); err != nil {
		return fmt.Errorf("cancel the move of %s: %w", id, err)
	}
	return nil
}
