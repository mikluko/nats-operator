package balancectl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// probeStream is a name no stream can carry, since nats-server refuses "/"
// in one, so a stepdown request for it moves nothing: the meta leader
// answers that the stream is not found.
const probeStream = "/"

const probeTimeout = 2 * time.Second

// stepdownReach finds which accounts' leaders a system connection can move:
// those whose stream and consumer stepdown APIs answer under
// [jwtplane.StepdownPrefix], as they do while the system account imports the
// account's jetstream-stepdown export. It remembers each account's answer.
type stepdownReach struct {
	ctx   context.Context
	nc    *nats.Conn
	known map[string]bool
	// err is the first probe that neither reached the API nor found it
	// absent; its account reads as unreachable.
	err error
}

func newStepdownReach(ctx context.Context, nc *nats.Conn) *stepdownReach {
	return &stepdownReach{ctx: ctx, nc: nc, known: map[string]bool{}}
}

// prefix is a [balance.Stepdown] Prefix.
func (r *stepdownReach) prefix(account string) (string, bool) {
	ok, seen := r.known[account]
	if !seen {
		var err error
		ok, err = r.probe(account)
		if err != nil && r.err == nil {
			r.err = err
		}
		r.known[account] = ok
	}
	if !ok {
		return "", false
	}
	return jwtplane.StepdownPrefix(account), true
}

func (r *stepdownReach) probe(account string) (bool, error) {
	for _, subject := range []string{
		jwtplane.StreamStepdownSubject(account, probeStream),
		jwtplane.ConsumerStepdownSubject(account, probeStream, probeStream),
	} {
		ctx, cancel := context.WithTimeout(r.ctx, probeTimeout)
		_, err := r.nc.RequestWithContext(ctx, subject, nil)
		cancel()
		switch {
		case errors.Is(err, nats.ErrNoResponders):
			return false, nil
		case err != nil:
			return false, fmt.Errorf("probe %s: %w", subject, err)
		}
	}
	return true, nil
}
