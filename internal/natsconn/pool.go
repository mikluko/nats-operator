package natsconn

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/types"
)

// Key names the resource a pooled connection belongs to.
type Key struct {
	Kind string
	types.NamespacedName
}

// Pool holds one connection per Key and is safe for concurrent use. A
// connection is reused while its Endpoint is byte-identical; any change, to
// the spec or to a Secret it reads, closes it and dials anew. A caller does
// not keep a returned connection across reconciles: the pool may close it.
type Pool struct {
	opts   []nats.Option
	notify atomic.Pointer[func(Key)]

	mu     sync.Mutex
	slots  map[Key]*slot
	closed bool
}

// slot is one Key's connection; sem, holding one token, guards the rest.
type slot struct {
	sem         chan struct{}
	conn        *nats.Conn
	fingerprint [sha256.Size]byte
	gone        bool
}

// PoolOption configures a Pool.
type PoolOption func(*Pool)

// WithNATSOptions applies opts to every dial, after the package's own and
// the pool's change handlers.
func WithNATSOptions(opts ...nats.Option) PoolOption {
	return func(p *Pool) { p.opts = append(p.opts, opts...) }
}

// NewPool returns an empty Pool.
func NewPool(opts ...PoolOption) *Pool {
	p := &Pool{slots: map[Key]*slot{}}
	for _, o := range opts {
		o(p)
	}
	return p
}

// ErrPoolClosed is returned by Get once Close has run.
var ErrPoolClosed = errors.New("natsconn: pool closed")

// OnChange calls f with a connection's Key whenever that connection
// disconnects, reconnects or closes, including connections dialed before
// the call; a later call replaces f. f runs on the connection's callback
// goroutine and must not block.
func (p *Pool) OnChange(f func(Key)) {
	p.notify.Store(&f)
}

// Get returns key's connection to ep, dialing when there is none, when ep
// differs from what it was dialed with, or when it has closed. A dial for
// one key does not wait on a dial for another. Waiting on a dial for the
// same key, and dialing, end with ctx, returning its error; a dial ctx
// abandons closes its connection once it arrives.
func (p *Pool) Get(ctx context.Context, key Key, ep Endpoint) (*nats.Conn, error) {
	fp := fingerprint(ep)
	for {
		s, err := p.slot(key)
		if err != nil {
			return nil, err
		}
		select {
		case <-s.sem:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if s.gone {
			s.sem <- struct{}{}
			continue
		}
		nc, err := s.get(ctx, ep, fp, p.dialOptions(key))
		s.sem <- struct{}{}
		return nc, err
	}
}

func (p *Pool) slot(key Key) (*slot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPoolClosed
	}
	s, ok := p.slots[key]
	if !ok {
		s = &slot{sem: make(chan struct{}, 1)}
		s.sem <- struct{}{}
		p.slots[key] = s
	}
	return s, nil
}

// get runs holding s.sem.
func (s *slot) get(ctx context.Context, ep Endpoint, fp [sha256.Size]byte, opts []nats.Option) (*nats.Conn, error) {
	if s.conn != nil && s.fingerprint == fp && !s.conn.IsClosed() {
		return s.conn, nil
	}
	s.close()
	nc, err := dialContext(ctx, ep, opts...)
	if err != nil {
		return nil, err
	}
	s.conn, s.fingerprint = nc, fp
	return nc, nil
}

// close runs holding s.sem.
func (s *slot) close() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (p *Pool) dialOptions(key Key) []nats.Option {
	cb := func(*nats.Conn) { p.signal(key) }
	return append([]nats.Option{
		nats.DisconnectErrHandler(func(*nats.Conn, error) { p.signal(key) }),
		nats.ReconnectHandler(cb),
		nats.ClosedHandler(cb),
	}, p.opts...)
}

func (p *Pool) signal(key Key) {
	if f := p.notify.Load(); f != nil {
		(*f)(key)
	}
}

// Forget closes key's connection, if any, and drops it from the pool.
func (p *Pool) Forget(key Key) {
	p.mu.Lock()
	s, ok := p.slots[key]
	delete(p.slots, key)
	p.mu.Unlock()
	if ok {
		s.retire()
	}
}

func (s *slot) retire() {
	<-s.sem
	defer func() { s.sem <- struct{}{} }()
	s.gone = true
	s.close()
}

// dialContext is Dial, returning ctx's error once ctx ends first and
// closing the connection that dial then makes.
func dialContext(ctx context.Context, ep Endpoint, opts ...nats.Option) (*nats.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		nc  *nats.Conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		nc, err := Dial(ep, opts...)
		done <- result{nc, err}
	}()
	select {
	case r := <-done:
		return r.nc, r.err
	case <-ctx.Done():
		go func() {
			if r := <-done; r.nc != nil {
				r.nc.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// Close closes every connection; Get fails with ErrPoolClosed afterwards.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	slots := p.slots
	p.slots = nil
	p.mu.Unlock()
	for _, s := range slots {
		s.retire()
	}
}

// Start blocks until ctx is done and then closes the pool, so a manager
// that runs it closes every connection on shutdown.
func (p *Pool) Start(ctx context.Context) error {
	<-ctx.Done()
	p.Close()
	return nil
}

func fingerprint(ep Endpoint) [sha256.Size]byte {
	h := sha256.New()
	writeLen(h, len(ep.Servers))
	for _, s := range ep.Servers {
		writeBytes(h, []byte(s))
	}
	writeBytes(h, ep.CA)
	writeBytes(h, ep.Creds)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeBytes(h hash.Hash, b []byte) {
	writeLen(h, len(b))
	h.Write(b)
}

func writeLen(h hash.Hash, n int) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	h.Write(b[:])
}
