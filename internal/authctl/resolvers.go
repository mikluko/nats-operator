package authctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

const (
	subjClaimsUpdate = "$SYS.REQ.CLAIMS.UPDATE"
	subjClaimsDelete = "$SYS.REQ.CLAIMS.DELETE"
	subjClaimsLookup = "$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP"
	subjPingVarz     = "$SYS.REQ.SERVER.PING.VARZ"
)

// rosterMisses is how many roster polls in a row a server misses to leave
// the roster: until then it is counted, and asked, as any other.
const rosterMisses = 3

// ErrOperatorGone is wrapped by a Resolvers Conn error for a NatsOperator
// that no longer exists; its roster is no longer polled.
var ErrOperatorGone = errors.New("NatsOperator does not exist")

// Resolvers is the Distributor over each NatsOperator's system connection
// to the full resolvers of the servers trusting it.
// The roster is every server that answered STATSZ within its last
// rosterMisses polls.
type Resolvers struct {
	// Conn returns the system connection to the servers trusting operator,
	// authenticated as a user holding the auth-controller preset.
	Conn func(ctx context.Context, operator types.NamespacedName) (*nats.Conn, error)
	// Wait bounds how long a request gathers replies; zero is two seconds.
	Wait time.Duration
	// Interval is how often rosters are polled; zero is ten seconds.
	Interval time.Duration
	// Log receives failed polls.
	Log logr.Logger

	mu        sync.Mutex
	operators map[types.NamespacedName]*resolverState
	subs      []chan event.GenericEvent
}

type resolverState struct {
	// send serializes pushes and delete sends to a NATS operator's servers.
	send sync.Mutex

	// Guarded by Resolvers.mu.
	roster map[string]bool
	// misses counts, per server of roster, the polls in a row it missed.
	misses map[string]int
	// trust is, per server of roster that reported the NATS operator JWTs it
	// runs under, the keys those list; a server absent from it is taken to
	// trust every key.
	trust map[string][]string
	// newest is, per account public key, the issue time of the newest JWT a
	// server acknowledged or held, and pushedAt when a server last
	// acknowledged a push of it.
	newest   map[string]int64
	pushedAt map[string]time.Time
	// request is the signed delete request, deletes the accounts it names,
	// issuer its signer, and acked the servers that acknowledged it.
	deletes []string
	issuer  string
	request string
	acked   map[string]bool
}

var (
	_ Distributor      = (*Resolvers)(nil)
	_ manager.Runnable = (*Resolvers)(nil)
)

// Subscribe returns a new channel that receives a NatsOperator, carrying
// only its name, whenever the servers trusting it change; it is called
// before r starts. One left undrained stalls roster polling once 64 changes
// are pending.
func (r *Resolvers) Subscribe() <-chan event.GenericEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan event.GenericEvent, 64)
	r.subs = append(r.subs, ch)
	return ch
}

func (r *Resolvers) wait() time.Duration {
	if r.Wait == 0 {
		return 2 * time.Second
	}
	return r.Wait
}

func (r *Resolvers) interval() time.Duration {
	if r.Interval == 0 {
		return 10 * time.Second
	}
	return r.Interval
}

// state returns operator's state, registering it for roster polls.
func (r *Resolvers) state(operator types.NamespacedName) *resolverState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operators == nil {
		r.operators = map[types.NamespacedName]*resolverState{}
	}
	st, ok := r.operators[operator]
	if !ok {
		st = &resolverState{newest: map[string]int64{}, pushedAt: map[string]time.Time{}, acked: map[string]bool{}}
		r.operators[operator] = st
	}
	return st
}

func (r *Resolvers) conn(ctx context.Context, operator types.NamespacedName) (*nats.Conn, error) {
	nc, err := r.Conn(ctx, operator)
	if err != nil {
		return nil, fmt.Errorf("%w: system connection for NatsOperator %s: %w", ErrUnreachable, operator, err)
	}
	return nc, nil
}

// Push is Distributor.Push; the first push of an account after a restart
// of the auth controller costs a CLAIMS.LOOKUP of every server.
// A server stores a pushed JWT whoever signed it, so nothing is sent where
// no server trusts the signer.
func (r *Resolvers) Push(ctx context.Context, operator types.NamespacedName, accountJWT string) error {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return fmt.Errorf("decode account JWT: %w", err)
	}
	st := r.state(operator)
	st.send.Lock()
	defer st.send.Unlock()
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return err
	}
	r.mu.Lock()
	distrusting := st.distrusting(roster, c.Issuer)
	newest, known := st.newest[c.Subject]
	r.mu.Unlock()
	untrusted := fmt.Errorf("%w: %d of %d servers do not list %s, the signer of account %s, in their NATS operator JWT",
		ErrUntrustedSigner, distrusting, len(roster), c.Issuer, c.Subject)
	if distrusting > 0 && distrusting == len(roster) {
		return untrusted
	}
	if !known {
		held, err := r.lookup(ctx, nc, c.Subject, len(roster))
		if err != nil {
			return err
		}
		for _, token := range held {
			if hc, err := jwt.DecodeAccountClaims(token); err == nil && hc.Subject == c.Subject {
				newest = max(newest, hc.IssuedAt)
			}
		}
	}
	if c.IssuedAt < newest {
		return fmt.Errorf("%w: account %s JWT issued at %s, one issued at %s exists", ErrStaleJWT, c.Subject,
			time.Unix(c.IssuedAt, 0).UTC().Format(time.RFC3339), time.Unix(newest, 0).UTC().Format(time.RFC3339))
	}
	var accepted int
	var errs []error
	err = collect(ctx, nc, subjClaimsUpdate, []byte(accountJWT), r.wait(), len(roster), func(data []byte) {
		var resp claimUpdateResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			errs = append(errs, fmt.Errorf("decode CLAIMS.UPDATE reply: %w", err))
			return
		}
		if resp.Error != nil {
			errs = append(errs, fmt.Errorf("server %s refused account %s: %s", resp.Server.ID, c.Subject, resp.Error.Description))
			return
		}
		accepted++
	})
	if err != nil {
		return err
	}
	if accepted > 0 {
		r.mu.Lock()
		st.newest[c.Subject] = max(newest, c.IssuedAt)
		st.pushedAt[c.Subject] = time.Now()
		r.mu.Unlock()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if accepted == 0 {
		return fmt.Errorf("no server acknowledged CLAIMS.UPDATE for account %s within %s", c.Subject, r.wait())
	}
	if distrusting > 0 {
		return untrusted
	}
	return nil
}

// Current is Distributor.Current over the roster; since lookup replies
// carry no server ID it counts matching replies, at most as many as servers
// trust the JWT's signer, and it knows only the pushes this Resolvers made.
func (r *Resolvers) Current(ctx context.Context, operator types.NamespacedName, accountJWT string) (authv1beta1.Distribution, error) {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return authv1beta1.Distribution{}, fmt.Errorf("decode account JWT: %w", err)
	}
	st := r.state(operator)
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return authv1beta1.Distribution{}, err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return authv1beta1.Distribution{}, err
	}
	held, err := r.lookup(ctx, nc, c.Subject, len(roster))
	if err != nil {
		return authv1beta1.Distribution{}, err
	}
	var current int
	for _, token := range held {
		if token == accountJWT {
			current++
		}
	}
	r.mu.Lock()
	current = min(current, len(roster)-st.distrusting(roster, c.Issuer))
	out := authv1beta1.Distribution{Servers: int32(len(roster)), Current: int32(current)}
	if at, ok := st.pushedAt[c.Subject]; ok {
		out.LastPushTime = &metav1.Time{Time: at}
	}
	r.mu.Unlock()
	return out, nil
}

// Lookup is Distributor.Lookup over the roster; a server that fails to
// read its resolver sends no reply and so makes the lookup ErrUnreachable.
func (r *Resolvers) Lookup(ctx context.Context, operator types.NamespacedName, account string) (string, error) {
	st := r.state(operator)
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return "", err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return "", err
	}
	held, err := r.lookup(ctx, nc, account, len(roster))
	if err != nil {
		return "", err
	}
	var newest string
	var issued int64
	var answered int
	for _, token := range held {
		if token == "" {
			answered++
			continue
		}
		c, err := jwt.DecodeAccountClaims(token)
		if err != nil || c.Subject != account {
			continue
		}
		answered++
		if newest == "" || c.IssuedAt > issued {
			newest, issued = token, c.IssuedAt
		}
	}
	if answered < len(roster) {
		return "", fmt.Errorf("%w: %d of %d servers answered CLAIMS.LOOKUP for account %s",
			ErrUnreachable, answered, len(roster), account)
	}
	return newest, nil
}

// Delete is Distributor.Delete; a request deleting the accounts the last
// one did, signed by the same key, changes nothing.
func (r *Resolvers) Delete(ctx context.Context, operator types.NamespacedName, request string) error {
	var accounts []string
	var issuer string
	if request != "" {
		c, err := jwt.DecodeGeneric(request)
		if err != nil {
			return fmt.Errorf("decode delete request: %w", err)
		}
		list, _ := c.Data["accounts"].([]any)
		for _, v := range list {
			if s, ok := v.(string); ok {
				accounts = append(accounts, s)
			}
		}
		slices.Sort(accounts)
		issuer = c.Issuer
	}
	st := r.state(operator)
	r.mu.Lock()
	if slices.Equal(st.deletes, accounts) && st.issuer == issuer {
		r.mu.Unlock()
		return nil
	}
	st.deletes, st.issuer, st.request = accounts, issuer, request
	st.acked = map[string]bool{}
	r.mu.Unlock()
	return r.sendDeletes(ctx, operator, st)
}

// Distrusting is Distributor.Distrusting over the roster, counted as Push
// and Current count the servers that do not trust a JWT's signer.
func (r *Resolvers) Distrusting(ctx context.Context, operator types.NamespacedName, key string) (servers, distrusting int, err error) {
	st := r.state(operator)
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return 0, 0, err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return 0, 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(roster), st.distrusting(roster, key), nil
}

// sendDeletes sends operator's delete request once if a server in the
// roster has not acknowledged it, and records every server that does.
func (r *Resolvers) sendDeletes(ctx context.Context, operator types.NamespacedName, st *resolverState) error {
	st.send.Lock()
	defer st.send.Unlock()
	r.mu.Lock()
	request := st.request
	r.mu.Unlock()
	if request == "" {
		return nil
	}
	nc, err := r.conn(ctx, operator)
	if err != nil {
		return err
	}
	roster, err := r.roster(ctx, st, nc)
	if err != nil {
		return err
	}
	r.mu.Lock()
	pending := 0
	for id := range roster {
		if !st.acked[id] {
			pending++
		}
	}
	r.mu.Unlock()
	if pending == 0 {
		return nil
	}
	var errs []error
	err = collect(ctx, nc, subjClaimsDelete, []byte(request), r.wait(), len(roster), func(data []byte) {
		var resp claimUpdateResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			errs = append(errs, fmt.Errorf("decode CLAIMS.DELETE reply: %w", err))
			return
		}
		if resp.Error != nil {
			errs = append(errs, fmt.Errorf("server %s refused the delete: %s", resp.Server.ID, resp.Error.Description))
			return
		}
		r.mu.Lock()
		if st.request == request {
			st.acked[resp.Server.ID] = true
		}
		r.mu.Unlock()
	})
	return errors.Join(append(errs, err)...)
}

// roster returns st's roster, polling it first if it was never polled.
func (r *Resolvers) roster(ctx context.Context, st *resolverState, nc *nats.Conn) (map[string]bool, error) {
	r.mu.Lock()
	roster := st.roster
	r.mu.Unlock()
	if roster != nil {
		return roster, nil
	}
	answered, trust, err := r.pollRoster(ctx, nc)
	if err != nil {
		return nil, err
	}
	r.setRoster(st, answered, trust)
	r.mu.Lock()
	defer r.mu.Unlock()
	return st.roster, nil
}

// pollRoster returns the IDs of the servers answering STATSZ within Wait,
// and the NATS operator keys of those that reported them.
func (r *Resolvers) pollRoster(ctx context.Context, nc *nats.Conn) (map[string]bool, map[string][]string, error) {
	var trust map[string][]string
	var wg sync.WaitGroup
	wg.Go(func() { trust = r.pollTrust(ctx, nc) })
	roster, err := r.pollStatsz(ctx, nc)
	wg.Wait()
	return roster, trust, err
}

// pollTrust returns, per server answering VARZ within Wait with the NATS
// operator JWTs it runs under, the identity and signing keys those list.
// A system user without the VARZ permission gets no answer and so an empty
// result.
func (r *Resolvers) pollTrust(ctx context.Context, nc *nats.Conn) map[string][]string {
	trust := map[string][]string{}
	_ = collect(ctx, nc, subjPingVarz, []byte("{}"), r.wait(), 0, func(data []byte) {
		var m struct {
			Server struct {
				ID string `json:"id"`
			} `json:"server"`
			Data struct {
				Operators []*jwt.OperatorClaims `json:"trusted_operators_claim"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &m); err != nil || m.Server.ID == "" {
			return
		}
		var keys []string
		for _, oc := range m.Data.Operators {
			if oc != nil {
				keys = append(append(keys, oc.Subject), oc.SigningKeys...)
			}
		}
		if len(keys) > 0 {
			trust[m.Server.ID] = keys
		}
	})
	return trust
}

func (r *Resolvers) pollStatsz(ctx context.Context, nc *nats.Conn) (map[string]bool, error) {
	roster := map[string]bool{}
	var errs []error
	err := collect(ctx, nc, subjPingStatsz, []byte("{}"), r.wait(), 0, func(data []byte) {
		var m struct {
			Server struct {
				ID string `json:"id"`
			} `json:"server"`
		}
		if err := json.Unmarshal(data, &m); err != nil {
			errs = append(errs, fmt.Errorf("decode STATSZ: %w", err))
			return
		}
		roster[m.Server.ID] = true
	})
	if err != nil {
		return nil, err
	}
	if len(roster) == 0 {
		return nil, errors.Join(append(errs, fmt.Errorf("%w: no server answered STATSZ", ErrUnreachable))...)
	}
	return roster, nil
}

// setRoster merges the servers that answered a poll into st's roster and
// records trust over what earlier polls reported, and forgets the
// acknowledgements and trust of servers that left it. It reports whether a
// server joined, left or answered again after missing a poll.
func (r *Resolvers) setRoster(st *resolverState, answered map[string]bool, trust map[string][]string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	misses, changed := mergeRoster(st.misses, answered)
	st.misses = misses
	st.roster = make(map[string]bool, len(misses))
	for id := range misses {
		st.roster[id] = true
	}
	for id := range st.acked {
		if !st.roster[id] {
			delete(st.acked, id)
		}
	}
	if st.trust == nil {
		st.trust = map[string][]string{}
	}
	maps.Copy(st.trust, trust)
	maps.DeleteFunc(st.trust, func(id string, _ []string) bool { return !st.roster[id] })
	return changed
}

// distrusting counts the servers of roster known not to trust issuer; the
// caller holds Resolvers.mu.
func (st *resolverState) distrusting(roster map[string]bool, issuer string) int {
	var n int
	for id := range roster {
		if keys, ok := st.trust[id]; ok && !slices.Contains(keys, issuer) {
			n++
		}
	}
	return n
}

// mergeRoster returns each roster server's missed-poll count after a poll
// answered by answered, and whether a server joined, left, or answered
// after a miss.
func mergeRoster(prev map[string]int, answered map[string]bool) (next map[string]int, changed bool) {
	next = make(map[string]int, max(len(prev), len(answered)))
	for id, n := range prev {
		switch {
		case answered[id]:
			changed = changed || n > 0
			next[id] = 0
		case n+1 < rosterMisses:
			next[id] = n + 1
		default:
			changed = true
		}
	}
	for id := range answered {
		if _, ok := prev[id]; !ok {
			changed = true
			next[id] = 0
		}
	}
	return next, changed
}

// lookup returns every CLAIMS.LOOKUP reply for account: its JWT, or "" from
// a server without one.
func (r *Resolvers) lookup(ctx context.Context, nc *nats.Conn, account string, want int) ([]string, error) {
	var out []string
	err := collect(ctx, nc, fmt.Sprintf(subjClaimsLookup, account), nil, r.wait(), want, func(data []byte) {
		out = append(out, string(data))
	})
	return out, err
}

// Start polls rosters every Interval until ctx ends.
func (r *Resolvers) Start(ctx context.Context) error {
	t := time.NewTicker(r.interval())
	defer t.Stop()
	for {
		r.poll(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (r *Resolvers) poll(ctx context.Context) {
	r.mu.Lock()
	ops := slices.Collect(maps.Keys(r.operators))
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, op := range ops {
		wg.Go(func() {
			if err := r.pollOperator(ctx, op); err != nil && ctx.Err() == nil {
				r.Log.Error(err, "roster poll", "operator", op)
			}
		})
	}
	wg.Wait()
}

func (r *Resolvers) pollOperator(ctx context.Context, operator types.NamespacedName) error {
	nc, err := r.Conn(ctx, operator)
	if errors.Is(err, ErrOperatorGone) {
		r.mu.Lock()
		delete(r.operators, operator)
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	st := r.state(operator)
	answered, trust, err := r.pollRoster(ctx, nc)
	if err != nil {
		return err
	}
	if r.setRoster(st, answered, trust) {
		r.notify(ctx, operator)
	}
	return r.sendDeletes(ctx, operator, st)
}

func (r *Resolvers) notify(ctx context.Context, operator types.NamespacedName) {
	r.mu.Lock()
	subs := slices.Clone(r.subs)
	r.mu.Unlock()
	for _, ch := range subs {
		ev := event.GenericEvent{Object: &authv1beta1.NatsOperator{ObjectMeta: metav1.ObjectMeta{Namespace: operator.Namespace, Name: operator.Name}}}
		select {
		case ch <- ev:
		case <-ctx.Done():
			return
		}
	}
}

// claimUpdateResponse is a reply to CLAIMS.UPDATE or CLAIMS.DELETE.
type claimUpdateResponse struct {
	Server struct {
		ID string `json:"id"`
	} `json:"server"`
	Error *apiError `json:"error"`
}

// collect publishes body to subject and hands each reply to handle until
// want replies arrived, where want is positive, or wait passed. A
// no-responders status means no server is subscribed, and wraps
// ErrUnreachable.
func collect(ctx context.Context, nc *nats.Conn, subject string, body []byte, wait time.Duration, want int, handle func(data []byte)) error {
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", inbox, err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for n := 0; want <= 0 || n < want; n++ {
		msg, err := sub.NextMsgWithContext(wctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			return fmt.Errorf("%w: no server subscribes to %s", ErrUnreachable, strings.TrimSpace(subject))
		}
		handle(msg.Data)
	}
	return nil
}
