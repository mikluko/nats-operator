package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/jsapi"
)

// Finalizer holds a JetStream controller resource until the server has been
// told of its deletion: a JetStream object resource's deletion policy has
// run, or a NatsClusterEvacuation's moves in flight are cancelled.
const Finalizer = "jetstream.nats.mikluko.io/finalizer"

// DefaultResync is how often a resource is compared to its server object
// when Syncer.Resync is zero.
const DefaultResync = 10 * time.Minute

// SettlingRecheck is how soon a synced object is read again, when sooner
// than the resync period, while its Raft group has no leader or a member
// that is not current, so its status follows the group as it settles.
const SettlingRecheck = 15 * time.Second

// MovingRecheck is how soon a synced object is read again, when sooner than
// the resync period, while its Raft group is moving to a new placement.
const MovingRecheck = 5 * time.Second

// Object is one resource's server object, bound to the resource's spec and
// connection.
type Object interface {
	// Describe names the object in condition messages, as "stream LEDGER".
	Describe() string
	// Fetch returns the object's info, or nil when it does not exist.
	Fetch(ctx context.Context) (*Info, error)
	// Desired returns the spec's fields as a Config: the fields it sets,
	// and nil for a key it requires absent.
	Desired() (Config, error)
	// Create creates the object from cfg.
	Create(ctx context.Context, cfg Config) (*Info, error)
	// Update replaces current's config with cfg.
	Update(ctx context.Context, current *Info, cfg Config) (*Info, error)
	// Delete deletes the object.
	Delete(ctx context.Context) error
	// WriteSpec writes cfg into the resource's spec and persists it: every
	// field when replace is set, otherwise only the fields spec omits.
	WriteSpec(ctx context.Context, cfg Config, replace bool) error
}

// Resource is the Kubernetes side of a Sync.
type Resource struct {
	// Object is the resource; its UID names the owner in the marker, and
	// its generation is read again after WriteSpec.
	Object   metav1.Object
	Policies jetstreamv1beta1.Policies
	Status   *jetstreamv1beta1.SyncStatus
	// OwnerExists reports whether a resource of the same kind has uid.
	OwnerExists func(ctx context.Context, uid types.UID) (bool, error)
}

// Syncer runs the lifecycle policies of one kind.
type Syncer struct {
	// Resync is how often a resource is compared to its server object,
	// DefaultResync when zero.
	Resync time.Duration
	// Now is the clock, time.Now when nil.
	Now func() time.Time
}

func (s Syncer) resync() time.Duration {
	if s.Resync > 0 {
		return s.Resync
	}
	return DefaultResync
}

func (s Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Sync brings o to r's spec under r's policies and records the outcome in
// r.Status, and returns o's info where it was read. A Terminal condition
// under Hold for the current generation returns at once, reading nothing.
// The error is one a retry may cure; the status already says so.
func (s Syncer) Sync(ctx context.Context, r Resource, o Object) (reconcile.Result, *Info, error) {
	st := r.Status
	if held(st, r.Object.GetGeneration(), r.Policies.TerminalPolicy) {
		return reconcile.Result{}, nil, nil
	}
	info, err := s.sync(ctx, r, o)
	gen := r.Object.GetGeneration()
	st.ObservedGeneration = gen
	var terminal *TerminalError
	var wait *WaitError
	switch {
	case errors.As(err, &terminal):
		setTerminal(st, gen, r.Policies.TerminalPolicy, s.now(), s.resync(), terminal)
		if r.Policies.TerminalPolicy == jetstreamv1beta1.TerminalRetry {
			return reconcile.Result{RequeueAfter: s.resync()}, info, nil
		}
		return reconcile.Result{}, info, nil
	case errors.As(err, &wait):
		clearTerminal(st)
		conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: wait.Reason, Message: wait.Message})
		return reconcile.Result{RequeueAfter: s.resync()}, info, nil
	case err != nil:
		clearTerminal(st)
		conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionSynced, Status: metav1.ConditionFalse, Reason: ReasonSyncFailed, Message: err.Error()})
		conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonSyncFailed, Message: err.Error()})
		return reconcile.Result{}, info, err
	}
	clearTerminal(st)
	conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSynced})
	now := metav1.NewTime(s.now())
	st.LastSyncedTime = &now
	return reconcile.Result{RequeueAfter: s.recheck(info)}, info, nil
}

// recheck returns when a synced object with info is read again.
func (s Syncer) recheck(info *Info) time.Duration {
	if info != nil && info.Moving && MovingRecheck < s.resync() {
		return MovingRecheck
	}
	if settling(info) && SettlingRecheck < s.resync() {
		return SettlingRecheck
	}
	return s.resync()
}

// settling reports whether info names a Raft group with no leader, or with a
// member that is offline or not current.
func settling(info *Info) bool {
	if info == nil || info.Cluster == nil {
		return false
	}
	if info.Cluster.Leader == "" {
		return true
	}
	for _, p := range info.Cluster.Replicas {
		if p == nil || !p.Current || p.Offline {
			return true
		}
	}
	return false
}

// held reports whether status carries a Terminal condition that Hold keeps
// until the generation changes.
func held(status *jetstreamv1beta1.SyncStatus, generation int64, policy jetstreamv1beta1.TerminalPolicy) bool {
	if policy == jetstreamv1beta1.TerminalRetry {
		return false
	}
	c := meta.FindStatusCondition(status.Conditions, ConditionTerminal)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == generation
}

func clearTerminal(status *jetstreamv1beta1.SyncStatus) {
	meta.RemoveStatusCondition(&status.Conditions, ConditionTerminal)
	status.NextCheckTime = nil
}

// sync decides ownership and applies spec, returning a TerminalError or a
// WaitError where the policies say so.
func (s Syncer) sync(ctx context.Context, r Resource, o Object) (*Info, error) {
	st := r.Status
	uid := r.Object.GetUID()
	pending := specPending(st, r.Object.GetGeneration())
	cur, err := o.Fetch(ctx)
	if err != nil {
		return nil, classify(err)
	}
	if cur == nil {
		if r.Policies.AdoptionPolicy == jetstreamv1beta1.AdoptionAdopt {
			msg := fmt.Sprintf("%s does not exist; adoptionPolicy Adopt never creates it", o.Describe())
			conditions.Set(&st.Conditions, r.Object.GetGeneration(), metav1.Condition{Type: ConditionAdopted, Status: metav1.ConditionFalse, Reason: ReasonNotFound, Message: msg})
			return nil, &WaitError{Reason: ReasonNotFound, Message: msg}
		}
		desired, err := o.Desired()
		if err != nil {
			return nil, err
		}
		m := Marker{UID: uid, Origin: jetstreamv1beta1.OwnershipCreated}
		info, err := o.Create(ctx, Overlay(nil, desired, m))
		if err != nil {
			return nil, classify(err)
		}
		return s.settle(ctx, r, o, info, m, nil)
	}

	m, err := s.claim(ctx, r, o, cur)
	if err != nil {
		return cur, err
	}
	desired, err := o.Desired()
	if err != nil {
		return cur, err
	}
	want := Overlay(cur.Config, desired, m)
	drift := Drift(cur.Config, want)
	if len(drift) == 0 {
		return s.settle(ctx, r, o, cur, m, nil)
	}
	info, err := o.Update(ctx, cur, want)
	if err != nil {
		return cur, classify(err)
	}
	if pending || st.Ownership == nil || st.Ownership.UID != uid {
		drift = nil
	}
	return s.settle(ctx, r, o, info, m, drift)
}

// specPending reports whether generation has not yet been applied, so a
// difference from the server is an edit to apply rather than drift.
func specPending(status *jetstreamv1beta1.SyncStatus, generation int64) bool {
	c := meta.FindStatusCondition(status.Conditions, ConditionSynced)
	return c == nil || c.ObservedGeneration != generation || c.Reason == ReasonSyncFailed
}

// claim returns the marker cur is to carry, adopting it where the policy
// allows, or the TerminalError the policy calls for.
func (s Syncer) claim(ctx context.Context, r Resource, o Object, cur *Info) (Marker, error) {
	uid := r.Object.GetUID()
	m, marked := ReadMarker(cur.Config.Metadata())
	if marked && m.UID == uid {
		return m, nil
	}
	if marked {
		alive, err := r.OwnerExists(ctx, m.UID)
		if err != nil {
			return Marker{}, err
		}
		if alive {
			return Marker{}, &TerminalError{
				Reason:  ReasonOwnedByOther,
				Message: fmt.Sprintf("%s is owned by another resource, UID %s", o.Describe(), m.UID),
			}
		}
	}
	policy := r.Policies.AdoptionPolicy
	if policy == "" || policy == jetstreamv1beta1.AdoptionNever {
		why := "carries no ownership marker"
		if marked {
			why = fmt.Sprintf("carries the marker of UID %s, which no longer exists", m.UID)
		}
		return Marker{}, &TerminalError{
			Reason:  ReasonExistsUnowned,
			Message: fmt.Sprintf("%s exists and %s; set spec.adoptionPolicy to Adopt or AdoptOrCreate to take it over", o.Describe(), why),
		}
	}
	msg := fmt.Sprintf("adopted existing %s; spec applied over the server config", o.Describe())
	if policy == jetstreamv1beta1.AdoptionAdopt {
		if err := o.WriteSpec(ctx, cur.Config, true); err != nil {
			return Marker{}, err
		}
		msg = fmt.Sprintf("adopted existing %s; spec written from the server", o.Describe())
	}
	conditions.Set(&r.Status.Conditions, r.Object.GetGeneration(), metav1.Condition{Type: ConditionAdopted, Status: metav1.ConditionTrue, Reason: ReasonFoundUnowned, Message: msg})
	return Marker{UID: uid, Origin: jetstreamv1beta1.OwnershipAdopted}, nil
}

// settle records that info now matches spec, reporting the drift corrected
// where there was any, and late-initializes spec under an adopting policy.
func (s Syncer) settle(ctx context.Context, r Resource, o Object, info *Info, m Marker, drift []string) (*Info, error) {
	if p := r.Policies.AdoptionPolicy; p == jetstreamv1beta1.AdoptionAdopt || p == jetstreamv1beta1.AdoptionAdoptOrCreate {
		if err := o.WriteSpec(ctx, info.Config, false); err != nil {
			return info, err
		}
	}
	st := r.Status
	st.Ownership = &jetstreamv1beta1.Ownership{Origin: m.Origin, UID: m.UID}
	gen := r.Object.GetGeneration()
	if len(drift) > 0 {
		conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionSynced, Status: metav1.ConditionFalse, Reason: ReasonDriftCorrected, Message: fmt.Sprintf("reapplied spec over drift in %s", strings.Join(drift, ", "))})

	} else {
		conditions.Set(&st.Conditions, gen, metav1.Condition{Type: ConditionSynced, Status: metav1.ConditionTrue, Reason: ReasonMatchesSpec, Message: messageMatches})
	}
	return info, nil
}

// JetStream API error codes classify tells apart.
const (
	// errCodeStreamInvalidConfig is nats-server's JSStreamInvalidConfigF, a
	// stream config refused as invalid, which it reports with code 500.
	errCodeStreamInvalidConfig jetstream.ErrorCode = 10052
	// errCodeClusterNoPeers is nats-server's JSClusterNoPeersErrF, reported
	// with code 400 while too few servers are online or have room to place
	// the object, which changes as servers come and go.
	errCodeClusterNoPeers jetstream.ErrorCode = 10005
)

// classify turns a JetStream API error refusing the request as invalid into
// a TerminalError, and returns any other error, a lack of peers to place
// the object on among them, as it is.
func classify(err error) error {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode == errCodeClusterNoPeers {
		return err
	}
	if apiErr.Code == http.StatusBadRequest || apiErr.ErrorCode == errCodeStreamInvalidConfig {
		return &TerminalError{Reason: ReasonRejected, Message: apiErr.Description}
	}
	return err
}

// Finalize runs policy for o as resource uid's deletion: Delete deletes o
// where it exists and carries uid's marker, Retain leaves it. Nothing is
// read under Retain.
func Finalize(ctx context.Context, uid types.UID, policy jetstreamv1beta1.DeletionPolicy, o Object) error {
	if policy != jetstreamv1beta1.DeletionDelete {
		return nil
	}
	cur, err := o.Fetch(ctx)
	if err != nil || cur == nil {
		return err
	}
	if m, ok := ReadMarker(cur.Config.Metadata()); !ok || m.UID != uid {
		return nil
	}
	if err := o.Delete(ctx); err != nil && !errors.Is(err, jsapi.ErrNotFound) {
		return err
	}
	return nil
}
