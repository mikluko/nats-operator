package lifecycle

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
)

// Condition types a JetStream object resource reports.
const (
	ConditionReady    = "Ready"
	ConditionSynced   = "Synced"
	ConditionTerminal = "Terminal"
	ConditionAdopted  = "Adopted"
)

// Condition reasons.
const (
	// ReasonSynced is Ready's reason while the server object matches spec.
	ReasonSynced = "Synced"
	// ReasonTerminal is Ready's reason while Terminal is True.
	ReasonTerminal = "Terminal"
	// ReasonSyncFailed is Ready's and Synced's reason after a failure a
	// retry may cure.
	ReasonSyncFailed = "SyncFailed"
	// ReasonObserveFailed is Ready's reason when the server object matches
	// spec but reading what else the server reports of it failed.
	ReasonObserveFailed = "ObserveFailed"

	// ReasonMatchesSpec is Synced's reason when the server object matches
	// spec.
	ReasonMatchesSpec = "MatchesSpec"
	// ReasonDriftCorrected is Synced's reason, False, for one resync after
	// the controller reapplied a spec the server object had drifted from.
	ReasonDriftCorrected = "DriftCorrected"

	// ReasonExistsUnowned is Terminal's reason when adoptionPolicy is Never
	// and the object exists without this resource's marker.
	ReasonExistsUnowned = "ExistsUnowned"
	// ReasonOwnedByOther is Terminal's reason when the object carries the
	// marker of another resource that exists.
	ReasonOwnedByOther = "OwnedByOther"
	// ReasonRejected is Terminal's reason when the server refuses the
	// config as invalid.
	ReasonRejected = "Rejected"

	// ReasonFoundUnowned is Adopted's reason once an unowned object is
	// adopted.
	ReasonFoundUnowned = "FoundUnowned"
	// ReasonNotFound is Adopted's and Ready's reason while adoptionPolicy is
	// Adopt and the object does not exist.
	ReasonNotFound = "NotFound"
)

// messageMatches is Synced's message while the server object matches spec.
const messageMatches = "server config matches spec as of last check"

// NotReady records on status that the resource cannot reach its server
// object for reason, a precondition the kind checks before Sync such as its
// connection.
func NotReady(status *jetstreamv1beta1.SyncStatus, generation int64, reason, message string) {
	status.ObservedGeneration = generation
	conditions.Set(&status.Conditions, generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message})
}

// setTerminal records a Terminal condition; under Retry it also sets when it
// is rechecked.
func setTerminal(status *jetstreamv1beta1.SyncStatus, generation int64, policy jetstreamv1beta1.TerminalPolicy, now time.Time, resync time.Duration, t *TerminalError) {
	conditions.Set(&status.Conditions, generation, metav1.Condition{Type: ConditionTerminal, Status: metav1.ConditionTrue, Reason: t.Reason, Message: t.Message})
	conditions.Set(&status.Conditions, generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonTerminal, Message: t.Message})
	meta.RemoveStatusCondition(&status.Conditions, ConditionSynced)
	status.NextCheckTime = nil
	if policy == jetstreamv1beta1.TerminalRetry {
		next := metav1.NewTime(now.Add(resync))
		status.NextCheckTime = &next
	}
}

// TerminalError is a failure that waits for an edit, or for the recheck
// terminalPolicy Retry makes, rather than for a retry.
type TerminalError struct {
	Reason  string
	Message string
}

func (e *TerminalError) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Message) }

// WaitError is a precondition not yet met, such as a stream a consumer
// needs: Ready goes False with Reason and the resource is rechecked on the
// resync period.
type WaitError struct {
	Reason  string
	Message string
}

func (e *WaitError) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Message) }
