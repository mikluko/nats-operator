package telemetry

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

// Event is a Kubernetes event a controller records: its reason, type and
// action, the kinds it regards, and when it is recorded.
type Event struct {
	Reason     string
	Type       string
	Action     string
	Controller string
	Regarding  []string
	When       string
}

// The events, as Events lists them.
var (
	MoveStarted = Event{
		Reason: "MoveStarted", Type: corev1.EventTypeNormal, Action: "Move", Controller: JetStreamController,
		Regarding: []string{"NatsBalancer", "NatsSystemBalancer", "NatsClusterEvacuation"},
		When:      "A balancer moves a stream's leader or placement, or an evacuation requests a stream's move.",
	}
	MoveDone = Event{
		Reason: "MoveDone", Type: corev1.EventTypeNormal, Action: "Move", Controller: JetStreamController,
		Regarding: []string{"NatsSystemBalancer", "NatsClusterEvacuation"},
		When:      "A move the system balancer or the evacuation requested is seen complete.",
	}
	MoveCancelled = Event{
		Reason: "MoveCancelled", Type: corev1.EventTypeNormal, Action: "CancelMove", Controller: JetStreamController,
		Regarding: []string{"NatsClusterEvacuation"},
		When:      "Deleting an evacuation cancels a move still in flight.",
	}
	MoveRefused = Event{
		Reason: "MoveRefused", Type: corev1.EventTypeWarning, Action: "Move", Controller: JetStreamController,
		Regarding: []string{"NatsClusterEvacuation"},
		When:      "The server refuses a move the evacuation requests.",
	}
	EvacuationRefused = Event{
		Reason: "EvacuationRefused", Type: corev1.EventTypeWarning, Action: "Evacuate", Controller: JetStreamController,
		Regarding: []string{"NatsClusterEvacuation"},
		When:      "The evacuation refuses to start because a server of its source NATS cluster carries the target tags.",
	}
	RolloutStep = Event{
		Reason: "RolloutStep", Type: corev1.EventTypeNormal, Action: "Rollout", Controller: ClusterController,
		Regarding: []string{"NatsCluster"},
		When:      "A rollout restarts a server, or starts removing or replacing one.",
	}
	GateBlocked = Event{
		Reason: "GateBlocked", Type: corev1.EventTypeWarning, Action: "Rollout", Controller: ClusterController,
		Regarding: []string{"NatsCluster"},
		When:      "A rollout's gate has been closed long enough for Progressing to read GateBlocked.",
	}
	ReconcileFailed = Event{
		Reason: "ReconcileFailed", Type: corev1.EventTypeWarning, Action: "Reconcile", Controller: ClusterController,
		Regarding: []string{"NatsCluster"},
		When:      "A reconcile fails on anything but a write conflict; Progressing reads ReconcileFailed with the same message.",
	}
	JWTPushed = Event{
		Reason: "JWTPushed", Type: corev1.EventTypeNormal, Action: "Push", Controller: AuthController,
		Regarding: []string{"NatsSystemAccount", "NatsAccount"},
		When:      "A newly signed account or system account JWT is pushed to the servers.",
	}
	JWTHeld = Event{
		Reason: "JWTHeld", Type: corev1.EventTypeWarning, Action: "Sign", Controller: AuthController,
		Regarding: []string{"NatsOperator", "NatsAccount"},
		When:      "An account JWT is not signed because its revocations cannot be recovered from the servers.",
	}
	UserKicked = Event{
		Reason: "UserKicked", Type: corev1.EventTypeNormal, Action: "Kick", Controller: AuthController,
		Regarding: []string{"NatsUser"},
		When:      "A deleted user's live connections are closed.",
	}
)

// Events are every event the controllers record.
var Events = []Event{
	MoveStarted,
	MoveDone,
	MoveCancelled,
	MoveRefused,
	EvacuationRefused,
	RolloutStep,
	GateBlocked,
	ReconcileFailed,
	JWTPushed,
	JWTHeld,
	UserKicked,
}

// Emit records e regarding obj through rec with the note format and args
// make. A nil rec records nothing.
func Emit(rec events.EventRecorder, obj runtime.Object, e Event, format string, args ...any) {
	if rec == nil {
		return
	}
	rec.Eventf(obj, nil, e.Type, e.Reason, e.Action, format, args...)
}
