package jwtplane

import (
	"fmt"

	"github.com/nats-io/jwt/v2"
)

// ExportPresetJetStreamStepdown is the export preset that lets the system
// account move an account's stream and consumer leaders.
const ExportPresetJetStreamStepdown = "jetstream-stepdown"

const (
	streamStepdownSubject   = "$JS.API.STREAM.LEADER.STEPDOWN.*"
	consumerStepdownSubject = "$JS.API.CONSUMER.LEADER.STEPDOWN.*.*"
	stepdownPrefix          = "acc."
)

// ExportPreset returns the exports a named export preset expands to.
func ExportPreset(name string) ([]Export, error) {
	if name != ExportPresetJetStreamStepdown {
		return nil, fmt.Errorf("unknown export preset %q", name)
	}
	return []Export{
		{Name: "jetstream-stepdown-stream", Type: jwt.Service, Subject: streamStepdownSubject},
		{Name: "jetstream-stepdown-consumer", Type: jwt.Service, Subject: consumerStepdownSubject},
	}, nil
}

// StepdownImports returns the system account's imports of the
// jetstream-stepdown exports of account, each under the prefix
// "acc.<account>.".
func StepdownImports(account string) []Import {
	exports, _ := ExportPreset(ExportPresetJetStreamStepdown)
	imports := make([]Import, 0, len(exports))
	for _, e := range exports {
		imports = append(imports, Import{
			Account:      account,
			Export:       e,
			LocalSubject: StepdownPrefix(account) + e.Subject,
		})
	}
	return imports
}

// StepdownPrefix is the prefix a system user puts before an account's
// leader-move API subject to reach it through the account's import.
func StepdownPrefix(account string) string {
	return stepdownPrefix + account + "."
}

// StreamStepdownSubject is where a system user requests a leader move of
// stream in account.
func StreamStepdownSubject(account, stream string) string {
	return StepdownPrefix(account) + "$JS.API.STREAM.LEADER.STEPDOWN." + stream
}

// ConsumerStepdownSubject is where a system user requests a leader move of
// consumer on stream in account.
func ConsumerStepdownSubject(account, stream, consumer string) string {
	return StepdownPrefix(account) + "$JS.API.CONSUMER.LEADER.STEPDOWN." + stream + "." + consumer
}
