package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func parseMap(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(s), &m))
	return m
}

func TestDiff(t *testing.T) {
	tests := []struct {
		name string
		want string
		got  string
		diff []string
	}{
		{
			name: "subset holds",
			want: `{replicas: 3, endpoints: {client: "nats://a:4222"}}`,
			got:  `{replicas: 3, readyReplicas: 3, endpoints: {client: "nats://a:4222", monitor: "http://a:8222"}}`,
		},
		{
			name: "integer and float are equal",
			want: `{observedGeneration: 1}`,
			got:  `{observedGeneration: 1.0}`,
		},
		{
			name: "scalar differs",
			want: `{version: 2.15.0, replicas: 3}`,
			got:  `{version: 2.15.1, replicas: 3}`,
			diff: []string{`.version: want "2.15.0", got "2.15.1"`},
		},
		{
			name: "absent subtree yields one line per scalar",
			want: `{config: {revision: abc, appliedBy: Restart}}`,
			got:  `{}`,
			diff: []string{
				`.config.appliedBy: want "Restart", got <absent>`,
				`.config.revision: want "abc", got <absent>`,
			},
		},
		{
			name: "conditions match by type and compare status only",
			want: `{conditions: [{type: Ready, status: "True", reason: AllServersReady, message: m}, {type: Progressing, status: "False"}]}`,
			got:  `{conditions: [{type: Progressing, status: "False", reason: Other}, {type: Ready, status: "True", reason: Other}]}`,
		},
		{
			name: "condition with wrong status",
			want: `{conditions: [{type: Ready, status: "True"}]}`,
			got:  `{conditions: [{type: Ready, status: "False"}]}`,
			diff: []string{`.conditions[type=Ready].status: want "True", got "False"`},
		},
		{
			name: "condition missing",
			want: `{conditions: [{type: Settled, status: "True"}]}`,
			got:  `{conditions: [{type: Ready, status: "True"}]}`,
			diff: []string{`.conditions[type=Settled].status: want "True", got <absent>`},
		},
		{
			name: "list items compared in turn as subsets",
			want: `{servers: [{name: a, ready: true}, {name: b, ready: true}]}`,
			got:  `{servers: [{name: a, ready: true, version: x}, {name: b, ready: false}]}`,
			diff: []string{`.servers[1].ready: want true, got false`},
		},
		{
			name: "strings are not HTML-escaped",
			want: `{restartReason: "2.15.0 -> 2.15.1"}`,
			got:  `{}`,
			diff: []string{`.restartReason: want "2.15.0 -> 2.15.1", got <absent>`},
		},
		{
			name: "list length differs",
			want: `{updated: [a]}`,
			got:  `{updated: [a, b]}`,
			diff: []string{`.updated: want 1 items, got 2 items`},
		},
		{
			name: "object where a scalar stands",
			want: `{jetstream: {metaLeader: a}}`,
			got:  `{jetstream: none}`,
			diff: []string{`.jetstream: want an object, got "none"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, m := range Diff(parseMap(t, tt.want), parseMap(t, tt.got)) {
				got = append(got, m.String())
			}
			require.Equal(t, tt.diff, got)
		})
	}
}
