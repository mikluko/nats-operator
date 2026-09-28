package main

import (
	"bytes"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// printedKind is the kind whose spec a story's bare printed patch targets,
// for a generator that prints one patch without naming its object.
var printedKind = map[string]string{
	"6": "NatsOperatorTrust",
	"9": "NatsOperatorTrust",
}

// TestGenerators pins every generator's output: each document it writes
// decodes strictly into its type, and each patch it prints decodes strictly
// into the spec of the kind it targets.
func TestGenerators(t *testing.T) {
	scheme := fixtureScheme(t)
	for story, gen := range generators {
		t.Run(story, func(t *testing.T) {
			dir := t.TempDir()
			var out bytes.Buffer
			require.NoError(t, gen(dir, &out))
			decodeDir(t, dir)

			patches := map[string]map[string]any{}
			if out.Len() > 0 {
				var printed map[string]any
				require.NoError(t, yaml.Unmarshal(out.Bytes(), &printed))
				if kind, ok := printedKind[story]; ok {
					patches[kind] = printed
				} else {
					for target, v := range printed {
						m, ok := v.(map[string]any)
						require.True(t, ok, "%s: %v", target, v)
						patches[target] = m
					}
				}
			}
			for target, p := range patches {
				require.Equal(t, []string{"patch"}, slices.Sorted(maps.Keys(p)), target)
				kind, _, _ := strings.Cut(target, " ")
				obj, err := scheme.New(schema.GroupVersionKind{Group: natsv1beta1.GroupVersion.Group, Version: natsv1beta1.GroupVersion.Version, Kind: kind})
				require.NoError(t, err, target)
				raw, err := yaml.Marshal(p["patch"])
				require.NoError(t, err)
				require.NoError(t, yaml.UnmarshalStrict(raw, obj), target)
			}
		})
	}
}
