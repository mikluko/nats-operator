package fixtures

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// TestGenerate pins every story's generated fixtures: each document written
// decodes strictly into its type, and each patch file, named for the kind it
// targets, decodes strictly into that kind.
func TestGenerate(t *testing.T) {
	scheme := fixtureScheme(t)
	kinds := map[string]schema.GroupVersionKind{}
	for gvk := range scheme.AllKnownTypes() {
		if gvk.Group == natsv1beta1.GroupVersion.Group {
			kinds[strings.ToLower(gvk.Kind)] = gvk
		}
	}
	root := t.TempDir()
	require.NoError(t, Generate(root))
	stories, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, stories, len(generators))
	for _, story := range stories {
		t.Run(story.Name(), func(t *testing.T) {
			dir := filepath.Join(root, story.Name(), "e2e")
			decodeDir(t, dir)
			patches, err := filepath.Glob(filepath.Join(dir, "*.json"))
			require.NoError(t, err)
			for _, path := range patches {
				gvk, ok := kinds[strings.TrimSuffix(filepath.Base(path), ".json")]
				require.True(t, ok, "%s names no kind of %s", path, natsv1beta1.GroupVersion.Group)
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				var patch map[string]any
				require.NoError(t, json.Unmarshal(raw, &patch))
				require.Equal(t, []string{"spec"}, slices.Sorted(maps.Keys(patch)), path)
				obj, err := scheme.New(gvk)
				require.NoError(t, err)
				require.NoError(t, yaml.UnmarshalStrict(raw, obj), path)
			}
		})
	}
}
