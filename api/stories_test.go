package api_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

const storiesDir = "../docs/content/stories"

// storyDoc is one YAML document of a story file.
type storyDoc struct {
	name string
	raw  []byte
	obj  *unstructured.Unstructured
}

// apiScheme holds every kind of the four groups.
func apiScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		natsv1beta1.AddToScheme,
		clusterv1beta1.AddToScheme,
		authv1beta1.AddToScheme,
		jetstreamv1beta1.AddToScheme,
	} {
		require.NoError(t, add(s))
	}
	return s
}

// storyFiles returns the story YAML files, split into manifests and
// status-*.yaml files.
func storyFiles(t *testing.T) (manifests, statuses []string) {
	t.Helper()
	err := filepath.WalkDir(storiesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" {
			return err
		}
		if strings.HasPrefix(d.Name(), "status-") {
			statuses = append(statuses, path)
		} else {
			manifests = append(manifests, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	require.NotEmpty(t, statuses)
	return manifests, statuses
}

// storyManifests returns every object in every story manifest file, each
// named by its file and position.
func storyManifests(t *testing.T) []storyDoc {
	t.Helper()
	files, _ := storyFiles(t)
	var docs []storyDoc
	for _, path := range files {
		f, err := os.Open(path)
		require.NoError(t, err)
		r := utilyaml.NewYAMLReader(bufio.NewReader(f))
		for i := 0; ; i++ {
			raw, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, path)
			var m map[string]any
			require.NoError(t, yaml.Unmarshal(raw, &m), path)
			if len(m) == 0 {
				continue
			}
			rel, err := filepath.Rel(storiesDir, path)
			require.NoError(t, err)
			obj := &unstructured.Unstructured{Object: m}
			docs = append(docs, storyDoc{
				name: rel + "#" + obj.GetKind() + "/" + obj.GetName(),
				raw:  bytes.TrimSpace(raw),
				obj:  obj,
			})
		}
		require.NoError(t, f.Close())
	}
	return docs
}

// TestStoryManifestsDecodeStrictly pins every field of every story manifest
// to a field of the Go types.
func TestStoryManifestsDecodeStrictly(t *testing.T) {
	s := apiScheme(t)
	for _, doc := range storyManifests(t) {
		t.Run(doc.name, func(t *testing.T) {
			obj, err := s.New(doc.obj.GroupVersionKind())
			require.NoError(t, err, "kind is not registered")
			require.NoError(t, yaml.UnmarshalStrict(doc.raw, obj))
		})
	}
}

// statusKind maps the kind a status-<kind>-*.yaml file names to its
// GroupVersionKind.
func statusKind(t *testing.T, s *runtime.Scheme, path string) schema.GroupVersionKind {
	t.Helper()
	name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "status-"), ".yaml")
	lower, _, _ := strings.Cut(name, "-")
	for gvk := range s.AllKnownTypes() {
		if strings.ToLower(gvk.Kind) == lower {
			return gvk
		}
	}
	require.Failf(t, "no kind for status file", "%s names %q", path, lower)
	return schema.GroupVersionKind{}
}

// TestStoryStatusesDecodeStrictly pins every block and field of every story
// status to a field of the Go status types.
func TestStoryStatusesDecodeStrictly(t *testing.T) {
	s := apiScheme(t)
	_, statuses := storyFiles(t)
	for _, path := range statuses {
		rel, err := filepath.Rel(storiesDir, path)
		require.NoError(t, err)
		t.Run(rel, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			obj, err := s.New(statusKind(t, s, path))
			require.NoError(t, err)
			require.NoError(t, yaml.UnmarshalStrict(raw, obj))
		})
	}
}
