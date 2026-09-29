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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/e2e/fixtures"
	"github.com/mikluko/nats-operator/internal/e2e/placeholders"
)

const storiesDir = "../docs/content/docs/stories"

// storyDoc is one YAML document of a story file.
type storyDoc struct {
	name string
	raw  []byte
	obj  *unstructured.Unstructured
}

// apiScheme returns a scheme holding every kind of the four groups.
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

// storyFiles returns the story manifest files and the status files, skipping
// delete files and fixtures.
func storyFiles(t *testing.T) (manifests, statuses []string) {
	t.Helper()
	return walkStories(t, false)
}

// fixtureFiles returns the manifest files and the status files of every
// story's fixture directory, skipping delete files.
func fixtureFiles(t *testing.T) (manifests, statuses []string) {
	t.Helper()
	return walkStories(t, true)
}

// walkStories returns the manifest and status files under storiesDir inside
// a fixture directory where inFixtureDir is set, and outside one where it is not.
func walkStories(t *testing.T, inFixtureDir bool) (manifests, statuses []string) {
	t.Helper()
	err := filepath.WalkDir(storiesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" {
			return err
		}
		if (filepath.Base(filepath.Dir(path)) == e2e.FixtureDir) != inFixtureDir {
			return nil
		}
		name, err := e2e.ParseFileName(d.Name())
		if err != nil {
			return err
		}
		switch name.Role {
		case e2e.RoleDelete:
		case e2e.RoleStatus:
			statuses = append(statuses, path)
		default:
			manifests = append(manifests, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	require.NotEmpty(t, statuses)
	return manifests, statuses
}

// storyManifests returns every object of the manifest files, placeholders
// stripped.
func storyManifests(t *testing.T) []storyDoc {
	t.Helper()
	files, _ := storyFiles(t)
	return manifestDocs(t, storiesDir, files)
}

// manifestDocs returns every object of files, which are under root,
// placeholders stripped.
func manifestDocs(t *testing.T, root string, files []string) []storyDoc {
	t.Helper()
	var docs []storyDoc
	for _, path := range files {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		if name, _ := e2e.ParseFileName(filepath.Base(path)); name.Role == e2e.RoleLive {
			raw, err = placeholders.Strip(raw)
			require.NoError(t, err, path)
		}
		r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
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
			rel, err := filepath.Rel(root, path)
			require.NoError(t, err)
			obj := &unstructured.Unstructured{Object: m}
			docs = append(docs, storyDoc{
				name: rel + "#" + obj.GetKind() + "/" + obj.GetName(),
				raw:  bytes.TrimSpace(raw),
				obj:  obj,
			})
		}
	}
	return docs
}

func TestStoryManifestsDecodeStrictly(t *testing.T) {
	decodeDocs(t, apiScheme(t), storyManifests(t))
}

// TestStoryFixturesDecodeStrictly pins every field of every manifest and
// status in a story's fixture directory, and of every fixture
// fixtures.Generate writes, to a field of the Go types, of the four groups
// or of Kubernetes.
func TestStoryFixturesDecodeStrictly(t *testing.T) {
	s := apiScheme(t)
	require.NoError(t, clientgoscheme.AddToScheme(s))
	manifests, statuses := fixtureFiles(t)
	decodeDocs(t, s, manifestDocs(t, storiesDir, manifests))
	decodeStatuses(t, s, statuses)

	gen := t.TempDir()
	require.NoError(t, fixtures.Generate(gen))
	generated, err := filepath.Glob(filepath.Join(gen, "*", e2e.FixtureDir, "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, generated)
	decodeDocs(t, s, manifestDocs(t, gen, generated))
}

// decodeDocs strictly decodes each of docs into its kind's Go type in s.
func decodeDocs(t *testing.T, s *runtime.Scheme, docs []storyDoc) {
	t.Helper()
	for _, doc := range docs {
		t.Run(doc.name, func(t *testing.T) {
			obj, err := s.New(doc.obj.GroupVersionKind())
			require.NoError(t, err, "kind is not registered")
			require.NoError(t, yaml.UnmarshalStrict(doc.raw, obj))
		})
	}
}

// statusKind maps the kind a status file names to its GroupVersionKind.
func statusKind(t *testing.T, s *runtime.Scheme, path string) schema.GroupVersionKind {
	t.Helper()
	name, err := e2e.ParseFileName(filepath.Base(path))
	require.NoError(t, err)
	for gvk := range s.AllKnownTypes() {
		if strings.ToLower(gvk.Kind) == name.Kind {
			return gvk
		}
	}
	require.Failf(t, "no kind for status file", "%s names %q", path, name.Kind)
	return schema.GroupVersionKind{}
}

// TestStoryStatusesDecodeStrictly pins every block and field of every story
// status, placeholders' example values included, to a field of the Go status
// types.
func TestStoryStatusesDecodeStrictly(t *testing.T) {
	_, statuses := storyFiles(t)
	decodeStatuses(t, apiScheme(t), statuses)
}

// decodeStatuses strictly decodes each status file, placeholders stripped,
// into the Go type in s of the kind its name gives.
func decodeStatuses(t *testing.T, s *runtime.Scheme, statuses []string) {
	t.Helper()
	for _, path := range statuses {
		rel, err := filepath.Rel(storiesDir, path)
		require.NoError(t, err)
		t.Run(rel, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			raw, err = placeholders.Strip(raw)
			require.NoError(t, err)
			obj, err := s.New(statusKind(t, s, path))
			require.NoError(t, err)
			require.NoError(t, yaml.UnmarshalStrict(raw, obj))
		})
	}
}
