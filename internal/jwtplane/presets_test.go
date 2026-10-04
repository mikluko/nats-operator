package jwtplane_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// crdEnum returns the enum the generated CRD in file admits at path, a
// chain of property names under the v1beta1 schema, "[]" stepping into
// array items.
func crdEnum(t *testing.T, file string, path ...string) []string {
	t.Helper()
	raw, err := os.ReadFile("../../config/crd/" + file)
	require.NoError(t, err)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.UnmarshalStrict(raw, &crd))
	require.Len(t, crd.Spec.Versions, 1)
	s := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	for _, p := range path {
		if p == "[]" {
			require.NotNil(t, s.Items, "%s: %v", file, path)
			s = s.Items.Schema
			continue
		}
		next, ok := s.Properties[p]
		require.True(t, ok, "%s: no property %q on %v", file, p, path)
		s = &next
	}
	var out []string
	for _, v := range s.Enum {
		var str string
		require.NoError(t, json.Unmarshal(v.Raw, &str))
		out = append(out, str)
	}
	require.NotEmpty(t, out, "%s: %v has no enum", file, path)
	return out
}

// TestPresetsMatchAPI pins the presets jwtplane expands to exactly the
// presets the API admits.
func TestPresetsMatchAPI(t *testing.T) {
	t.Run("user presets", func(t *testing.T) {
		admitted := crdEnum(t, "auth.nats-operator.io_natsusers.yaml", "spec", "preset")
		var expanded []string
		for _, p := range jwtplane.UserPresets() {
			expanded = append(expanded, string(p))
		}
		require.ElementsMatch(t, admitted, expanded)
	})
	t.Run("export presets", func(t *testing.T) {
		admitted := crdEnum(t, "auth.nats-operator.io_natsaccounts.yaml", "spec", "exports", "[]", "preset")
		require.Equal(t, []string{jwtplane.ExportPresetJetStreamStepdown}, admitted)
		for _, p := range admitted {
			_, err := jwtplane.ExportPreset(p)
			require.NoError(t, err, p)
		}
	})
}
