package authctl_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
)

// testTieredLimits checks that an account's JetStream limits by tier are
// signed into its JWT in place of limits for the account, and that the API
// server refuses the two together.
func (e *env) testTieredLimits(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata: {name: tiered, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  limits:
    jetstream:
      tiers:
        - name: R1
          diskStorage: 10Gi
          streams: 10
        - name: R3
          memoryStorage: 1Gi
          diskStorage: 50Gi
          maxAckPending: 1000
          diskMaxStreamBytes: 1Gi
          maxBytesRequired: true
`)
	acc := &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "tiered"), acc)
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
	})
	ac, err := jwt.DecodeAccountClaims(acc.Status.JWT)
	require.NoError(t, err)
	require.Equal(t, jwt.JetStreamLimits{}, ac.Limits.JetStreamLimits)
	require.Equal(t, jwt.JetStreamTieredLimits{
		"R1": {MemoryStorage: -1, DiskStorage: 10 << 30, Streams: 10, Consumer: -1},
		"R3": {MemoryStorage: 1 << 30, DiskStorage: 50 << 30, Streams: -1, Consumer: -1, MaxAckPending: 1000, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true},
	}, ac.Limits.JetStreamTieredLimits)

	var both map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(`
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata: {name: tiered-and-not, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  limits:
    jetstream:
      streams: 10
      tiers:
        - name: R1
          diskStorage: 10Gi
`), &both))
	err = e.c.Create(t.Context(), &unstructured.Unstructured{Object: both})
	require.True(t, apierrors.IsInvalid(err), "got %v", err)
	require.ErrorContains(t, err, "jetstream limits are set either for the account or by tier")
}
