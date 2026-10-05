package authctl_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// testOutsideImports checks that an account's imports from the system
// account and from an account named by public key are signed into its JWT,
// and that the API server refuses an import naming its exporter both ways,
// one by public key without the export's subject, and share on a Stream.
func (e *env) testOutsideImports(t *testing.T) {
	seed, err := jwtplane.GenerateSeed(nkeys.PrefixByteAccount)
	require.NoError(t, err)
	kp, err := nkeys.FromSeed(seed)
	require.NoError(t, err)
	outside, err := kp.PublicKey()
	require.NoError(t, err)

	e.apply(t, `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata: {name: monitor, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  imports:
    - accountRef: {kind: NatsSystemAccount, name: sys}
      export: account-monitoring-streams
      localSubject: "sys.events.>"
      allowTrace: true
    - publicKey: `+outside+`
      export: tickets
      subject: tickets.open
      type: Service
      share: true
`)
	acc := &authv1beta1.NatsAccount{}
	sys := &authv1beta1.NatsSystemAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "monitor"), acc)
		e.get(ct, key("nats-system", "sys"), sys)
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
	})
	require.Equal(t, []authv1beta1.ImportStatus{
		{Export: "sys/account-monitoring-streams", Subject: "$SYS.ACCOUNT." + acc.Status.PublicKey + ".>", LocalSubject: "sys.events.>", Type: authv1beta1.ExportTypeStream},
		{Export: outside + "/tickets", Subject: "tickets.open", Type: authv1beta1.ExportTypeService},
	}, acc.Status.Imports)
	c, err := jwt.DecodeAccountClaims(acc.Status.JWT)
	require.NoError(t, err)
	require.Len(t, c.Imports, 2)
	byName := map[string]*jwt.Import{}
	for _, i := range c.Imports {
		byName[i.Name] = i
	}
	require.Equal(t, &jwt.Import{Name: "account-monitoring-streams", Account: sys.Status.PublicKey, Subject: jwt.Subject("$SYS.ACCOUNT." + acc.Status.PublicKey + ".>"), LocalSubject: "sys.events.>", Type: jwt.Stream, AllowTrace: true}, byName["account-monitoring-streams"])
	require.Equal(t, &jwt.Import{Name: "tickets", Account: outside, Subject: "tickets.open", Type: jwt.Service, Share: true}, byName["tickets"])

	for name, tt := range map[string]struct{ imp, message string }{
		"both":                {`{accountRef: {kind: NatsSystemAccount, name: sys}, publicKey: ` + outside + `, export: x, subject: x, type: Stream}`, "an import names its exporter by accountRef or by publicKey"},
		"key without subject": {`{publicKey: ` + outside + `, export: x, type: Stream}`, "subject and type are set with publicKey and only then"},
		"ref with subject":    {`{accountRef: {kind: NatsSystemAccount, name: sys}, export: x, subject: x}`, "subject and type are set with publicKey and only then"},
		"share on a stream":   {`{publicKey: ` + outside + `, export: x, subject: x, type: Stream, share: true}`, "share is set only on a Service import"},
		"trace on a service":  {`{publicKey: ` + outside + `, export: x, subject: x, type: Service, allowTrace: true}`, "allowTrace is set only on a Stream import"},
	} {
		t.Run(name, func(t *testing.T) {
			var m map[string]any
			require.NoError(t, yaml.Unmarshal([]byte(`
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata: {name: refused, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  imports: [`+tt.imp+`]
`), &m))
			err := e.c.Create(t.Context(), &unstructured.Unstructured{Object: m})
			require.True(t, apierrors.IsInvalid(err), "got %v", err)
			require.ErrorContains(t, err, tt.message)
		})
	}
}
