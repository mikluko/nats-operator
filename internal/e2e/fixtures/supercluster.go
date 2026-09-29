package fixtures

import (
	"text/template"

	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// superclusterData is what superclusterTemplate renders.
type superclusterData struct {
	OperatorIdentity, OperatorSigning, SystemIdentity, SystemSigning string
	// WestCredsLines is the west creds file, line by line.
	WestCredsLines []string
}

// supercluster writes story 6's fixtures into dir, with the NATS operator
// and system account JWTs its NatsOperatorTrust takes as the patch file
// natsoperatortrust.json.
func supercluster(dir string) error {
	var s superclusterData
	op, opSeed, err := keys(nkeys.PrefixByteOperator)
	if err != nil {
		return err
	}
	sys, sysSeed, err := keys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	s.OperatorIdentity, s.OperatorSigning = opSeed[0], opSeed[1]
	s.SystemIdentity, s.SystemSigning = sysSeed[0], sysSeed[1]
	sysPub, err := sys.Identity.PublicKey()
	if err != nil {
		return err
	}
	operatorJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "acme", Keys: op, SystemAccount: sysPub})
	if err != nil {
		return err
	}
	systemJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys}, op)
	if err != nil {
		return err
	}
	if s.WestCredsLines, err = userCreds(jwtplane.User{
		Name: "west-cluster-controller", SystemAccount: true, Preset: jwtplane.PresetClusterController,
	}, sys); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-home.yaml", superclusterTemplate, s); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-west.yaml", credsSecretTemplate, secret{Name: "west-cluster-controller-creds", Namespace: "nats-system", Lines: s.WestCredsLines}); err != nil {
		return err
	}
	return writePatch(dir, "natsoperatortrust.json", map[string]string{"operatorJWT": operatorJWT, "systemAccountJWT": systemJWT})
}

var superclusterTemplate = parseFixture("supercluster", `{{template "keys" (dict "name" "acme-operator" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
{{template "keys" (dict "name" "sys" "identity" .SystemIdentity "signing" .SystemSigning)}}---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata:
  name: acme
  namespace: nats-system
spec:
  systemAccountRef:
    name: sys
{{template "adopt" "acme-operator"}}---
{{template "systemAccount"}}---
{{template "sysuser" "cluster-controller"}}---
{{template "sysuser" "auth-controller"}}---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata:
  name: auth-controller
  namespace: nats-system
spec:
  servers: ["nats://east.nats-system.svc:4222"]
  credentials:
    secretKeyRef:
      name: auth-controller-creds
`)

var credsSecretTemplate = template.Must(template.New("creds").Parse(generated + credsSecret))
