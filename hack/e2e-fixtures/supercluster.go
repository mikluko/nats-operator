package main

import (
	"fmt"
	"io"
	"text/template"

	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

type seeds struct {
	OperatorIdentity, OperatorSigning, SystemIdentity, SystemSigning string
	// WestCredsLines is the west creds file, line by line.
	WestCredsLines []string
}

// supercluster writes story 6's fixtures into dir and prints the NATS
// operator and system account JWTs its NatsOperatorTrust takes as a
// substitution's patch.
func supercluster(dir string, out io.Writer) error {
	var s seeds
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
	if err := writeTemplate(dir, "00-home.yaml", home, s); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-west.yaml", west, secret{Name: "west-cluster-controller-creds", Namespace: "nats-system", Lines: s.WestCredsLines}); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "patch: {spec: {operatorJWT: %s, systemAccountJWT: %s}}\n", operatorJWT, systemJWT)
	return err
}

var home = parseFixture("home", `{{template "keys" (dict "name" "acme-operator" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
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

var west = template.Must(template.New("west").Parse(generated + credsSecret))
