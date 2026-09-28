package main

import (
	"fmt"
	"io"
	"text/template"
	"time"

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
	systemJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys}, op, time.Now())
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

var home = template.Must(template.New("home").Parse(generated + `apiVersion: v1
kind: Secret
metadata:
  name: acme-operator-keys
  namespace: nats-system
stringData:
  identity: {{.OperatorIdentity}}
  signing-1: {{.OperatorSigning}}
---
apiVersion: v1
kind: Secret
metadata:
  name: sys-keys
  namespace: nats-system
stringData:
  identity: {{.SystemIdentity}}
  signing-1: {{.SystemSigning}}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata:
  name: acme
  namespace: nats-system
spec:
  systemAccountRef:
    name: sys
  keys:
    identity:
      secretKeyRef: {name: acme-operator-keys, key: identity}
    signing:
      - name: signing-1
        secretKeyRef: {name: acme-operator-keys, key: signing-1}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata:
  name: sys
  namespace: nats-system
spec:
  operatorRef:
    name: acme
  keys:
    identity:
      secretKeyRef: {name: sys-keys, key: identity}
    signing:
      - name: signing-1
        secretKeyRef: {name: sys-keys, key: signing-1}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata:
  name: cluster-controller
  namespace: nats-system
spec:
  accountRef:
    kind: NatsSystemAccount
    name: sys
  preset: cluster-controller
  credentials:
    secretKeyRef:
      name: cluster-controller-creds
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata:
  name: auth-controller
  namespace: nats-system
spec:
  accountRef:
    kind: NatsSystemAccount
    name: sys
  preset: auth-controller
  credentials:
    secretKeyRef:
      name: auth-controller-creds
---
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
`))

var west = template.Must(template.New("west").Parse(generated + credsSecret))
