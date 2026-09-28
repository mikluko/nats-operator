package main

import (
	"fmt"
	"io"
	"text/template"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// leafnodes writes story 10's fixtures: in e2e/00-hub.yaml, the hub's auth
// plane, a NATS operator acme, system account sys and account telemetry
// adopting generated keys, the trust object the hub reads, and the system
// users the hub and the auth controller run as; in e2e/00-edge.yaml, the
// leaf creds each edge site's connections read, signed by the accounts'
// signing keys, as External Secrets would carry them there. It prints the
// literal JWTs the edge's NatsOperatorTrust and NatsAccountTrust take, as
// the patches of substitutions.
func leafnodes(dir string, out io.Writer) error {
	var h hub
	op, opSeeds, err := keys(nkeys.PrefixByteOperator)
	if err != nil {
		return err
	}
	sys, sysSeeds, err := keys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	tel, telSeeds, err := keys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	h.OperatorIdentity, h.OperatorSigning = opSeeds[0], opSeeds[1]
	h.SystemIdentity, h.SystemSigning = sysSeeds[0], sysSeeds[1]
	h.TelemetryIdentity, h.TelemetrySigning = telSeeds[0], telSeeds[1]
	sysPub, err := sys.Identity.PublicKey()
	if err != nil {
		return err
	}
	telPub, err := tel.Identity.PublicKey()
	if err != nil {
		return err
	}
	now := time.Now()
	operatorJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "acme", Keys: op, SystemAccount: sysPub})
	if err != nil {
		return err
	}
	systemJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys}, op, now)
	if err != nil {
		return err
	}
	telemetryJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: tel, NoExpiry: true}, op, now)
	if err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-hub.yaml", hubTemplate, h); err != nil {
		return err
	}
	leaf := []string{jwt.ConnectionTypeLeafnode}
	creds := []struct {
		secret  string
		user    jwtplane.User
		account jwtplane.Keys
	}{
		{"edge-site-1-leaf-creds", jwtplane.User{Name: "edge-site-1", AllowedConnectionTypes: leaf, Permissions: &jwtplane.Permissions{
			Publish:   jwtplane.SubjectPermissions{Allow: []string{"telemetry.>"}},
			Subscribe: jwtplane.SubjectPermissions{Allow: []string{"telemetry.>", "_INBOX.>"}},
		}}, tel},
		{"edge-site-2-system-leaf-creds", jwtplane.User{Name: "edge-site-2-system", SystemAccount: true, Preset: jwtplane.PresetLeafnode}, sys},
		{"edge-site-2-leaf-creds", jwtplane.User{Name: "edge-site-2", AllowedConnectionTypes: leaf}, tel},
	}
	var edge []secret
	for _, c := range creds {
		lines, err := userCreds(c.user, c.account)
		if err != nil {
			return fmt.Errorf("%s: %w", c.secret, err)
		}
		edge = append(edge, secret{Name: c.secret, Namespace: "nats-system", Lines: lines})
	}
	if err := writeTemplate(dir, "00-edge.yaml", edgeTemplate, edge); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "NatsOperatorTrust acme:\n  patch: {spec: {operatorJWT: %s, systemAccountJWT: %s}}\nNatsAccountTrust telemetry:\n  patch: {spec: {publicKey: %s, jwt: %s}}\n",
		operatorJWT, systemJWT, telPub, telemetryJWT)
	return err
}

// hub is what hubTemplate renders: seeds.
type hub struct {
	OperatorIdentity, OperatorSigning   string
	SystemIdentity, SystemSigning       string
	TelemetryIdentity, TelemetrySigning string
}

var edgeTemplate = template.Must(template.New("edge").Parse(generated +
	`{{range $i, $s := .}}{{if $i}}---
{{end}}{{template "secret" $s}}{{end}}{{define "secret"}}` + credsSecret + `{{end}}`))

var hubTemplate = template.Must(template.New("hub").Parse(generated + `apiVersion: v1
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
apiVersion: v1
kind: Secret
metadata:
  name: telemetry-keys
  namespace: nats-system
stringData:
  identity: {{.TelemetryIdentity}}
  signing-1: {{.TelemetrySigning}}
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
# The edge preloads this account's JWT, which therefore never expires.
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata:
  name: telemetry
  namespace: nats-system
spec:
  operatorRef:
    name: acme
  jwtTTL: 0s
  keys:
    identity:
      secretKeyRef: {name: telemetry-keys, key: identity}
    signing:
      - name: signing-1
        secretKeyRef: {name: telemetry-keys, key: signing-1}
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsOperatorTrust
metadata:
  name: acme
  namespace: nats-system
spec:
  operatorRef:
    name: acme
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
  servers: ["nats://prod-east.nats-system.svc:4222"]
  credentials:
    secretKeyRef:
      name: auth-controller-creds
`))
