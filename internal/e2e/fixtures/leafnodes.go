package fixtures

import (
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// leafnodes writes story 10's fixtures into dir, with the JWTs the leaf's
// NatsOperatorTrust and NatsAccountTrust take as the patch files
// natsoperatortrust.json and natsaccounttrust.json.
func leafnodes(dir string) error {
	var h leafnodesData
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
	systemJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys}, op)
	if err != nil {
		return err
	}
	telemetryJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: tel, NoExpiry: true}, op, now)
	if err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-hub.yaml", leafnodesHubTemplate, h); err != nil {
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
	if err := writeTemplate(dir, "00-edge.yaml", leafnodesEdgeTemplate, edge); err != nil {
		return err
	}
	if err := writePatch(dir, "natsoperatortrust.json", map[string]string{"operatorJWT": operatorJWT, "systemAccountJWT": systemJWT}); err != nil {
		return err
	}
	return writePatch(dir, "natsaccounttrust.json", map[string]string{"publicKey": telPub, "jwt": telemetryJWT})
}

// leafnodesData is what leafnodesHubTemplate renders.
type leafnodesData struct {
	OperatorIdentity, OperatorSigning   string
	SystemIdentity, SystemSigning       string
	TelemetryIdentity, TelemetrySigning string
}

var leafnodesEdgeTemplate = parseFixture("leafnodes-edge", `{{range $i, $s := .}}{{if $i}}---
{{end}}{{template "secret" $s}}{{end}}`)

var leafnodesHubTemplate = parseFixture("leafnodes-hub", `{{template "keys" (dict "name" "acme-operator" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
{{template "keys" (dict "name" "sys" "identity" .SystemIdentity "signing" .SystemSigning)}}---
{{template "keys" (dict "name" "telemetry" "identity" .TelemetryIdentity "signing" .TelemetrySigning)}}---
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
{{template "adopt" "telemetry"}}---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsOperatorTrust
metadata:
  name: acme
  namespace: nats-system
spec:
  operatorRef:
    name: acme
---
{{template "sysuser" "cluster-controller"}}---
{{template "sysuser" "auth-controller"}}---
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
`)
