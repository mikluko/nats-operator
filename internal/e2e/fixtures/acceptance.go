package fixtures

import (
	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// acceptanceData is what acceptanceTemplate renders.
type acceptanceData struct {
	OperatorIdentity, OperatorSigning     string
	SystemIdentity, SystemSigning         string
	MonitoringIdentity, MonitoringSigning string
	RuntimeCreds                          []string
}

// acceptance writes story 9's fixtures into dir, with the NATS operator and
// system account JWTs its NatsOperatorTrust takes as the patch file
// natsoperatortrust.json.
func acceptance(dir string) error {
	var a acceptanceData
	var op, sys, monitoring jwtplane.Keys
	for _, k := range []struct {
		kind           nkeys.PrefixByte
		identity, sign *string
		keys           *jwtplane.Keys
	}{
		{nkeys.PrefixByteOperator, &a.OperatorIdentity, &a.OperatorSigning, &op},
		{nkeys.PrefixByteAccount, &a.SystemIdentity, &a.SystemSigning, &sys},
		{nkeys.PrefixByteAccount, &a.MonitoringIdentity, &a.MonitoringSigning, &monitoring},
	} {
		keys, seeds, err := keys(k.kind)
		if err != nil {
			return err
		}
		*k.identity, *k.sign, *k.keys = seeds[0], seeds[1], keys
	}
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
	if a.RuntimeCreds, err = userCreds(jwtplane.User{Name: "monitoring-runtime"}, monitoring); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-home.yaml", acceptanceTemplate, a); err != nil {
		return err
	}
	for _, remote := range []string{"dev-east", "prod-west"} {
		lines, err := userCreds(jwtplane.User{Name: remote + "-cluster-controller", SystemAccount: true, Preset: jwtplane.PresetClusterController}, sys)
		if err != nil {
			return err
		}
		if err := writeTemplate(dir, "00-"+remote+".yaml", credsSecretTemplate, secret{Name: remote + "-cluster-controller-creds", Namespace: "nats-system", Lines: lines}); err != nil {
			return err
		}
	}
	return writePatch(dir, "natsoperatortrust.json", map[string]string{"operatorJWT": operatorJWT, "systemAccountJWT": systemJWT})
}

var acceptanceTemplate = parseFixture("acceptance", `{{template "keys" (dict "name" "acme" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
{{template "keys" (dict "name" "sys" "identity" .SystemIdentity "signing" .SystemSigning)}}---
{{template "keys" (dict "name" "monitoring-prod" "identity" .MonitoringIdentity "signing" .MonitoringSigning)}}---
{{template "sysuser" "cluster-controller"}}---
{{template "sysuser" "auth-controller"}}---
{{template "sysuser" "jetstream-controller"}}---
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
---
{{template "secret" (secret "monitoring-runtime-creds" "monitoring" .RuntimeCreds)}}`)
