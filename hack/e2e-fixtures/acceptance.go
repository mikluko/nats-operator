package fixtures

import (
	"text/template"

	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// accept is what acceptanceHome renders.
type accept struct {
	OperatorIdentity, OperatorSigning     string
	SystemIdentity, SystemSigning         string
	MonitoringIdentity, MonitoringSigning string
	RuntimeCreds                          []string
}

// acceptance writes story 9's fixtures into dir, with the NATS operator and
// system account JWTs its NatsOperatorTrust takes as the patch file
// natsoperatortrust.json.
func acceptance(dir string) error {
	var a accept
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
	if err := writeTemplate(dir, "00-home.yaml", acceptanceHome, a); err != nil {
		return err
	}
	for _, remote := range []string{"dev-east", "prod-west"} {
		lines, err := userCreds(jwtplane.User{Name: remote + "-cluster-controller", SystemAccount: true, Preset: jwtplane.PresetClusterController}, sys)
		if err != nil {
			return err
		}
		if err := writeTemplate(dir, "00-"+remote+".yaml", west, secret{Name: remote + "-cluster-controller-creds", Namespace: "nats-system", Lines: lines}); err != nil {
			return err
		}
	}
	if err := writeTemplate(dir, "01-runtime-streams.yaml", acceptanceRuntime, nil); err != nil {
		return err
	}
	return writePatch(dir, "natsoperatortrust.json", map[string]string{"operatorJWT": operatorJWT, "systemAccountJWT": systemJWT})
}

var acceptanceHome = parseFixture("home", `{{template "keys" (dict "name" "acme" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
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

var acceptanceRuntime = template.Must(template.New("runtime").Parse(generated + `apiVersion: batch/v1
kind: Job
metadata:
  name: runtime-streams
  namespace: monitoring
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: nats
          image: natsio/nats-box:0.20.0
          env:
            - {name: NATS_URL, value: "nats://prod-east.nats-system.svc:4222"}
            - {name: NATS_CREDS, value: /creds/user.creds}
          command:
            - sh
            - -ec
            - |
              for s in REQUESTS RESPONSES; do
                until nats stream info "$s" >/dev/null 2>&1 ||
                  nats stream add "$s" --subjects "$(echo "$s" | tr A-Z a-z).>" --storage file --replicas 3 --defaults; do
                  sleep 5
                done
              done
          volumeMounts:
            - {name: creds, mountPath: /creds}
      volumes:
        - name: creds
          secret: {secretName: monitoring-runtime-creds}
`))
