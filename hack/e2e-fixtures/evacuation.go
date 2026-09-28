package main

import (
	"io"
	"text/template"

	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// evacuation writes story 11's fixtures into dir.
func evacuation(dir string, _ io.Writer) error {
	var e evac
	for _, k := range []struct {
		kind           nkeys.PrefixByte
		identity, sign *string
		keys           *jwtplane.Keys
	}{
		{nkeys.PrefixByteOperator, &e.OperatorIdentity, &e.OperatorSigning, nil},
		{nkeys.PrefixByteAccount, &e.SystemIdentity, &e.SystemSigning, nil},
		{nkeys.PrefixByteAccount, &e.OrdersIdentity, &e.OrdersSigning, &e.orders},
		{nkeys.PrefixByteAccount, &e.PaymentsIdentity, &e.PaymentsSigning, &e.payments},
	} {
		keys, seeds, err := keys(k.kind)
		if err != nil {
			return err
		}
		*k.identity, *k.sign = seeds[0], seeds[1]
		if k.keys != nil {
			*k.keys = keys
		}
	}
	var err error
	if e.OrdersCreds, err = userCreds(jwtplane.User{Name: "orders"}, e.orders); err != nil {
		return err
	}
	if e.PaymentsCreds, err = userCreds(jwtplane.User{Name: "payments"}, e.payments); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-auth.yaml", evacuationAuth, e); err != nil {
		return err
	}
	return writeTemplate(dir, "01-workload.yaml", evacuationWorkload, nil)
}

// evac is what evacuationAuth renders.
type evac struct {
	OperatorIdentity, OperatorSigning string
	SystemIdentity, SystemSigning     string
	OrdersIdentity, OrdersSigning     string
	PaymentsIdentity, PaymentsSigning string
	OrdersCreds, PaymentsCreds        []string
	orders, payments                  jwtplane.Keys
}

var evacuationAuth = template.Must(template.New("auth").Funcs(funcs).Parse(generated + `{{define "keys"}}apiVersion: v1
kind: Secret
metadata:
  name: {{.name}}-keys
  namespace: nats-system
stringData:
  identity: {{.identity}}
  signing-1: {{.signing}}
{{end}}{{define "adopt"}}  keys:
    identity:
      secretKeyRef: {name: {{.}}-keys, key: identity}
    signing:
      - name: signing-1
        secretKeyRef: {name: {{.}}-keys, key: signing-1}
{{end}}{{define "sysuser"}}apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata:
  name: {{.}}
  namespace: nats-system
spec:
  accountRef:
    kind: NatsSystemAccount
    name: sys
  preset: {{.}}
  credentials:
    secretKeyRef:
      name: {{.}}-creds
{{end}}{{define "account"}}apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata:
  name: {{.}}
  namespace: nats-system
spec:
  operatorRef:
    name: acme
  limits:
    jetstream:
      memoryStorage: 64Mi
      diskStorage: 1Gi
{{template "adopt" .}}{{end}}{{define "connection"}}apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata:
  name: {{.}}
  namespace: {{.}}
spec:
  servers: ["nats://prod-east.nats-system.svc:4222"]
  credentials:
    secretKeyRef:
      name: {{.}}-creds
{{end}}{{template "keys" (dict "name" "acme" "identity" .OperatorIdentity "signing" .OperatorSigning)}}---
{{template "keys" (dict "name" "sys" "identity" .SystemIdentity "signing" .SystemSigning)}}---
{{template "keys" (dict "name" "orders" "identity" .OrdersIdentity "signing" .OrdersSigning)}}---
{{template "keys" (dict "name" "payments" "identity" .PaymentsIdentity "signing" .PaymentsSigning)}}---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata:
  name: acme
  namespace: nats-system
spec:
  systemAccountRef:
    name: sys
{{template "adopt" "acme"}}---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata:
  name: sys
  namespace: nats-system
spec:
  operatorRef:
    name: acme
{{template "adopt" "sys"}}---
{{template "account" "orders"}}---
{{template "account" "payments"}}---
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
apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata:
  name: prod-east-sys
  namespace: nats-system
spec:
  servers: ["nats://prod-east.nats-system.svc:4222"]
  credentials:
    secretKeyRef:
      name: jetstream-controller-creds
---
{{template "secret" (secret "orders-creds" "orders" .OrdersCreds)}}---
{{template "secret" (secret "payments-creds" "payments" .PaymentsCreds)}}---
{{template "connection" "orders"}}---
{{template "connection" "payments"}}{{define "secret"}}` + credsSecret + `{{end}}`))

var evacuationWorkload = template.Must(template.New("workload").Parse(generated + `apiVersion: jetstream.nats.mikluko.io/v1beta1
kind: NatsStream
metadata:
  name: orders
  namespace: orders
spec:
  connectionRef:
    name: orders
  name: ORDERS
  subjects: ["orders.>"]
  replicas: 3
  placement:
    cluster: prod-east
---
apiVersion: jetstream.nats.mikluko.io/v1beta1
kind: NatsKeyValue
metadata:
  name: sessions
  namespace: payments
spec:
  connectionRef:
    name: payments
  name: sessions
  replicas: 3
  placement:
    cluster: prod-east
---
apiVersion: batch/v1
kind: Job
metadata:
  name: runtime-streams
  namespace: orders
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
              nats stream info audit >/dev/null 2>&1 ||
                nats stream add audit --subjects 'audit.>' --storage file --replicas 3 --cluster prod-east --defaults
              nats stream info EVENTS >/dev/null 2>&1 ||
                nats stream add EVENTS --subjects 'events.>' --storage file --replicas 3 --defaults
          volumeMounts:
            - {name: creds, mountPath: /creds}
      volumes:
        - name: creds
          secret: {secretName: orders-creds}
`))
