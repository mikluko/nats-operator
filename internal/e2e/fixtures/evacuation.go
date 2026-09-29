package fixtures

import (
	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// evacuation writes story 11's fixtures into dir.
func evacuation(dir string) error {
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
	return writeTemplate(dir, "00-auth.yaml", evacuationAuth, e)
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

var evacuationAuth = parseFixture("auth", `{{define "account"}}apiVersion: auth.nats.mikluko.io/v1beta1
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
{{template "systemAccount"}}---
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
{{template "connection" "payments"}}`)
