package fixtures

import (
	"fmt"
	"time"

	"github.com/nats-io/nkeys"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// joinAccounts are the accounts story 13's existing NATS cluster preloads and
// its NatsAccountTrusts copy, each written to the patch file
// natsaccounttrust-<name>.json.
var joinAccounts = []string{"orders", "payments"}

// joinAccount is one preloaded account of joinData.
type joinAccount struct {
	Name, PublicKey, JWT string
}

// joinData is what joinCentralTemplate renders.
type joinData struct {
	OperatorJWT, SystemPub, SystemJWT string
	Accounts                          []joinAccount
}

// joinCreds is what joinCredsTemplate renders.
type joinCreds struct {
	ClusterController, Orders []string
}

// joinSupercluster writes story 13's fixtures into dir: a NATS cluster,
// central, that none of the controllers deployed, holding its system account
// and accounts as static preloads, with the JWTs the story's trust objects
// take as patch files.
func joinSupercluster(dir string) error {
	op, _, err := keys(nkeys.PrefixByteOperator)
	if err != nil {
		return err
	}
	sys, _, err := keys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	var d joinData
	if d.SystemPub, err = sys.Identity.PublicKey(); err != nil {
		return err
	}
	if d.OperatorJWT, err = jwtplane.SignOperator(jwtplane.Operator{Name: "central", Keys: op, SystemAccount: d.SystemPub}); err != nil {
		return err
	}
	if d.SystemJWT, err = jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: sys}, op); err != nil {
		return err
	}
	var creds joinCreds
	now := time.Now()
	for _, name := range joinAccounts {
		acct, _, err := keys(nkeys.PrefixByteAccount)
		if err != nil {
			return err
		}
		a := joinAccount{Name: name}
		if a.PublicKey, err = acct.Identity.PublicKey(); err != nil {
			return err
		}
		if a.JWT, err = jwtplane.SignAccount(jwtplane.Account{
			Name: name, Keys: acct, Limits: jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{}},
		}, op, now); err != nil {
			return err
		}
		d.Accounts = append(d.Accounts, a)
		if err := writePatch(dir, "natsaccounttrust-"+name+".json", map[string]string{"publicKey": a.PublicKey, "jwt": a.JWT}); err != nil {
			return err
		}
		if name == "orders" {
			if creds.Orders, err = userCreds(jwtplane.User{Name: "orders"}, acct); err != nil {
				return fmt.Errorf("orders-creds: %w", err)
			}
		}
	}
	if creds.ClusterController, err = userCreds(jwtplane.User{
		Name: "west-cluster-controller", SystemAccount: true, Preset: jwtplane.PresetClusterController,
	}, sys); err != nil {
		return fmt.Errorf("west-cluster-controller-creds: %w", err)
	}
	if err := writeTemplate(dir, "00-central.yaml", joinCentralTemplate, d); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-creds.yaml", joinCredsTemplate, creds); err != nil {
		return err
	}
	return writePatch(dir, "natsoperatortrust.json", map[string]string{"operatorJWT": d.OperatorJWT, "systemAccountJWT": d.SystemJWT})
}

var joinCredsTemplate = parseFixture("join-creds", `# Minted by whoever runs central's NATS operator and delivered here by them.
{{template "secret" (secret "west-cluster-controller-creds" "nats-system" .ClusterController)}}---
{{template "secret" (secret "orders-creds" "orders" .Orders)}}`)

var joinCentralTemplate = parseFixture("join-central", `# The existing supercluster's one member, central: a MEMORY resolver
# preloading its system account and accounts, a gateway in the clear that
# admits no gateway its own list does not name, and west named in that list.
apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-config
  namespace: central
data:
  nats.conf: |
    server_name: $POD_NAME
    port: 4222
    http_port: 8222
    cluster {
      name: central
      port: 6222
      routes: [
        nats-route://nats-0.nats-headless.central.svc.cluster.local:6222
        nats-route://nats-1.nats-headless.central.svc.cluster.local:6222
        nats-route://nats-2.nats-headless.central.svc.cluster.local:6222
      ]
    }
    gateway {
      name: central
      port: 7222
      reject_unknown: true
      gateways: [
        {name: west, urls: ["nats://nats-west.example.net:7222"]}
      ]
    }
    jetstream {
      store_dir: /data
      max_memory_store: 64M
      max_file_store: 1G
    }
    operator: {{.OperatorJWT}}
    system_account: {{.SystemPub}}
    resolver: MEMORY
    resolver_preload: {
      {{.SystemPub}}: {{.SystemJWT}}
{{- range .Accounts}}
      {{.PublicKey}}: {{.JWT}}
{{- end}}
    }
---
apiVersion: v1
kind: Service
metadata:
  name: nats-headless
  namespace: central
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app: nats}
  ports:
    - {name: route, port: 6222}
---
apiVersion: v1
kind: Service
metadata:
  name: nats-gateway
  namespace: central
  annotations:
    external-dns.alpha.kubernetes.io/hostname: nats-central.example.net
spec:
  type: LoadBalancer
  selector: {app: nats}
  ports:
    - {name: gateway, port: 7222}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nats
  namespace: central
spec:
  serviceName: nats-headless
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels: {app: nats}
  template:
    metadata:
      labels: {app: nats}
    spec:
      containers:
        - name: nats
          image: nats:2.15.0
          args: ["--config", "/etc/nats/nats.conf"]
          env:
            - name: POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
          resources:
            requests: {cpu: 50m, memory: 128Mi}
            limits: {memory: 128Mi}
          readinessProbe:
            httpGet: {path: /healthz, port: 8222}
          volumeMounts:
            - {name: config, mountPath: /etc/nats}
            - {name: data, mountPath: /data}
      volumes:
        - name: config
          configMap: {name: nats-config}
        - name: data
          emptyDir: {}
`)
