package fixtures

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// takeoverData is what takeoverMessagingTemplate and takeoverSeedsTemplate
// render.
type takeoverData struct {
	OperatorJWT, SystemPub, SystemJWT, OrdersJWT string
	// Operator, System and Orders are each an identity seed and a signing
	// seed, in that order.
	Operator, System, Orders [2]string
	// Sys, SysOld, App, Worker and Old are creds files, line by line.
	Sys, SysOld, App, Worker, Old []string
}

// takeover writes story 14's fixtures into dir: a NATS cluster with a full
// resolver whose NATS operator, system account, account and users have the
// claims nsc gives them, the system account and the account each revoking
// one user, the seeds and the sys user's creds the story hands
// to the auth controller, and the public keys its status files must show as
// patch files.
func takeover(dir string) error {
	var d takeoverData
	op, err := nscKeys(nkeys.PrefixByteOperator)
	if err != nil {
		return err
	}
	sys, err := nscKeys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	orders, err := nscKeys(nkeys.PrefixByteAccount)
	if err != nil {
		return err
	}
	d.Operator, d.System, d.Orders = op.seeds, sys.seeds, orders.seeds
	d.SystemPub = sys.identityPub

	oc := jwt.NewOperatorClaims(op.identityPub)
	oc.Name = "acme"
	oc.SigningKeys.Add(op.signingPub)
	oc.SystemAccount = sys.identityPub
	if d.OperatorJWT, err = oc.Encode(op.identity); err != nil {
		return err
	}

	if d.Sys, _, err = nscUser("sys", sys, sys.signing); err != nil {
		return err
	}
	var sysOldPub string
	if d.SysOld, sysOldPub, err = nscUser("sys-old", sys, sys.signing); err != nil {
		return err
	}
	revokedAt := time.Now().Truncate(time.Second)
	sc := jwt.NewAccountClaims(sys.identityPub)
	sc.Name = "SYS"
	sc.SigningKeys.Add(sys.signingPub)
	sc.Exports = jwt.Exports{
		{Name: "account-monitoring-services", Subject: "$SYS.REQ.ACCOUNT.*.*", Type: jwt.Service, ResponseType: jwt.ResponseTypeStream, AccountTokenPosition: 4},
		{Name: "account-monitoring-streams", Subject: "$SYS.ACCOUNT.*.>", Type: jwt.Stream, AccountTokenPosition: 3},
	}
	sc.RevokeAt(sysOldPub, revokedAt)
	if d.SystemJWT, err = sc.Encode(op.signing); err != nil {
		return err
	}

	if d.App, _, err = nscUser("orders-app", orders, orders.identity); err != nil {
		return err
	}
	if d.Worker, _, err = nscUser("orders-worker", orders, orders.signing); err != nil {
		return err
	}
	var oldPub string
	if d.Old, oldPub, err = nscUser("orders-old", orders, orders.signing); err != nil {
		return err
	}
	ac := jwt.NewAccountClaims(orders.identityPub)
	ac.Name = "orders"
	ac.SigningKeys.Add(orders.signingPub)
	ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: 10 << 30, Streams: 10, Consumer: 100}
	ac.Exports = jwt.Exports{{Name: "orders-events", Subject: "orders.events.>", Type: jwt.Stream}}
	ac.RevokeAt(oldPub, revokedAt)
	if d.OrdersJWT, err = ac.Encode(op.signing); err != nil {
		return err
	}

	if err := writeTemplate(dir, "00-messaging.yaml", takeoverMessagingTemplate, d); err != nil {
		return err
	}
	if err := writeTemplate(dir, "00-seeds.yaml", takeoverSeedsTemplate, d); err != nil {
		return err
	}
	if err := writeStatusPatch(dir, "natsoperator-status.json", map[string]any{
		"publicKey":     op.identityPub,
		"signingKeys":   []string{op.signingPub},
		"systemAccount": map[string]string{"publicKey": sys.identityPub},
	}); err != nil {
		return err
	}
	if err := writeStatusPatch(dir, "natssystemaccount-status.json", map[string]any{
		"publicKey": sys.identityPub,
		"revocations": []map[string]any{
			{"publicKey": sysOldPub, "at": revokedAt.UTC().Format(time.RFC3339), "issuers": []string{sys.signingPub}},
		},
	}); err != nil {
		return err
	}
	issuers := []string{orders.identityPub, orders.signingPub}
	slices.Sort(issuers)
	return writeStatusPatch(dir, "natsaccount-status.json", map[string]any{
		"publicKey": orders.identityPub,
		"revocations": []map[string]any{
			{"publicKey": oldPub, "at": revokedAt.UTC().Format(time.RFC3339), "issuers": issuers},
		},
	})
}

// nscKeyPairs is an identity key and one signing key of a NATS operator or
// an account, as nsc generates them, with their seeds in that order.
type nscKeyPairs struct {
	identity, signing       nkeys.KeyPair
	identityPub, signingPub string
	seeds                   [2]string
}

// nscKeys generates the identity key and signing key of kind.
func nscKeys(kind nkeys.PrefixByte) (nscKeyPairs, error) {
	var k nscKeyPairs
	pairs := [2]*nkeys.KeyPair{&k.identity, &k.signing}
	pubs := [2]*string{&k.identityPub, &k.signingPub}
	for i := range pairs {
		kp, err := nkeys.CreatePair(kind)
		if err != nil {
			return k, err
		}
		if *pubs[i], err = kp.PublicKey(); err != nil {
			return k, err
		}
		seed, err := kp.Seed()
		if err != nil {
			return k, err
		}
		*pairs[i], k.seeds[i] = kp, string(seed)
	}
	return k, nil
}

// nscUser signs a new user name of account with signer, the account's
// identity key or its signing key, and returns its creds file line by line
// and its public key.
func nscUser(name string, account nscKeyPairs, signer nkeys.KeyPair) ([]string, string, error) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, "", err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, "", err
	}
	uc := jwt.NewUserClaims(pub)
	uc.Name = name
	if signer != account.identity {
		uc.IssuerAccount = account.identityPub
	}
	token, err := uc.Encode(signer)
	if err != nil {
		return nil, "", err
	}
	seed, err := kp.Seed()
	if err != nil {
		return nil, "", err
	}
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		return nil, "", err
	}
	return strings.Split(strings.TrimRight(string(creds), "\n"), "\n"), pub, nil
}

// writeStatusPatch writes status as the JSON merge patch {"status": status}
// to dir/name.
func writeStatusPatch(dir, name string, status map[string]any) error {
	raw, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), append(raw, '\n'), 0o600)
}

var takeoverSeedsTemplate = parseFixture("takeover-seeds", `# The seeds nsc keeps in its keystore, and the creds of the sys user it made.
{{template "keys" (dict "name" "acme-operator" "identity" (index .Operator 0) "signing" (index .Operator 1))}}---
{{template "keys" (dict "name" "sys" "identity" (index .System 0) "signing" (index .System 1))}}---
{{template "keys" (dict "name" "orders" "identity" (index .Orders 0) "signing" (index .Orders 1))}}---
{{template "secret" (secret "auth-controller-creds" "nats-system" .Sys)}}`)

var takeoverMessagingTemplate = parseFixture("takeover-messaging", `# The existing NATS cluster: the config nsc generates for a full resolver,
# preloading the system account alone, and a Job that pushes the account
# orders as nsc push does, creates a stream in it and checks that its
# revoked user and the system account's are refused.
apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-config
  namespace: messaging
data:
  nats.conf: |
    server_name: $POD_NAME
    port: 4222
    http_port: 8222
    cluster {
      name: messaging
      port: 6222
      routes: [
        nats-route://nats-0.nats-headless.messaging.svc.cluster.local:6222
        nats-route://nats-1.nats-headless.messaging.svc.cluster.local:6222
        nats-route://nats-2.nats-headless.messaging.svc.cluster.local:6222
      ]
    }
    jetstream {
      store_dir: /data
      max_memory_store: 64M
      max_file_store: 1G
    }
    operator: {{.OperatorJWT}}
    system_account: {{.SystemPub}}
    resolver {
      type: full
      dir: /data/jwt
      allow_delete: false
      interval: "2m"
      timeout: "1.9s"
    }
    resolver_preload: {
      {{.SystemPub}}: {{.SystemJWT}}
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: nsc
  namespace: messaging
stringData:
  orders.jwt: {{.OrdersJWT}}
  sys.creds: |
{{range .Sys}}{{if .}}    {{.}}{{end}}
{{end}}  sys-old.creds: |
{{range .SysOld}}{{if .}}    {{.}}{{end}}
{{end}}  orders-app.creds: |
{{range .App}}{{if .}}    {{.}}{{end}}
{{end}}  orders-worker.creds: |
{{range .Worker}}{{if .}}    {{.}}{{end}}
{{end}}  orders-old.creds: |
{{range .Old}}{{if .}}    {{.}}{{end}}
{{end}}---
apiVersion: v1
kind: Service
metadata:
  name: nats
  namespace: messaging
spec:
  selector: {app: nats}
  ports:
    - {name: client, port: 4222}
---
apiVersion: v1
kind: Service
metadata:
  name: nats-headless
  namespace: messaging
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app: nats}
  ports:
    - {name: route, port: 6222}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nats
  namespace: messaging
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
---
apiVersion: batch/v1
kind: Job
metadata:
  name: nsc-push
  namespace: messaging
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: nats
          image: natsio/nats-box:0.20.0
          env:
            - {name: NATS_URL, value: "nats://nats.messaging.svc:4222"}
          command:
            - sh
            - -ec
            - |
              pushed=$(nats --creds /nsc/sys.creds req --replies 3 --timeout 5s '$SYS.REQ.CLAIMS.UPDATE' "$(cat /nsc/orders.jwt)" | grep -o '"code":200' | wc -l)
              [ "$pushed" -eq 3 ]
              nats --creds /nsc/orders-app.creds stream info ORDERS >/dev/null 2>&1 ||
                nats --creds /nsc/orders-app.creds stream add ORDERS --subjects 'orders.>' --storage file --replicas 3 --defaults
              nats --creds /nsc/orders-worker.creds pub orders.created '{"id": 1}'
              if refused=$(nats --creds /nsc/orders-old.creds pub orders.created '{"id": 0}' 2>&1); then
                echo "the revoked user published"
                exit 1
              fi
              echo "$refused" | grep -qi 'authorization violation'
              if refused=$(nats --creds /nsc/sys-old.creds pub orders.created '{"id": 0}' 2>&1); then
                echo "the revoked user of the system account published"
                exit 1
              fi
              echo "$refused" | grep -qi 'authorization violation'
          volumeMounts:
            - {name: nsc, mountPath: /nsc}
      volumes:
        - name: nsc
          secret: {secretName: nsc}
`)
