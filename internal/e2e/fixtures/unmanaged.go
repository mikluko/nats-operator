package fixtures

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"text/template"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// fixture is what manifests renders.
type fixture struct {
	OperatorJWT, SystemPub, SystemJWT, AccountPub, AccountJWT string
	Creds, CA, Cert, Key                                      string
}

// unmanaged writes story 3's fixture into dir: a NATS cluster none of the
// controllers deployed.
func unmanaged(dir string) error {
	var f fixture
	op, err := nkeys.CreateOperator()
	if err != nil {
		return err
	}
	sys, err := nkeys.CreateAccount()
	if err != nil {
		return err
	}
	acct, err := nkeys.CreateAccount()
	if err != nil {
		return err
	}
	user, err := nkeys.CreateUser()
	if err != nil {
		return err
	}
	opPub, err := op.PublicKey()
	if err != nil {
		return err
	}
	if f.SystemPub, err = sys.PublicKey(); err != nil {
		return err
	}
	if f.AccountPub, err = acct.PublicKey(); err != nil {
		return err
	}
	userPub, err := user.PublicKey()
	if err != nil {
		return err
	}

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "messaging"
	oc.SystemAccount = f.SystemPub
	if f.OperatorJWT, err = oc.Encode(op); err != nil {
		return err
	}
	sc := jwt.NewAccountClaims(f.SystemPub)
	sc.Name = "SYS"
	if f.SystemJWT, err = sc.Encode(op); err != nil {
		return err
	}
	ac := jwt.NewAccountClaims(f.AccountPub)
	ac.Name = "payments"
	ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	if f.AccountJWT, err = ac.Encode(op); err != nil {
		return err
	}
	uc := jwt.NewUserClaims(userPub)
	uc.Name = "payments"
	uc.IssuerAccount = f.AccountPub
	userJWT, err := uc.Encode(acct)
	if err != nil {
		return err
	}
	seed, err := user.Seed()
	if err != nil {
		return err
	}
	creds, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		return err
	}
	f.Creds = string(creds)
	if f.CA, f.Cert, f.Key, err = certificates(); err != nil {
		return err
	}
	return writeTemplate(dir, "00-messaging.yaml", manifests, f)
}

// certificates returns a CA and a server certificate and key it signs, for
// the client Service's names, valid for a hundred years.
func certificates() (ca, cert, key string, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "messaging-nats-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(100, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return "", "", "", err
	}
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "nats.messaging.svc"},
		DNSNames:     []string{"nats.messaging.svc", "nats.messaging.svc.cluster.local"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(100, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caTmpl, &srvKey.PublicKey, caKey)
	if err != nil {
		return "", "", "", err
	}
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return "", "", "", err
	}
	enc := func(typ string, der []byte) string {
		var b bytes.Buffer
		_ = pem.Encode(&b, &pem.Block{Type: typ, Bytes: der})
		return b.String()
	}
	return enc("CERTIFICATE", caDER), enc("CERTIFICATE", srvDER), enc("EC PRIVATE KEY", keyDER), nil
}

var manifests = template.Must(template.New("").Funcs(template.FuncMap{"indent": indent}).Parse(generated + `apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-config
  namespace: messaging
data:
  nats.conf: |
    server_name: $POD_NAME
    port: 4222
    http_port: 8222
    tls {
      cert_file: /etc/nats-tls/tls.crt
      key_file: /etc/nats-tls/tls.key
    }
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
      max_file_store: 100G
    }
    operator: {{.OperatorJWT}}
    system_account: {{.SystemPub}}
    resolver: MEMORY
    resolver_preload: {
      {{.SystemPub}}: {{.SystemJWT}}
      {{.AccountPub}}: {{.AccountJWT}}
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: nats-tls
  namespace: messaging
stringData:
  tls.crt: |
{{indent 4 .Cert}}
  tls.key: |
{{indent 4 .Key}}
---
apiVersion: v1
kind: Secret
metadata:
  name: nats-client
  namespace: messaging
stringData:
  ca.crt: |
{{indent 4 .CA}}
  nats.creds: |
{{indent 4 .Creds}}
---
apiVersion: v1
kind: Secret
metadata:
  name: messaging-nats-ca
  namespace: payments
stringData:
  ca.crt: |
{{indent 4 .CA}}
---
apiVersion: v1
kind: Secret
metadata:
  name: payments-secrets
  namespace: payments
stringData:
  nats.creds: |
{{indent 4 .Creds}}
---
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
            - {name: tls, mountPath: /etc/nats-tls}
            - {name: data, mountPath: /data}
      volumes:
        - name: config
          configMap: {name: nats-config}
        - name: tls
          secret: {secretName: nats-tls}
        - name: data
          emptyDir: {}
---
# The streams the payments services created at runtime.
apiVersion: batch/v1
kind: Job
metadata:
  name: runtime-streams
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
            - {name: NATS_URL, value: "tls://nats.messaging.svc:4222"}
            - {name: NATS_CREDS, value: /client/nats.creds}
            - {name: NATS_CA, value: /client/ca.crt}
          command:
            - sh
            - -ec
            - |
              nats stream info PAYMENTS >/dev/null 2>&1 ||
                nats stream add PAYMENTS --subjects 'payments.>' --storage file --replicas 3 \
                  --retention limits --max-age 168h --max-bytes=-1 --discard old --defaults
              nats stream info LEDGER >/dev/null 2>&1 ||
                nats stream add LEDGER --subjects 'ledger.>' --storage file --replicas 3 --defaults
          volumeMounts:
            - {name: client, mountPath: /client}
      volumes:
        - name: client
          secret: {secretName: nats-client}
`))

// indent prefixes every line of s with n spaces.
func indent(n int, s string) string {
	pad := bytes.Repeat([]byte(" "), n)
	var out bytes.Buffer
	for _, line := range bytes.SplitAfter([]byte(s), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		out.Write(pad)
		out.Write(line)
	}
	return string(bytes.TrimRight(out.Bytes(), "\n"))
}
