---
title: Metrics over verified TLS
weight: 12
params:
  category: Metrics
  tags: [ServiceMonitor]
  e2e:
    scrapeMetrics: true
---

A platform engineer has Prometheus, run by prometheus-operator in `monitoring`, scrape the controllers' metrics, and wants every scrape to verify the certificate it is served. The controllers are to connect to nothing but the API server, DNS and the NATS clusters in `nats-system`.

## The certificate

Each controller serves its metrics under the certificate in one Secret in the release namespace, which must name every enabled controller's metrics Service. For the release `nats-operator` in `nats-operator`, a `Certificate` from `ca-issuer`, an existing cert-manager CA ClusterIssuer, which writes its CA to the Secret's `ca.crt`:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: nats-operator-metrics
  namespace: nats-operator
spec:
  secretName: nats-operator-metrics-tls
  issuerRef:
    kind: ClusterIssuer
    name: ca-issuer
  dnsNames:
    - nats-operator-cluster-controller-metrics.nats-operator.svc
    - nats-operator-auth-controller-metrics.nats-operator.svc
    - nats-operator-jetstream-controller-metrics.nats-operator.svc
```

## The chart

```yaml
metrics:
  service:
    enabled: true
  tls:
    secretName: nats-operator-metrics-tls
  scraper:
    serviceAccount: monitoring/prometheus
  serviceMonitor:
    enabled: true
networkPolicy:
  enabled: true
  from:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}
  egress:
    - to: [{ipBlock: {cidr: 10.96.0.1/32}}]
      ports: [{protocol: TCP, port: 443}]
    - to: [{ipBlock: {cidr: 172.18.0.2/32}}]
      ports: [{protocol: TCP, port: 6443}]
    - to:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kube-system}}
          podSelector: {matchLabels: {k8s-app: kube-dns}}
      ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
    - to:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: nats-system}}
      ports: [{protocol: TCP, port: 4222}, {protocol: TCP, port: 8222}]
```

Each ServiceMonitor scrapes with the Prometheus pod's token, which `metrics.scraper.serviceAccount` grants `/metrics`, and verifies the certificate against the Secret's `ca.crt` as its Service's DNS name, such as `nats-operator-cluster-controller-metrics.nats-operator.svc`.

The first two egress rules are the API server: the address of the `kubernetes` Service in `default`, and the addresses of its endpoints, which `kubectl -n default get endpointslices -l kubernetes.io/service-name=kubernetes` lists; both addresses above are examples. Port `4222` is the NATS cluster's client port, which a `NatsConnection` below names, and `8222` its servers' monitoring port, which the cluster controller reads.

## Under the policy

A NATS cluster and a stream, reconciled through those rules alone.

{{< manifest "01-natscluster.yaml" >}}

{{< manifest "01-status-natscluster-demo.yaml" >}}

{{< manifest "02-natsconnection.yaml" >}}

{{< manifest "02-natsstream.yaml" >}}

{{< manifest "02-status-natsstream.yaml" >}}

## Outside the policy

The controllers reach nothing the policy omits. A NATS cluster in `nats-outside`, which no egress rule admits, comes up, as the cluster controller deploys it through the API server; a `NatsConnection` to it stays unready, as the JetStream controller's dial is dropped.

{{< manifest "01-natscluster-outside.yaml" >}}

{{< manifest "01-status-natscluster-outside.yaml" >}}

{{< manifest "02-natsconnection-outside.yaml" >}}

{{< manifest "02-status-natsconnection-outside.yaml" >}}

## Scraping by hand

As the ServiceMonitor does, through a port-forward to the cluster controller's metrics Service:

```sh
kubectl -n nats-operator get secret nats-operator-metrics-tls -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
kubectl -n nats-operator port-forward svc/nats-operator-cluster-controller-metrics 8080 &
curl --cacert ca.crt \
  --resolve nats-operator-cluster-controller-metrics.nats-operator.svc:8080:127.0.0.1 \
  -H "Authorization: Bearer $(kubectl -n monitoring create token prometheus)" \
  https://nats-operator-cluster-controller-metrics.nats-operator.svc:8080/metrics
```
