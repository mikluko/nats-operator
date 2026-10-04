---
title: Scrape the controllers' metrics over verified TLS
weight: 12
params:
  e2e:
    scrapeMetrics: true
---

This guide shows you how to have Prometheus scrape the metrics of the controllers and verify the certificate of every scrape.
It also shows you how to limit the controllers to connections to the API server, DNS and the NATS clusters in `nats-system`.

## Before you begin

You need:

- The chart installed as the release `nats-operator` in the namespace `nats-operator`.
  [Install]({{< relref "/docs/install" >}}) shows how.
- Prometheus, run by prometheus-operator in the namespace `monitoring` under the ServiceAccount `prometheus`.
- cert-manager, and a CA ClusterIssuer named `ca-issuer`.
- The namespaces `nats-system` and `nats-outside`, and a StorageClass named `standard`.

## Issue the certificate

Every controller serves its metrics with the certificate in one Secret in the namespace of the release.
The certificate must have the DNS name of the metrics Service of every controller that is enabled.
Apply a `Certificate` from `ca-issuer`, which also writes its CA to `ca.crt` in the Secret:

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

## Set the chart values

Upgrade the release with these values:

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

The first two egress rules are for the API server, and both addresses are examples.
Replace them with the addresses of your Kubernetes cluster:

1. For the first rule, read the address of the Service `kubernetes`:

   ```sh
   kubectl -n default get service kubernetes
   ```

1. For the second rule, read the addresses of its endpoints:

   ```sh
   kubectl -n default get endpointslices -l kubernetes.io/service-name=kubernetes
   ```

The last rule is for the NATS clusters in `nats-system`.
Port 4222 is the client port, which a `NatsConnection` has in `spec.servers`, and port 8222 is the monitoring port of the servers, which the cluster controller reads.

With these values, each ServiceMonitor scrapes with the token of the Prometheus pod, and `metrics.scraper.serviceAccount` allows that ServiceAccount to read `/metrics`.
The ServiceMonitor verifies the certificate against `ca.crt` in the Secret, under the DNS name of the Service, such as `nats-operator-cluster-controller-metrics.nats-operator.svc`.
[Metrics]({{< relref "/docs/reference/chart#metrics" >}}) and [Network policy]({{< relref "/docs/reference/chart#network-policy" >}}) in the chart reference describe the other values for metrics and the network policy.

## Check that the controllers work under the policy

Apply a NATS cluster in `nats-system`:

{{< manifest "01-natscluster.yaml" >}}

Read its status:

```sh
kubectl -n nats-system get natscluster demo -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-demo.yaml" >}}

Apply a connection to the NATS cluster, and a stream on it:

{{< manifest "02-natsconnection.yaml" >}}

{{< manifest "02-natsstream.yaml" >}}

Read the status of the stream:

```sh
kubectl -n nats-system get natsstream events -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsstream.yaml" >}}

## Check that the policy blocks other destinations

No egress rule allows the namespace `nats-outside`.
Apply a NATS cluster there:

{{< manifest "01-natscluster-outside.yaml" >}}

Read its status:

```sh
kubectl -n nats-outside get natscluster outside -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-outside.yaml" >}}

The NATS cluster is Ready, because the cluster controller deploys it through the API server.

Apply a `NatsConnection` to it:

{{< manifest "02-natsconnection-outside.yaml" >}}

Read its status:

```sh
kubectl -n nats-system get natsconnection outside -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsconnection-outside.yaml" >}}

`Ready` stays False with the reason `ConnectFailed`, because the policy drops the connection from the JetStream controller.

## Scrape by hand

To scrape the cluster controller as its ServiceMonitor does:

1. Write the CA to a file:

   ```sh
   kubectl -n nats-operator get secret nats-operator-metrics-tls -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
   ```

1. Forward the port of the metrics Service to your machine:

   ```sh
   kubectl -n nats-operator port-forward svc/nats-operator-cluster-controller-metrics 8080 &
   ```

1. Scrape with a token of the ServiceAccount `prometheus`:

   ```sh
   curl --cacert ca.crt \
     --resolve nats-operator-cluster-controller-metrics.nats-operator.svc:8080:127.0.0.1 \
     -H "Authorization: Bearer $(kubectl -n monitoring create token prometheus)" \
     https://nats-operator-cluster-controller-metrics.nats-operator.svc:8080/metrics
   ```
