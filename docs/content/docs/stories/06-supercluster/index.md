---
title: A supercluster across Kubernetes clusters
weight: 6
params:
  e2e:
    waits:
      - {step: 1, wait: 3m, reason: "the gateways connect once both NATS clusters' servers are up"}
    clusters:
      - name: east
        files: [e2e/00-home.yaml, 01-natsoperatortrust.yaml, 01-east.yaml, 01-east-auth.yaml]
      - name: west
        files: [e2e/00-west.yaml, 01-natsoperatortrust.yaml, 01-west.yaml, 01-status-natscluster-west.yaml]
    substitutions:
      - files: [01-natsoperatortrust.yaml]
        reason: the JWTs of the NATS operator and system account e2e/00-home.yaml adopts, from internal/e2e/fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-east.yaml, 01-west.yaml]
        reason: >-
          three servers share one kind node, on a host every kind cluster of the run shares, with one storage class; no cert-manager, so the gateways
          run without TLS
        patch:
          spec:
            resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 5Gi}}}}}
            gateway:
              remotes:
                - {name: east, url: "nats://nats-east.example.net:7222"}
                - {name: west, url: "nats://nats-west.example.net:7222"}
              tls: null
---

Two Kubernetes clusters, `east` and `west`, each with its own NATS cluster, joined by gateways into one supercluster with no hub. `east` is the home cluster: the auth controller runs there, holds the signing key, and every account is declared there.

## Trust roots

Identical in both Kubernetes clusters and replicated by GitOps: the trust roots every NATS cluster boots from.

{{< manifest "01-natsoperatortrust.yaml" >}}

The gateways take their certificates from a private CA whose key pair every member's cert-manager holds, replicated the same way: gateways authenticate each other by certificate alone, so a public issuer would admit any certificate it signs, and the cluster controller holds the servers until the certificate Secret carries `ca.crt`, which an ACME issuer never writes.

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: nats-gateway-ca
spec:
  ca:
    secretName: nats-gateway-ca   # in cert-manager's own namespace
```

## The home cluster

There is no supercluster resource: each NatsCluster lists the gateways it joins, the same list in every member. The external Service is rendered from a template.

{{< manifest "01-east.yaml" >}}

The remote Kubernetes cluster's controllers run as system users declared here and carried across by External Secrets or SOPS.

{{< manifest "01-east-auth.yaml" >}}

## The remote Kubernetes cluster

The same shape, with no auth controller and no signing key. It receives every account's JWT through the resolver.

{{< manifest "01-west.yaml" >}}

{{< manifest "01-status-natscluster-west.yaml" >}}
