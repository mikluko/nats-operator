---
title: Build a supercluster across Kubernetes clusters
weight: 6
params:
  category: Several NATS clusters
  tags: [gateways]
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

This guide shows you how to join NATS clusters in two Kubernetes clusters into one supercluster with gateways.
The manifests deploy the NATS cluster `east` in the Kubernetes cluster `east` and the NATS cluster `west` in the Kubernetes cluster `west`.
`east` is the home cluster: the auth controller runs there, and every account is declared there.

## Before you begin

You need:

- Two Kubernetes clusters, with kubeconfig contexts named `east` and `west`.
  Each manifest on this page is followed by the command that applies it with `--context`.
- In both, the cluster controller and the JetStream controller, cert-manager, the namespace `nats-system`, and LoadBalancer Services that the other Kubernetes cluster can reach.
  [Install]({{< relref "/docs/install" >}}) shows how to install the controllers.
- In `east`, the auth controller, a `NatsOperator` with its system account `sys`, and the cluster controller's user with its creds in the Secret `cluster-controller-creds`.
  [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}) declares them.
- The DNS names `nats-east.example.net` and `nats-west.example.net` pointing at the gateway Services, for example through external-dns.
- The StorageClass `gp3` in both, or your own in its place in the manifests.

## Copy the trust roots to both Kubernetes clusters

Copy the NATS operator JWT and the system account JWT from the status of the `NatsOperator` in `east` into a `NatsOperatorTrust`, and apply the same one in both Kubernetes clusters.
Keep the two copies alike with GitOps, and copy the JWTs again whenever you rotate a key of the NATS operator.

{{< manifest "01-natsoperatortrust.yaml" >}}

## Create the gateway CA

Gateways accept each other by certificate alone.
Give them a private CA: replicate its key pair into the Secret `nats-gateway-ca` in `nats-system` of both Kubernetes clusters, and create an Issuer for it in both:

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: nats-gateway-ca
  namespace: nats-system
spec:
  ca:
    secretName: nats-gateway-ca
```

Do not use a public issuer or an ACME issuer.
[Trust, resolver, gateways, leaf nodes]({{< relref "/docs/design/v1#43-trust-resolver-gateways-leaf-nodes" >}}) in the design explains why neither works.

Do not use a ClusterIssuer either.
A ClusterIssuer gives a gateway certificate, and with it a place in the supercluster, to anyone who can write a `NatsCluster` in any namespace that the cluster controller watches.

## Deploy the NATS cluster in the home cluster

Apply the `NatsCluster` `east`.
Its name is its gateway name.
`gateway.remotes` lists every member of the supercluster, itself included.
Give every member the same list.

{{< manifest "01-east.yaml" >}}

- To render the remotes as seeds and let gossip find the rest of the members, set `gateway.discovery` to `Gossip` in place of `Explicit`.
- `gateway.service` is the template of the external Service, and `gateway.advertise` is the address that the servers advertise.
- A `NatsCluster` whose gateway has no `tls` is refused, unless the cluster controller runs with `--allow-gateway-without-tls`.

A change to the gateway restarts the servers.
[Gateway]({{< relref "/docs/reference/api#Gateway" >}}) in the API reference lists every field.

## Declare the users of the controllers in the other Kubernetes cluster

No auth controller runs in `west`, so declare the users of its controllers in `east`:

{{< manifest "01-east-auth.yaml" >}}

The auth controller writes their creds Secrets in `east`.
Deliver the Secrets `west-cluster-controller-creds` and `west-jetstream-controller-creds` to `nats-system` in `west`, for example with External Secrets or SOPS.

## Deploy the NATS cluster in the other Kubernetes cluster

Apply the `NatsCluster` `west`.
It refers to the same `NatsOperatorTrust` and lists the same gateway remotes as `east`, and its controller connects with the creds from `east`.
`west` receives the JWT of every account through its resolver.

{{< manifest "01-west.yaml" >}}

## Check the gateways

Read the status of `west`:

```sh
kubectl --context west -n nats-system get natscluster west -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-west.yaml" >}}

`GatewaysConnected` is True once every remote member is connected.
