---
title: A supercluster across Kubernetes clusters
weight: 6
---

Two Kubernetes clusters, `east` and `west`, each with its own NATS cluster, joined by gateways into one supercluster with no hub. `east` is the home cluster: the auth controller runs there, holds the signing key, and every account is declared there.

## Trust roots

Identical in both Kubernetes clusters and replicated by GitOps: the trust roots every NATS cluster boots from.

{{< manifest "natsoperatortrust.yaml" >}}

## The home cluster

There is no supercluster resource: each NatsCluster lists the gateways it joins, the same list in every member. The external Service is rendered from a template.

{{< manifest "east.yaml" >}}

The remote cluster's controllers run as system users declared here and carried across by External Secrets or SOPS.

{{< manifest "east-auth.yaml" >}}

## The remote cluster

The same shape, with no auth controller and no signing key. It receives every account's JWT through the resolver.

{{< manifest "west.yaml" >}}

{{< manifest "status-natscluster-west.yaml" >}}
