---
title: A supercluster across Kubernetes clusters
weight: 6
params:
  e2e:
    skip: needs more than one Kubernetes cluster
---

Two Kubernetes clusters, `east` and `west`, each with its own NATS cluster, joined by gateways into one supercluster with no hub. `east` is the home cluster: the auth controller runs there, holds the signing key, and every account is declared there.

## Trust roots

Identical in both Kubernetes clusters and replicated by GitOps: the trust roots every NATS cluster boots from.

{{< manifest "01-natsoperatortrust.yaml" >}}

## The home cluster

There is no supercluster resource: each NatsCluster lists the gateways it joins, the same list in every member. The external Service is rendered from a template.

{{< manifest "01-east.yaml" >}}

The remote cluster's controllers run as system users declared here and carried across by External Secrets or SOPS.

{{< manifest "01-east-auth.yaml" >}}

## The remote cluster

The same shape, with no auth controller and no signing key. It receives every account's JWT through the resolver.

{{< manifest "01-west.yaml" >}}

{{< manifest "01-status-natscluster-west.yaml" >}}
