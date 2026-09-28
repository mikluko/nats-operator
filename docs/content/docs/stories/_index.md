---
title: Stories
weight: 2
---

The resource API, told as user stories. Each page is the API section of [the design]({{< relref "/docs/design/v1" >}}) for the part it covers.

API groups, all `v1beta1`, every kind namespaced:

| group | kinds |
|---|---|
| `nats.mikluko.io` | `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust`, `NatsConnection` |
| `cluster.nats.mikluko.io` | `NatsCluster` |
| `auth.nats.mikluko.io` | `NatsOperator`, `NatsSystemAccount`, `NatsAccount`, `NatsUser` |
| `jetstream.nats.mikluko.io` | `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsBalancer`, `NatsSystemBalancer`, `NatsClusterEvacuation` |

## Following a story

Every manifest on a story's page is followed by the `kubectl` command that applies it, or deletes what it names; a status file is what `kubectl get -o yaml` shows once the controllers have acted, and values that differ from one Kubernetes cluster to the next are examples. The manifests name their namespaces, `nats-system` and in some stories `orders`, `payments` or `monitoring`, which must exist first:

```sh
for ns in nats-system orders payments monitoring; do kubectl create namespace "$ns"; done
```

A story spanning Kubernetes clusters names each Kubernetes cluster in its commands with `--context`, the kubeconfig context of the one the manifest goes to, and needs the namespaces in each.

## Running the stories

The stories run end to end on kind clusters, from a checkout of the repository, with `just e2e`; [`hack/e2e`](https://github.com/mikluko/nats-operator/tree/main/hack/e2e) says what the harness needs and how a story tells it what to expect.
