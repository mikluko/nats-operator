---
title: Stories
---

The resource API, told as user stories. Each page is the API section of the design (`docs/design/v1.md`) for the part it covers; no controller reads these manifests yet.

Each story is a page bundle: this narrative, and beside it the manifests as real YAML files that the page renders.

API groups, all `v1beta1`, every kind namespaced:

| group | kinds |
|---|---|
| `nats.mikluko.io` | `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust`, `NatsConnection` |
| `cluster.nats.mikluko.io` | `NatsCluster` |
| `auth.nats.mikluko.io` | `NatsOperator`, `NatsSystemAccount`, `NatsAccount`, `NatsUser` |
| `jetstream.nats.mikluko.io` | `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsBalancer`, `NatsSystemBalancer`, `NatsClusterEvacuation` |

A `status-*.yaml` file is the `status` stanza `kubectl get -o yaml` would show for the object it names.


