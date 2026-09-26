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

## Steps and expectations

`just e2e` runs the stories against a Kubernetes cluster. Every file name starts with a step number, and a step applies its manifests, deletes the objects its `NN-delete-*.yaml` files name, then waits until the live objects match its expectations:

- `NN-status-<kind>[-<qualifier>].yaml` is the `status` stanza `kubectl get -o yaml` would show for the object it names: the story's only object of that kind, or else the one the qualifier names.
- `NN-live-<kind>[-<qualifier>].yaml` is the whole object as `kubectl get -o yaml` shows it.

A live object matches when it holds every field the file states. Conditions are matched by type, and only their status is compared. A value tagged `!any` is an example: it differs from run to run, and any value there matches.

A story's front matter may set `params.e2e.after`, the number of the story whose end state it starts from, and `params.e2e.skip`, why the harness does not run it.


