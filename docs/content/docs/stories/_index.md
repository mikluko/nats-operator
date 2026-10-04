---
title: Stories
weight: 2
---

The stories are the guides to the controllers, one task each, with the manifests that do it.
Each manifest uses the resource API, and the [API reference]({{< relref "/docs/reference/api" >}}) lists every kind and field.

If you are new to the controllers, start with [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}), a tutorial that deploys a NATS cluster with JetStream and creates a stream on it.

The other stories are how-to guides:

- The auth plane: [put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}), [let a team declare its own users and streams]({{< relref "/docs/stories/04-team-self-service" >}}), and [share a stream and a service between accounts]({{< relref "/docs/stories/05-account-wiring" >}}).
- JetStream: [manage JetStream on a NATS cluster that you did not deploy]({{< relref "/docs/stories/03-unmanaged" >}}), [balance leaders and copies]({{< relref "/docs/stories/07-balancing" >}}), [move a stream to another NATS cluster]({{< relref "/docs/stories/08-stream-transfer" >}}), and [retire a NATS cluster by moving every stream off it]({{< relref "/docs/stories/11-evacuation" >}}).
- Several NATS clusters: [build a supercluster across Kubernetes clusters]({{< relref "/docs/stories/06-supercluster" >}}), [deploy a production supercluster of three NATS clusters]({{< relref "/docs/stories/09-acceptance" >}}), [join an edge NATS cluster to a hub as a leaf]({{< relref "/docs/stories/10-leafnodes" >}}), and [add a NATS cluster to a supercluster that you did not deploy]({{< relref "/docs/stories/13-join-supercluster" >}}).
- Metrics: [scrape the controllers' metrics over verified TLS]({{< relref "/docs/stories/12-metrics" >}}).

## Following a story

1. Create the namespaces that the manifests use, `nats-system` and, in some stories, `orders`, `payments` or `monitoring`:

   ```sh
   for ns in nats-system orders payments monitoring; do kubectl create namespace "$ns"; done
   ```

   In a story that spans Kubernetes clusters, create them in each one.

1. Apply each manifest with the `kubectl` command that follows it on the page.
   In a story that spans Kubernetes clusters, the command sets the kubeconfig context of its Kubernetes cluster with `--context`.

1. Compare what `kubectl get -o yaml` shows with the status file on the page.
   A value that differs from one Kubernetes cluster to the next is an example.

## Running the stories

To run the stories end to end on kind clusters from a checkout of the repository, run `just e2e`.
[`hack/e2e`](https://github.com/mikluko/nats-operator/tree/main/hack/e2e) describes what the harness needs.
