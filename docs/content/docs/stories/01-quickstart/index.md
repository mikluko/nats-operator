---
title: A NATS cluster with JetStream
weight: 1
params:
  tutorial: true
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
      - files: [02-natscluster-8gi.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 512Mi}, limits: {memory: 512Mi}}}}
      - files: [01-status-natscluster-at-rest.yaml]
        reason: max_memory_store derives from the substituted 256Mi limit
        patch: {status: {jetstream: {limits: {maxMemoryStore: 192Mi}}}}
---

In this tutorial you deploy a three-server NATS cluster with JetStream on one Kubernetes cluster.
You then restart its servers with more memory, create a stream and publish a message to it.

## Before you begin

You need:

- A Kubernetes cluster with the cluster controller and the JetStream controller installed. [Install]({{< relref "/docs/install" >}}) shows how.
- Nodes with 3 CPUs and 24Gi of memory that pods can still request. Each of the three servers requests 1 CPU, and 8Gi of memory by the end.
- A StorageClass named `standard`. A kind cluster has one.
- The `kubectl` tool, with its current context set to that Kubernetes cluster.
- The [NATS CLI](https://github.com/nats-io/natscli).

Each manifest on this page is followed by the command that applies it.
[Following a story]({{< relref "/docs/stories#following-a-story" >}}) says more about the manifests and the status files.

## Create the namespace

Every object in this tutorial is in the namespace `nats-system`.
Create it:

```sh
kubectl create namespace nats-system
```

The output is:

```text
namespace/nats-system created
```

## Deploy the NATS cluster

Apply the `NatsCluster` named `demo`.
It asks for three servers, each with a 20Gi volume for JetStream.

{{< manifest "01-natscluster.yaml" >}}

The output is:

```text
natscluster.cluster.nats.mikluko.io/demo created
```

Wait until the NATS cluster is ready:

```sh
kubectl -n nats-system wait --for=condition=Ready natscluster/demo --timeout=5m
```

The output is:

```text
natscluster.cluster.nats.mikluko.io/demo condition met
```

List the pods:

```sh
kubectl -n nats-system get pods
```

The output is similar to this:

```text
NAME       READY   STATUS    RESTARTS   AGE
demo-0-0   2/2     Running   0          26s
demo-1-0   2/2     Running   0          26s
demo-2-0   2/2     Running   0          26s
```

Notice the names.
The cluster controller creates one StatefulSet for each server, so the pod of the server `demo-0` is `demo-0-0`.

If a pod stays `Pending`, the nodes do not have the CPU or the memory that the manifest requests.

Now look at what the `NatsCluster` reports:

```sh
kubectl -n nats-system get natscluster demo -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-at-rest.yaml" >}}

Notice that `Settled` is True.
Every Raft group has a leader, and every member of every group is current.

Notice `endpoints.client` as well.
It is the address that clients connect to, and you use it when you create the stream.

## Raise the memory limit

Apply the same `NatsCluster` with its memory request and limit raised from 4Gi to 8Gi.

{{< manifest "02-natscluster-8gi.yaml" >}}

The output is:

```text
natscluster.cluster.nats.mikluko.io/demo configured
```

A pod gets a new memory limit only when it restarts.
The cluster controller restarts one server at a time, and waits for `Settled` before it restarts the next.
[Rollout]({{< relref "/docs/design/v1#44-rollout" >}}) in the design has the details.

Look at the `NatsCluster` again while the servers restart:

```sh
kubectl -n nats-system get natscluster demo -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natscluster-mid-rollout.yaml" >}}

Notice `rollout`.
It lists the servers that have restarted, the server that is restarting and the servers still to restart.
Yours differs with the moment you look.

Wait until the last server has restarted:

```sh
kubectl -n nats-system wait --for=condition=Progressing=false natscluster/demo --timeout=10m
```

The output is:

```text
natscluster.cluster.nats.mikluko.io/demo condition met
```

Check the memory limit of a server:

```sh
kubectl -n nats-system get pod demo-0-0 -o jsonpath='{.spec.containers[0].resources.limits.memory}{"\n"}'
```

The output is:

```text
8Gi
```

## Create a stream

The JetStream controller connects to a NATS cluster through a `NatsConnection`.
Apply one that has the client address from the status of the `NatsCluster`.

{{< manifest "03-natsconnection.yaml" >}}

The output is:

```text
natsconnection.nats.mikluko.io/demo created
```

Apply the `NatsStream`.
It declares the stream `ORDERS` on the subjects `orders.>`, with three replicas.

{{< manifest "03-natsstream.yaml" >}}

The output is:

```text
natsstream.jetstream.nats.mikluko.io/orders created
```

The manifest sets `adoptionPolicy`, `deletionPolicy` and `terminalPolicy` to their defaults.
[NatsStreamSpec]({{< relref "/docs/reference/api#NatsStreamSpec" >}}) in the API reference lists the values of each.

Look at what the `NatsStream` reports:

```sh
kubectl -n nats-system get natsstream orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "03-status-natsstream.yaml" >}}

Notice that `Ready` and `Synced` are True.
The stream exists on the servers and matches the manifest.
Your stream is empty, so `server.bytes` is 0 and `server.messages` is absent.

## Publish a message

1. Open a second terminal.
   In it, forward the client port of the NATS cluster to your machine, and leave the command running:

   ```sh
   kubectl -n nats-system port-forward svc/demo 4222:4222
   ```

   The output is similar to this:

   ```text
   Forwarding from 127.0.0.1:4222 -> 4222
   ```

1. In the first terminal, publish a message on a subject of the stream:

   ```sh
   nats -s nats://localhost:4222 pub orders.created '{"id": 1}'
   ```

   The output is similar to this:

   ```text
   11:54:45 Published 9 bytes to "orders.created"
   ```

1. Read the state of the stream:

   ```sh
   nats -s nats://localhost:4222 stream info ORDERS
   ```

   The output ends with the state of the stream, similar to this:

   ```text
   State:

                         Host Version: 2.15.0
                   Required API Level: 0 hosted at level 5
                             Messages: 1
                                Bytes: 53 B
                       First Sequence: 1 @ 2026-10-04 11:54:45
                        Last Sequence: 1 @ 2026-10-04 11:54:45
                     Active Consumers: 0
                   Number of Subjects: 1
   ```

   Notice `Messages: 1`.
   The stream stored the message that you published.

You have built a three-server NATS cluster with JetStream, restarted it with more memory, and declared a stream on it as a Kubernetes resource.

## Clean up

1. Remove the stream from the servers.
   Deleting a `NatsStream` leaves its stream in place, and the cluster controller does not delete a NATS cluster that has a stream.

   ```sh
   nats -s nats://localhost:4222 stream rm ORDERS -f
   ```

   The command prints nothing.

1. In the second terminal, press Ctrl+C to stop the port-forward.

1. Delete the three resources, and then the namespace:

   ```sh
   kubectl -n nats-system delete natsstream orders
   kubectl -n nats-system delete natsconnection demo
   kubectl -n nats-system delete natscluster demo
   kubectl delete namespace nats-system
   ```

   The output is:

   ```text
   natsstream.jetstream.nats.mikluko.io "orders" deleted from nats-system namespace
   natsconnection.nats.mikluko.io "demo" deleted from nats-system namespace
   natscluster.cluster.nats.mikluko.io "demo" deleted from nats-system namespace
   namespace "nats-system" deleted
   ```

   Deleting the namespace also deletes the three volumes, which outlast the `NatsCluster`.

To do the tutorial again, start from [Create the namespace](#create-the-namespace).

## What's next

- This NATS cluster has no auth plane, so every client is in the global account. [Owning the auth plane]({{< relref "/docs/stories/02-auth-plane" >}}) puts a NATS cluster under a NATS operator, with accounts and users declared as resources.
- [Stories]({{< relref "/docs/stories" >}}) lists the guides for the other tasks.
- The [API reference]({{< relref "/docs/reference/api#NatsCluster" >}}) has every field of `NatsCluster`, `NatsConnection` and `NatsStream`.
