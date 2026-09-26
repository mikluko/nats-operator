---
title: A production supercluster
weight: 9
---

Everything from the earlier stories at once, modelled on a real deployment with its names invented and its five NATS clusters reduced to the three that differ: a larger home cluster, a development cluster pinned to one zone, and a production cluster in another region whose placement tag is not its name. Four services run in two environments, each service an account.

## Trust roots and clusters

The trust roots and the gateway list are the same in every Kubernetes cluster; GitOps keeps them alike.

{{< manifest "natsoperatortrust.yaml" >}}

{{< manifest "clusters.yaml" >}}

## The auth plane

The operator, the system account, and the production account chain: checks exports a service to monitoring, which exports streams and services to core and to the collector.

{{< manifest "auth.yaml" >}}

Every account carries a `service` and a `readonly` user, and every remote cluster two controller users.

{{< manifest "users.yaml" >}}

## JetStream and balancing

The monitoring team adopts the streams its application created, pools them, and balances within its account; the platform team balances each NATS cluster.

{{< manifest "jetstream.yaml" >}}
