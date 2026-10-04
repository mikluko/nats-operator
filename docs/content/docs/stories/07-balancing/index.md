---
title: Balance JetStream leaders and copies
weight: 7
params:
  e2e:
    waits:
      - {step: 1, wait: 3m, reason: "the balancers settle after story 4's streams move"}
    after: 4
---

This guide shows you how to keep the leaders and the copies of streams spread evenly over the servers of a NATS cluster.
The manifests create a `NatsSystemBalancer` that balances every account of the NATS cluster `demo`, and a `NatsBalancer` that balances the account `payments` in pools of its own streams.

## Before you begin

You need:

- The NATS cluster `demo` and its NATS operator from [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}), with the Secret `jetstream-controller-creds`.
- The account `payments`, and the `NatsConnection` `demo` in the namespace `payments`, from [Let a team declare its own users and streams]({{< relref "/docs/stories/04-team-self-service" >}}).

## Balance every account of the NATS cluster

As the platform team, apply a `NatsConnection` with the creds of the system account user `jetstream-controller`, and a `NatsSystemBalancer` on it.
A NATS cluster has at most one `NatsSystemBalancer`.

{{< manifest "01-system.yaml" >}}

- `moves.placement` lets the balancer move copies between servers. It is off unless you set it.
- `moves.leader` lets the balancer move leaders, in the accounts that have the `jetstream-stepdown` export.
- `interval` is the time between passes. A pass makes at most one move.

A balancer keeps a stream inside its NATS cluster.
To move a stream to another NATS cluster, see [Move a stream to another NATS cluster]({{< relref "/docs/stories/08-stream-transfer" >}}).
[NatsSystemBalancerSpec]({{< relref "/docs/reference/api#NatsSystemBalancerSpec" >}}) in the API reference lists every field.

## Let the system balancer move the leaders of an account

The system balancer can make a placement move in any account.
It makes a leader move only in an account that exports the preset `jetstream-stepdown`.
Apply the account `payments` with that export added:

{{< manifest "01-account-export.yaml" >}}

The manifest repeats the limits that the account already has.
If you leave them out, the account loses them.
[jetstream-stepdown]({{< relref "/docs/reference/nats-permissions#jetstream-stepdown" >}}) in the NATS permissions reference lists the subjects of the preset.

## Check the system balancer

Read its status:

```sh
kubectl -n nats-system get natssystembalancer demo -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natssystembalancer.yaml" >}}

- `capabilities.leader` is Partial, because the account `orders` has no `jetstream-stepdown` export. The balancer leaves the leaders of `orders` where they are.
- `servers` counts the leaders and the copies on each server, and `skew` is how uneven they are.
  The NATS cluster has three servers, and each of its three streams has three copies, so every server has a copy of every stream and the balancer can move only leaders.
- `lastMove` is absent until the balancer has made a move.

## Balance the streams of an account in pools

As the payments team, apply a `NatsBalancer` on the team's own connection, and a stream with the label that the pool `requests` selects:

{{< manifest "01-account.yaml" >}}

The balancer evens the leaders in each pool separately.
A pool selects `NatsStream`, `NatsKeyValue` and `NatsObjectStore` resources in the namespace of the balancer by label, and the consumers of a stream belong to the pool of the stream.

- A stream that no pool selects, or that has no resource, is in the default pool.
- A stream that several pools select is in the first of them, and the condition `Overlapping` is True.
- If you declare no pools, the whole account is one pool.

[NatsBalancerSpec]({{< relref "/docs/reference/api#NatsBalancerSpec" >}}) in the API reference lists every field.

## Check the account balancer

Read its status:

```sh
kubectl -n payments get natsbalancer payments -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsbalancer.yaml" >}}

`pools` counts the streams of each pool, and `leaderSkew` is how uneven their leaders are.
`REQ_07` is in the pool `requests`, and `PAYMENTS` has no pool label, so it is in the default pool.
A pool of one stream has a `leaderSkew` of 1.

The account balancer yields to the system balancer.
While the system balancer has a move pending on a stream of the account, `Holding` is True with the reason `YieldingToSystemBalancer`, and the account balancer moves nothing.
Neither balancer moves anything while the NATS cluster is not Settled.
[Balancing]({{< relref "/docs/design/v1#65-balancing" >}}) in the design describes how the two balancers work together.
