---
title: A production supercluster
weight: 9
params:
  e2e:
    waits:
      - {step: 1, wait: 6m, reason: "eleven servers in three Kubernetes clusters start and join by gateways before the accounts are distributed to all of them"}
      - {step: 2, wait: 3m, reason: "the balancers wait for their NATS cluster to settle before their first pass"}
    clusters:
      - name: prod-east
        files: [e2e/00-home.yaml, 01-natsoperatortrust.yaml, 01-prod-east.yaml, 01-auth.yaml, 01-users.yaml, e2e/01-runtime-streams.yaml,
          e2e/01-status-job.yaml, 01-status-natscluster-prod-east.yaml, 01-status-natsaccount-monitoring-prod.yaml, 02-jetstream.yaml,
          02-status-natsstream-requests.yaml, 02-status-natsbalancer.yaml, 02-status-natssystembalancer.yaml]
      - name: dev-east
        files: [e2e/00-dev-east.yaml, 01-natsoperatortrust.yaml, 01-dev-east.yaml, 01-status-natscluster-dev-east.yaml]
      - name: prod-west
        files: [e2e/00-prod-west.yaml, 01-natsoperatortrust.yaml, 01-prod-west.yaml, 01-status-natscluster-prod-west.yaml]
    substitutions:
      - files: [01-natsoperatortrust.yaml]
        reason: the JWTs of the NATS operator and system account whose keys e2e/00-home.yaml holds, from hack/e2e-fixtures
        patch: {spec: {operatorJWT: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJVR1AyWFRZQlM2T1dMQTQ2T1BLT0dQNEZCMjNIT0REQzNUMzZMTVNEQkNBRUlKRU9PWlpBIiwiaWF0IjoxNzkwNTg5NDY5LCJpc3MiOiJPQ09MUVZZVEM3NVhZVFRaWUhRNVpJTk5ZQjJWSE5TQ0tQS0dHVkVDRTRHWUs1N1FHVTNCVkZMSyIsIm5hbWUiOiJhY21lIiwic3ViIjoiT0NPTFFWWVRDNzVYWVRUWllIUTVaSU5OWUIyVkhOU0NLUEtHR1ZFQ0U0R1lLNTdRR1UzQlZGTEsiLCJuYXRzIjp7InNpZ25pbmdfa2V5cyI6WyJPQktJSVg1UVc1MkpJT09TWEdES1dOSUZPUVdVWVE0NEE3R0RKWktGUjRHUkZMWllZM0dXVFhMNyJdLCJzeXN0ZW1fYWNjb3VudCI6IkFDRU1QTk9HNjZUTFRCTVJPU1RGTFBYNUVCMjdIWEJRR1pLVDJDRlRFTjZBREcyUDdXTlZNRUdRIiwidHlwZSI6Im9wZXJhdG9yIiwidmVyc2lvbiI6Mn19.pdRnnTy4oRjJw7Uq3yyBxzvnMTGj-rkYhm9iGdZpnyFnK4AFM2lOVYKeehMzaNZwhd0Pf0t43kjv5CCIU22sAg, systemAccountJWT: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiI1M1k3VkRTU1dDN1hMRVpQSzNPTkZZUjZFR0xBVlQ0QlRWUUFINVJFN1Y2NzdJT0NPWTJRIiwiaWF0IjoxNzkwNTg5NDY5LCJpc3MiOiJPQktJSVg1UVc1MkpJT09TWEdES1dOSUZPUVdVWVE0NEE3R0RKWktGUjRHUkZMWllZM0dXVFhMNyIsIm5hbWUiOiJzeXMiLCJzdWIiOiJBQ0VNUE5PRzY2VExUQk1ST1NURkxQWDVFQjI3SFhCUUdaS1QyQ0ZURU42QURHMlA3V05WTUVHUSIsIm5hdHMiOnsibGltaXRzIjp7InN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwid2lsZGNhcmRzIjp0cnVlLCJjb25uIjotMSwibGVhZiI6LTF9LCJzaWduaW5nX2tleXMiOlsiQUJHWkVIQUpBVkdGWFJFQldQTzNFR09DRjJGU0tGMkRHTjZIVVhBWVpJWlMyUkJJRFVWVVFHNzIiXSwiZGVmYXVsdF9wZXJtaXNzaW9ucyI6eyJwdWIiOnt9LCJzdWIiOnt9fSwiYXV0aG9yaXphdGlvbiI6e30sInR5cGUiOiJhY2NvdW50IiwidmVyc2lvbiI6Mn19.FHqVLGXfEOu-FK7MH7d8d7HqzrhnYSgGHnSPmjoH1_qpEIAb3h4FkDG3jVXdMtxa5iTPsumA4ghMn2nE1TPrBw}}
      - files: [01-auth.yaml]
        kind: NatsOperator
        reason: the keys e2e/00-home.yaml holds, which sign the trust JWTs above
        patch: {spec: {keys: {identity: {secretKeyRef: {name: acme-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: acme-keys, key: signing-1}}]}}}
      - files: [01-auth.yaml]
        kind: NatsSystemAccount
        reason: the keys e2e/00-home.yaml holds, which sign the trust JWTs above and the remote clusters' controller creds
        patch: {spec: {keys: {identity: {secretKeyRef: {name: sys-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: sys-keys, key: signing-1}}]}}}
      - files: [01-auth.yaml]
        kind: NatsAccount
        name: monitoring-prod
        reason: the keys e2e/00-home.yaml holds, which sign the creds of the application creating its streams at runtime
        patch: {spec: {keys: {identity: {secretKeyRef: {name: monitoring-prod-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: monitoring-prod-keys, key: signing-1}}]}}}
      - files: [01-prod-east.yaml, 01-dev-east.yaml, 01-prod-west.yaml]
        reason: >-
          eleven servers share three kind nodes on one host of 6 CPUs and 12G, with one storage class; no cert-manager, so the
          gateways run without TLS
        patch:
          spec:
            resources: {requests: {cpu: 50m, memory: 192Mi}, limits: {memory: 192Mi}}
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats.prod-east.acme.example:7222"}
                - {name: dev-east, url: "nats://nats.dev-east.acme.example:7222"}
                - {name: prod-west, url: "nats://nats.prod-west.acme.example:7222"}
              tls: null
      - files: [01-prod-east.yaml]
        reason: a memory store of 10Gi does not fit a 192Mi server
        patch: {spec: {jetstream: {limits: {maxMemoryStore: 64Mi}}}}
      - files: [01-dev-east.yaml]
        reason: the kind node carries no zone label
        patch: {spec: {podTemplate: {spec: {nodeSelector: null}}}}
---

Everything from the earlier stories at once, modelled on a real deployment with its names invented and its five NATS clusters reduced to the three that differ: a larger home cluster, a development cluster pinned to one zone, and a production cluster in another region whose placement tag is not its name. Four services run in two environments, each service an account.

## Trust roots and clusters

The trust roots and the gateway list are the same in every Kubernetes cluster; GitOps keeps them alike.

{{< manifest "01-natsoperatortrust.yaml" >}}

{{< manifest "01-prod-east.yaml" >}}

{{< manifest "01-dev-east.yaml" >}}

{{< manifest "01-prod-west.yaml" >}}

Each NatsCluster reports every other member's gateways connected.

{{< manifest "01-status-natscluster-prod-west.yaml" >}}

## The auth plane

The NATS operator, the system account, and the production account chain: checks exports a service to monitoring, which exports streams and services to core and to the collector. The development chain repeats it under `-dev` names and is left out, so the page shows each account's wiring once.

{{< manifest "01-auth.yaml" >}}

Every account reaches every server of the supercluster through the resolver.

{{< manifest "01-status-natsaccount-monitoring-prod.yaml" >}}

Every account carries a `service` and a `readonly` user, and every remote cluster two controller users.

{{< manifest "01-users.yaml" >}}

## JetStream and balancing

The monitoring team adopts the streams its application created, pools them, and balances within its account; the platform team balances each NATS cluster.

{{< manifest "02-jetstream.yaml" >}}

{{< manifest "02-status-natsstream-requests.yaml" >}}

{{< manifest "02-status-natsbalancer.yaml" >}}

{{< manifest "02-status-natssystembalancer.yaml" >}}
