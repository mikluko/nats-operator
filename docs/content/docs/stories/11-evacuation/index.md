---
title: Retiring a NATS cluster
weight: 11
params:
  e2e:
    waits:
      - {step: 1, wait: 4m, reason: "six servers start before streams move off prod-east"}
    substitutions:
      - files: [01-natscluster-prod-east.yaml, 01-evacuation.yaml]
        kind: NatsCluster
        reason: six servers share one kind node, on a host every Kubernetes cluster of the run shares
        patch:
          spec:
            replicas: 3
            resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}
      - files: [01-natscluster-prod-east.yaml]
        kind: NatsCluster
        name: prod-east
        reason: >-
          two members of story 9's supercluster in one Kubernetes cluster, with one storage class; a memory store of
          10Gi does not fit a 192Mi server; no cert-manager, so the gateways run without TLS
        patch:
          spec:
            jetstream: {limits: null, volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              tls: null
              service: {annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east.example.net}}
              advertise: nats-prod-east.example.net:7222
      - files: [01-evacuation.yaml]
        kind: NatsCluster
        name: prod-east-2
        reason: >-
          two members of story 9's supercluster in one Kubernetes cluster, with one storage class; a memory store of
          10Gi does not fit a 192Mi server; no cert-manager, so the gateways run without TLS
        patch:
          spec:
            jetstream: {limits: null, volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              tls: null
              service: {annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east-2.example.net}}
              advertise: nats-prod-east-2.example.net:7222
---

The platform team retires the NATS cluster `prod-east`: it rolls out `prod-east-2` beside it in the same supercluster, moves every stream across, and only then deletes `prod-east`.

## The evacuation

The NATS cluster to retire:

{{< manifest "01-natscluster-prod-east.yaml" >}}

Its replacement, with a tag of its own, and one system-level evacuation of the whole of `prod-east`.

{{< manifest "01-evacuation.yaml" >}}

Resources whose own spec pins `prod-east` are left alone and listed; the evacuation is not Ready until their owners move them. A moved stream that no resource owns keeps a config naming `prod-east`, and is listed under `stalePlacement`: while `prod-east` exists, an update that changes that stream's placement moves it back there.

{{< manifest "01-status-natsclusterevacuation.yaml" >}}

## Deleting `prod-east`

Deletion waits while its NATS cluster still holds JetStream data.

{{< manifest "02-delete-natscluster-prod-east.yaml" >}}

{{< manifest "02-status-natscluster-prod-east.yaml" >}}
