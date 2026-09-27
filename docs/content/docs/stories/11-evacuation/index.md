---
title: Retiring a NATS cluster
weight: 11
params:
  e2e:
    substitutions:
      - files: [01-natscluster-prod-east.yaml, 01-evacuation.yaml]
        kind: NatsCluster
        reason: >-
          nats 2.15.1 is not published; hack/e2e.sh loads 2.15.0 as localhost/nats under both tags, which
          the kubelet does not pull. Six servers share one 3G minikube node.
        patch:
          spec:
            image: localhost/nats
            podTemplate: {spec: {containers: [{name: nats, imagePullPolicy: Never}]}}
            replicas: 3
            resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}
      - files: [01-natscluster-prod-east.yaml]
        kind: NatsCluster
        name: prod-east
        reason: >-
          what every member of story 9's supercluster carries, which the story leaves out: JetStream, the
          auth plane and the gateways, in one Kubernetes cluster and without TLS
        patch:
          spec:
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            auth: {trustRef: {name: acme}, systemCredentials: {secretKeyRef: {name: cluster-controller-creds}}}
            gateway:
              discovery: Explicit
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              service:
                type: LoadBalancer
                annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east.example.net}
              advertise: nats-prod-east.example.net:7222
      - files: [01-evacuation.yaml]
        kind: NatsCluster
        name: prod-east-2
        reason: >-
          what every member of story 9's supercluster carries, which the story leaves out: JetStream, the
          auth plane and the gateways, in one Kubernetes cluster and without TLS
        patch:
          spec:
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            auth: {trustRef: {name: acme}, systemCredentials: {secretKeyRef: {name: cluster-controller-creds}}}
            gateway:
              discovery: Explicit
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              service:
                type: LoadBalancer
                annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east-2.example.net}
              advertise: nats-prod-east-2.example.net:7222
---

The infra team replaces `prod-east`: it rolls out `prod-east-2` beside it in the same supercluster, moves every JetStream asset across, and only then deletes the old cluster.

## The evacuation

The old cluster:

{{< manifest "01-natscluster-prod-east.yaml" >}}

A new cluster with a tag of its own, and one system-level evacuation of the whole old cluster.

{{< manifest "01-evacuation.yaml" >}}

Resources whose own spec pins the old cluster are left alone and listed; the evacuation is not Ready until their owners move them. A moved stream that no resource owns keeps a config naming the old cluster, and is listed under `stalePlacement`: while `prod-east` exists, an update that changes that stream's placement moves it back there.

{{< manifest "01-status-natsclusterevacuation.yaml" >}}

## Deleting the old cluster

Deletion waits while the cluster still holds JetStream data.

{{< manifest "02-delete-natscluster-prod-east.yaml" >}}

{{< manifest "02-status-natscluster-prod-east.yaml" >}}
