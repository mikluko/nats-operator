---
title: Leaf nodes at the edge
weight: 10
params:
  e2e:
    clusters:
      - name: hub
        files: [e2e/00-hub.yaml, 01-hub.yaml]
      - name: edge
        files: [e2e/00-edge.yaml, 01-edge.yaml, 01-edge-operator.yaml, 01-status-natscluster-edge-site-1.yaml]
    substitutions:
      - files: [01-edge-operator.yaml]
        kind: NatsOperatorTrust
        reason: the JWTs of the NATS operator and system account e2e/00-hub.yaml adopts, from hack/e2e-fixtures
        patch: {spec: {operatorJWT: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJCSlUzSjVQQTVRUEFPQ0hVWDVITVhWVFFKWE9PRkFRNUxKRFNYN0ZHRzVXVFg3TVlQSE5RIiwiaWF0IjoxNzkwNDU4MDYxLCJpc3MiOiJPQlBWSExNWk40WjY0T09IR0tHN0VOWDJKWUxWTDRYVEpKSTJEMkJGV0JITTRRTkRHT0JaSVFHVyIsIm5hbWUiOiJhY21lIiwic3ViIjoiT0JQVkhMTVpONFo2NE9PSEdLRzdFTlgySllMVkw0WFRKSkkyRDJCRldCSE00UU5ER09CWklRR1ciLCJuYXRzIjp7InNpZ25pbmdfa2V5cyI6WyJPQ0I2TFY0WktMUk5UWUtLTk9IMzdZVjNHNTNNTExIWkhJTFVSU1BVRlRPT1ZCMjVBRTM3SDc2UyJdLCJzeXN0ZW1fYWNjb3VudCI6IkFDWk5WVzZDUDZJWE1UMk81Uk40RUZDTTNXSDJCV0FWVlpWQUdRQ0FXWk1CNDI2NFkzNFpOQlZKIiwidHlwZSI6Im9wZXJhdG9yIiwidmVyc2lvbiI6Mn19.q6BXu2N_tLyQ1qOKnQq2Ne4kvPmYjunLmcsNapb6L3uQ5-8oE264DXBeYqcBOMCPXIPXqh3dRk9c0ekFx78NCg, systemAccountJWT: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJBS1NDSUY1RFY0SDZGVUU3TzQ1SFpZQ0NZUDZTU1pZS0xSSFJQQkJPUUNZV0FKS1pUUDNRIiwiaWF0IjoxNzkwNDU4MDYxLCJpc3MiOiJPQ0I2TFY0WktMUk5UWUtLTk9IMzdZVjNHNTNNTExIWkhJTFVSU1BVRlRPT1ZCMjVBRTM3SDc2UyIsIm5hbWUiOiJzeXMiLCJzdWIiOiJBQ1pOVlc2Q1A2SVhNVDJPNVJONEVGQ00zV0gyQldBVlZaVkFHUUNBV1pNQjQyNjRZMzRaTkJWSiIsIm5hdHMiOnsibGltaXRzIjp7InN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwid2lsZGNhcmRzIjp0cnVlLCJjb25uIjotMSwibGVhZiI6LTF9LCJzaWduaW5nX2tleXMiOlsiQUQ1TDNWQllVU0pBWFpSQ1FaQU5BWEFNUlBBUU1IREJOTkxSRzJFTVNUU0xISE5WS0dMRklSVkIiXSwiZGVmYXVsdF9wZXJtaXNzaW9ucyI6eyJwdWIiOnt9LCJzdWIiOnt9fSwiYXV0aG9yaXphdGlvbiI6e30sInR5cGUiOiJhY2NvdW50IiwidmVyc2lvbiI6Mn19.yuSrlhge4kobuSLcSJREBE7CTf7-rXcCLlCqELvYEPpKZIkewzVM6vnvsrBQMFSyP_NNlMV8i0dxTl8yJTDQCg}}
      - files: [01-edge-operator.yaml]
        kind: NatsAccountTrust
        reason: the key and JWT of the telemetry account e2e/00-hub.yaml adopts, from hack/e2e-fixtures
        patch: {spec: {publicKey: ACN25U6DCEF2KXHQLKZERG774MXE56M7EDEU2655PJAD2F5XWV32N2G2, jwt: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJWSlhJUlhFM05aUElYM1NERlZVSVpMTkJBUkZEQzNPMjVFSFhTRVEzQ0NPRFZFUFBFSExBIiwiaWF0IjoxNzkwNDU4MDYxLCJpc3MiOiJPQ0I2TFY0WktMUk5UWUtLTk9IMzdZVjNHNTNNTExIWkhJTFVSU1BVRlRPT1ZCMjVBRTM3SDc2UyIsIm5hbWUiOiJ0ZWxlbWV0cnkiLCJzdWIiOiJBQ04yNVU2RENFRjJLWEhRTEtaRVJHNzc0TVhFNTZNN0VERVUyNjU1UEpBRDJGNVhXVjMyTjJHMiIsIm5hdHMiOnsibGltaXRzIjp7InN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwid2lsZGNhcmRzIjp0cnVlLCJjb25uIjotMSwibGVhZiI6LTF9LCJzaWduaW5nX2tleXMiOlsiQUE3SUxNSklBV0JCRUdSRkZMWU42Q1pUNVNJS1k1SU1FQk9MUVNaWFhBM1RER1NOTVVQUVRIR1AiXSwiZGVmYXVsdF9wZXJtaXNzaW9ucyI6eyJwdWIiOnt9LCJzdWIiOnt9fSwiYXV0aG9yaXphdGlvbiI6e30sInR5cGUiOiJhY2NvdW50IiwidmVyc2lvbiI6Mn19.523SspTzkXsV099jNd5Cwu_AVCsePj6MH6PAPqPxbyQQoPvynE_Ho9M-CNOBxr_tNswimO8PPh0DRSb1kZk5BQ}}
      - files: [01-hub.yaml, 01-edge.yaml, 01-edge-operator.yaml]
        kind: NatsCluster
        reason: >-
          nats 2.15.1 is not published; hack/e2e.sh loads 2.15.0 as localhost/nats under both tags, which
          the kubelet does not pull. A Kubernetes cluster is one 3G minikube node.
        patch:
          spec:
            image: localhost/nats
            podTemplate: {spec: {containers: [{name: nats, imagePullPolicy: Never}]}}
            resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}
      - files: [01-hub.yaml]
        kind: NatsCluster
        reason: a 3G minikube node holds three servers; no cert-manager, so the leafnode listener runs without TLS
        patch: {spec: {replicas: 3, leafnodes: {tls: null}}}
      - files: [01-edge.yaml]
        kind: NatsConnection
        name: hub
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-system
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-telemetry
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
---

An edge site runs a small NATS cluster in its own Kubernetes cluster. It joins the production hub as a leaf: local clients publish telemetry that reaches the hub's `telemetry` account, and a local stream keeps accepting while the link is down. The leaf is not a supercluster member and has no auth plane of its own.

## The hub

The hub's NatsCluster gains a leafnode listener, and the edge site gets a user in the account its traffic belongs to, usable only as a leaf.

{{< manifest "01-hub.yaml" >}}

## The edge

A leaf is a NatsCluster that dials out through a NatsConnection, the same kind the JetStream controller uses. Its JetStream runs in a domain of its own.

{{< manifest "01-edge.yaml" >}}

{{< manifest "01-status-natscluster-edge-site-1.yaml" >}}

## An edge that enforces the hub's accounts

A second site trusts the hub's NATS operator, so its clients authenticate against the hub's accounts locally. It reads the same trust roots the supercluster members do, and resolves accounts over a second remote bound to the system account; without that remote it cannot fetch an account it has not cached. It preloads the telemetry account so that its clients authenticate while the link is down. A preload is a copy that no fetch replaces, so this leaf runs a `Full` resolver on its JetStream volume, whose sync with the hub keeps the copy current, and the copy expires with the account's `jwtTTL` unless the `NatsAccount` sets `jwtTTL: 0`.

{{< manifest "01-edge-operator.yaml" >}}
