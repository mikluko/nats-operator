---
title: Take over a NATS operator and accounts made with nsc
weight: 14
params:
  category: The auth plane
  tags: [keys, adoption]
  e2e:
    waits:
      - {step: 0, wait: 3m, reason: "the existing NATS cluster's servers start, and its accounts are pushed to them"}
      - {step: 1, wait: 2m, reason: "the auth controller finds the servers before it pushes the system account"}
    substitutions:
      - files: [01-status-natsoperator.yaml]
        reason: the public keys of the NATS operator and system account e2e/00-messaging.yaml runs under, from internal/e2e/fixtures
        patchFile: e2e/natsoperator-status.json
      - files: [01-status-natssystemaccount.yaml]
        reason: the public key of the system account e2e/00-messaging.yaml runs under, from internal/e2e/fixtures
        patchFile: e2e/natssystemaccount-status.json
      - files: [02-status-natsaccount.yaml, 03-status-natsaccount.yaml]
        reason: the public key of the account the Job in e2e/00-messaging.yaml pushes, and the user that account revokes, from internal/e2e/fixtures
        patchFile: e2e/natsaccount-status.json
---
