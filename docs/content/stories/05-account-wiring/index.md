---
title: Wiring accounts together
weight: 5
---

The monitoring account publishes check results and answers execute requests; the core account consumes both. The platform team declares the wiring on the accounts, and the auth controller signs it into both JWTs.

## The exporting account

A stream export and a service export. The service is private: only the accounts it names may import it.

{{< manifest "exporter.yaml" >}}

## The importing account

An import names the export it takes. The subject and type come from the exporting account, so the two sides cannot disagree.

{{< manifest "importer.yaml" >}}

{{< manifest "status-natsaccount-core.yaml" >}}
