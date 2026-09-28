# One StatefulSet per NATS server

Each server is its own StatefulSet with `replicas: 1`, its own ConfigMap and an explicit `server_name`, following the official ClickHouse Kubernetes operator's choice of a StatefulSet per replica to avoid version-config mismatch mid-rollout. A single StatefulSet forces a choice between one mutable ConfigMap, which reaches old servers on any reload or crash-restart, and revision-named ConfigMaps, which give up hot reload; it also only ever removes its highest ordinal and cannot change its volume template in place. Per-server StatefulSets give per-server config, removal of a chosen server, and volume replacement one server at a time.

## Considered options

A single StatefulSet with `updateStrategy: OnDelete` (still one ConfigMap and one volume template), partition stepping (fixes the order), and owning pods and PVCs directly (reimplements identity and PVC retention).
