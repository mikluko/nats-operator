# nats-operator

Installs the CRDs of all four API groups under `nats.mikluko.io` and any subset of three controllers: the cluster controller deploys and rolls NATS clusters, the auth controller mints and distributes their auth plane, and the JetStream controller manages and balances JetStream.

Prerequisites, values, upgrade and uninstall: [Install](https://mikluko.github.io/nats-operator/docs/install/).

Each controller has its own switch; the CRDs are installed whatever they are set to.

| Value               | Default |
|---------------------|---------|
| `cluster.enabled`   | `true`  |
| `auth.enabled`      | `true`  |
| `jetstream.enabled` | `true`  |
