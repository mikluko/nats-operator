# The cluster holds signing keys only; identities may stay offline

A NATS operator's identity key signs only its own JWT, and signing keys listed in the NATS operator and account JWTs sign everything else, including activation tokens. So `NatsOperator.spec.jwt` (signed offline) and an account's `publicKey` replace identity seeds, and a Kubernetes cluster need hold only signing seeds, in Secrets that may be synced from an external store. The cost is that every NATS operator JWT change, which is also a supercluster-wide restart, needs the offline identity to re-sign it.
