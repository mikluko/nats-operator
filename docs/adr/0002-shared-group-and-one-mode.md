# No controller reads another's group; shared kinds live in nats.mikluko.io

Every kind two controllers need (`NatsConnection`, `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust`) lives in `nats.mikluko.io`, and no controller reads another controller's group. The JetStream controller therefore has one mode: it reaches every NATS cluster, managed or not, through a `NatsConnection` whose credentials decide the account, and never reads `NatsCluster` or `NatsAccount`. Kind-tagged references into other groups (`clusterRef: {kind: NatsCluster}`, `accountRef`) and an auth controller that minted JetStream credentials were tried in the stories and dropped for this.

## Consequences

A balancer cannot see a rollout; it pauses per step through its own Settled gate instead of for the whole rollout.
