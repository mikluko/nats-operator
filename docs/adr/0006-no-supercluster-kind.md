# No supercluster resource; gateways are a property of NatsCluster

Each `NatsCluster` carries `gateway.remotes` and `gateway.discovery`, as nats-server's own gateway config does, and the supercluster is what those lists form. A `NatsSupercluster` kind was drafted and dropped: it created nothing and only replicated a member list, which GitOps already replicates, and its name read like something that runs. The same list sits in every member and is kept alike outside the API.
