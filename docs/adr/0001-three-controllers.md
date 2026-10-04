# Three controllers, one API group each

The cluster, the auth plane and JetStream are three separately deployable controllers, each owning one API group, so that JetStream and auth management work against NATS clusters this project did not deploy and each part installs, is granted RBAC and is versioned on its own. A group marks one controller's boundary rather than a count of kinds, which is why `cluster.nats-operator.io` holds `NatsCluster` alone and `NatsCluster` was not folded into the shared group.
