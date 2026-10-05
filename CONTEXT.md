# nats-operator

Software that runs inside Kubernetes clusters to deploy NATS clusters, join them into a supercluster, own their auth plane, and keep JetStream leadership and placement even.

## Language

### Two words that collide

**Controller**:
One of this project's three separately deployable programs, each owning one API group: the cluster controller, the JetStream controller, and the auth controller.
_Avoid_: operator (unqualified), manager

**Managed NATS cluster**:
A NATS cluster deployed by the cluster controller. The JetStream and auth controllers also act on NATS clusters that are not managed.
_Avoid_: owned cluster, internal cluster

**NATS operator**:
The root of trust in NATS JWT auth: the identity whose signing keys sign account JWTs.
_Avoid_: operator (unqualified), root account

**NATS cluster**:
A set of NATS servers joined by routes and sharing one gateway name.
_Avoid_: cluster (unqualified), deployment

**Kubernetes cluster**:
One Kubernetes control plane and its nodes.
_Avoid_: cluster (unqualified), environment

### Topology

**Supercluster**:
NATS clusters joined by gateways into one JetStream meta group, with no hub above them.
_Avoid_: mesh, federation

**NATS system**:
A supercluster, or a NATS cluster in none: the servers a connection of one system account reaches.
_Avoid_: installation, environment

**Leaf**:
A NATS cluster joined to a hub by leafnode connections, bridging accounts without joining the hub's supercluster or its JetStream meta group.
_Avoid_: edge cluster, satellite, spoke

**Hub**:
The NATS cluster a leaf connects to. Not the home cluster's NATS cluster, which is a hub only if leaves happen to connect to it.
_Avoid_: upstream, parent

**Home cluster**:
The one Kubernetes cluster in a supercluster whose auth controller holds the NATS operator signing key and where accounts are declared.
_Avoid_: primary, hub, control plane

### Resources

**Adopt**:
A resource taking charge of what already exists in NATS, made outside this project: a stream, a consumer or a bucket, or the keys of a NATS operator or an account and the JWT the servers hold for it.
_Avoid_: take over, import

### Auth plane

**Auth plane**:
The NATS operator, its accounts with their limits and cross-account imports and exports, and their users.
_Avoid_: credentials, nsc store

**System account**:
The NATS account through which servers are observed and administered, and the account the controllers' own users belong to.
_Avoid_: SYS user, admin account

**Identity key**:
The key that names a NATS operator or an account; a NATS operator's signs only the NATS operator JWT, and neither needs to be present where signing happens.
_Avoid_: root key, master key

**Signing key**:
A key a NATS operator JWT or an account JWT lists as allowed to sign on its behalf, and the only kind of key a Kubernetes cluster has to hold.
_Avoid_: secondary key

**Trust roots**:
The NATS operator JWT and system account JWT whose signatures a NATS cluster accepts.
_Avoid_: CA, trust bundle, certificates

### Balancing

**Pool**:
A declared group of streams within one account, with their consumers, across which evenness is judged apart from the account's other streams; streams in no declared pool form the account's default pool.
_Avoid_: family, group, shard set, stream set

**Settled**:
The state of a NATS cluster in which every Raft group has a leader and every member of every group is online and current.
_Avoid_: healthy, ready, stable

**Leader move**:
Handing a Raft group's leadership to another of its members; no data moves.
_Avoid_: stepdown, rebalance

**Placement move**:
Changing which servers hold a stream's copies; stream data is copied between servers.
_Avoid_: migration, rebalance

**Evacuation**:
Placement moves of every stream off one NATS cluster to another of the same supercluster, save one whose resource sets a placement cluster, ahead of retiring the first.
_Avoid_: eviction, drain, migration

**System balancer**:
What evens leaders and copies across the servers of one NATS cluster, over every account in it.
_Avoid_: global balancer, cluster balancer

**Account balancer**:
What evens leaders and copies within the pools of one account, yielding to the system balancer.
_Avoid_: balancing policy, balancer config
