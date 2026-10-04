package main

import "github.com/mikluko/nats-operator/internal/jwtplane"

// natsGo marks a call made inside nats.go's jetstream package rather than by
// a subject in this module's code.
const natsGo = "nats.go jetstream"

// identity is one user a controller connects to nats-server as.
type identity struct {
	Heading string
	// Connection is a Markdown sentence naming where the credentials come
	// from.
	Connection string
	// Preset is the NatsUser preset that grants every call; empty where no
	// preset does.
	Preset jwtplane.UserPreset
	Calls  []call
}

// call is one subject a controller publishes a request to. Subject holds
// "*" for each token the controller fills in, and Sources the internal/
// packages whose code carries it, or natsGo.
type call struct {
	Subject string
	Use     string
	Sources []string
}

var identities = []identity{
	{
		Heading:    "Cluster controller",
		Connection: "Connects as the system account user a `NatsCluster` names in `auth.systemCredentials`.",
		Preset:     jwtplane.PresetClusterController,
		Calls: []call{
			{"$SYS.REQ.SERVER.PING.STATSZ", "Lists the servers of the NATS cluster.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.PING.JSZ", "Reads every account's streams, consumers and the meta group, for `Settled`.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.PING.GATEWAYZ", "Reads each server's gateway connections, for `GatewaysConnected`.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.PING.LEAFZ", "Reads each server's leaf connections, for `LeafnodesConnected`.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.*.VARZ", "Reads the configuration a server has loaded, by server ID.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.*.RELOAD", "Reloads a server's configuration, by server ID.", []string{"internal/sysobs"}},
			{"$JS.API.SERVER.EVACUATE", "Moves a server's JetStream assets off it before it is removed.", []string{"internal/sysobs"}},
			{"$JS.API.META.LEADER.STEPDOWN", "Moves the meta leader off a server being removed.", []string{"internal/sysobs"}},
			{"$JS.API.SERVER.REMOVE", "Removes a server from the JetStream meta group.", []string{"internal/sysobs"}},
		},
	},
	{
		Heading:    "Auth controller",
		Connection: "Connects through the `NatsConnection` its `--system-connection` flag names, as a user of a `NatsOperator`'s system account.",
		Preset:     jwtplane.PresetAuthController,
		Calls: []call{
			{"$SYS.REQ.SERVER.PING.STATSZ", "Lists the servers that should answer the requests below.", []string{"internal/authctl"}},
			{"$SYS.REQ.SERVER.PING.VARZ", "Reads the NATS operator JWT each server runs under, for the keys it trusts.", []string{"internal/authctl"}},
			{"$SYS.REQ.CLAIMS.UPDATE", "Pushes an account JWT to every resolver.", []string{"internal/authctl"}},
			{"$SYS.REQ.CLAIMS.DELETE", "Deletes accounts from every `Full` resolver.", []string{"internal/authctl"}},
			{"$SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP", "Reads the JWT the resolvers hold for an account, by account public key.", []string{"internal/authctl"}},
			{"$SYS.REQ.SERVER.PING.CONNZ", "Finds a revoked user's connections.", []string{"internal/authctl"}},
			{"$SYS.REQ.SERVER.*.KICK", "Disconnects one of them, by server ID.", []string{"internal/authctl"}},
		},
	},
	{
		Heading:    "JetStream controller, as a system account user",
		Connection: "Connects through the `NatsConnection` a `NatsSystemBalancer` or `NatsClusterEvacuation` names in `connectionRef`, as a system account user.",
		Preset:     jwtplane.PresetJetStreamController,
		Calls: []call{
			{"$SYS.REQ.SERVER.PING.STATSZ", "Lists the servers of the NATS cluster and their tags.", []string{"internal/sysobs"}},
			{"$SYS.REQ.SERVER.PING.JSZ", "Reads every account's streams and consumers, with their leaders and replicas, and asks the meta leader which servers it counts offline, for a `NatsClusterEvacuation`.", []string{"internal/sysobs", "internal/balancectl"}},
			{"$SYS.REQ.SERVER.*.JSZ", "Asks the server a balancer's connection reaches, by server ID, whether it is in the same NATS system as a `NatsClusterEvacuation`'s connection.", []string{"internal/balancectl"}},
			{"acc.*.$JS.API.STREAM.LEADER.STEPDOWN.*", "Moves a stream leader through the account's `jetstream-stepdown` export, and probes whether the system account imports it.", []string{"internal/balance", "internal/balancectl"}},
			{"acc.*.$JS.API.CONSUMER.LEADER.STEPDOWN.*.*", "Moves a consumer leader the same way.", []string{"internal/balance", "internal/balancectl"}},
			{"$JS.API.ACCOUNT.STREAM.MOVE.*.*", "Moves a stream's copies off a server, or onto servers carrying an evacuation's tags.", []string{"internal/balance"}},
			{"$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.*", "Rolls back the moves in progress when an unfinished `NatsClusterEvacuation` is deleted.", []string{"internal/balance"}},
		},
	},
	{
		Heading: "JetStream controller, as an account user",
		Connection: "Connects through the `NatsConnection` a `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore` or `NatsBalancer` names in `connectionRef`, " +
			"as a user of the account that owns the resources. No preset grants these subjects: the user's `permissions` must allow them and a subscription to `" +
			jwtplane.InboxPrefix(jwtplane.PresetJetStreamController) + ".>`, or the user sets none.",
		Calls: []call{
			{"$JS.API.STREAM.INFO.*", "Reads a stream, or the stream behind a bucket.", []string{"internal/lifecycle"}},
			{"$JS.API.STREAM.CREATE.*", "Creates a `NatsStream`'s stream.", []string{"internal/lifecycle"}},
			{"$JS.API.STREAM.UPDATE.*", "Updates it.", []string{"internal/lifecycle"}},
			{"$JS.API.STREAM.DELETE.*", "Deletes it.", []string{"internal/lifecycle"}},
			{"$JS.API.CONSUMER.INFO.*.*", "Reads a consumer.", []string{"internal/lifecycle"}},
			{"$JS.API.CONSUMER.CREATE.*.*", "Creates or updates a `NatsConsumer`'s consumer.", []string{"internal/lifecycle"}},
			{"$JS.API.CONSUMER.DELETE.*.*", "Deletes it.", []string{"internal/lifecycle"}},
			{"$JS.API.CONSUMER.LIST.*", "Lists a `NatsStream`'s consumers while its stream moves to another NATS cluster.", []string{"internal/streamctl"}},
			{"$JS.API.INFO", "Reads the account's JetStream limits before a key-value bucket is created or updated.", []string{natsGo}},
			{"$JS.API.STREAM.CREATE.*", "Creates the stream behind a `NatsKeyValue` or `NatsObjectStore`.", []string{natsGo}},
			{"$JS.API.STREAM.UPDATE.*", "Updates it.", []string{natsGo}},
			{"$JS.API.STREAM.DELETE.*", "Deletes it.", []string{natsGo}},
			{"$SYS.REQ.USER.INFO", "Reads the account a `NatsBalancer`'s connection belongs to.", []string{"internal/balancectl"}},
			{"$JS.API.STREAM.LIST", "Lists the account's streams, for a `NatsBalancer`.", []string{natsGo}},
			{"$JS.API.STREAM.INFO.*", "Reads a stream, for a `NatsBalancer`.", []string{natsGo}},
			{"$JS.API.CONSUMER.LIST.*", "Lists a stream's consumers, for a `NatsBalancer`.", []string{natsGo}},
			{"$JS.API.STREAM.LEADER.STEPDOWN.*", "Moves a stream leader, for a `NatsBalancer`.", []string{"internal/balance"}},
			{"$JS.API.CONSUMER.LEADER.STEPDOWN.*.*", "Moves a consumer leader, for a `NatsBalancer`.", []string{"internal/balance"}},
			{"$JS.API.ACCOUNT.STREAM.MOVE.*.*", "Moves a stream's copies off a server, for a `NatsBalancer`.", []string{"internal/balance"}},
		},
	},
}
