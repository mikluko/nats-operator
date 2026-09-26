// Package lifecycle carries what every JetStream object resource shares:
// the ownership marker in the server object's metadata, the adoption,
// deletion and terminal policies, drift correction on resync, the JetStream
// API requests, and the status conditions that report them. A kind supplies
// an Object that converts its spec to and from the server's JSON config.
package lifecycle
