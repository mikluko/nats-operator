// Package jwtplane builds and signs the auth plane's JWTs: the NATS operator,
// the system account, accounts, users and activation tokens.
//
// Every function is pure over its inputs: keys arrive as nkeys key pairs and
// specs as plain structs, and nothing here reads Kubernetes or a NATS server.
// Operator JWTs are signed with the operator's identity key; everything else
// is signed with a signing key, never an identity key, so a Kubernetes
// cluster needs to hold signing seeds only.
package jwtplane
