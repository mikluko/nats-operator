// Package jwtplane builds and signs the auth plane's JWTs: the NATS operator,
// the system account, accounts, users and activation tokens.
//
// No function reaches a NATS server or the Kubernetes API. The NATS operator
// JWT is signed with the NATS operator's identity key and everything else with
// a signing key, so a Kubernetes cluster needs to hold signing seeds only.
package jwtplane
