// Package auth holds the auth controller's reconcilers: NatsOperator,
// NatsSystemAccount and NatsAccount, which mint or adopt keys and sign their
// JWTs, NatsUser, which signs users and revokes them on deletion, and the
// reference forms of NatsOperatorTrust and NatsAccountTrust,
// whose status mirrors the JWTs they name.
//
// Seeds are read from and generated into Secrets only; status carries public
// keys, JWTs and the names of generated seed Secrets. Every reference that
// crosses a namespace is admitted through internal/grant on each reconcile.
package auth
