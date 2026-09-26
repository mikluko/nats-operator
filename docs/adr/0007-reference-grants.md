# Cross-namespace references are gated by NatsReferenceGrant alone

A reference crosses namespaces only where a `NatsReferenceGrant` in the target namespace admits it, modelled on Gateway API's `ReferenceGrant` and checked on every reconcile so that deleting a grant revokes what it admitted. It replaced both an allow-list field on `NatsAccount` and a requester-RBAC check at admission, which needed a webhook and could not revoke. The name is not `NatsGrant` because NATS already grants permissions and imports.
