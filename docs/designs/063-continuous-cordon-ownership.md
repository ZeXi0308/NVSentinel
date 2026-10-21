# ADR-063: Behavior — Keep Ownership Through a Continuous Cordon

## Context

A new fault can arrive while a previous fault's validation keeps the same node cordoned. The cordon boolean does not identify its owner. Treating that boolean as a manual cordon prevents automatic release after the new validation succeeds.

## Decision

Fault quarantine and lifecycle manager share an ownership claim for one continuous cordon. Requests refer to that claim through the Kubernetes API. This extends ADR-060's ownership policy for overlapping faults and request deletion.

## Implementation

- The Node annotation `nvsentinel.nvidia.com/cordon-claim` contains a unique ID, the Node UID, and an optional `preserveCordon` decision.
- Fault quarantine creates an owned claim when it acquires cordon. A pre-existing or unknown cordon receives no new owned claim.
- A later fault inherits a matching claim only while a quarantine, pending handoff, or matching owned validation session keeps it active.
- A detached, malformed, preserved, or different-node claim cannot authorize inheritance.
- Fault quarantine captures ownership and claim identity in the same version-protected Node update as the quarantine.
- A ValidationRequest carries `nvsentinel.nvidia.com/cordon-claim-id`. Its session entry retains that ID, Node UID, and request UID.
- Lifecycle manager checks these identities again before release. A legacy request cannot release a newer claim.
- Persistent handoff retains the claim while lifecycle manager registers its successor request.
- Deleting an owned request removes only its association. It never releases the node. Other trusted owned requests retain their release qualification.
- A manual, unknown, or preserved association keeps its preservation decision in remaining associations and the active claim.
- Successful final release removes the claim atomically with the scheduling gate. Removing the last association without a pending successor retires the claim and retains cordon.
- Claim fields are additive. Requests without claims retain the previous ownership checks.

## Rationale

Ownership belongs to the continuous cordon, while validation results belong to individual requests. Durable identities allow both controllers to restart between handoff steps.

## Consequences

### Positive

- Repeated faults can complete validation and return automatically to service.
- Deleting an obsolete owned request leaves a valid successor able to release the node.
- Pre-existing manual cordons remain after successful validation.

### Negative

- An ownership claim adds persistent coordination state to the Node.
- Old sessions without object UIDs cannot prove their release rights.

### Mitigations

- Each claim requires matching identities and an active association.
- Enable strict ownership only after old requests, sessions, quarantines, and pending handoffs finish.
- Deploy the claim-aware consumer before its producer. Drain active claims before reverting either controller.
- Replace the producer without overlapping old and new processes. Older producers cannot honor the shared claim.

## Alternatives Considered

### Promote ownership when lifecycle manager registers a request

Rejected because the previous request can disappear before registration. The request's original ownership would also disagree with its session entry.

### Reuse historical cordon labels

Rejected because historical labels do not identify an active cordon or the current Node object.

### Copy the current UID into an old session

Rejected because a reused name does not prove that the original request or Node still exists.

## Notes

Cordon on an already cordoned node makes no state change. Operators must coordinate new maintenance intent explicitly during active repair.

## References

- [Preserve Cordon Ownership During Validation](060-validation-cordon-ownership.md)
- [Node Validation](049-node-validation.md)
