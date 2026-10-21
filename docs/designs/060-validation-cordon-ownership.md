# ADR-060: Behavior — Preserve Cordon Ownership During Validation

Extended by [ADR-063](063-continuous-cordon-ownership.md) for overlapping requests and ownership claim lifetime.

## Context

Fault quarantine records whether a node was cordoned before a fault. Validation currently removes cordon without this information.

## Decision

Post-remediation validation can release only a cordon owned by NVSentinel. A pre-existing cordon remains after validation succeeds.

This deployment enables the behavior with `schedulingGate.cordon.onlyOwned`. The default preserves compatibility.

## Implementation

- Fault quarantine derives ownership during its version-protected node update.
- It records `owned`, `pre-existing`, or `unknown` in the ValidationRequest annotation `nvsentinel.nvidia.com/cordon-ownership`.
- The request's Node owner reference binds this information to the Node UID.
- Lifecycle manager persists the Node UID, request UID, and ownership in its validation session.
- A decision to preserve cordon remains until the session ends, including across controller restarts.
- Final release checks the current node, successful validation, matching identities, ownership, and absence of quarantine.
- Fault quarantine's opt-in `validation.persistHandoff` retains the validation source annotation until lifecycle manager registers the request.
- A request carries the source annotation's SHA-256. Lifecycle manager consumes the matching source and registers the session in one version-protected Node update.
- An unconsumed source blocks release. A newer fault's source remains until its own request registers.
- Deleting a request removes its session association. With `onlyOwned`, deletion does not authorize scheduling-gate release.
- Missing or conflicting ownership information preserves cordon.
- In this mode, the same ownership check gates removal of configured validation taints.

## Rationale

Repair eligibility and permission to resume scheduling are separate decisions. Kubernetes cordon itself has no ownership field.

## Consequences

### Positive

- Existing maintenance decisions survive automatic GPU repair.
- Controller restarts retain the release decision.
- All GPU nodes can receive repair without an additional maintenance taint.

### Negative

- Requests from older producers have no ownership grant.
- Overlapping sessions with ambiguous ownership can retain cordon after successful validation.

### Mitigations

- The behavior is opt-in.
- Upgrade lifecycle manager before enabling persistent handoff in fault quarantine. Drain pending handoffs before reverting lifecycle manager.
- Operators can review retained cordons and release them explicitly.

## Alternatives Considered

### Additional maintenance taint

Rejected because it adds a second scheduling restriction that operators must remove separately.

### Infer ownership from audit labels

Rejected because historical labels can outlive the operation they describe.

## Notes

An operator who cordons an already cordoned node does not change its Kubernetes state. Such intent requires explicit coordination.

## References

- [Node Validation](049-node-validation.md)
