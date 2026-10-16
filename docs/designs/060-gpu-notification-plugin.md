# ADR-060: Integration — Independent GPU Notification Plugin

## Context

The operator has approved moving the existing GPU notification worker into this NVSentinel checkout. Its former location in ClusterBoard coupled its build to an unrelated application repository. The worker observes node isolation, workload drain progress, and circuit-breaker status. It records Slack delivery state in a separate MongoDB collection.

## Decision

Keep the worker as an independently built plugin under `plugins/gpu-notifier/`. Preserve its behavior and persistent identifiers while removing ClusterBoard imports. This is an approved local deployment choice; it is not an accepted upstream NVIDIA feature proposal.

## Implementation

- Keep the `app.gpunotify` entry point, environment variables, collection name, and record keys compatible with the existing pilot.
- Package the HTTP client and MongoDB driver with the plugin. Keep Slack transport and owner lookup inside the plugin.
- Retain the independent Helm release, read-only Kubernetes permissions, and collection-scoped MongoDB account.
- Read Node, Pod, and circuit-breaker state. Send notifications through Slack's API.
- Build and test the plugin independently. Preserve the existing synthetic replay test across process restarts.

## Rationale

The notification worker belongs with node fault operations. Independent packaging allows it to change without rebuilding remediation components. Existing MongoDB records let the moved worker continue the same Slack threads.

## Consequences

### Positive

- Building the worker no longer needs ClusterBoard source or repository write access.
- Fault processing continues independently of notification delivery.
- Existing pilot state remains readable after migration.

### Negative

- The plugin owns its Slack transport and user lookup code.
- The pilot uses polling and can miss transitions between observations.
- The plugin requires MongoDB for notification state, including when NVSentinel uses another event-store backend.

### Mitigations

- Keep transport, delivery retries, and state compatibility covered by tests.
- Report observed progress only. Full incident history remains separate work.
- Keep notification state isolated from NVSentinel's event collections.

## Alternatives Considered

### Continue building inside ClusterBoard

Rejected for this deployment because the notifier has an independent runtime and ownership boundary.

### Put Slack delivery inside fault-quarantine

Rejected because external messaging failures should not affect fault-control loops.

### Replace the event exporter

Rejected because event export and user notifications serve different purposes. This plugin does not replace ADR-012's event history export.

## Notes

The move preserves the pilot's workload policy, retry timing, and circuit-breaker behavior. Owner confirmation actions, Slurm ingestion, and incident analytics remain separate work. Packaging this local module does not publish it to an upstream repository or enable it in an existing cluster.

## References

- [Health Events Exporter](012-health-events-exporter.md)
- [Circuit Breaker Reset Mechanism](022-circuit-breaker-reset-mechanism.md)
