# GPU fault notification plugin

This independent worker reads NVSentinel Node annotations, Pod ownership, and circuit-breaker status. It sends Slack notifications and saves delivery progress in MongoDB. Its Kubernetes access is read-only. NVSentinel continues to control isolation, draining, repair, and release.

The plugin builds from this directory. Its entry point remains `app.gpunotify`. [ADR-060](../../docs/designs/060-gpu-notification-plugin.md) records this local deployment choice.

## Notification behavior

- Each observed isolation cycle gets one parent message. The parent shows the current stage; stage changes appear in its thread. Fault evidence creates an incident. A routine validation session does not create one.
- SkyPilot workload identity uses `skypilot-cluster-name`; owner email uses `skypilot-user`. The worker asks training and development owners to save work and finish their tasks. It excludes finished Pods, login Pods, and long-NotReady Pods. The default NotReady threshold is 300 seconds and must match node-drainer.
- One workload across several affected nodes gets one owner message. Outstanding tasks receive daily reminders. Owner lookup failures retry every five minutes. Fewer affected nodes preserve the reminder deadline; a newly affected node can trigger an updated request.
- Drain, remediation, or validation failure, CONTACT_SUPPORT, and isolation beyond 24 hours trigger an infra mention. The 24-hour request includes tasks still waiting for their owners. Manual repair and drain progress are tracked separately.
- A separate five-second check reads the circuit-breaker ConfigMap. A trip creates a cluster-level infra message. Reset updates the same thread. Kubernetes availability and Slack retry delays affect delivery time.
- The `gpu_notification_state` collection stores threads, observed transitions, reminders, and pending deliveries. A dedicated account can access only this collection. NVSentinel's event collections stay unchanged.
- Failed sends use exponential backoff, capped at 15 minutes. Slack HTTP 429 responses use `Retry-After`. Pending deliveries retry during source outages. Invalid data for one node leaves other notifications running.

The private test channel must contain the bot and test owners. The app needs `chat:write` and `users:read.email`. It sends messages and resolves owners by email. Channel invitation and owner-confirmation actions remain separate work. Existing Grafana fault alerts remain enabled.

## Local setup and tests

From the repository root:

```bash
make -C plugins/gpu-notifier setup
make -C plugins/gpu-notifier lint-test
```

From `plugins/gpu-notifier/`, run an offline demo:

```bash
poetry run python -m app.gpunotify demo
```

The demo prints synthetic isolation, drain, repair-request, validation, and release stages. It uses memory and makes no Kubernetes, MongoDB, or Slack calls.

To send this demo to the pilot channel, use a hidden prompt in zsh:

```zsh
read -rs 'GPU_SLACK_BOT_TOKEN?Bot token: '
export GPU_SLACK_BOT_TOKEN
poetry run python -m app.gpunotify demo --send --channel C0C995HP9HT
unset GPU_SLACK_BOT_TOKEN
```

Demo messages have `[SIMULATION]`, use `demo-gpu-node`, and mention nobody. Each invocation creates a new synthetic thread. The app-level Slack token is unused. Keep credentials in environment variables, Kubernetes Secrets, or a secret manager.

## MongoDB account and certificate

The pilot uses Xinjiang's existing MongoDB. Confirm its application database name; the NVSentinel chart default is `HealthEventsDatabase`. `bootstrap-mongo-user.js` creates the notification collection and a dedicated role. The role grants `find`, `insert`, `update`, and `remove` on that collection. The worker uses `remove` to release its lease at shutdown.

Xinjiang uses mutual TLS. The chart requests a client certificate from `mongo-ca-issuer` in the release namespace. It stores the certificate in `gpu-notifier-mongo-client`. The default MongoDB username is `CN=gpu-notifier,OU=GPU-Notifications,O=Meshy` in `$external`.

Check the issued certificate's subject with `openssl x509 -in client.crt -noout -subject -nameopt RFC2253`. Set `GPU_NOTIFY_MONGO_USERNAME` if the actual subject differs.

Run provisioning from a machine with `mongosh`, database access, and administrator credentials. Supply the administrator URI through a hidden prompt. Keep its authentication method consistent with that administrator account. Xinjiang's root account uses SCRAM with `authSource=admin`, plus the required TLS CA and client PEM options. URI-encode credentials when constructing the connection string.

From the plugin directory:

```zsh
read 'GPU_NOTIFY_MONGO_DATABASE?Existing application database name: '
read -rs 'GPU_NOTIFY_MONGO_ADMIN_URI?Mongo administrator URI: '
export GPU_NOTIFY_MONGO_DATABASE GPU_NOTIFY_MONGO_ADMIN_URI
mongosh --nodb --quiet --file bootstrap-mongo-user.js
unset GPU_NOTIFY_MONGO_ADMIN_URI
```

The script preserves an existing matching account. Different existing grants stop provisioning for review. The administrator credentials stay outside the worker Pod. Optional `GPU_NOTIFY_MONGO_AUTH_MECHANISM=scram` creates a database-local user for SCRAM deployments; initial creation then needs `GPU_NOTIFY_MONGO_PASSWORD`. Re-running preserves existing passwords.

The worker's X.509 URI uses `authMechanism=MONGODB-X509&authSource=%24external`, the deployed replica-set name, and TLS. The chart mounts these files:

- `tlsCertificateKeyFile=/etc/gpu-notifier-mongo/client.pem`: cert-manager's combined certificate and key.
- `tlsCAFile=/etc/gpu-notifier-mongo/ca.crt`: the MongoDB CA.

The Kubernetes Secret `gpu-notifier` supplies `mongo-uri`, `mongo-database`, and `slack-bot-token`. A local replay uses local certificate file paths and reachable database hostnames instead.

The chart sets `GPU_NOTIFY_MONGO_CERT_FILE` to the mounted PEM. The worker checks its contents every 30 seconds. A change makes it exit gracefully; Kubernetes restarts it with the renewed certificate. This reloads PyMongo's TLS context. Deployments that omit this setting must restart the worker after certificate renewal.

See MongoDB's [collection-scoped roles](https://www.mongodb.com/docs/manual/reference/method/db.createrole/) and [X.509 authentication](https://www.mongodb.com/docs/manual/tutorial/configure-x509-client-authentication/).

## Test persistence across two processes

`replay` uses real MongoDB with a synthetic fault. The first process stops at draining. The second reads the saved thread and continues to release. It makes no Kubernetes or owner-lookup calls. With `--send`, it sends `[SIMULATION]` messages and mentions nobody.

From the plugin directory:

```zsh
read -rs 'GPU_NOTIFY_MONGO_URI?Notifier Mongo URI: '
read 'GPU_NOTIFY_MONGO_DATABASE?Existing application database name: '
read -rs 'GPU_SLACK_BOT_TOKEN?Bot token: '
export GPU_NOTIFY_MONGO_URI GPU_NOTIFY_MONGO_DATABASE GPU_SLACK_BOT_TOKEN
GPU_NOTIFY_TEST_SESSION="mongo-restart-1"
poetry run python -m app.gpunotify replay --stage start \
  --session "$GPU_NOTIFY_TEST_SESSION" --send --channel C0C995HP9HT
```

After that process exits, run:

```zsh
poetry run python -m app.gpunotify replay --stage resume \
  --session "$GPU_NOTIFY_TEST_SESSION" --send --channel C0C995HP9HT
unset GPU_SLACK_BOT_TOKEN GPU_NOTIFY_MONGO_URI
```

Both commands print the same `incident` and `thread_ts`. The phase changes from `draining` to `released`; `completed` becomes true. Repeating a completed stage sends nothing. After a delivery failure, repeat that stage after its saved retry delay. A new test needs a new session name.

Omit `--send` from both commands to print messages while testing Mongo persistence. Preview, Slack replay, and live worker records use separate scopes. Keep the mode, session, and channel unchanged between the two processes.

Opt-in integration tests use a disposable Mongo database. Set `GPU_NOTIFY_TEST_MONGO_URI`; `GPU_NOTIFY_TEST_MONGO_DATABASE` defaults to `gpu_notifier_test`. From the plugin directory:

```bash
poetry run python -m unittest discover -s tests -p 'test_gpunotify_mongo.py' -v
node --test bootstrap-mongo-user.test.cjs
```

The integration tests verify cross-process thread continuity and persisted delivery retries. Each test cleans up its own scoped records. The bootstrap tests check account privileges and repeat-run behavior.

## Build and configure the observer

Build a private image from the repository root:

```bash
make -C plugins/gpu-notifier docker-build IMAGE=private.registry.example/gpu-notifier:pilot
```

Publish through your private registry workflow, then deploy an immutable image digest. The plugin does not add an image-publishing workflow.

The independent chart contains a ServiceAccount, read-only Kubernetes RBAC, Deployment, client Certificate, and PodMonitor. Its default is zero replicas. Enabling one replica requires `cluster`, `slackChannelId`, and `infraSlackIds` values. The schema rejects missing notification destinations.

The Xinjiang override preserves the existing pilot identifiers. From the repository root:

```bash
helm lint plugins/gpu-notifier/chart -f plugins/gpu-notifier/chart/values-xinjiang.yaml
helm template gpu-notifier plugins/gpu-notifier/chart --namespace nvsentinel \
  -f plugins/gpu-notifier/chart/values-xinjiang.yaml --set replicas=0
```

Install the paused release after selecting the target context:

```zsh
read 'GPU_NOTIFY_KUBE_CONTEXT?Xinjiang Kubernetes context: '
helm upgrade --install gpu-notifier plugins/gpu-notifier/chart \
  --kube-context "$GPU_NOTIFY_KUBE_CONTEXT" --namespace nvsentinel \
  -f plugins/gpu-notifier/chart/values-xinjiang.yaml --set replicas=0
```

Wait for the certificate to be Ready. Provision the Mongo account and worker Secret, then complete the replay test. Enable the tested image:

```zsh
read 'GPU_NOTIFY_IMAGE?Private image digest (registry/repository@sha256:...): '
helm upgrade --install gpu-notifier plugins/gpu-notifier/chart \
  --kube-context "$GPU_NOTIFY_KUBE_CONTEXT" --namespace nvsentinel \
  -f plugins/gpu-notifier/chart/values-xinjiang.yaml \
  --set replicas=1 --set-string image="$GPU_NOTIFY_IMAGE"
```

For an existing installation, keep release `gpu-notifier`, namespace `nvsentinel`, its Secrets, and the same Mongo database. Preserve cluster `xinjiang` and channel `C0C995HP9HT`. These values retain the saved state scope and Slack threads. Upgrade that release instead of running a second observer. `infraSlackIds` in the pilot override targets the pilot operator; change it when the agreed escalation group expands.

Keep one replica and the Recreate strategy. The Mongo lease rejects another worker with the same cluster/channel scope. After a crash, the lease expires in five minutes. Hosts need synchronized clocks.

## Runtime settings and monitoring

- `GPU_SLACK_BOT_TOKEN`: bot token from the worker Secret.
- `GPU_NOTIFY_CLUSTER`: stable cluster identifier.
- `GPU_NOTIFY_MONGO_URI`, `GPU_NOTIFY_MONGO_DATABASE`: notification store connection and database.
- `GPU_NOTIFY_MONGO_CERT_FILE`: optional certificate/key PEM path; the chart sets it for certificate renewal.
- `GPU_NOTIFY_INFRA_IDS`: required space-separated Slack user IDs.
- `GPU_NOTIFY_BREAKER_NAMESPACE`: defaults to `nvsentinel`; the chart uses the release namespace.
- `GPU_SLACK_EXPECTED_TEAM_ID`: optional workspace ID from Slack `auth.test`.
- `GPU_NOTIFY_NOT_READY_SECONDS`: defaults to `300`.

The worker reads paginated Node and SkyPilot Pod inventories every 30 seconds. It refreshes the mounted service-account token on each read. Complete inventories identify removed nodes. Node UID distinguishes a replacement. Startup resumes saved threads and attaches to ongoing isolations.

The breaker reader only needs `get` on ConfigMap `circuit-breaker`. `data.status` is `TRIPPED` or `CLOSED`. Missing, unreadable, or unknown status preserves the previous observation. Each trip keeps the ConfigMap UID. Reset updates the thread and asks infra to confirm processing resumed.

`/healthz` supplies startup readiness and process liveness. `/metrics` on port 8080 exposes polling progress, parsing errors, pending deliveries, and unresolved owners. The PodMonitor works with Xinjiang's VictoriaMetrics operator. Configure these alerts through the existing infra notification route:

- Missing worker: `absent_over_time(gpu_notify_last_poll_success_timestamp_seconds{cluster="xinjiang"}[5m])`.
- Stale node collection: `time() - gpu_notify_last_poll_success_timestamp_seconds{cluster="xinjiang"} > 300`.
- Stale breaker collection: `time() - gpu_notify_last_breaker_poll_success_timestamp_seconds{cluster="xinjiang"} > 300`.
- Delivery backlog: `gpu_notify_pending_deliveries{cluster="xinjiang"} > 0` for 20 minutes.
- Missing infra configuration: `gpu_notify_infra_configuration_missing{cluster="xinjiang"} > 0`.

Grafana alert rules remain deployment configuration. The chart adds metrics collection, not those alert rules.

## Reported evidence and limits

Stages come from `dgxc.nvidia.com/nvsentinel-state` and `nvsentinel.nvidia.com/validation-state`. Fault checks and codes come from `quarantineHealthEvent`. The `remediation-succeeded` message means a maintenance request was submitted. The final message confirms Node Ready and uncordoned. Exact repair and validation results require a later collector that correlates RebootNode and ValidationRequest objects.

Polling can miss transitions or a full release/re-isolation between observations. A source outage leaves a gap. This worker reports observed operational progress; it does not build the full incident history.

Slack delivery and its Mongo checkpoint are separate operations. A crash between them can duplicate a message on retry. Records currently have no TTL; choose retention before broadening the pilot. This plugin requires MongoDB for notification state, even if NVSentinel uses PostgreSQL for events.

Owner-confirmation actions, automatic channel invitations, Slurm ingestion, regional Mongo migration, and cross-cluster statistics remain future work.

Slack references: [messages and threads](https://docs.slack.dev/reference/methods/chat.postMessage/), [parent-message updates](https://docs.slack.dev/reference/methods/chat.update/).
