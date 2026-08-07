# Operations and recovery

## Install and lifecycle

Run `observatory install` once from the Observatory checkout. It creates user-level state, a SQLite data directory, a bounded offline spool, a Compose environment file, and a random Grafana admin password. Re-running the command is idempotent and does not touch observed repositories.

`doctor` is read-only and reports whether the state directory, Python runtime, privacy defaults, and Compose surface are available. `start` and `stop` manage only the Compose project. `status` reports API, Collector, and Grafana health separately and explicitly says `inference_path: unmanaged/no-proxy`; it does not claim a provider is healthy. `configure <client>` produces a capability-backed plan by default. `configure claude|codex|gemini --apply` can reconcile only their documented user-level telemetry settings; `configure all --apply` applies those native plans and reports partial/unknown clients without blocking inference. `configure --remove` removes only Observatory state unless `--apply` is also supplied. If intake is unavailable, `ingest`, `record-outcome`, and `git-snapshot` use the bounded spool; `flush` replays it after recovery (or into the local store with `--offline`).
`retention --prometheus-days N --tempo-hours N --loki-hours N` updates the installed host policy and reconciles the Compose environment; restart the stack after changing it. Prometheus consumes the time/size values directly; Tempo and Loki expand their duration values at startup. The normalized SQLite event store remains operator-managed and is deleted only through the explicit audited `prune --confirm` path.

For direct API integrations, use the caller-owned `ProviderResponseAdapter` after the application receives a response. For explicit engineering outcomes, use `record-outcome`, `run-outcome`, or `git-snapshot`; these create correlation records and never assert that a model caused a test, build, commit, or review result. `run-outcome` executes only the argument list supplied by the operator and stores metadata, not stdout/stderr.

Docker Desktop must be running in Linux-container/WSL2 mode. Default host bindings are loopback-only: API `127.0.0.1:8787`, OTLP gRPC `127.0.0.1:4317`, OTLP HTTP `127.0.0.1:4318`, Collector health `127.0.0.1:13133`, and Grafana `127.0.0.1:3000`. Databases and backend HTTP endpoints remain internal to the Compose network.

## Persistence and retention

The API bind-mounts the installed host state directory so the CLI and live service share `data/events.sqlite3`, spool, and secret state. Named volumes are used for Collector queues, Tempo, Loki, Prometheus, and Grafana. The Collector's file-backed queue has a 256 MiB storage cap and fsync-enabled checkpoints; exporter queues are also bounded by item count. Do not use `docker compose down -v` during normal operation. Local state survives container restarts but not disk loss or host destruction.

The initial profile targets 30 days of Prometheus metrics, 30 days of Tempo traces, 14 days of Loki logs, and longer-lived normalized metadata in the SQLite store. These are configurable starting defaults that require host-specific disk measurement before being called production capacity. `backup` emits a SHA-256 checksum and SQLite integrity result. Backups must still be encrypted by the host/operator, stored outside the repository, and restored into fresh volumes before they count as verified.

The SQLite store contains an append-only ingest ledger, bitemporal field-level measurement facts, outcome observations, and attribution edges in addition to the normalized event payload. The application path never updates or deletes those evidence projections. `migrate`, `backup`, `restore`, and `prune --confirm` are explicit administrative commands; migration scripts run in transactions and startup backfills missing projections for legacy event rows. Pruning records an audit action and temporarily removes/reinstates the evidence guards inside one SQLite transaction. No automatic normalized-event retention policy is enabled by default.

## Failure isolation

| Failure | Observable state | Required behavior |
|---|---|---|
| Collector unavailable | telemetry degraded / exporter diagnostics | Client inference remains on its normal route; bounded adapters spool or drop |
| Store unavailable | API not ready or intake degraded | No provider call is made by the Observatory; CLI falls back to bounded spool |
| Grafana unavailable | dashboard unavailable | API/store/Collector continue independently |
| Malformed event | rejected count and sanitized diagnostic | Valid sibling events continue; raw malformed payload is not retained |
| Queue saturated | bounded queue and retry diagnostics | Telemetry may be lost after bounded retries; monitor Collector queue/storage health and keep inference independent |
| Docker Desktop stopped | engine unavailable | Clients continue; `doctor`/`status` report the Observatory gap |
| Observatory removed | no managed telemetry state | Restore/remove only Observatory-owned client configuration; provider endpoint and credentials remain client-owned |

## Privacy and secrets

Provider credentials are never accepted by the API or Collector. Grafana's generated admin secret lives under the user state directory and is mounted as a Docker secret. The repository contains no provider or Grafana secret. Prompt/completion content and sensitive tool data remain disabled by default. If rich capture is ever added, it requires separate encrypted storage, shorter retention, an audit trail, and canary tests proving the default path remains clean.

## Upgrade posture

Image tags are pinned by default and overrideable through Compose variables. `update --pull` creates an integrity-checked SQLite backup under the installed state directory before pulling images, and reports degraded unless the API, Collector, and Grafana pass bounded post-update readiness gates. The backup is retained for operator-directed recovery; the command does not silently roll back image state. Before an upgrade: run `doctor`, run `docker compose config`, validate the new Collector config against the exact binary, and preserve the previous image tag for rollback. The normalized event contract and migration version must remain compatible with replayed JSONL fixtures.
