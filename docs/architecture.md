# Architecture and evidence boundary

## Durable invariant

The Observatory is an observation plane, not an inference gateway:

```text
LLM client -> existing provider/inference route
LLM client -. asynchronous, bounded telemetry .-> localhost OTLP or external adapter
                                                        -> Collector
                                                        -> redaction/normalization
                                                        -> durable stores
                                                        -> Grafana
```

Collector, storage, Grafana, adapter, queue, and project-resolution failure cannot be allowed to change an inference endpoint, provider credential, request payload, or client exit path. The current code implements this boundary by keeping intake and storage in separate host-side modules and never importing a provider SDK or changing provider environment variables.

## Signal and storage roles

- OpenTelemetry is the transport/correlation foundation. The pinned default Collector image is `otel/opentelemetry-collector-contrib:0.158.0`; its exact configuration must be validated against the binary used for deployment.
- Traces represent model, agent, workflow, tool, retry, and lifecycle operations when the source emits enough context.
- Logs/events represent adapter diagnostics, outcomes, malformed records, and state transitions.
- Metrics contain bounded aggregates only. Trace IDs, session IDs, raw paths, prompts, completions, and tool arguments do not become metric labels.
- The first host-side normalized store is SQLite in WAL mode. It is a local single-user profile behind `EventStore`; the contract can be replayed into PostgreSQL or ClickHouse later without changing adapters or the normalized envelope.
- Tempo, Loki, Prometheus, and Grafana are independent Compose services with named volumes. Local volumes are restart-durable, not disaster-recovery durable; external encrypted backups and object storage are required before making a host-loss durability claim.
- `/v1/analytics/comparison` exposes bounded provider/model/client aggregates for API consumers; Prometheus exposes the same comparison-safe success, token, cost, and latency dimensions plus a top-100 context view across project/repository/branch/provider/model/execution/workflow/status. Session and trace IDs remain API/Tempo drill-down fields rather than Prometheus labels.

## Canonical event

`src/observatory/contracts.py` owns the versioned `NormalizedEvent`. It separates project identity, execution hierarchy, LLM identity, measurements, performance, reliability, outcome correlation, and provenance. Unknown providers, models, event types, and extension fields are retained. `usage.source` and `provenance.fields` distinguish provider/client/gateway/estimated/derived evidence; no estimate is silently promoted to authoritative usage.

The SQLite table stores a redacted canonical JSON payload plus indexed dimensions. `event_id` is the idempotency boundary. Exact replays return `duplicate`; conflicting payloads are retained as a redacted conflict diagnostic without overwriting the first observation. Event time and receipt time remain distinct. The `ingest_ledger` records every insert/duplicate/conflict attempt, `measurement_facts` stores field-level evidence and quality, `outcome_events` stores explicit engineering outcomes, and `attribution_edges` stores project/session/workflow/agent/subagent/parent/task relationships with both observed and received times. These projections are append-only and preserve unknown providers and future fields.

## Privacy boundary

`src/observatory/privacy.py` runs before persistence and API delivery. Default capture excludes prompt/completion/message content, sensitive tool arguments/results, credentials, environment values, and raw paths. Content capture is explicit opt-in and bounded; authorization and credential fields remain redacted even then. This policy is intentionally conservative because current GenAI semantic conventions are still in development and payload attributes may contain sensitive content.

## Attribution boundary

`src/observatory/project.py` runs read-only Git commands with argument arrays and timeouts. Remote credentials, query strings, and fragments are removed. A repository with no remote, no commit, broken metadata, or no Git context still receives a deterministic fallback identity. Native OTLP records may supply `llm.observatory.project.*` dimensions; a supplied root/path is hashed into a stable project/worktree ID and never persisted raw. Resolution never creates telemetry files or dependencies in the observed repository.

## Capability ladder

Adapters are selected by evidence, not by provider name:

1. Native OTLP when the client documents a global exporter and the exporter is verified.
2. Structured stream/log adapters when they are external, bounded, fail-open, and redacted before persistence.
3. API response-boundary instrumentation only for applications that already own the API call; it is optional and may require application-level instrumentation.
4. Explicit `UNKNOWN` when the client does not expose a safe supported signal.

The generic JSONL adapter and caller-owned provider-response adapter are implemented. Claude Code, Codex, and Gemini have evidence-backed, opt-in global configuration plans; Cursor, Kimi, and Grok remain structured-output/discovery-only until their native telemetry contracts are verified. OpenRouter and direct APIs use route-aware caller-owned response adapters rather than a mandatory proxy. The capability matrix records installed versions and first-party findings without pretending equivalent field coverage.

## OTel semantic-convention boundary

OpenTelemetry GenAI conventions are currently development-stage and evolving. Preserve incoming instrumentation scope, schema URL, adapter version, and convention revision. Use GenAI vocabulary where it is stable enough for transport, but keep the Observatory model independently versioned and additive. Provider identity, gateway identity, and target-provider identity are separate fields; OpenRouter is not a mandatory inference path.

## Outcome boundary

Tests, builds, CI, commits, PRs, corrections, and task completion are independently observed events. When an outcome and another event carry the same source-reported `task_id`, the store records an explicit `outcome_correlation` attribution edge; this is a join aid, not a causal claim. The normalized outcome records both `correlation_id` and an optional `correlation_basis` such as `task_id`, `session_id`, `worktree`, or `operator`, alongside the evidence source. It does not contain causal fields such as `caused_by`. Dashboards should say “associated with,” “same worktree,” or “observed after,” not “model caused.”

## Known limits

- Docker Desktop/WSL runtime startup has not been proven in this sandbox; the Docker daemon was stopped/unavailable during reconnaissance.
- The first store is single-host SQLite; it does not provide multi-user scale, replication, or host-loss durability.
- Native client configuration has not been exercised against disposable live client profiles in this environment; client-specific hooks and end-to-end telemetry remain unverified. Configuration writes are guarded by `--apply` and conflict checks.
- Static Compose validation does not prove that pinned images exist locally, that all component configurations are accepted by those images, or that Grafana panels render correctly.
