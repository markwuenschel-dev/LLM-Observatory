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

- OpenTelemetry is the transport/correlation foundation. The default deployment pins the Collector, Alpine queue initializer, Tempo, Loki, Prometheus, and Grafana images by digest; their exact configurations must be validated against the binaries used for deployment. Explicit `*_IMAGE` overrides are the controlled upgrade/promotion seam.
- Traces represent model, agent, workflow, tool, retry, and lifecycle operations when the source emits enough context.
- Logs/events represent adapter diagnostics, outcomes, malformed records, and state transitions.
- Metrics contain bounded aggregates only. Trace IDs, session IDs, raw paths, prompts, completions, and tool arguments do not become metric labels.
- The first host-side normalized store is SQLite in WAL mode. It is a local single-user profile behind `EventStore`; the contract can be replayed into PostgreSQL or ClickHouse later without changing adapters or the normalized envelope.
- The normalized SQLite store has a finite 2 GiB default budget, including WAL sidecars. At the budget boundary intake rejects telemetry and reports the Observatory degraded rather than allowing disk growth to interfere with inference; `OBSERVATORY_MAX_DATABASE_BYTES` can override the API container budget.
- Tempo, Loki, Prometheus, and Grafana are independent Compose services with named volumes. Local volumes are restart-durable, not disaster-recovery durable; external encrypted backups and object storage are required before making a host-loss durability claim.
- `doctor` resolves the five installed backend volume names and compares their Docker-reported total usage with a configurable 16 GiB soft budget (`OBSERVATORY_MAX_BACKEND_VOLUME_BYTES`). `start` refuses an already-over-budget installed stack before bringing services up; service retention, the Collector file-queue cap, and the normalized-store budget provide the ongoing bounded-growth controls. This is an application guard, not a Docker volume quota.
- The Collector exposes its own bounded self-metrics on the internal-only port 8888; Prometheus scrapes queue size/capacity and enqueue/send failure counters separately from the client metric exporter on port 8889. This makes telemetry-plane degradation observable without turning it into an inference dependency.
- Grafana's default Observatory Events Prometheus-compatible datasource evaluates the normalized metric families against SQLite `observed_at` time, so dashboard range selection is event-time aware. The real Prometheus datasource remains explicit for Collector self-metrics; the compatibility facade is read-only, bounded to 10,000 range points, and accepts only the provisioned dashboard query subset.
- `/v1/analytics/comparison` exposes bounded provider/model/family/variant/client aggregates for API consumers; the event-time Prometheus compatibility facade exposes the same comparison-safe success, token, cost, and latency dimensions plus a bounded top-500 context catalog across project/repository/branch/provider/model/family/variant/execution/workflow/agent/subagent/parent-agent/status. Context-scoped token, performance, reliability, workflow, agent, and execution series carry the same bounded attribution labels used by the filtered Efficiency, Reliability, Execution, Skills/Workflows, and Agent Hierarchy dashboards. Session and trace IDs remain API/Tempo drill-down fields rather than Prometheus labels. Prometheus selector, range, label, series, row, and matrix budgets are finite; the event-time facade rejects lookbacks longer than 366 days. The bound is deliberate; the API remains the complete query surface when a deployment exceeds the catalog limit.
- `AgentBehavior` is the normalized metadata-only agent/swarm contract: bounded tool-call counts and names plus counts of files inspected, files changed, commands executed, and tests invoked. Execution retains source-reported agent, subagent, and parent-agent identity for hierarchy drill-down without inferring ownership. Reliability separately preserves agent failures, reassessment counts, and rework-loop counts alongside retries, rate limits, timeouts, tool failures, and aborts. The store persists these dimensions in indexed columns and field-level measurement facts, while raw paths, commands, arguments, results, and test payloads are removed at the default privacy boundary.

## Canonical event

`src/observatory/contracts.py` owns the versioned `NormalizedEvent`. It separates project identity, execution hierarchy, LLM identity (including optional model family and variant/version), measurements, performance, reliability, outcome correlation, and provenance. Unknown providers, models, event types, and extension fields are retained. `usage.source` and `provenance.fields` distinguish provider/client/gateway/estimated/derived evidence; no estimate is silently promoted to authoritative usage.

The SQLite table stores a redacted canonical JSON payload plus indexed dimensions. `event_id` is the idempotency boundary. Exact replays return `duplicate`; conflicting payloads are retained as a redacted conflict diagnostic without overwriting the first observation. Event time and receipt time remain distinct. The `ingest_ledger` records every insert/duplicate/conflict attempt, `measurement_facts` stores field-level evidence and quality, `outcome_events` stores explicit engineering outcomes, and `attribution_edges` stores project/session/workflow/agent/subagent/parent-agent/task relationships with both observed and received times. Fallback event IDs project only bounded identity metadata and omit raw content, paths, credentials, and response bodies. These projections are append-only and preserve unknown providers and future fields.

## Privacy boundary

`src/observatory/privacy.py` runs before persistence and API delivery. Default capture excludes prompt/completion/message content, sensitive tool arguments/results, credentials, environment values, raw paths, and raw agent activity lists. Content capture is explicit opt-in and bounded; authorization and credential fields remain redacted even then. Metadata-only behavior counts and bounded tool names remain available for agent/swarm analysis. This policy is intentionally conservative because current GenAI semantic conventions are still in development and payload attributes may contain sensitive content.

## Attribution boundary

`src/observatory/project.py` runs read-only Git commands with argument arrays and timeouts. Remote credentials, query strings, and fragments are removed. A repository with no remote, no commit, broken metadata, or no Git context still receives a deterministic fallback identity. Host-side adapters can resolve or hash a local root before persistence; native OTLP records should supply the safe `llm.observatory.project.id` plus bounded repository/branch/commit dimensions because the shared Collector allowlist removes raw roots, remotes, and path-shaped attributes before fan-out. When a native client instead reports a common working-directory resource attribute, the Collector derives the same kind of local hash before deleting that raw path. Resolution never creates telemetry files or dependencies in the observed repository.

## Capability ladder

Adapters are selected by evidence, not by provider name:

1. Native OTLP when the client documents a global exporter and the exporter is verified.
2. Structured stream/log adapters when they are external, bounded, fail-open, and redacted before persistence.
3. API response-boundary instrumentation only for applications that already own the API call; it is optional and may require application-level instrumentation.
4. Explicit `UNKNOWN` when the client does not expose a safe supported signal.

The generic JSONL adapter and caller-owned provider-response adapter are implemented. Claude Code, Codex, and Gemini have evidence-backed, opt-in global configuration plans; Cursor, Kimi, and Grok remain structured-output/discovery-only until their native telemetry contracts are verified. OpenRouter and direct APIs use route-aware caller-owned response adapters rather than a mandatory proxy. The capability matrix records installed versions and first-party findings without pretending equivalent field coverage.

## OTel semantic-convention boundary

OpenTelemetry GenAI conventions are currently development-stage and evolving. Preserve incoming instrumentation scope, schema URL, adapter version, and convention revision. Use GenAI vocabulary where it is stable enough for transport, but keep the Observatory model independently versioned and additive. Provider identity, gateway identity, and target-provider identity are separate fields; OpenRouter is not a mandatory inference path.

The Efficiency dashboard defaults to `model.operation` so token, latency, and cost panels do not silently mix tool, outcome, or telemetry records; operators can broaden the event-type selector when that comparison is intentional.

## Outcome boundary

Tests, builds, CI, commits, PRs, corrections, and task completion are independently observed events. When an outcome and another event carry the same source-reported `task_id`, the store records an explicit `outcome_correlation` attribution edge; this is a join aid, not a causal claim. The normalized outcome records both `correlation_id` and an optional `correlation_basis` such as `task_id`, `session_id`, `worktree`, or `operator`, alongside the evidence source. It does not contain causal fields such as `caused_by`. Dashboards should say “associated with,” “same worktree,” or “observed after,” not “model caused.”

## Known limits

- The disposable Compose runtime gate passed on 2026-08-08 with Docker Engine 29.6.2, including pinned-image startup, OTLP delivery, the fail-closed Collector privacy boundary, all ten dashboard provisions and query probes, event-time queries, restart/recovery, full-state restore, and independent telemetry-service failure isolation. A host reboot remains a separate manual gate.
- The first store is single-host SQLite; it does not provide multi-user scale, replication, or host-loss durability.
- Native client configuration has not been exercised against disposable live client profiles in this environment; client-specific hooks and end-to-end telemetry remain unverified. Configuration writes are guarded by `--apply` and conflict checks.
- The runtime gate validates pinned image configurations, API-level dashboard provisioning, synthetic Prometheus visibility, event-time range/filter behavior, and Collector privacy redaction across downstream stores. The current unauthenticated browser check reaches Grafana's login page; no authenticated visual evidence is claimed, so human review of every panel and screen size remains a release gate.
