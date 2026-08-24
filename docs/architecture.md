# Architecture and evidence boundary

## Durable invariant

The Observatory is an observation plane, not an inference gateway:

```text
LLM client -> existing provider/inference route
LLM client -. asynchronous, bounded telemetry .-> localhost OTLP or external adapter
                                                        -> Collector
                                                        -> control/intake API :8787 -> host SQLite
                                                        -> bounded read API :8788 -> Grafana/Prometheus
```

Collector, storage, Grafana, adapter, queue, and project-resolution failure cannot be allowed to change an inference endpoint, provider credential, request payload, or client exit path. The current code implements this boundary by keeping intake and storage in separate host-side modules and never importing a provider SDK or changing provider environment variables.

## Signal and storage roles

- OpenTelemetry is the transport/correlation foundation. The default deployment pins the Collector, Alpine queue initializer, Tempo, Loki, Prometheus, and Grafana images by digest; their exact configurations must be validated against the binaries used for deployment. Explicit `*_IMAGE` overrides are the controlled upgrade/promotion seam.
- Traces represent model, agent, workflow, tool, retry, and lifecycle operations when the source emits enough context.
- Logs/events represent adapter diagnostics, outcomes, malformed records, and state transitions.
- Metrics contain bounded aggregates only. Trace IDs, session IDs, raw paths, prompts, completions, and tool arguments do not become metric labels.
- Tempo receives every span; the normalized store receives only spans carrying LLM identity. A separate `traces/normalizer` pipeline applies `filter/normalizer_spans` before the shared privacy chain, because a client emitting high-volume internal spans with no model, provider, session, or measurement (observed: ~758/sec from `codex-app-server`) otherwise dominates the store without contributing anything an analysis can use. Span-level drill-down remains available in Tempo.
- The SQLite WAL sidecar is bounded by `journal_size_limit`. `storage_bytes()` counts the WAL, so an untruncated sidecar consumes the store budget and stops intake even when the real data volume is small; passive autocheckpoints cannot reset it while the dashboard read plane holds readers.
- The first host-side normalized store is SQLite in WAL mode at the canonical host path `%LOCALAPPDATA%\\LLM-Observatory\\data\\events.sqlite3`. The API runs natively on the host, while Compose reaches it through `host.docker.internal`; no Windows-to-Docker database bind mount participates in dashboard reads. It is a local single-user profile behind `EventStore`; the contract can be replayed into PostgreSQL or ClickHouse later without changing adapters or the normalized envelope.
- Control/intake and dashboard reads are separate HTTP planes. Health and intake use the control plane; Grafana, Prometheus, and query clients use the read plane. The read plane admits GET for queries and POST only for the Prometheus compatibility paths under `/api/v1/` (Grafana 12 POSTs those even when the datasource is provisioned as GET). Every other method and every intake path stay 404 on this plane. The read plane admits at most 12 concurrent dashboard operations, uses independent read-only SQLite connections, enforces a two-second query budget, and returns `503` with `Retry-After: 1` when saturated or overdue. This prevents dashboard work from occupying health or intake capacity.
- The normalized SQLite store has a finite 2 GiB default budget, including WAL sidecars. At the budget boundary intake rejects telemetry and reports the Observatory degraded rather than allowing disk growth to interfere with inference; `OBSERVATORY_MAX_DATABASE_BYTES` can override the host API budget.
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

Bindings come from two sources. A globally-configured client hook reports the
session and the working directory together. Failing that, `src/observatory/sessions.py`
discovers the same fact from the client's own per-project session directory
(`~/.claude/projects/<encoded-working-directory>/<session-id>.jsonl`) -- a
host-level external source that needs no configuration in any client and places
nothing inside an observed repository. Only directory and file *names* are read;
session transcripts contain prompts and completions and are never opened. The
encoded directory name is ambiguous (both path separators and literal hyphens
encode to `-`), so it is resolved against the filesystem rather than guessed, and
a name that does not resolve to a real directory yields no binding instead of a
fabricated project.

Native OTLP clients emit a session identifier but no working directory, so
native telemetry alone resolves to `project:unknown`. A globally-configured
client hook does know the working directory and reports the *same* session
identifier, so `session_projects` (migration `013_session_projects.sql`) binds
one to the other and an otherwise-unattributed event inherits that project
before it is persisted. Enrichment happens at intake, never as an update to a
stored row, and the derivation is recorded as `derived:session_binding` in field
provenance so a consumer can tell it apart from client-reported identity. Only a
resolved project is ever bound, the first binding wins, and an event whose
session has no binding stays `project:unknown` rather than being guessed.

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

## Observation boundary

`src/observatory/observation.py` owns the deployment's self-knowledge. Service
health and observation are different claims: the Collector, Grafana, and every
backend can report healthy while the normalized store has accepted nothing.
`OBSERVATION_CAPABLE` is therefore derived from what the store actually received
— a reachable store with headroom *and* at least one client whose telemetry
landed inside the freshness budget — not from uptime. Per-client state is
authoritative and single-valued: `UNSUPPORTED` for caller-owned clients that no
host-level configuration can reach, `NOT_CONFIGURED` when a fix exists,
`CONFIGURED_UNVERIFIED` when configuration is recorded but nothing has ever
arrived, `DEGRADED` when telemetry is stale or unattributable, and `OBSERVING`
only when it is fresh and resolves to a project. A client is reported once under
its canonical name even when its telemetry arrives under a different
`service.name`. Coverage grades are always accompanied by their numerator and
denominator so missing telemetry is never silently compared as zero telemetry.

Observation verdicts are themselves recorded. `observation_snapshots` and
`observation_client_snapshots` (migration `014_observation_snapshots.sql`) are
append-only and rewrite-guarded, so a degradation that is recorded cannot later
be tidied away. A point-in-time verdict cannot say how long a blackout lasted;
the record can, and it distinguishes an interval that was red from an interval
nobody was watching.

## Outcome boundary

An outcome that declares no correlation basis is recorded but unjoinable: the
store only builds an `outcome_correlation` edge when it is told which shared
identifier to match on. Every shipped collector therefore declares one --
`observatory hook` uses the session it already reports, and `run-outcome` /
`git-snapshot` accept `--correlation-basis`, `--task-id`, and `--session-id`.
`EventStore.outcome_value()` is the only aggregation that crosses
`attribution_edges`, so it is the only place spend can be related to a validated
engineering result; it is served as `/v1/analytics/outcome-value`. Every row
carries the basis that produced the link, an explicit `association_only` flag,
and a per-measure coverage fraction, so a client that never reports cost is
reported as `cost: null` with ratio `0.0` rather than ranking as the cheapest.

Tests, builds, CI, commits, PRs, corrections, and task completion are independently observed events. When an outcome and another event carry the same source-reported `task_id`, the store records an explicit `outcome_correlation` attribution edge; this is a join aid, not a causal claim. The normalized outcome records both `correlation_id` and an optional `correlation_basis` such as `task_id`, `session_id`, `worktree`, or `operator`, alongside the evidence source. It does not contain causal fields such as `caused_by`. Dashboards should say “associated with,” “same worktree,” or “observed after,” not “model caused.”

## Automatic outcome observation

Outcomes that only exist when a human remembers to record one are outcomes that
never accumulate, and an analysis surface fed by nothing stays empty however long
it runs. `git_commit_outcomes` observes commits with a read-only `git log` across
the projects already known from the host session directory -- no hook, no file,
and nothing installed in any repository. Only counts are retained: commit
messages, author identities, and file paths are never read into the event,
because each can carry exactly the content the privacy boundary excludes.

A commit carries no session or task of its own, so it is associated by bounded
temporal proximity within the same project (`project_window`). This is the
weakest basis the store accepts and is treated as such: the window is finite and
clamped, it is recorded on the outcome so a reader can see how loose the
association is, the number of links one outcome may sweep in is capped, it never
reaches across projects, and it never associates work that happened *after* the
commit -- a commit cannot be the outcome of work that had not yet occurred.

## Schema-drift boundary

Provider schema drift is silent by construction: after a client upgrade a field
simply stops arriving, nothing errors, and every aggregate keeps returning
numbers that are merely smaller. Coverage was previously computed live and
discarded, so there was nothing to compare against. Each observation snapshot
now records the per-signal-family coverage ratio for every client (migration
`016_snapshot_signal_coverage.sql`), and `coverage_drift()` compares the latest
against the median of that client's own recent snapshots. The baseline is a
median so one bad sample can neither raise a false finding nor mask a real one,
a small dip is not reported, and both ratios travel with the finding so the
operator judges the size of the drop rather than trusting a flag. It is surfaced
by `observe --history` and `/v1/observation/history`.

## Engineering-value boundary

`EventStore.engineering_value()` (`/v1/analytics/engineering-value`) ranks
configurations -- provider, model, client, agent, skill -- by validated outcome
relative to effort. It is the question the store exists to answer and therefore
the easiest one to answer dishonestly, so four guards are structural rather than
advisory. Effort is summed over *distinct* associated events, so an operation
linked to several outcomes is counted once. A success rate is computed only over
outcomes that can actually fail: a landed commit is neither a pass nor a
failure, and counting it as "not passed" would drag every configuration toward
zero and invent a precise number out of a category error, so such outcomes are
reported separately as `outcomes_non_binary`. A configuration with fewer than
`MIN_RANKING_OUTCOMES` pass/fail outcomes is returned under
`insufficient_evidence` with the reason stated and is not ranked. And
`cost_per_success` is withheld unless cost was reported for every associated
event, so a client that reports no cost can never rank cheapest by staying
silent. Rows carry `association_only`; a configuration is associated with an
outcome through the stated basis and is never shown to have caused it.

## Known limits

- The disposable Compose runtime gate was rerun on 2026-08-22 against the host-native control/read split and passed with 29 of 29 checks, re-establishing runtime evidence for the container-to-host bearer bridge, the GET-only read plane, dashboard query/filter execution, full-state restore, and failure isolation. The earlier run below predates the split and is retained only as history.
- The disposable Compose runtime gate passed on 2026-08-08 with Docker Engine 29.6.2, including pinned-image startup, OTLP delivery, the fail-closed Collector privacy boundary, all ten dashboard provisions and query probes, event-time queries, restart/recovery, full-state restore, and independent telemetry-service failure isolation. That run predates the host-native API split: the API was then a Compose service at `observatory-api:8787`, so the evidence does not carry over to any path that now traverses the host control plane on `8787` or the read plane on `8788`. The gate has not been rerun, and a fresh disposable run is pending. A host reboot remains a separate manual gate.
- The first store is single-host SQLite; it does not provide multi-user scale, replication, or host-loss durability.
- Native client configuration has not been exercised against disposable live client profiles in this environment; client-specific hooks and end-to-end telemetry remain unverified. Configuration writes are guarded by `--apply` and conflict checks.
- The runtime gate is written to validate pinned image configurations, API-level dashboard provisioning, synthetic Prometheus visibility, event-time range/filter behavior, and Collector privacy redaction across downstream stores; the script has been updated for the split but its post-split result is pending. Its 2026-08-08 unauthenticated browser check reached Grafana's login page; no authenticated visual evidence is claimed, so human review of every panel and screen size remains a release gate.
