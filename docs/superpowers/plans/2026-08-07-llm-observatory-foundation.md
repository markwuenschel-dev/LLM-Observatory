# LLM Observatory Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Current execution status (2026-08-08):** Tasks 1–8 below are implemented in the current working tree. Their focused commands and the full suite are green; the disposable Docker, queue-saturation, and independent seam checks are recorded in [`docs/production-readiness.md`](../../production-readiness.md). The remaining open *release* boundaries are external: a post-fix real-client emission, a stable-state host reboot, a human visual sweep, off-host encrypted recovery, and signed image promotion. Historical “initial failure” wording is retained only as context and does not describe the current checkout.

**Goal:** Build the first production-useful vertical slice of a provider-agnostic LLM Observatory: metadata-first event intake, explicit provenance and privacy contracts, automatic Git project attribution, a durable local store, an out-of-band OTel/Grafana deployment, and a diagnostic CLI.

**Architecture:** A host-side Python package owns normalized event contracts, redaction, project resolution, SQLite persistence, and the `observatory` CLI. Native client telemetry is optional and goes to a localhost-only OpenTelemetry Collector; the Collector exports traces and metrics to Grafana-compatible backends without entering an inference path. A Docker Compose stack provides the Collector, Tempo, Prometheus, Grafana, and the host API as independently restartable services. Provider/client adapters are capability-declared and may emit partial metadata; they must never invent authoritative usage or require repository files.

**Tech Stack:** Python 3.11+, standard-library HTTP/SQLite/argparse, OpenTelemetry Collector Contrib, Grafana, Tempo, Prometheus, Docker Compose, JSON Lines, JSON Schema-shaped versioned envelopes, and unittest-compatible tests.

## Global Constraints

- Use **OpenTelemetry as the durable telemetry foundation** and preserve a backend-neutral normalized event model.
- Telemetry MUST be out-of-band; the Observatory must not proxy, redirect, authenticate, mutate, or block inference.
- Repositories being observed require zero Observatory-specific files or dependencies by default.
- Metadata-only collection is the default; prompt/completion/tool payload capture is explicit opt-in and redacted otherwise.
- Every measurement retains provenance such as `provider`, `client`, `gateway`, `estimated`, `inferred`, or `derived`.
- Unknown providers, models, repositories, fields, and event versions are retained rather than discarded.
- Duplicate events are idempotent by `event_id`; late and out-of-order events remain queryable.
- Grafana is the initial visualization layer, not the telemetry architecture.
- Credentials and secrets never enter Git, Grafana, normalized events, or fixture payloads.
- Windows is a first-class host; Docker/WSL capability must be diagnosed rather than assumed.
- Completion claims must distinguish code existence, local machine verification, runtime proof, and provider/client capability evidence.

---

## Current-State Truth

> This plan records the initial design and work packages. The live implementation and release verdict are maintained in [`docs/production-readiness.md`](../../production-readiness.md); the rows below describe the current checkout rather than the original empty-tree assumption.

| Area | Current status | Classification | Canonical owner | Evidence | Known gap | Next relevant action |
|---|---|---|---|---|---|---|
| Repository | Dirty working tree over the initial commit; implementation, tests, dashboards, deployment, and scripts are present but not committed | Verified locally | This working tree | `git status --short`, `git log -1 --oneline` | Changes still require an intentional review/commit boundary | Review the diff, then commit only when the owner requests it |
| Host | Windows; Python 3.14.x; Docker Engine 29.6.2 is available to the runtime gate | Verified locally | Host environment | Runtime acceptance output and local version probes | Docker Desktop/host reboot and off-host recovery remain operator gates | Run the documented host-reboot and recovery checks |
| Installed clients | Claude Code 2.1.226, Codex executable present but blocked by local access policy, Cursor 3.14.27, Kimi 0.28.1, and Grok 0.2.118; Gemini/OpenRouter CLIs absent | Verified locally / blocked | Capability registry | Capability matrix, bounded `--version` probes, and `doctor` | Real provider emission and subscription-mode behavior remain unverified | Run the explicit operator-authorized provider acceptance harness |
| Telemetry foundation | OTel Collector, host normalizer, SQLite ledger, Tempo, Loki, Prometheus, and Grafana are implemented with bounded queues, privacy allowlists, and restart-durable volumes | Verified locally and in disposable runtime | OTel Collector configuration and normalized contract | Full test suite, `scripts/verify.py`, and `scripts/runtime-acceptance.ps1` | Immutable image promotion and host-loss recovery remain external gates | Record approved image digests and rehearse off-host restore |
| Visualization | Ten provisioned Grafana dashboards execute their metric/log/trace targets and event-time queries in the disposable runtime gate | Query/runtime verified; visual review pending | Provisioned Grafana/Tempo files | Runtime dashboard sweep and `docs/production-readiness.md` | Human visual usability review is not automated | Perform the documented visual sweep before production sign-off |

## Facts, Assumptions, and Unknowns

- **Fact:** The repository is greenfield, so a clean initial architecture can be established without migration of existing application contracts.
- **Fact:** The host is Windows and has Docker/Compose binaries, but this sandbox reported access warnings for Docker's user config; `doctor` must surface that as a diagnosis rather than hiding it.
- **Fact:** OTel GenAI semantic conventions are evolving; experimental attributes and provider-specific fields are versioned and labeled.
- **Assumption:** The first deployment is single-user, single-host, private, and local-network-only. The persistence interface remains replaceable for a later PostgreSQL or analytical backend.
- **Assumption:** Native telemetry is unavailable or inconsistent for some subscription clients. The initial adapter contract therefore supports global logs/hooks/exports and explicitly represents missing fields.
- **Fact:** Current first-party capability evidence and bounded local probes are recorded in `docs/capability-evidence.md` and `docs/capability-matrix.yaml`; unverified modes remain explicitly partial or unknown.
- **Fact:** The pinned images and current disposable Compose profile pass the runtime gate; the stable host state after the recent reboot still requires an operator reinstall/restart check.

## Source-of-Truth and Authority Map

| Contract | Canonical owner | Consumers | Rule |
|---|---|---|---|
| Normalized event shape | `src/observatory/contracts.py` | intake, adapters, store, tests, fixtures, metrics | Additive versioning; preserve unknown fields in `extensions` |
| Privacy policy | `src/observatory/privacy.py` and resolved config | every intake path and fixture generator | Redaction happens before persistence and before telemetry export |
| Project identity | `src/observatory/project.py` | intake and CLI | Resolve from an explicit path using Git subprocess argument lists; never require repository edits |
| Persistence | `src/observatory/store.py` | API, CLI, query functions | `event_id` uniqueness is the idempotency boundary; SQLite is an implementation, not the domain contract |
| Client capability claims | `docs/capability-matrix.yaml` | configure/doctor/docs | Every row carries confidence and evidence; unsupported is visible |
| Deployment topology | `compose.yaml` plus `deployment/` | CLI lifecycle, operators, tests | Compose services must be able to fail without changing client inference behavior |
| Dashboard semantics | `dashboards/` provisioning files and query names | Grafana | Panels use normalized dimensions and label evidence quality |

## Canonical Data Flow

```text
client hook / native OTLP / adapter / explicit CLI event
        -> host-side redaction and envelope validation
        -> project identity resolution and provenance enrichment
        -> idempotent SQLite normalized store
        -> local metrics endpoint and optional OTel export
        -> Prometheus / Tempo / Grafana
```

The inference flow is deliberately absent from this graph. A client may emit telemetry asynchronously, fail to emit it, or stop the Observatory entirely while its normal inference request continues through its normal provider path.

## Normalized Event Contract

`NormalizedEvent` is a JSON object with these required keys and optional maps:

```json
{
  "schema_version": "1.0",
  "event_id": "client-generated-stable-id",
  "event_type": "model.operation",
  "observed_at": "2026-08-07T14:00:00Z",
  "received_at": "2026-08-07T14:00:01Z",
  "source": {"kind": "client", "name": "example", "version": "unknown"},
  "project": {"project_id": "repo:sha256:...", "repository": "example", "root": "C:/work/example", "remote": null, "branch": null, "commit": null, "worktree": null},
  "execution": {"trace_id": null, "span_id": null, "parent_event_id": null, "session_id": null, "workflow_id": null, "agent_id": null, "subagent_id": null, "parent_agent_id": null, "role": null, "skill": null, "lane": null},
  "llm": {"provider": "unknown", "model": "unknown", "model_family": null, "client": "example", "auth_mode": "unknown", "route": "unknown", "reasoning_effort": null},
  "usage": {"input_tokens": null, "output_tokens": null, "cached_tokens": null, "reasoning_tokens": null, "total_tokens": null, "cost": null, "source": "unknown"},
  "performance": {"latency_ms": null, "time_to_first_token_ms": null, "duration_ms": null},
  "reliability": {"status": "unknown", "error_kind": null, "retry_count": null, "rate_limited": null, "agent_failure": null, "reassessment_count": null, "rework_count": null},
  "outcome": {"kind": null, "status": null, "correlation_id": null, "correlation_basis": null, "evidence_source": null},
  "provenance": {"fields": {}, "adapter": "unknown", "semantic_conventions": "gen_ai.experimental", "content_capture": "disabled"},
  "attributes": {},
  "extensions": {}
}
```

The store accepts unknown `event_type`, provider, model, and extension keys. It rejects only malformed envelopes that cannot be assigned a stable `event_id`, `schema_version`, or `observed_at`; those are returned as explicit intake errors and never cause a client request to be proxied.

## Dependency-Aware Work Packages

### Task 1: Repository skeleton and canonical contracts

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `src/observatory/__init__.py`
- Create: `src/observatory/contracts.py`
- Create: `src/observatory/clock.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Produces `NormalizedEvent.from_mapping(value: Mapping[str, Any], *, received_at: datetime, source_kind: str, source_name: str) -> NormalizedEvent`.
- Produces `NormalizedEvent.to_mapping() -> dict[str, Any]` and `NormalizedEvent.to_json() -> str`.
- Produces `canonical_json(value: Any) -> str` and `stable_event_id(value: Mapping[str, Any]) -> str`.

- [x] Write tests for required keys, UTC timestamps, deterministic IDs, unknown extension retention, and rejection of missing event IDs/timestamps.
- [x] Run `python -m unittest tests.test_contracts -v`; the current focused run passed 17 tests.
- [x] Implement immutable dataclass-backed contracts with explicit normalization of nullable sections and no provider-specific branches.
- [x] Run the focused test again; the current focused run passed 17 tests.
- [x] Run `python -m compileall src tests`; the current run completed without syntax errors.

### Task 2: Privacy policy and metadata-first redaction

**Files:**
- Create: `src/observatory/privacy.py`
- Modify: `src/observatory/contracts.py`
- Test: `tests/test_privacy.py`
- Create: `docs/privacy.md`

**Interfaces:**
- Produces `PrivacyPolicy(content_capture: bool = False, hash_sensitive_values: bool = False, max_string_length: int = 512)`. The non-reversible redaction marker is the secure default; hashing is an explicit correlation opt-in.
- Produces `redact_mapping(value: Mapping[str, Any], policy: PrivacyPolicy) -> dict[str, Any]`.
- Produces `redact_event(event: NormalizedEvent, policy: PrivacyPolicy) -> NormalizedEvent`.

- [x] Add tests proving `prompt`, `completion`, `content`, `tool.arguments`, `tool.result`, authorization headers, and keys containing `token`/`secret` are not persisted under default policy.
- [x] Add a test proving opt-in content capture is bounded and records `provenance.content_capture = "enabled"`.
- [x] Implement deny-by-key redaction before store serialization; replace sensitive scalar values with stable non-reversible hashes only when the policy allows correlation.
- [x] Document defaults, opt-in behavior, deletion expectations, and the explicit boundary that redaction cannot recover secrets already emitted by a client outside this process.
- [x] Run `python -m unittest tests.test_privacy -v`; the current focused run passed 7 tests.

### Task 3: External Git project resolution

**Files:**
- Create: `src/observatory/project.py`
- Modify: `src/observatory/contracts.py`
- Test: `tests/test_project.py`
- Test fixture: temporary repository and non-repository paths in `tests/test_project.py`

**Interfaces:**
- Produces `resolve_project(path: str | Path, *, git_timeout_seconds: float = 2.0) -> ProjectIdentity`.
- Produces `ProjectIdentity.to_mapping() -> dict[str, Any]`.

- [x] Add tests using a temporary Git repository for root, remote, branch, commit, and worktree-safe identity extraction.
- [x] Add tests for a non-repository path and a repository with no commit; the result is an explicit unresolved or partial identity, never an exception from the intake path.
- [x] Implement subprocess calls with argument arrays, timeouts, and sanitized absolute paths; derive a stable unresolved ID from the canonical path.
- [x] Add an invariant test that no generated file is placed inside the resolved repository root.
- [x] Run `python -m unittest tests.test_project -v`; the current focused run passed 3 tests.

### Task 4: Durable idempotent store and query surface

**Files:**
- Create: `src/observatory/store.py`
- Create: `src/observatory/migrations/001_initial.sql`
- Create: `src/observatory/query.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Produces `EventStore(path: str | Path)` with `initialize()`, `append(event: NormalizedEvent) -> AppendResult`, `get(event_id: str) -> NormalizedEvent | None`, `summary(filters: Mapping[str, str]) -> dict[str, Any]`, and `close()`.
- `AppendResult` distinguishes `inserted`, `duplicate`, and `rejected` without throwing for duplicate events.
- Query filters are allow-listed dimensions: time range, project, provider, model, client, event type, status, and evidence source.

- [x] Add tests for schema creation, WAL mode, unique event IDs, duplicate replay, late events, unknown fields, query filters, and atomic failure on malformed records.
- [x] Implement a versioned migration runner and one transaction per append; store canonical JSON plus indexed dimensions so the schema can evolve without losing extensions.
- [x] Implement summary counters for events, successful/failed operations, input/output tokens by provenance, and distinct projects/models.
- [x] Run `python -m unittest tests.test_store -v`; the current focused run passed 18 tests.

### Task 5: Host intake API and offline-safe CLI ingestion

**Files:**
- Create: `src/observatory/api.py`
- Create: `src/observatory/intake.py`
- Create: `src/observatory/otel_bridge.py`
- Create: `src/observatory/cli.py`
- Modify: `pyproject.toml`
- Test: `tests/test_api.py`
- Test: `tests/test_otel_bridge.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces `POST /v1/events` for one JSON envelope or a bounded JSON array; response includes inserted/duplicate/rejected counts.
- Produces `GET /healthz`, `GET /readyz`, `GET /metrics`, and `GET /v1/summary`.
- Produces OTLP/JSON-compatible `POST /v1/traces`, `/v1/metrics`, and `/v1/logs` normalization endpoints; the Collector supplies batching/retry and the API supplies redaction/idempotent storage.
- CLI commands are `install`, `doctor`, `start`, `stop`, `status`, `configure`, `open`, `update`, `ingest`, `resolve-project`, and `run-api`.

- [x] Add tests for bounded request size, malformed JSON, redaction-before-persistence, duplicate responses, health readiness, and metrics output.
- [x] Add a CLI test proving `ingest --offline` writes only to the configured user data directory and continues successfully when the API is unavailable.
- [x] Implement the API with the standard-library HTTP server so the core can run before optional dependencies are installed; bind to loopback by default.
- [x] Implement the OTLP JSON bridge for resource spans, logs, and metrics while preserving trace/span IDs, schema URLs, source identity, usage provenance, and metadata-only attributes.
- [x] Implement `ingest` as asynchronous-tolerant: HTTP delivery is best effort, while offline JSONL spooling is the durable fallback and never invokes provider inference.
- [x] Run `python -m unittest tests.test_api tests.test_cli -v`; the current focused run passed 41 tests.

### Task 6: OTel Collector, Tempo, Prometheus, Grafana, and Compose deployment

**Files:**
- Create: `compose.yaml`
- Create: `deployment/otel-collector/config.yaml`
- Create: `deployment/tempo/tempo.yaml`
- Create: `deployment/prometheus/prometheus.yml`
- Create: `deployment/grafana/provisioning/datasources/datasources.yaml`
- Create: `deployment/grafana/provisioning/dashboards/dashboards.yaml`
- Create: `dashboards/global-observatory.json`
- Create: `Dockerfile`
- Test: `tests/test_deployment.py`

**Interfaces:**
- Compose exposes only loopback ports by default: API `8787`, OTLP gRPC `4317`, OTLP HTTP `4318`, Grafana `3000`; internal services communicate on the Compose network.
- Collector has `memory_limiter`, `batch`, bounded exporter queues, explicit health endpoint, and OTLP-to-Tempo plus Prometheus metrics export.
- Collector exports JSON OTLP signals to the host normalizer as a bounded, queued secondary path while retaining Tempo/Loki/Prometheus fan-out.
- Grafana provisions Prometheus and Tempo data sources and loads a global dashboard with evidence-quality labels.

- [x] Add static tests for loopback port bindings, no provider credentials in Compose/config/dashboard files, default content capture disabled, healthchecks, persistent volumes, and OTel processor ordering.
- [x] Implement the Compose stack with digest-pinned image references; use `restart: unless-stopped` and named volumes for stateful services.
- [x] Implement the dashboard family with all-project defaults and variables for project, provider, model, client, route, agent role, workflow, branch, and status.
- [x] Add a verification script that parses JSON and YAML-like required keys without requiring a running Docker daemon.
- [x] Run `python -m unittest tests.test_deployment -v`; the current focused run passed 16 tests.
- [x] The disposable runtime gate validated Compose normalization, startup, readiness, recovery, and dashboard provisioning; runtime startup remains separately recorded from static validation.

### Task 7: Capability registry and adapter seams

**Files:**
- Create: `docs/capability-matrix.yaml`
- Create: `src/observatory/adapters/__init__.py`
- Create: `src/observatory/adapters/base.py`
- Create: `src/observatory/adapters/jsonl.py`
- Test: `tests/test_adapters.py`

**Interfaces:**
- Produces `CapabilityRecord` with `provider`, `client`, `capabilities`, `auth_modes`, `confidence`, `evidence`, and `last_verified`.
- Produces `ObservationAdapter.iter_events() -> Iterator[Mapping[str, Any]]` and `AdapterRegistry.get(name: str)`.
- The generic JSONL adapter is supported; unverified native adapters are represented as partial/unknown records rather than faked.

- [x] Add adapter contract tests for bounded lines, malformed record isolation, unknown fields, source metadata, and deterministic event IDs.
- [x] Implement the generic adapter and registry without importing provider SDKs into the core package.
- [x] Populate the capability matrix only from first-party or local evidence, with unsupported/unknown fields explicit.
- [x] Run `python -m unittest tests.test_adapters -v`; the current focused run passed 17 tests.

### Task 8: Failure-isolation and operational validation

**Files:**
- Create: `tests/test_failure_isolation.py`
- Create: `scripts/verify.py`
- Create: `docs/architecture.md`
- Create: `docs/operations.md`
- Modify: `README.md`

- [x] Add tests proving API/storage/Collector endpoint failure affects only telemetry delivery or local spool state, not any inference function or provider request path in this repository.
- [x] Add tests proving malformed telemetry, duplicate events, saturated offline spool limits, unknown provider/model/repository, and restart-safe migrations have explicit outcomes.
- [x] Document the distinction between `inference healthy` and `telemetry degraded`, installation, diagnostics, retention, backup, deletion, and recovery.
- [x] Run `python scripts/verify.py`; the current verifier returns zero failures.
- [x] Run the full suite with `python -m unittest discover -s tests -v`; the current full-suite evidence is 187 passing tests.

## Parallel Execution Strategy

The reconnaissance lanes are read-only and independent: provider capabilities, OTel semantics, storage/Grafana, attribution schema, operations/security, and testing/CLI UX. They merge into the architecture document and capability matrix before adapters or dashboards make provider-specific claims. Implementation lanes are then disjoint by ownership: contracts/privacy/project resolution; store/query; API/CLI; deployment; adapters; validation. One integration owner reconciles cross-boundary changes, and one verifier reruns the full suite plus static deployment checks.

## Verification and Failure/Recovery Matrix

| Scenario | Expected outcome | Machine evidence |
|---|---|---|
| Collector unavailable | Client/inference path unchanged; adapter queues or drops telemetry according to bounded policy | Failure-isolation test and offline spool test |
| Grafana unavailable | Intake and store remain usable; dashboard is merely unavailable | API/store tests plus Compose service independence |
| Store unavailable | Intake returns explicit degraded status or spools boundedly; no inference call is made | API fault-injection test |
| Malformed event | Rejected with reason; valid sibling records continue | Intake batch test |
| Duplicate event | Idempotent duplicate result; one row remains | Store uniqueness test |
| Unknown provider/model/repository | Event retained with unknown labels and provenance | Contract/store test |
| Queue saturated | Bounded drop/spool signal; process remains responsive | Spool limit test |
| Restart | SQLite migration/WAL state remains readable; services use persistent volumes | Restart simulation and `docker compose config` |
| Observatory removed | Client configuration can be restored/disabled; no provider credential is required | Removal/restore documentation and config test |
| Secret-like input | Not persisted under default policy | Privacy redaction test and repository scan |
| Prompt/completion input | Not persisted by default | Privacy fixture assertion |

## Risk Register

| Risk | Impact | Evidence needed | Can work proceed? | Owner |
|---|---|---|---|---|
| Client-native telemetry differs or is absent | Some clients remain partial | Current first-party docs plus local help/log inspection | Yes, through generic adapter and explicit unknowns | Provider lane |
| OTel GenAI conventions evolve | Attribute names may change | Semconv version recorded per event and additive extensions | Yes | OTel lane |
| Docker daemon unavailable | Runtime proof cannot be completed | `docker info`, `docker compose config` | Yes for static/unit work; runtime claim blocked | Operations lane |
| SQLite throughput ceiling | High-volume future use may require migration | Load test and store interface | Yes for single-user baseline; migration remains a planned decision | Storage lane |
| Sensitive values arrive under unexpected keys | Privacy leakage | deny-by-key plus bounded recursive sanitizer tests | Yes after adversarial tests pass | Security lane |
| Grafana dashboards overclaim causality | Misleading comparisons | Evidence-quality labels and outcome correlation docs | Yes if causal language is prohibited | Integration owner |

## Definition of Done for This Plan

- The package, CLI, API, store, contracts, privacy policy, deployment files, dashboards, capability matrix, documentation, and tests exist at the exact paths above.
- All focused and full unittest commands pass locally.
- `python scripts/verify.py` passes and rejects deliberate contract violations in a negative test.
- `docker compose config` passes if the daemon/config access permits it; otherwise the limitation is explicitly recorded.
- No provider credentials, prompt/completion content, or repository-specific telemetry files are introduced.
- The README states what is implemented, what is supported but not locally verified, what remains partial/unknown, and how to recover from Observatory failure.
- Final reporting distinguishes implemented code, locally machine-verified behavior, static deployment validation, runtime container proof, and provider/client evidence.

The definition above describes the code and validation plan, not unconditional production sign-off. The current release ledger keeps the post-fix real-client run, stable-state host reboot, human dashboard visual sweep, off-host encrypted recovery, and signed image promotion as explicit external gates.

## Execution brief

- **Implementation state:** Tasks 1–8 are implemented and their focused checks are green in the current working tree.
- **Highest-leverage dependency:** the versioned `NormalizedEvent` plus privacy/provenance contract remains the compatibility boundary for future clients and backends.
- **Integration owner:** the main implementation thread owns cross-boundary schema and deployment integration; the connected-impact metric-context repair is independently verified.
- **Verification owner:** the main implementation thread ran the full suite, static verifier, disposable runtime gate, dedicated queue-saturation gate, and fresh read-only independent seam review.
- **Highest-risk assumption:** a single-host SQLite canonical store is sufficient for the first private deployment; its repository interface makes later migration explicit and lossless.
- **Likely failure mode:** a locally valid adapter or dashboard silently treats estimated or client-reported usage as provider-authoritative; provenance tests and dashboard evidence labels prevent this.
- **Remaining release evidence:** one fresh user-authorized real-client acceptance run after the Claude plain-key mapping fix, a stable-state host reboot, human visual review, off-host encrypted restore, and organization-specific signed-image promotion.
