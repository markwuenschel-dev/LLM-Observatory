# LLM Observatory Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

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

| Area | Current status | Classification | Canonical owner | Evidence | Known gap | Next relevant action |
|---|---|---|---|---|---|---|
| Repository | Empty Git checkout on `main`; no commits or tracked files | Confirmed | This working tree | `git status --short --branch`, `Get-ChildItem -Force` | No project conventions, tests, or baseline | Establish package layout and initial commit-ready files |
| Host | Windows; Python 3.14.6; Node 25.2.0; Docker 29.6.2; Compose 5.3.1 | Verified locally | Host environment | Version commands in task log | Docker config access warning in sandbox | Add `doctor` diagnostics and verify Compose independently |
| Installed clients | Claude Code 2.1.224, Codex executable present but blocked by local access policy, Cursor 3.14.27, Kimi 0.28.1; Gemini/OpenRouter CLIs absent | Verified locally / blocked | Capability registry | `Get-Command`, `--version` commands | Native telemetry details still need first-party evidence | Maintain capability matrix with confidence states |
| Telemetry foundation | Current OTel docs expose Collector pipelines, memory limiting, batching, queued exporters, and evolving GenAI conventions | Verified from current first-party docs | OTel Collector configuration | Context7 sources under `open-telemetry` | Exact component versions and client support vary | Pin Compose images and validate configs |
| Visualization | Grafana supports file provisioning; Tempo supports persistent single-binary Docker storage | Verified from current first-party docs | Provisioned Grafana/Tempo files | Context7 sources under `grafana` | Runtime container startup not yet proven | Add provisioning files and static validation |

## Facts, Assumptions, and Unknowns

- **Fact:** The repository is greenfield, so a clean initial architecture can be established without migration of existing application contracts.
- **Fact:** The host is Windows and has Docker/Compose binaries, but this sandbox reported access warnings for Docker's user config; `doctor` must surface that as a diagnosis rather than hiding it.
- **Fact:** OTel GenAI semantic conventions are evolving; experimental attributes and provider-specific fields must be versioned and labeled.
- **Assumption:** The first deployment is single-user, single-host, private, and local-network-only. The persistence interface remains replaceable for a later PostgreSQL or analytical backend.
- **Assumption:** Native telemetry is unavailable or inconsistent for some subscription clients. The initial adapter contract therefore supports global logs/hooks/exports and explicitly represents missing fields.
- **Unknown:** Exact current telemetry behavior and supported global configuration of each installed client. No adapter may claim support until the capability lane records evidence.
- **Unknown:** Whether Docker Desktop is running and whether all pinned images can be pulled in this environment. Static validation must not be confused with runtime proof.

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
  "execution": {"trace_id": null, "span_id": null, "parent_event_id": null, "session_id": null, "workflow_id": null, "agent_id": null, "subagent_id": null, "role": null, "skill": null, "lane": null},
  "llm": {"provider": "unknown", "model": "unknown", "model_family": null, "client": "example", "auth_mode": "unknown", "route": "unknown", "reasoning_effort": null},
  "usage": {"input_tokens": null, "output_tokens": null, "cached_tokens": null, "reasoning_tokens": null, "total_tokens": null, "cost": null, "source": "unknown"},
  "performance": {"latency_ms": null, "time_to_first_token_ms": null, "duration_ms": null},
  "reliability": {"status": "unknown", "error_kind": null, "retry_count": null, "rate_limited": null},
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

- [ ] Write tests for required keys, UTC timestamps, deterministic IDs, unknown extension retention, and rejection of missing event IDs/timestamps.
- [ ] Run `python -m unittest tests.test_contracts -v`; expected initial failures identify missing package symbols.
- [ ] Implement immutable dataclass-backed contracts with explicit normalization of nullable sections and no provider-specific branches.
- [ ] Run the focused test again; expected result is all contract tests passing.
- [ ] Run `python -m compileall src tests`; expected result is no syntax errors.

### Task 2: Privacy policy and metadata-first redaction

**Files:**
- Create: `src/observatory/privacy.py`
- Modify: `src/observatory/contracts.py`
- Test: `tests/test_privacy.py`
- Create: `docs/privacy.md`

**Interfaces:**
- Produces `PrivacyPolicy(content_capture: bool = False, hash_sensitive_values: bool = True, max_string_length: int = 512)`.
- Produces `redact_mapping(value: Mapping[str, Any], policy: PrivacyPolicy) -> dict[str, Any]`.
- Produces `redact_event(event: NormalizedEvent, policy: PrivacyPolicy) -> NormalizedEvent`.

- [ ] Add failing tests proving `prompt`, `completion`, `content`, `tool.arguments`, `tool.result`, authorization headers, and keys containing `token`/`secret` are not persisted under default policy.
- [ ] Add a test proving opt-in content capture is bounded and records `provenance.content_capture = "enabled"`.
- [ ] Implement deny-by-key redaction before store serialization; replace sensitive scalar values with stable non-reversible hashes only when the policy allows correlation.
- [ ] Document defaults, opt-in behavior, deletion expectations, and the explicit boundary that redaction cannot recover secrets already emitted by a client outside this process.
- [ ] Run `python -m unittest tests.test_privacy -v`; expected result is all tests passing.

### Task 3: External Git project resolution

**Files:**
- Create: `src/observatory/project.py`
- Modify: `src/observatory/contracts.py`
- Test: `tests/test_project.py`
- Create: `tests/fixtures/non_repo/README.md`

**Interfaces:**
- Produces `resolve_project(path: str | Path, *, git_timeout_seconds: float = 2.0) -> ProjectIdentity`.
- Produces `ProjectIdentity.to_mapping() -> dict[str, Any]`.

- [ ] Add tests using a temporary Git repository for root, remote, branch, commit, and worktree-safe identity extraction.
- [ ] Add tests for a non-repository path and a repository with no commit; expected result is an explicit unresolved or partial identity, never an exception from the intake path.
- [ ] Implement subprocess calls with argument arrays, timeouts, and sanitized absolute paths; derive a stable unresolved ID from the canonical path.
- [ ] Add an invariant test that no generated file is placed inside the resolved repository root.
- [ ] Run `python -m unittest tests.test_project -v`; expected result is all tests passing.

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

- [ ] Add tests for schema creation, WAL mode, unique event IDs, duplicate replay, late events, unknown fields, query filters, and atomic failure on malformed records.
- [ ] Implement a versioned migration runner and one transaction per append; store canonical JSON plus indexed dimensions so the schema can evolve without losing extensions.
- [ ] Implement summary counters for events, successful/failed operations, input/output tokens by provenance, and distinct projects/models.
- [ ] Run `python -m unittest tests.test_store -v`; expected result is all tests passing.

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

- [ ] Add tests for bounded request size, malformed JSON, redaction-before-persistence, duplicate responses, health readiness, and metrics output.
- [ ] Add a CLI test proving `ingest --offline` writes only to the configured user data directory and continues successfully when the API is unavailable.
- [ ] Implement the API with the standard-library HTTP server so the core can run before optional dependencies are installed; bind to loopback by default.
- [ ] Implement the OTLP JSON bridge for resource spans, logs, and metrics while preserving trace/span IDs, schema URLs, source identity, usage provenance, and metadata-only attributes.
- [ ] Implement `ingest` as asynchronous-tolerant: HTTP delivery is best effort, while offline JSONL spooling is the durable fallback and never invokes provider inference.
- [ ] Run `python -m unittest tests.test_api tests.test_cli -v`; expected result is all tests passing.

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

- [ ] Add static tests for loopback port bindings, no provider credentials in Compose/config/dashboard files, default content capture disabled, healthchecks, persistent volumes, and OTel processor ordering.
- [ ] Implement the Compose stack with pinned image tags supplied by the current capability evidence; use `restart: unless-stopped` and named volumes for stateful services.
- [ ] Implement the dashboard with all-project defaults and variables for project, provider, model, client, route, agent role, workflow, branch, and status.
- [ ] Add a verification script that parses JSON and YAML-like required keys without requiring a running Docker daemon.
- [ ] Run `python -m unittest tests.test_deployment -v`; expected result is all static deployment tests passing.
- [ ] If Docker is available, run `docker compose config`; expected result is a normalized Compose configuration with no errors. Record runtime startup separately.

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

- [ ] Add adapter contract tests for bounded lines, malformed record isolation, unknown fields, source metadata, and deterministic event IDs.
- [ ] Implement the generic adapter and registry without importing provider SDKs into the core package.
- [ ] Populate the capability matrix only from first-party or local evidence, with unsupported/unknown fields explicit.
- [ ] Run `python -m unittest tests.test_adapters -v`; expected result is all tests passing.

### Task 8: Failure-isolation and operational validation

**Files:**
- Create: `tests/test_failure_isolation.py`
- Create: `scripts/verify.py`
- Create: `docs/architecture.md`
- Create: `docs/operations.md`
- Modify: `README.md`

- [ ] Add tests proving API/storage/Collector endpoint failure affects only telemetry delivery or local spool state, not any inference function or provider request path in this repository.
- [ ] Add tests proving malformed telemetry, duplicate events, saturated offline spool limits, unknown provider/model/repository, and restart-safe migrations have explicit outcomes.
- [ ] Document the distinction between `inference healthy` and `telemetry degraded`, installation, diagnostics, retention, backup, deletion, and recovery.
- [ ] Run `python scripts/verify.py`; expected result is a nonzero exit for missing required contracts and zero for the complete local tree.
- [ ] Run the full suite with `python -m unittest discover -s tests -v`; expected result is zero failures.

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

## Execution brief

- **First work:** finish the current read-only capability reports and implement Tasks 1–3.
- **Highest-leverage dependency:** the versioned `NormalizedEvent` plus privacy/provenance contract.
- **Immediately available lanes:** store/query, deployment, and adapter registry after contract names are locked.
- **Blocked lanes:** provider-specific configuration until first-party capability evidence is recorded; runtime Compose proof until Docker daemon access is confirmed.
- **Integration owner:** the main implementation thread owns cross-boundary schema and deployment integration.
- **Verification owner:** the main implementation thread runs the full suite and independent failure checks.
- **Highest-risk assumption:** a single-host SQLite canonical store is sufficient for the first private deployment; its repository interface must make later migration explicit and lossless.
- **Likely failure mode:** a locally valid adapter or dashboard silently treats estimated or client-reported usage as provider-authoritative; provenance tests prevent this.
- **Evidence required before completion:** focused/full test output, privacy scan, Compose config validation, capability evidence with confidence labels, and separate runtime/visual proof where available.
