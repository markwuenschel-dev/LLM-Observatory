# Production-readiness gates

This document separates repository evidence from runtime and provider evidence. A green local test run is necessary, but it is not a claim that the full deployment has been exercised.

## Current evidence

| Gate | Evidence | State |
|---|---|---|
| Normalized contract, redaction, adapters, projections | `tests/` and `scripts/verify.py` | Verified locally |
| Provider/client capability catalog and configure-all parity | Executable `CLIENT_SPECS` catalog, capability contract tests, and read-only doctor probes | Verified locally; provider behavior remains evidence-scoped |
| Append-only ledger, migrations, backfill, host-state and backend-volume backup/restore, audited purge | `tests/test_store.py`, `tests/test_maintenance.py` | Verified locally |
| Full-state restore rollback | Staged host-file and named-volume preimages plus simulated mid-restore failure test | Verified locally and in the current disposable Docker gate |
| Upgrade rollback | Pre-update SQLite backup, Compose image-ID capture/re-tag, validated database restore, stack recreation, and simulated failed-start rollback test | Verified locally; live image-update rollback remains an operator gate |
| Collector privacy boundary | Exact-key fail-closed allowlist, blocked-value rules, and downstream privacy canaries | Verified locally and in the current disposable Docker gate across the normalizer, Tempo, Loki, and Prometheus paths |
| Native OTLP project attribution | Collector derives a deterministic project ID from an explicit root/path or common native working-directory resource attribute before raw path deletion | Verified in the current disposable Docker gate; clients that emit no project context remain `project:unknown` |
| Normalized agent behavior and reliability contract | `AgentBehavior`, source-reported agent/subagent/parent-agent hierarchy, migrations `010_agent_behavior.sql`, `011_reliability_dimensions.sql`, and `012_parent_agent.sql`, immutable measurement facts, Prometheus context metrics, Agent Hierarchy dashboard, and default raw-activity redaction | Verified locally and in the current disposable Docker gate; source-reported non-unknown parent identities still require a real client/profile |
| Offline spool, replay, bounded input, malformed-batch behavior | `tests/test_failure_isolation.py`, `tests/test_api.py` | Verified locally |
| Exporter queue saturation and inference isolation | `scripts/queue-saturation-acceptance.ps1` with a two-item blackhole-exporter queue, Collector self-metrics (`queue_capacity=2`, `enqueue_failed_spans=63`), log evidence, and inference sentinel | Verified runtime on the digest-pinned Collector 0.158.0 image; latest disposable run passed with 64 attempts |
| Collector self-observability | Collector internal Prometheus reader on `8888`, Prometheus scrape job, and Reliability panels for queue/failure counters | Verified locally and in the current disposable Docker gate |
| Normalized-store byte budget, degraded readiness, and capacity metrics | `tests/test_store.py`, `tests/test_api.py` | Verified locally |
| Backend-volume capacity guard | `observatory doctor` resolves only the five installed backend volumes and reports Docker-reported usage against the configurable soft budget; `observatory start` refuses an already-over-budget installed stack; unit coverage includes size parsing, missing-volume warning, unrelated-volume isolation, and the start refusal | Verified locally; current live Docker volume measurement is an operator check |
| Windows install, idempotent state, Compose environment alignment | `tests/test_cli.py`, `observatory install` | Verified locally |
| First-run dashboard usability | `observatory demo` / `observatory install --demo` seed the bundled six-record metadata-only walkthrough idempotently; clean production installs remain free of synthetic rows unless explicitly requested | Verified locally |
| Compose interpolation, loopback ports, dashboard JSON | `docker compose config --quiet`, dashboard parse | Verified locally |
| Container startup, health checks, restart recovery, Collector binary validation | `scripts/runtime-acceptance.ps1` on Docker Engine 29.6.2, including a Claude-shaped plain-key OTLP log probe (`claude-code` -> `anthropic`, model/session identity, usage, cost, duration, and TTFT) | Verified in the current disposable runtime gate on 2026-08-08 using digest-pinned service images |
| Independent adversarial verification | Sealed independent-verification round 1 packet, baseline ratchet, 187-test unit gate, repository verifier, and a fresh read-only recheck of the repaired OTLP metric-context seam (18 bridge tests, 35 contract/store checks, direct two-event SQLite probe) | Partial; the repaired seam and local/static evidence pass, but the sealed round-1 verifier could not access Docker's named pipe, so its queue/runtime criteria were `BLOCKED-UNVERIFIABLE` and its mechanical verdict was `FAIL` |
| Native telemetry from supported client profiles | `scripts/provider-acceptance.ps1` provides an explicit-command, before/after, privacy, identity, and repository-cleanliness gate. An authorized Claude Code 2.1.226 session on 2026-08-08 emitted 7 real OTLP log events; startup, privacy, repository-cleanliness, and configuration cleanup passed, but the identity/model assertion exposed that Claude's documented plain event keys were dropped or ignored. The Collector allowlist and bridge mapping are now fixed and covered by a regression test plus the disposable runtime gate. | Partial; a post-fix real-client rerun is still required |
| Grafana datasource/dashboard provisioning and synthetic OTLP delivery | `scripts/runtime-acceptance.ps1` | Verified in the current disposable runtime gate |
| Grafana dashboard query execution, visual usability, and filter behavior | The current isolated harness executed 80 metric/log/trace panel queries across all ten dashboards, including Agent Hierarchy parent-agent selectors, event-time range queries, a scoped Tempo session TraceQL query, Collector self-metric queries, and project-scoped event/token/retry/execution/workflow/agent/outcome filters | Query and filter contracts verified at runtime; broader human visual sweep remains pending |

The reproducible runtime gate is [`scripts/runtime-acceptance.ps1`](../scripts/runtime-acceptance.ps1). It uses an isolated Compose project with dynamically allocated loopback ports, installs into a disposable state directory by default, sends synthetic JSONL and OTLP data (including a Collector privacy canary and recovery log), rejects malformed telemetry without destabilizing readiness, verifies Grafana datasource/dashboard provisioning, event-time Prometheus-compatible query visibility, and real Prometheus Collector self-metrics, exercises a full stopped-stack backend-volume backup/removal/restore into fresh volumes, restarts services, and checks with an unmanaged inference sentinel that Grafana, Collector, and normalizer/storage outages do not make the inference path unavailable. Run it only after Docker Desktop is reachable:

```powershell
pwsh -NoProfile -File .\scripts\runtime-acceptance.ps1
```

The script exits non-zero on any failed gate and emits `observatory.runtime-acceptance/v1` JSON. The current 2026-08-08 isolated run verified fresh install, six JSONL events including agent-failure/reassessment/rework dimensions, a documented Claude-shaped plain-key log mapping to provider/model/session/measurements, OTLP trace/log/metric delivery, malformed-event rejection, the fail-closed Collector privacy canaries, event-time filter queries through the Observatory Events datasource, Collector self-metrics through the real Prometheus datasource, downstream absence checks through the normalized API, Tempo, Loki, and Prometheus, all ten dashboards and 80 panel queries, stopped-stack full-state restore, service restart recovery, unmanaged inference sentinels, and failure isolation. The runtime gate proves parent-agent query/provisioning compatibility; a real client/profile is still needed to populate non-unknown parent identities. Its default cleanup removes the isolated Compose project and named volumes; pass `-KeepVolumes` when deliberately inspecting retained backend state, and pass `-KeepState` when retaining the generated host state. This harness still does not replace provider-profile acceptance or a human visual sweep of the dashboards.

## Explicitly pending external gates

These are not hidden behind the local green gates:

| Gate | Current boundary | Required proof |
|---|---|---|
| Native client emission | One user-authorized Claude Code session emitted real OTel logs, but the first run failed the normalized provider/model identity assertion; the documented plain-key mapping and fail-closed Collector allowlist are now corrected. | Run one post-fix, user-authorized Claude/Codex/Gemini or other supported-client session and pass source, model, session, retry, and privacy assertions end to end. |
| Host reboot/startup | The operator rebooted the Windows host on 2026-08-08 and Docker Desktop was restarted manually. The default Compose project then used a deleted visual-QA temporary bind path (`C:\\Users\\Nalakram\\AppData\\Local\\Temp\\llm-observatory-visual-20260807`); the API repeatedly exited with code 4 and `Permission denied: /var/lib/observatory/data`, while the isolated disposable runtime gate remained green. Read-only `status`/`doctor` now expose the stale Compose environment-file and API bind evidence. | Failed for the current stale live project; reinstall the stable `%LOCALAPPDATA%\\LLM-Observatory` state and rerun `doctor`/`status` after a host reboot. |
| Full single-host disaster recovery | `backup --full-state --backend-volumes` covers the five Compose named volumes with manifest/checksum validation and stopped-stack guards; the current disposable Docker gate restored into fresh volumes and retained normalized events, Prometheus metrics, Tempo traces, and Loki data. | Current runtime proof; off-host encrypted backup storage and host-loss rehearsal remain operator/environment gates. |
| Immutable image promotion | Default Compose and queue-gate references are pinned to verified image digests; organization-specific signature/provenance promotion is not available in this local repository. | Resolve and record approved signed image digests in the deployment promotion process, then exercise an update/rollback with the promoted references. |

## Runtime acceptance sequence

From a clean disposable state directory:

```powershell
$env:PYTHONPATH = 'src'
python -m observatory.cli install
python -m observatory.cli doctor
python -m observatory.cli start
python -m observatory.cli ingest --file .\examples\synthetic-events.jsonl
python -m observatory.cli status
python -m observatory.cli open
```

Then verify API readiness, `/metrics`, Collector health at `http://127.0.0.1:13133/`, every provisioned dashboard, Collector-to-normalizer delivery, and service restart recovery. Stop Grafana, the Collector, and the API independently and confirm that the normalizer/client-plan path remains available. A host restart and real provider/client emission remain separate manual gates.

## Safety boundaries

The baseline is metadata-only. The API and Collector do not accept provider credentials, the Collector has no production debug exporter, client configuration never changes provider endpoints or proxy variables, and the Compose API shares the installed host state directory with offline CLI maintenance. Grafana's default Observatory Events datasource is a bounded, read-only Prometheus compatibility facade over SQLite that evaluates normalized aggregates by event `observed_at`; the separate Prometheus datasource serves Collector self-metrics. The event-time facade accepts only the dashboard metric families/operators, caps query ranges at 10,000 points, rejects oversized expressions/regex matchers, and bounds returned series and matrix cells. Use `/v1/summary` or `/v1/analytics/comparison` with `start` and `end` filters for the complete unbounded SQLite analysis surface.

The default deployment is a single-host profile. SQLite, local backend volumes, and file-backed Collector queues are restart-durable but are not a high-availability or disaster-recovery claim. Measure host disk capacity, encrypt backups outside the repository, and test restore into fresh volumes before treating the deployment as production capacity.

## Mandatory failure coverage

| Required failure case | Current evidence | State |
|---|---|---|
| Collector unavailable | Runtime stop/restart plus unmanaged inference sentinel and API readiness check | Verified runtime |
| Grafana unavailable | Runtime stop/restart plus unmanaged inference sentinel | Verified runtime |
| Telemetry storage/normalizer unavailable | Runtime API stop, independent client plan, and unmanaged inference sentinel | Verified runtime for the single-host normalizer boundary |
| Malformed telemetry | HTTP 400 rejection with readiness preserved; sibling/unit rejection tests | Verified runtime and locally |
| Exporter queue saturation | Two-item non-blocking blackhole exporter, Collector self-metric failure counters, log evidence, inference sentinel | Verified runtime |
| Unknown provider/model | Normalized event retained and surfaced in store tests | Verified locally |
| Unknown repository | Deterministic non-Git fallback identity and project-resolution tests | Verified locally |
| Duplicate events | Idempotent duplicate status and append-only ledger tests | Verified locally |
| Machine restart | Container restart and full-stack restore are verified; Windows host reboot is not automated | Partial; host reboot pending |
| Observatory removal | Ownership-aware client configuration removal and conflict tests; provider harness cleanup path | Verified locally; real-client restoration pending |
| Provider credentials in telemetry | API/Collector rejection, fail-closed Collector allowlist, and redaction canaries | Verified locally and in the current synthetic runtime canary; real-client validation remains pending |
| Prompt/completion persistence by default | API, OTLP, privacy-policy, and expanded Collector canaries | Verified locally and in the current synthetic runtime canary; real-client validation remains pending |
