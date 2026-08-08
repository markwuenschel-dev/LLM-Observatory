# LLM Observatory

LLM Observatory is a provider-agnostic, metadata-first telemetry platform for LLM-assisted development. It observes clients externally and keeps normal inference on its existing provider path.

## Current state

The repository contains a production-oriented metadata-first baseline:

- versioned normalized events with unknown-field retention;
- metadata-only agent behavior counts (tool calls, files inspected/changed, commands, and tests) with bounded tool names, explicit agent/subagent/parent-agent hierarchy, plus agent-failure and reassessment/rework reliability dimensions;
- provenance-aware usage and outcome fields plus append-only ledger, measurement, and attribution projections;
- default redaction of prompt/completion content, sensitive tool values, credentials, and raw paths;
- automatic read-only Git project resolution;
- idempotent SQLite WAL storage with conflict diagnostics and a finite capacity budget;
- bounded loopback HTTP intake, health, metrics, summary, and event queries;
- offline-safe JSONL ingestion and lifecycle CLI;
- capability-backed client configuration plans for Claude Code, Codex, and Gemini, a generic adapter registry, and caller-owned direct-provider/OpenRouter response adapters;
- provisioned Docker Compose services for the OTel Collector, Tempo, Loki, Prometheus, Grafana, and the normalizer API.
- provisioned dashboard families for global comparison, efficiency, projects, reliability, outcomes, skills/workflows, agent hierarchy, and sessions;

Client-native capability claims remain explicitly partial or unknown until verified against current first-party documentation and disposable local profiles. `configure <client>` is plan-only by default; `--apply` is required for guarded user-level telemetry settings. The Observatory does not set provider base URLs, proxy variables, credentials, or repository files.

## Quick start on Windows

```powershell
python -m pip install .
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" install
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" doctor
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" start
python -m observatory.cli open
```

The package can be run from a checkout with `PYTHONPATH=src` or installed with `python -m pip install .`. Docker Desktop must be running in Linux-container/WSL2 mode before `start`. The generated state directory contains the SQLite database, bounded spool, Compose environment file, and Grafana secret; it is never created in an observed repository. Compose bind-mounts that same state directory into the API container, so offline CLI maintenance and the live API see one SQLite store. Status/doctor also probe the loopback-only Collector health endpoint at `127.0.0.1:13133` so normalizer, exporter, and dashboard health remain distinct. `doctor` reports exact Docker backend-volume usage against a configurable 16 GiB soft budget, and `start` refuses an already-over-budget installed stack; this is an application guard, not a volume quota. Read-only `status`/`doctor` diagnostics also inspect the default Compose project's generated environment-file label and API state bind; stale or missing paths are reported as degraded evidence and are never repaired implicitly.

To inspect a populated walkthrough immediately, run `python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" demo` after `install` and before `open`. The command is explicitly opt-in, idempotent, provider-free, and metadata-only; omit it when the state must contain only real telemetry. `install --demo` combines the two operations.

## Ingest metadata safely

JSON Lines records must include `schema_version`, `event_id`, `event_type`, and a timezone-aware `observed_at`. Unknown provider/model values are valid; model family and variant/version are retained as separate optional dimensions when a client reports them. The default path redacts sensitive fields before persistence and before API delivery.

```powershell
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" ingest --file .\examples\synthetic-events.jsonl --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" flush --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" status --url http://127.0.0.1:8787/healthz
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" configure claude
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" uninstall --apply
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" retention --prometheus-days 30 --tempo-hours 720 --loki-hours 336
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" backup "$env:USERPROFILE\LLM-Observatory-state.zip" --full-state
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" backup "$env:USERPROFILE\LLM-Observatory-full-state.zip" --full-state --backend-volumes --include-secret --timeout 300
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" record-outcome --kind tests --status passed --correlation-id run-123 --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" run-outcome --kind tests --offline -- python -m unittest
```

The `--backend-volumes` backup requires the Compose stack to be stopped and `--include-secret`; it includes the five durable backend volumes plus the Grafana secret needed for portable restoration. Encrypt that archive before storage. Restore it with `restore SOURCE --full-state --backend-volumes --restore-secret --overwrite` after installing the target state and stopping its stack; ordinary `--full-state` archives remain host-state-only.

For an operator-authorized real-client gate, use [`scripts/provider-acceptance.ps1`](scripts/provider-acceptance.ps1) with an explicit `-ClientCommand`. It is plan-only and credential-free by default; it starts an isolated Compose project, requires a new event matching the configured client/provider/source, checks privacy and a hash-based repository snapshot, restores owned client settings after explicit apply, and exits non-zero when the client emits no attributable telemetry. No provider command is run automatically.

## Architecture invariant

```text
normal LLM inference -> normal provider/client path
                         \
                          async bounded telemetry -> OTel Collector -> stores -> Grafana
```

Collector, storage, Grafana, malformed events, and telemetry queue saturation may degrade observability but must not block or redirect inference. See `docs/privacy.md`, `docs/capability-matrix.yaml`, and `docs/superpowers/plans/2026-08-07-llm-observatory-foundation.md` for the evidence boundary and implementation contract.

## Verification

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -v
python scripts/verify.py
docker compose --env-file "$env:LOCALAPPDATA\LLM-Observatory\compose.env" config
# When Docker Desktop is running, execute the full disposable runtime gate:
pwsh -NoProfile -File .\scripts\runtime-acceptance.ps1
```

The current disposable runtime gate passed on 2026-08-08 with Docker Engine 29.6.2 and digest-pinned service images, covering all provisioned dashboards, event-time query/filter behavior, fail-closed Collector privacy canaries, restart/recovery, full-state restore, queue recovery, and inference failure isolation. Static and unit verification still do not prove that a real client emits telemetry or that every dashboard panel is visually usable. Grafana's default `Observatory Events` Prometheus-compatible datasource evaluates normalized aggregates by event `observed_at`, so the dashboard time picker is event-time scoped; Collector self-metrics remain on the separate real `Prometheus` datasource. Use the API `start`/`end` filters for complete time-bounded SQLite analysis. Real-client acceptance, host reboot, signed image promotion, and a human visual sweep remain separate gates.
