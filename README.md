# LLM Observatory

LLM Observatory is a provider-agnostic, metadata-first telemetry platform for LLM-assisted development. It observes clients externally and keeps normal inference on its existing provider path.

## Current state

The repository contains the first vertical slice:

- versioned normalized events with unknown-field retention;
- provenance-aware usage and outcome fields plus append-only ledger, measurement, and attribution projections;
- default redaction of prompt/completion content, sensitive tool values, credentials, and raw paths;
- automatic read-only Git project resolution;
- idempotent SQLite WAL storage with conflict diagnostics;
- bounded loopback HTTP intake, health, metrics, summary, and event queries;
- offline-safe JSONL ingestion and lifecycle CLI;
- capability-backed client configuration plans for Claude Code, Codex, and Gemini, a generic adapter registry, and caller-owned direct-provider/OpenRouter response adapters;
- provisioned Docker Compose services for the OTel Collector, Tempo, Loki, Prometheus, Grafana, and the normalizer API.
- provisioned dashboard families for global comparison, efficiency, projects, reliability, outcomes, skills/workflows, agents, and sessions;

Client-native capability claims remain explicitly partial or unknown until verified against current first-party documentation and disposable local profiles. `configure <client>` is plan-only by default; `--apply` is required for guarded user-level telemetry settings. The Observatory does not set provider base URLs, proxy variables, credentials, or repository files.

## Quick start on Windows

```powershell
python -m pip install .
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" install
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" doctor
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" start
python -m observatory.cli open
```

The package can be run from a checkout with `PYTHONPATH=src` or installed with `python -m pip install .`. Docker Desktop must be running in Linux-container/WSL2 mode before `start`. The generated state directory contains the SQLite database, bounded spool, Compose environment file, and Grafana secret; it is never created in an observed repository. Compose bind-mounts that same state directory into the API container, so offline CLI maintenance and the live API see one SQLite store. Status/doctor also probe the loopback-only Collector health endpoint at `127.0.0.1:13133` so normalizer, exporter, and dashboard health remain distinct.

## Ingest metadata safely

JSON Lines records must include `schema_version`, `event_id`, `event_type`, and a timezone-aware `observed_at`. Unknown provider/model values are valid. The default path redacts sensitive fields before persistence and before API delivery.

```powershell
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" ingest --file .\examples\synthetic-events.jsonl --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" flush --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" status --url http://127.0.0.1:8787/healthz
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" configure claude
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" retention --prometheus-days 30 --tempo-hours 720 --loki-hours 336
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" record-outcome --kind tests --status passed --correlation-id run-123 --offline
python -m observatory.cli --state-dir "$env:LOCALAPPDATA\LLM-Observatory" run-outcome --kind tests --offline -- python -m unittest
```

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
```

Static and unit verification do not prove that Docker Desktop is running, that a real client emits telemetry, or that a dashboard is visually usable. Prometheus dashboard panels expose current store snapshots; use the API `start`/`end` filters for time-bounded SQLite analysis. These remain separate runtime/manual gates.
