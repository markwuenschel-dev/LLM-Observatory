# Production-readiness gates

This document separates repository evidence from runtime and provider evidence. A green local test run is necessary, but it is not a claim that the full deployment has been exercised.

## Current evidence

| Gate | Evidence | State |
|---|---|---|
| Normalized contract, redaction, adapters, projections | `tests/` and `scripts/verify.py` | Verified locally |
| Append-only ledger, migrations, backfill, backup/restore, audited purge | `tests/test_store.py`, `tests/test_maintenance.py` | Verified locally |
| Offline spool, replay, bounded input, malformed-batch behavior | `tests/test_failure_isolation.py`, `tests/test_api.py` | Verified locally |
| Windows install, idempotent state, Compose environment alignment | `tests/test_cli.py`, `observatory install` | Verified locally |
| Compose interpolation, loopback ports, dashboard JSON | `docker compose config --quiet`, dashboard parse | Static only |
| Container startup, health checks, restart recovery, Collector binary validation | Docker Desktop runtime | Pending runtime gate |
| Native telemetry from supported client profiles | Disposable Claude/Codex/Gemini profiles | Pending provider gate |
| Grafana/Tempo/Loki visual and query behavior | Running Compose stack with synthetic fixture | Pending runtime gate |

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

Then verify API readiness, `/metrics`, Collector health at `http://127.0.0.1:13133/`, every provisioned dashboard, Collector-to-normalizer delivery, a service restart, and a host restart. Stop Grafana, the Collector, and the API independently and confirm that an ordinary provider/client request remains on its original route.

## Safety boundaries

The baseline is metadata-only. The API and Collector do not accept provider credentials, the Collector has no production debug exporter, client configuration never changes provider endpoints or proxy variables, and the Compose API shares the installed host state directory with offline CLI maintenance. Prometheus panels expose current store snapshots; use `/v1/summary` or `/v1/analytics/comparison` with `start` and `end` filters for time-bounded SQLite analysis.

The default deployment is a single-host profile. SQLite, local backend volumes, and file-backed Collector queues are restart-durable but are not a high-availability or disaster-recovery claim. Measure host disk capacity, encrypt backups outside the repository, and test restore into fresh volumes before treating the deployment as production capacity.
