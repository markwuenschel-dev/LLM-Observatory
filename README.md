# LLM Observatory

[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/downloads/)
[![uv / uvx](https://img.shields.io/badge/uv%20%2F%20uvx-latest-6E56CF?style=for-the-badge&logo=astral&logoColor=white)](https://docs.astral.sh/uv/)
[![pnpm](https://img.shields.io/badge/pnpm-11%2B-F69220?style=for-the-badge&logo=pnpm&logoColor=white)](https://pnpm.io/)
[![Docker Desktop](https://img.shields.io/badge/Docker%20Desktop-Linux%20containers-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/products/docker-desktop/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-OTLP-F5A800?style=for-the-badge&logo=opentelemetry&logoColor=white)](https://opentelemetry.io/)
[![Grafana](https://img.shields.io/badge/Grafana-dashboards-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Tests](https://img.shields.io/badge/tests-190%20passing-2EA44F?style=for-the-badge)](tests/)
[![Last commit](https://img.shields.io/github/last-commit/markwuenschel-dev/LLM-Observatory?style=for-the-badge)](https://github.com/markwuenschel-dev/LLM-Observatory/commits/main)

Provider-agnostic, metadata-first observability for LLM-assisted development.

LLM Observatory watches client activity externally and sends bounded telemetry to an OpenTelemetry Collector, normalized SQLite projections, Prometheus, Tempo, Loki, and Grafana. Normal inference stays on its existing provider path: the Observatory is not an inference proxy, credential store, or provider gateway.

## Why this exists

- Compare models, sessions, projects, outcomes, skills, workflows, and agent hierarchies.
- Correlate usage, latency, cost, reliability dimensions, tests, builds, commits, and reviews without claiming causation.
- Keep prompts, completions, secrets, sensitive tool values, and raw filesystem paths out of the default persistence path.
- Continue client inference when telemetry, dashboards, storage, or the bounded Collector queue is degraded.
- Start with synthetic metadata, then add explicitly authorized native client telemetry.

## Stack

| Layer | Technology |
| --- | --- |
| Runtime | Python 3.14+ recommended; package metadata remains compatible with Python 3.11+ |
| Fast tooling | [uv](https://docs.astral.sh/uv/) and [uvx](https://docs.astral.sh/uv/guides/tools/) |
| Optional JavaScript tooling | Node.js current/LTS plus [pnpm](https://pnpm.io/) 11+ |
| Telemetry | OpenTelemetry Protocol (OTLP) and Collector |
| Storage | SQLite WAL, Prometheus, Tempo, Loki |
| Dashboards | Grafana |
| Local orchestration | Docker Desktop in Linux-container/WSL2 mode |

## Quick start

### Windows PowerShell

Install the current Python 3.14+ and Docker Desktop, then run the CLI directly from the checkout with `uvx`:

```powershell
cd C:\Users\Nalakram\Documents\GitHub\LLM-Observatory
$state = "$env:LOCALAPPDATA\LLM-Observatory"

uvx --python 3.14 --from . observatory --state-dir $state install --demo
uvx --python 3.14 --from . observatory --state-dir $state doctor
uvx --python 3.14 --from . observatory --state-dir $state start
uvx --python 3.14 --from . observatory open
```

The explicit `install --demo` walkthrough is provider-free, metadata-only, and idempotent. Omit `--demo` when the state must contain only real telemetry. The generated state directory contains the SQLite database, bounded spool, Compose environment, and Grafana secret; it is never created inside an observed repository.

For a normal editable development environment:

```powershell
$state = "$env:LOCALAPPDATA\LLM-Observatory"
uv python install 3.14
uv venv --python 3.14
.\.venv\Scripts\Activate.ps1
uv pip install -e .
observatory --state-dir $state install
```

Docker Desktop must be running in Linux-container/WSL2 mode before `start`. Default host bindings are loopback-only:

- Grafana: [http://127.0.0.1:3000](http://127.0.0.1:3000)
- API health: [http://127.0.0.1:8787/healthz](http://127.0.0.1:8787/healthz)
- OTLP gRPC: `127.0.0.1:4317`
- OTLP HTTP: `127.0.0.1:4318`
- Collector health: `127.0.0.1:13133`

### Recovering stale Compose state

If `doctor` reports that the live `llm-observatory` project references stale generated state, stop retrying `start`. Reconcile only that project, preserving its named volumes and host state:

```powershell
$state = "$env:LOCALAPPDATA\LLM-Observatory"
observatory --state-dir $state install
docker compose --project-name llm-observatory `
  --env-file "$state\compose.env" down --remove-orphans
observatory --state-dir $state start
observatory --state-dir $state doctor
```

Do not add `-v`: removing volumes deletes durable telemetry and dashboard data. The command above removes only the stale project containers and network so Compose can recreate them against the installed state.

## Ingest a first event set

```powershell
uvx --python 3.14 --from . observatory --state-dir $state ingest `
  --file .\examples\synthetic-events.jsonl --offline
uvx --python 3.14 --from . observatory --state-dir $state flush --offline
uvx --python 3.14 --from . observatory --state-dir $state status
```

JSONL records require `schema_version`, `event_id`, `event_type`, and a timezone-aware `observed_at`. Unknown provider and model values are valid. The default path redacts sensitive fields before persistence and API delivery.

## Optional pnpm / JavaScript tooling

This repository has no JavaScript application or `package.json`; pnpm is an optional, forward-looking toolchain for dashboard extensions or a future web UI. Install the current pnpm release with Node.js available:

```powershell
npm install --global pnpm@latest
pnpm --version
pnpm self-update
```

Do not install pnpm as a prerequisite for the Python Observatory CLI.

## Always-on client telemetry

Configure the user-level clients once. Their normal subscription/API inference path stays unchanged; Observatory receives metadata only, with prompt/tool content disabled:

```powershell
uvx --python 3.14 --from . observatory --state-dir $state configure claude --apply --traces
uvx --python 3.14 --from . observatory --state-dir $state configure codex --apply --traces
uvx --python 3.14 --from . observatory --state-dir $state configure gemini --apply --traces
uvx --python 3.14 --from . observatory --state-dir $state configure kimi --apply
uvx --python 3.14 --from . observatory --state-dir $state configure grok --apply
```

Claude Code, Codex, and Gemini use their documented global OTLP settings. Kimi and Grok use marked global observation hooks that call the fail-open `observatory hook` adapter. The hook resolves the current working directory to a privacy-safe project identity, so a new Git repository appears automatically without repository files, dependencies, or hooks. If the Collector or API is unavailable, the hook spools bounded metadata or drops it; it never blocks inference. Cursor remains adapter-only until a verified global hook/OTLP contract exists.

Review a plan without changing user files by omitting `--apply`. The Observatory never sets provider base URLs, proxy variables, credentials, or repository files. A real-client acceptance run requires an explicit operator-authorized client command:

```powershell
pwsh -NoProfile -File .\scripts\provider-acceptance.ps1 `
  -Client claude `
  -ClientCommand claude,"--print","<operator-authorized acceptance prompt>"
```

The harness is plan-only and credential-free by default. It uses an isolated Compose project, requires attributable new telemetry, checks repository privacy through hashes, and never runs a provider command automatically.

## Operational commands

The commands below assume the editable environment is active. Otherwise use the `uvx --python 3.14 --from . observatory` form from Quick start.

```powershell
observatory --state-dir $state status
observatory --state-dir $state retention --prometheus-days 30 --tempo-hours 720 --loki-hours 336
observatory --state-dir $state backup "$env:USERPROFILE\LLM-Observatory-state.zip" --full-state
observatory --state-dir $state record-outcome --kind tests --status passed --correlation-id run-123 --offline
observatory --state-dir $state run-outcome --kind tests --offline -- python -m unittest
observatory --state-dir $state stop
```

Do not use `docker compose down -v` during normal operation. Use the explicit audited `prune --confirm` path for normalized-event deletion. Encrypt backup archives before storing them outside the host.

## Architecture invariant

```text
normal LLM inference -> normal provider/client path
                         \
                          async bounded telemetry -> OTel Collector -> stores -> Grafana
```

Collector, storage, Grafana, malformed events, and telemetry queue saturation may degrade observability but must not block or redirect inference.

## Verification

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p "test_*.py"
python scripts/verify.py
docker compose --env-file "$env:LOCALAPPDATA\LLM-Observatory\compose.env" config

# Requires Docker Desktop; runs the isolated full runtime gate.
pwsh -NoProfile -File .\scripts\runtime-acceptance.ps1

# Isolated exporter backpressure proof.
pwsh -NoProfile -File .\scripts\queue-saturation-acceptance.ps1
```

The local unit suite currently reports 190 passing tests and the static verifier reports no failures. Runtime acceptance proves service, query, privacy, recovery, and failure-isolation behavior; it does not replace a real-client telemetry run, a human dashboard visual sweep, off-host encrypted recovery, host reboot proof, or signed-image promotion.

## Documentation

- [Operations and recovery](docs/operations.md)
- [Architecture](docs/architecture.md)
- [Privacy boundary](docs/privacy.md)
- [Capability evidence](docs/capability-evidence.md)
- [Production readiness](docs/production-readiness.md)
- [Superpowers implementation plan](docs/superpowers/plans/2026-08-07-llm-observatory-foundation.md)

## License and status

The repository does not currently declare a license file. Treat this as an active, production-oriented foundation with explicit remaining deployment gates—not as a completed production sign-off.
