# Production-readiness gates

This document separates repository evidence from runtime and provider evidence. A green local test run is necessary, but it is not a claim that the full deployment has been exercised.

> **Runtime gate status (2026-08-22): the disposable Docker runtime gate has been
> rerun on the host-native split and PASSES.** `pwsh -NoProfile -File
> .\scripts
untime-acceptance.ps1` returned `observatory.runtime-acceptance/v1`
> with `status: pass`, exit 0, and **29 of 29 checks passing, none failing**:
> `engine, install, doctor, start, api_bridge, health, synthetic_ingest,
> synthetic_agent_behavior, collector_delivery, claude_plain_key_mapping,
> project_attribution, project_attribution_working_directory, grafana,
> grafana_metrics, event_time_query_range, collector_self_observability,
> session_trace_query, collector_privacy_boundary, malformed_telemetry,
> dashboard_query_probe, dashboard_filter_probe, restart_recovery,
> grafana_failure_isolation, collector_failure_isolation,
> normalizer_outage_queue_recovery, full_disaster_recovery,
> storage_failure_isolation, api_failure_isolation, final_status`. This
> re-establishes runtime evidence for every gate whose path traverses the API,
> including the container-to-host bearer bridge (`api_bridge`), the corrected
> `httpMethod: GET` datasource against the GET-only read plane
> (`dashboard_query_probe`, `dashboard_filter_probe`), full-state restore into
> fresh volumes, and the four failure-isolation gates whose unmanaged inference
> sentinel was unaffected throughout (`inference_path: unmanaged/no-proxy`). A
> human visual sweep of the dashboards and a Windows host reboot remain separate
> manual gates.

> **Superseded — runtime gate status (2026-08-21):** the HTTP API has moved out of Docker Compose to run natively on the Windows host, split into a control/intake plane on `8787` and a GET-only dashboard-read plane on `8788`, with containers reaching it through `host.docker.internal` using generated bearer-token secrets. The last passing disposable Docker runtime gate ran on 2026-08-08 against the previous architecture, in which the API was a Compose service at `observatory-api:8787`. **That evidence does not carry over to any gate whose path traverses the API, and the gate has not been rerun.** Rows below that previously read "the current disposable Docker gate" are re-scoped accordingly. Local and static evidence is unaffected and still current: the unit suite (210 passed, 168 subtests, as of 2026-08-22), `python scripts/verify.py` (`{"failures": [], "status": "pass"}`), and `docker compose config --quiet` (exit 0 with an install-generated environment file).

> **Live-host findings (2026-08-22).** The stack was found reporting healthy while
> persisting nothing for 13 days. Six defects were identified against the running
> host and fixed; each is covered by a regression test in
> `tests/test_session_attribution.py` unless noted.
>
> | # | Defect | Evidence when found | State |
> |---|---|---|---|
> | 1 | Host API dead while Collector accepted OTLP | 717,360 spans / 41,607 metric points / 40,505 log records failed to `host.docker.internal:8787`; store last written 2026-08-09 | Fixed by restart; store now ingesting live |
> | 2 | Collector `send_batch_max_size: 512` exceeded Intake `max_records: 256`; any oversized batch returned HTTP 400 (`api.py:99`) and the Collector dropped all 512 as permanent | 99 rejected batches, `dropped_items: 512` per log line | Aligned to 256; **verified** 0 new rejections over 47,228 records |
> | 3 | SQLite WAL never truncated (no `journal_size_limit`); `storage_bytes()` counts the WAL, so it consumed the store budget | WAL reached 2.25 GB beside a 3.88 GB database | Bounded at 64 MiB; 2,249,301,720 bytes reclaimed; **verified** pinned at 64 MiB under load |
> | 4 | `_backfill_projections()` re-read every event's payload on **every** store open | 4 GB scan; API never reached readiness | Bounded anti-join; store open **0.25 s** over 389,478 events |
> | 5 | `os.kill(pid, 0)` reports a *detached* Windows process as dead (`WinError 87`), so `stop`/`uninstall`/`update`/`restore` orphaned a live API and deleted its process record | `stop` returned "removed stale host API process record" while pid 99912 held 8787/8788 | Windows-native liveness check; `stop` now reports "host API process … stopped" |
> | 6 | Normalized-store budget lived only in the launching process environment | `doctor` warned at 2 GiB while the API ran at 8 GiB | Budget persisted in `storage.max_database_bytes` + Compose env; **verified** with no env override |
>
> **Volume.** `codex-app-server` emitted ~758 structurally-empty internal spans/sec
> (model/provider/session unknown or null, no name, no measurements) — 99% of
> normalized-store volume at **350 GB/day**, which no budget survives. A
> `traces/normalizer` pipeline now filters them; Tempo still receives every span.
> Measured after the change: **5.43 GB/day**, all `model.operation`,
> `tool.operation`, metric, and log signals preserved.
>
> **Attribution.** 100% of live native-OTLP events were `project:unknown`
> (repository/branch NULL). Root cause: the Collector's `transform/project_identity`
> requires one of nine working-directory keys at `context: resource`, and neither
> client emits any of them. `session_projects` (migration 013) now binds a
> hook-reported session to its resolved project, and an unattributed event with a
> bound session inherits it, stamped `derived:session_binding` in field provenance.
> **Verified end to end against the live host**: a real `observatory hook` invocation
> produced `project=repo_sha256:7b0b039f…`, `repo=LLM-Observatory`,
> `branch=land/host-native-api-read-lane-split`, and created the binding.
>
> **Observation verdict and outcome correlation (added 2026-08-22).** `observe` and
> `GET /v1/observation` report a global `OBSERVATION_CAPABLE` verdict and one
> authoritative runtime state per client (`OBSERVING` / `DEGRADED` /
> `CONFIGURED_UNVERIFIED` / `NOT_CONFIGURED` / `UNSUPPORTED`) with last event time,
> project-attribution health, per-signal-family coverage, and exact blockers. Every
> coverage grade carries its numerator and denominator. Against this host the verdict
> is `NOT_OBSERVATION_CAPABLE`, correctly, because attribution is unresolved.
>
> Separately, the shipped outcome collectors produced **zero** `outcome_correlation`
> edges: none declared a correlation basis, and the store only builds an edge when told
> which shared identifier to join on. `observatory hook` now declares its session, and
> `run-outcome`/`git-snapshot` accept `--correlation-basis`, `--task-id`, and
> `--session-id`. `EventStore.outcome_value()` (`GET /v1/analytics/outcome-value`) is
> the first aggregation to cross `attribution_edges`. **Verified live**: a real
> `pytest` run recorded through `run-outcome`, correlated by `session_id` to a real
> Claude Code session in this repository, reports `claude-opus-5` at **$52.36 across
> 556 associated events** with cost coverage stated as **317/556**, and a client that
> reports no cost appears as `null` at ratio `0.0` rather than as the cheapest option.
> Rows carry `association_only: true`; the contract has no `caused_by` field.

> **Project attribution without client configuration (2026-08-22).** Attribution no
> longer depends on writing a hook into user-level client settings.
> `observatory bind-sessions --apply` discovers session-to-project bindings from
> `~/.claude/projects/<encoded-working-directory>/<session-id>.jsonl`, reading only
> directory and file names. **Verified live on this host:** 203 sessions discovered
> across **11 distinct projects**, all bound; live `claude-code` attribution moved
> from **0.0% to 81.9%** within two minutes, with events resolving to real
> repositories and branches (`Realmwalkers` / `main`, `ibkr-strategy-engine` / `main`
> — repositories other than this one, from concurrent real sessions). `codex`
> remains 0% attributed: it stores sessions elsewhere and is reported as such rather
> than assumed. Because stored events are immutable, the whole-window attribution
> ratio recovers only as attributed telemetry accumulates; `observe` therefore
> grades attribution health on recent telemetry and reports the window ratio beside
> it, and correctly still returns `NOT_OBSERVATION_CAPABLE` until enough attributed
> telemetry has accrued.

> **Codex cannot be attributed, and this is a client limitation rather than a gap
> to close (2026-08-22).** Across 513,814 `codex-app-server` events the telemetry
> carries `trace_id`/`span_id` on 98.7% and **zero** `session_id`, `workflow_id`,
> `agent_id`, `task_id`, `worktree`, `commit_sha`, `branch`, or `repository`. Codex
> does record a working directory in its own rollout files
> (`~/.codex/sessions/**/rollout-*.jsonl`, first line `type: session_meta`), but the
> telemetry contains no identifier to join those to, so no host-level source can
> bind it. `observe` states this as an exact blocker rather than implying a fix
> exists, and distinguishes it from `claude-code`, whose sessions are present and
> bindable. Attribution for codex requires the client to emit a working-directory or
> session attribute.
>
> **Continuous recording started (2026-08-22).** A Windows scheduled task
> `LLM Observatory Recorder` runs `bind-sessions --apply` followed by
> `observe --record` every 15 minutes. Its first scheduled execution recorded a
> snapshot and returned `5`, the CLI's own not-observation-capable exit code, so the
> scheduler's Last Result doubles as an alert signal. Remove it with
> `schtasks /Delete /TN "LLM Observatory Recorder" /F`. The longitudinal evidence the
> release boundary needs now accrues without further action; the weeks themselves
> have not yet elapsed.

> **Automatic engineering-outcome capture (2026-08-22).** Before this, the only
> outcomes in the store were 2 manual `tests` records and 135 client lifecycle
> events; no code observed commits, tests, CI, or PRs, so the analysis surface
> would have stayed empty no matter how long the deployment ran. `collect-outcomes
> --apply` now observes commits with a read-only `git log` across every project
> known from the host session directory. **Verified live:** 11 projects scanned, 10
> with commits, **358 commit outcomes recorded**, lifting `outcome_correlation`
> edges from ~130 to **5,256** across three bases (`session_id`, `project_window`,
> and `None` for honestly unjoinable records). Commit messages, author identities,
> and file paths are never retained.
>
> `engineering-value` still reports `ranked: 0` with every configuration under
> `insufficient_evidence`, and the reason is recorded rather than hidden: no commit
> has yet correlated, because the temporal window looks backwards from a commit for
> telemetry in the same project, and project attribution only began working after
> the newest commit landed. The rule itself is proven by test — association within
> the window, rejection outside it, rejection across projects, and rejection of work
> that happened after the commit. Correlation will occur as attributed telemetry and
> new commits overlap.

> **Correlation performance and intake isolation (2026-08-22).** The temporal
> association query had no supporting index: SQLite fell back to the
> `outcome_kind` index, which is useless because almost every row has
> `outcome_kind NULL`, so each lookup scanned the whole events table. Correlation
> runs on the append path, so that cost would have been paid on every commit the
> scheduled recorder ingests. Measured on the live 500k-event store, replaying
> correlations took **over 9 minutes and starved intake to 1,270 unavailable
> records**. Migration `015_temporal_correlation_indexes.sql` adds
> `(project_id, observed_at)` and `(session_id, observed_at)`, the query was split
> into two index-friendly branches instead of one `OR`, and the replay commits in
> small batches so maintenance never holds the writer lock against intake. After:
> **15.7 seconds and 3 unavailable records** -- a ~35x improvement with intake
> effectively unaffected.

> **Pass/fail outcome supply (2026-08-22).** Commits record that work landed, never
> whether it worked, so a store fed only by commits can never produce a success
> rate however long it runs -- `engineering-value` correctly refused to rank on
> 358 commits plus a single test result. `collect-outcomes` now also observes
> GitHub Actions results through an authenticated `gh`, recording run metadata
> only (conclusion, head SHA, workflow name, timestamp) and never job logs.
> **Verified live:** **511 CI runs recorded across 7 of 11 projects**, mapping
> `success/failure/cancelled/timed_out` onto `passed/failed/aborted/timeout`; a run
> still in progress is skipped rather than guessed at, and an unavailable `gh` is
> treated as an absent source rather than an error.
>
> None of those CI outcomes correlate yet, and the reason is measured rather than
> assumed: attributed LLM telemetry begins at **2026-08-23T01:10**, when attribution
> started working, while the newest CI run is **2026-08-22T03:58** -- the histories
> do not overlap. 14 commits from today, in projects with attributed telemetry, did
> correlate. New CI runs will correlate against the telemetry that precedes them.

> **Why no CI outcome has correlated yet, diagnosed at project level.** Five
> projects have both CI runs and bound telemetry, but in every one the newest CI
> run (2026-08-20/21) predates that project's telemetry window (2026-08-22/23):
> `ibkr-strategy-engine` 34 runs vs telemetry from 08-22T21:16, `Realmwalkers` 100
> vs 08-22T23:43, `perf-lab-api` 100 vs 08-22T23:44; `leave-sprint` and
> `Compounding-Quality-RAG` have telemetry only from 08-08/09, older than their
> runs; `dominion-realm` and `Project-Eilixa` have 100 runs each and no bound
> telemetry at all. The association window looks backwards from an outcome, so it
> requires a CI run *after* the work it should be associated with. **No CI has run
> in any of these repositories since project attribution began working.** The next
> push to any of them will produce a correlating outcome. This was verified by
> executing the association query directly against a real CI outcome -- it is a
> data-overlap fact, not a defect in the rule, which is separately covered by test.

> **Schema-drift detection (2026-08-22).** The last of the failure modes this
> deployment is meant to detect. Signal-family coverage was computed live and
> never persisted, so a client that quietly stopped reporting cost or session
> identity after an upgrade would have degraded invisibly. Snapshots now record
> the ratios per client, and `coverage_drift()` compares each family against the
> median of that client's own recent history. **Verified live:** ratios are being
> recorded (`claude-code` cost 0.137, latency 0.631, model_identity 0.366;
> `codex` cost 0.000, latency 0.961), and five tests cover a field that stops
> arriving, stable coverage, a small dip that must not be reported, an outlier
> that must not move the median, and a single snapshot that cannot yield a
> finding.

> **Which comparisons are actually obtainable (measured 2026-08-22).** Dimension
> coverage against real telemetry, scoped per client and event type so absent
> data is never averaged against reporting clients:
>
> | dimension | claude-code `model.operation` (9,379) | codex `model.operation` (9,986) |
> |---|---|---|
> | session | 100% | 0% |
> | latency | 97% | 16% |
> | repository | 54% | 0% |
> | model | 44% | 84% |
> | tokens | 42% | 0% |
> | cost | 21% | 0% |
> | agent | 20% | 0% |
> | retries | 21% | 0% |
> | skill / workflow / subagent / task_id | 0% | 0% |
>
> Two boundaries follow, and neither is a defect. **`skill`, `workflow`,
> `subagent`, and `task_id` are reported by no client at all**, so the
> Skills/Workflows and subagent comparisons cannot be populated from native
> telemetry however long the deployment runs; they require a client that emits
> them or an explicit operator-supplied `--task-id`. And **codex cannot be
> cost-compared**: it reports model identity but no session, cost, tokens, or
> repository, so including it in a cost ranking would compare a reporting client
> against an absent one. `observe` grades these per signal family so the gap is
> visible rather than inferred from an empty dashboard.

> **Skill and workflow made obtainable (2026-08-22).** The dimension measurement
> above showed `skill` and `workflow` at 0% across every client and event type,
> which would have made the Skills/Workflows comparisons permanently empty rather
> than merely awaiting data. The cause was not that clients withhold it: the hook
> payload carries the invoked tool and its identifier, and `build_hook_event` kept
> that only as a tool attribute, never mapping it onto `execution.skill` or
> `execution.workflow_id`. It now does, for `Skill`/`Workflow` tool invocations,
> reading **only the identifier** -- the rest of `tool_input` is arguments, which
> the privacy boundary excludes, and a test asserts a sensitive argument value
> never reaches the event. `grok` already has `PostToolUse` hooks applied on this
> host, so the dimension becomes populated as those hooks fire; `claude` requires
> `configure claude --traces --apply` first.

> **Still unverified.** The user-level Claude Code `SessionStart` hook has *not*
> been written to `~/.claude/settings.json`, so real Claude Code sessions do not yet
> produce bindings automatically — run
> `observatory configure claude --traces --apply`. The disposable Docker runtime
> gate has still not been rerun on the host-native split. A stale orphan copy of the
> package remains at `C:\Program Files\Python314\Lib\site-packages\observatory`
> (pip does not track it); it is currently shadowed by a user-site editable install
> but should be deleted.

## Current evidence

| Gate | Evidence | State |
|---|---|---|
| Normalized contract, redaction, adapters, projections | `tests/` and `scripts/verify.py` | Verified locally |
| Provider/client capability catalog and configure-all parity | Executable `CLIENT_SPECS` catalog, capability contract tests, and read-only doctor probes | Verified locally; provider behavior remains evidence-scoped |
| Append-only ledger, migrations, backfill, host-state and backend-volume backup/restore, audited purge | `tests/test_store.py`, `tests/test_maintenance.py` | Verified locally |
| Full-state restore rollback | Staged host-file and named-volume preimages plus simulated mid-restore failure test | Verified locally; the disposable Docker gate that covered it ran on 2026-08-08 against the pre-split Compose API and has not been rerun |
| Upgrade rollback | Pre-update SQLite backup, Compose image-ID capture/re-tag, validated database restore, stack recreation, and simulated failed-start rollback test | Verified locally; live image-update rollback remains an operator gate |
| Collector privacy boundary | Exact-key fail-closed allowlist, blocked-value rules, and downstream privacy canaries | Verified locally; the normalizer/Tempo/Loki/Prometheus runtime canaries passed in the 2026-08-08 pre-split gate and await a rerun on the host-native split |
| Native OTLP project attribution | Collector derives a deterministic project ID from an explicit root/path or common native working-directory resource attribute before raw path deletion | Verified in the 2026-08-08 pre-split disposable Docker gate; that path now traverses the host-native control plane and awaits a rerun. Clients that emit no project context remain `project:unknown` |
| Normalized agent behavior and reliability contract | `AgentBehavior`, source-reported agent/subagent/parent-agent hierarchy, migrations `010_agent_behavior.sql`, `011_reliability_dimensions.sql`, and `012_parent_agent.sql`, immutable measurement facts, Prometheus context metrics, Agent Hierarchy dashboard, and default raw-activity redaction | Verified locally; its runtime half passed only in the 2026-08-08 pre-split disposable gate and awaits a rerun. Source-reported non-unknown parent identities still require a real client/profile |
| Offline spool, replay, bounded input, malformed-batch behavior | `tests/test_failure_isolation.py`, `tests/test_api.py` | Verified locally |
| Exporter queue saturation and inference isolation | `scripts/queue-saturation-acceptance.ps1` with a two-item blackhole-exporter queue, Collector self-metrics (`queue_capacity=2`, `enqueue_failed_spans=63`), log evidence, and inference sentinel | Verified runtime on the digest-pinned Collector 0.158.0 image; the run passed with 64 attempts. This gate uses its own standalone Collector and never touches the Observatory API, so the host-native split does not invalidate it |
| Collector self-observability | Collector internal Prometheus reader on `8888`, Prometheus scrape job, and Reliability panels for queue/failure counters | Verified locally; the runtime half passed in the 2026-08-08 pre-split disposable gate and awaits a rerun |
| Normalized-store byte budget, degraded readiness, and capacity metrics | `tests/test_store.py`, `tests/test_api.py` | Verified locally |
| Backend-volume capacity guard | `observatory doctor` resolves only the five installed backend volumes and reports Docker-reported usage against the configurable soft budget; `observatory start` refuses an already-over-budget installed stack; unit coverage includes size parsing, missing-volume warning, unrelated-volume isolation, and the start refusal | Verified locally; current live Docker volume measurement is an operator check |
| Windows install, idempotent state, Compose environment alignment | `tests/test_cli.py`, `observatory install` | Verified locally |
| First-run dashboard usability | `observatory demo` / `observatory install --demo` seed the bundled six-record metadata-only walkthrough idempotently; clean production installs remain free of synthetic rows unless explicitly requested | Verified locally |
| Compose interpolation, loopback ports, dashboard JSON | `docker compose config --quiet`, dashboard parse | Verified locally |
| Container startup, health checks, restart recovery, Collector binary validation | `scripts/runtime-acceptance.ps1` on Docker Engine 29.6.2, including a Claude-shaped plain-key OTLP log probe (`claude-code` -> `anthropic`, model/session identity, usage, cost, duration, and TTFT) | Previous disposable runtime gate passed before the host-native API/read-lane split; current Compose/static contracts pass, but the updated host-native runtime gate remains pending |
| Independent adversarial verification | Sealed independent-verification round 1 packet, baseline ratchet, 198-test unit gate, repository verifier, and a fresh read-only recheck of the repaired OTLP metric-context seam (18 bridge tests, 35 contract/store checks, direct two-event SQLite probe) | Partial; the repaired seam and local/static evidence pass, but the sealed round-1 verifier could not access Docker's named pipe, so its queue/runtime criteria were `BLOCKED-UNVERIFIABLE` and its mechanical verdict was `FAIL` |
| Native telemetry from supported client profiles | `scripts/provider-acceptance.ps1` provides an explicit-command, before/after, privacy, identity, and repository-cleanliness gate. An authorized Claude Code 2.1.226 session on 2026-08-08 emitted 7 real OTLP log events; startup, privacy, repository-cleanliness, and configuration cleanup passed, but the identity/model assertion exposed that Claude's documented plain event keys were dropped or ignored. The Collector allowlist and bridge mapping are now fixed and covered by a regression test, and by the 2026-08-08 disposable runtime gate as it stood before the host-native API split. | Partial; both a post-fix real-client rerun and a post-split disposable runtime rerun are still required |
| Grafana datasource/dashboard provisioning and synthetic OTLP delivery | `scripts/runtime-acceptance.ps1`, `scripts/verify.py`, and deployment contract tests | Previous disposable runtime gate passed; the updated host-native bridge and bearer-secret path are statically verified and await a fresh Docker runtime gate |
| Grafana dashboard query execution, visual usability, and filter behavior | On 2026-08-08 the isolated harness executed 80 metric/log/trace panel queries across all ten dashboards, including Agent Hierarchy parent-agent selectors, event-time range queries, a scoped Tempo session TraceQL query, Collector self-metric queries, and project-scoped event/token/retry/execution/workflow/agent/outcome filters — but against the pre-split combined-plane API. Every one of those queries now traverses the GET-only read plane at `host.docker.internal:8788`, and a defect on that path was found statically and fixed in this change: the provisioned Observatory Events datasource was set to `httpMethod: POST`, which the read plane rejects outright (`src/observatory/api.py:838-848`), so every panel would have failed. | Pending; the 80-query evidence is pre-split and does not carry over. The `httpMethod: GET` correction in `deployment/grafana/provisioning/datasources/datasources.yaml` is statically verified only and has not been executed against a running stack. Human visual sweep also remains pending |

The reproducible runtime gate is [`scripts/runtime-acceptance.ps1`](../scripts/runtime-acceptance.ps1). It uses an isolated Compose project with dynamically allocated loopback ports, installs into a disposable state directory by default, sends synthetic JSONL and OTLP data (including a Collector privacy canary and recovery log), rejects malformed telemetry without destabilizing readiness, verifies Grafana datasource/dashboard provisioning, event-time Prometheus-compatible query visibility, and real Prometheus Collector self-metrics, exercises a full stopped-stack backend-volume backup/removal/restore into fresh volumes, restarts services, and checks with an unmanaged inference sentinel that Grafana, Collector, and normalizer/storage outages do not make the inference path unavailable. Run it only after Docker Desktop is reachable:

```powershell
pwsh -NoProfile -File .\scripts\runtime-acceptance.ps1
```

The script exits non-zero on any failed gate and emits `observatory.runtime-acceptance/v1` JSON. The most recent passing run was on 2026-08-08, against the previous architecture in which the API was a Compose service at `observatory-api:8787`. That run verified fresh install, six JSONL events including agent-failure/reassessment/rework dimensions, a documented Claude-shaped plain-key log mapping to provider/model/session/measurements, OTLP trace/log/metric delivery, malformed-event rejection, the fail-closed Collector privacy canaries, event-time filter queries through the Observatory Events datasource, Collector self-metrics through the real Prometheus datasource, downstream absence checks through the normalized API, Tempo, Loki, and Prometheus, all ten dashboards and 80 panel queries, stopped-stack full-state restore, service restart recovery, unmanaged inference sentinels, and failure isolation. The host-native control/read split invalidated that evidence for every gate whose path traverses the API, and the gate has not been rerun; the script itself has been updated for the split but its result is pending. When it does pass again it will prove parent-agent query/provisioning compatibility; a real client/profile is still needed to populate non-unknown parent identities. Its default cleanup removes the isolated Compose project and named volumes; pass `-KeepVolumes` when deliberately inspecting retained backend state, and pass `-KeepState` when retaining the generated host state. This harness still does not replace provider-profile acceptance or a human visual sweep of the dashboards.

## Explicitly pending external gates

These are not hidden behind the local green gates:

| Gate | Current boundary | Required proof |
|---|---|---|
| Native client emission | One user-authorized Claude Code session emitted real OTel logs, but the first run failed the normalized provider/model identity assertion; the documented plain-key mapping and fail-closed Collector allowlist are now corrected. | Run one post-fix, user-authorized Claude/Codex/Gemini or other supported-client session and pass source, model, session, retry, and privacy assertions end to end. |
| Host reboot/startup | The host-native API owns the canonical `%LOCALAPPDATA%\\LLM-Observatory\\data\\events.sqlite3` path, while Compose contains only the telemetry backends and reaches the API through `host.docker.internal`; lifecycle startup records and probes the managed API process on both control and read ports. | Runtime proof still requires a fresh Windows host reboot and post-reboot `doctor`/`status`; the former deleted Docker bind path is no longer part of the hot read path. |
| Full single-host disaster recovery | `backup --full-state --backend-volumes` covers the five Compose named volumes with manifest/checksum validation and stopped-stack guards; the 2026-08-08 pre-split disposable Docker gate restored into fresh volumes and retained normalized events, Prometheus metrics, Tempo traces, and Loki data. | Rerun the disposable gate on the host-native split to re-establish restore proof, then add off-host encrypted backup storage and a host-loss rehearsal, which remain operator/environment gates. |
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

Then verify both API planes independently: control/intake readiness at `http://127.0.0.1:8787/readyz` and read-plane readiness at `http://127.0.0.1:8788/readz`, with `/metrics` now served only by the read plane at `http://127.0.0.1:8788/metrics` (`src/observatory/api.py:834-848`). Then check Collector health at `http://127.0.0.1:13133/`, every provisioned dashboard, Collector-to-normalizer delivery, and service restart recovery. Stop Grafana, the Collector, and each API plane independently and confirm that the normalizer/client-plan path remains available. A host restart and real provider/client emission remain separate manual gates.

## Safety boundaries

The baseline is metadata-only. The host API and Collector do not accept provider credentials, the Collector has no production debug exporter, client configuration never changes provider endpoints or proxy variables, and the host-native API shares the installed state with offline CLI maintenance without a Windows-to-Docker database bind. Grafana's default Observatory Events datasource is a bounded, read-only Prometheus compatibility facade over SQLite that evaluates normalized aggregates by event `observed_at`; the separate Prometheus datasource serves Collector self-metrics. The event-time facade accepts only the dashboard metric families/operators, caps query ranges at 10,000 points, rejects oversized expressions/regex matchers, and bounds returned series and matrix cells. Use `/v1/summary` or `/v1/analytics/comparison` with `start` and `end` filters for the complete unbounded SQLite analysis surface.

The default deployment is a single-host profile. SQLite, local backend volumes, and file-backed Collector queues are restart-durable but are not a high-availability or disaster-recovery claim. Measure host disk capacity, encrypt backups outside the repository, and test restore into fresh volumes before treating the deployment as production capacity.

## Mandatory failure coverage

| Required failure case | Current evidence | State |
|---|---|---|
| Collector unavailable | Runtime stop/restart plus unmanaged inference sentinel and API readiness check | Verified in the 2026-08-08 pre-split runtime gate; rerun pending |
| Grafana unavailable | Runtime stop/restart plus unmanaged inference sentinel | Verified in the 2026-08-08 pre-split runtime gate; rerun pending |
| Telemetry storage/normalizer unavailable | Runtime API stop, independent client plan, and unmanaged inference sentinel | Verified in the 2026-08-08 pre-split runtime gate for the single-host normalizer boundary; the API is now a host process rather than a Compose service, so this rerun is pending |
| Malformed telemetry | HTTP 400 rejection with readiness preserved; sibling/unit rejection tests | Verified locally; the runtime half is from the 2026-08-08 pre-split gate and awaits a rerun |
| Exporter queue saturation | Two-item non-blocking blackhole exporter, Collector self-metric failure counters, log evidence, inference sentinel | Verified runtime; this standalone Collector gate does not traverse the API and is unaffected by the split |
| Unknown provider/model | Normalized event retained and surfaced in store tests | Verified locally |
| Unknown repository | Deterministic non-Git fallback identity and project-resolution tests | Verified locally |
| Duplicate events | Idempotent duplicate status and append-only ledger tests | Verified locally |
| Machine restart | Container restart and full-stack restore are verified; Windows host reboot is not automated | Partial; host reboot pending |
| Observatory removal | Ownership-aware client configuration removal and conflict tests; provider harness cleanup path | Verified locally; real-client restoration pending |
| Provider credentials in telemetry | API/Collector rejection, fail-closed Collector allowlist, and redaction canaries | Verified locally; the synthetic runtime canary is from the 2026-08-08 pre-split gate. Both a rerun and real-client validation remain pending |
| Prompt/completion persistence by default | API, OTLP, privacy-policy, and expanded Collector canaries | Verified locally; the synthetic runtime canary is from the 2026-08-08 pre-split gate. Both a rerun and real-client validation remain pending |
