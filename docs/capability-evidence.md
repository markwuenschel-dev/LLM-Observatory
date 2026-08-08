# Provider and client capability evidence

This matrix records current evidence collected on 2026-08-08. A capability claim describes what the client or provider documents, not what the Observatory has already integrated. `VERIFIED_FIRST_PARTY` means current first-party material was inspected; `VERIFIED_LOCALLY` means the installed executable or local behavior was observed; `INSTALLED_NOT_VERIFIED` means an executable was found but its bounded version probe was blocked, timed out, or did not yield trustworthy output; `PARTIAL` means only some modes or fields are covered; `SUPPORTED_NOT_LOCALLY_VERIFIED` means documented but not exercised here; `UNKNOWN` means no safe claim.

| Target | Evidence-backed current capability | Safe Observatory posture |
|---|---|---|
| Claude Code 2.1.226 | First-party monitoring documents OTel metrics and logs/events, beta traces, session/tool/API/retry/auth data, model and token fields, user/managed settings, and subscription/API separation. Installed version is locally verified; telemetry is not locally enabled. | `configure claude --apply` updates only user-level `~/.claude/settings.json` telemetry env keys, keeps prompt/tool/raw-body flags disabled, and never changes the inference endpoint. Traces require the explicit `--traces` opt-in. |
| Codex CLI | Current first-party configuration schema documents opt-in OTel log, metric, and trace exporters under global `~/.codex/config.toml`; current issue evidence also distinguishes CLI/app-server scope and shows that non-interactive `codex exec` may omit token-usage metrics. The executable is discoverable locally but `--version` is blocked by Windows access policy. | `configure codex --apply` writes a marked user-level `[otel]` block only when no conflicting `[otel]` table exists; a conflict is reported rather than silently overwriting settings. Subscription authentication remains separate from API-key billing, and CLI/app-server/session/subagent coverage remains partial until exercised. |
| Gemini CLI | Current first-party docs document native OTLP logs/metrics/traces, session/model/request/response/tool/token/agent fields, hooks, and user/system configuration. The documented upstream `logPrompts` default is enabled, so the Observatory must explicitly set it to `false`; the CLI is not installed locally. | `configure gemini --apply` merges only the documented user-level `telemetry` object into `~/.gemini/settings.json`, explicitly disables prompt logging, and uses the local Collector without reusing Gemini CLI credentials. |
| Cursor Agent/Desktop | First-party CLI docs document JSON/stream-JSON session, model, tool, request, duration, and `session_id` events; no first-party GenAI OTLP exporter was verified. Cursor Desktop/CLI 3.14.27 is locally installed, but `cursor-agent` is absent. | The implemented generic JSONL/response adapter is the safe extension seam; no client-specific process bridge is installed by default. Treat token usage and native OTel as unknown/partial; project-local rules/hooks are contamination risks. |
| Kimi Code 0.28.1 | First-party docs document stream-json, session/state/wire records, global hooks, subagents, and token-usage records; no native OTLP exporter was found. Kimi is locally installed. | The generic JSONL adapter and fail-open hook boundary are the safe extension seams; no client-specific process bridge is installed by default. Do not treat wire usage as billing-authoritative until verified. |
| Grok CLI 0.2.118 | First-party material documents streaming JSON, sessions, model/request/tool IDs, hooks/plugins, usage, errors, and `parent_tool_use_id`; native OTel was not verified. | The generic JSONL/response adapter is the safe extension seam; no client-specific process bridge is installed by default. Zero/omitted usage remains unknown and OAuth/billing behavior remains partial. |
| OpenRouter | First-party material documents optional OTLP broadcast, metadata/generation records, provider attempts, requested/served model, native tokens, cost, latency, request IDs, and route/gateway state. | Represent OpenRouter as a gateway/route dimension only when inference already uses it. It is never a mandatory Observatory proxy. |
| Direct Anthropic/OpenAI/Google APIs | First-party APIs expose response IDs, usage, models, tools, errors, and request metadata; base SDK native OTLP was not established uniformly. | `ProviderResponseAdapter` accepts an already-completed response mapping, preserves provider-reported usage, and never creates or proxies an inference request. The caller owns latency/retry/session semantics; no repository integration is required by the Observatory baseline. |
| Direct xAI SDK | First-party SDK material documents optional console/OTLP exporters, OTel environment variables, model/usage/tool/error/timing fields, and exact cost ticks. Local SDK integration is not verified. | Candidate native OTel adapter with sensitive attributes disabled; cost remains provider-response evidence. |

Primary first-party sources inspected include:

- [Claude Code monitoring](https://code.claude.com/docs/en/monitoring-usage)
- [Codex monitoring and telemetry](https://developers.openai.com/codex/agent-approvals-security#monitoring-and-telemetry)
- [Codex current configuration schema](https://github.com/openai/codex/blob/main/codex-rs/core/config.schema.json)
- [Gemini CLI telemetry](https://geminicli.com/docs/cli/telemetry/)
- [Gemini CLI configuration](https://geminicli.com/docs/reference/configuration/)
- [Cursor CLI output](https://docs.cursor.com/en/cli/reference/output-format)
- [Kimi Code hooks and sessions](https://moonshotai.github.io/kimi-code/en/customization/hooks)
- [Kimi Code stream-json command](https://moonshotai.github.io/kimi-code/en/reference/kimi-command)
- [Grok CLI and hooks](https://docs.x.ai/build/features/skills-plugins-marketplaces)
- [Grok CLI reference](https://docs.x.ai/build/cli/reference)
- [OpenRouter OTLP broadcast](https://openrouter.ai/docs/guides/features/broadcast/otel-collector)
- [xAI SDK](https://github.com/xai-org/xai-sdk-python)

## Integration rule

No adapter is complete merely because a client emits a plausible JSON record. Its contract test must prove: no repository-local files, no provider endpoint mutation, bounded asynchronous delivery, redaction before persistence, stable event IDs, explicit usage provenance, and restoration/removal without clobbering user changes.

The YAML matrix is the evidence record; the executable `CLIENT_SPECS` catalog is the configuration surface consumed by `doctor` and `configure`. Every canonical capability field and the structured `auth_modes` metadata in every matrix row are parity-tested against that catalog. The test prevents capability vocabulary, authentication-mode, or status drift, but it does not upgrade a documented claim into local provider verification.

## Configuration safety rule

`observatory configure <client>` is a plan by default and returns a degraded exit code until applied. `--apply` is required for user-level client-file changes; `--force` is required to replace conflicting telemetry-only keys. `--remove` without `--apply` removes only Observatory state. Removal with `--apply` deletes only keys or marked blocks recorded as Observatory-managed and leaves later user edits intact. No command writes to a repository, provider credential store, base URL, proxy variable, or inference request path.
