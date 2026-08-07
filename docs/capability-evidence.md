# Provider and client capability evidence

This matrix records current evidence collected on 2026-08-07. A capability claim describes what the client or provider documents, not what the Observatory has already integrated. `VERIFIED_FIRST_PARTY` means current first-party material was inspected; `VERIFIED_LOCALLY` means the installed executable or local behavior was observed; `PARTIAL` means only some modes or fields are covered; `SUPPORTED_NOT_LOCALLY_VERIFIED` means documented but not exercised here; `UNKNOWN` means no safe claim.

| Target | Evidence-backed current capability | Safe Observatory posture |
|---|---|---|
| Claude Code 2.1.224 | First-party monitoring documents OTel metrics and logs/events, beta traces, session/tool/API/retry/auth data, model and token fields, user/managed settings, and subscription/API separation. Installed version is locally verified; telemetry is not locally enabled. | `configure claude --apply` updates only user-level `~/.claude/settings.json` telemetry env keys, keeps prompt/tool/raw-body flags disabled, and never changes the inference endpoint. Traces require the explicit `--traces` opt-in. |
| Codex CLI | First-party docs document opt-in OTel logs, metrics, and traces, conversation/API/SSE/WebSocket/tool/approval/error events, model/duration/status fields, and global `~/.codex/config.toml`. The executable is discoverable locally but `--version` is blocked by Windows access policy. | `configure codex --apply` writes a marked user-level `[otel]` block only when no conflicting `[otel]` table exists; a conflict is reported rather than silently overwriting settings. Subscription authentication remains separate from API-key billing and session/subagent joins remain partial. |
| Gemini CLI | First-party docs document native OTLP logs/metrics/traces, session/model/request/response/tool/token/agent fields, hooks, and user/system configuration. The CLI is not installed locally. | `configure gemini --apply` merges only the documented user-level `telemetry` object into `~/.gemini/settings.json`, disables prompt logging, and uses the local Collector without reusing Gemini CLI credentials. |
| Cursor Agent/Desktop | JSON/stream-JSON and hooks expose session/model/tool/request/duration data; no first-party GenAI OTLP exporter was verified. Desktop is locally installed, but `cursor-agent` is absent. | Structured stream adapter is the safe candidate. Treat token usage and native OTel as unknown/partial; project-local rules/hooks are contamination risks. |
| Kimi Code 0.28.1 | First-party docs document stream-json, session/state/wire records, global hooks, subagents, and token-usage records; no native OTLP exporter was found. Kimi is locally installed. | Structured stream adapter with fail-open hooks; do not treat wire usage as billing-authoritative until verified. |
| Grok CLI 0.2.118 | First-party material documents streaming JSON, sessions, model/request/tool IDs, usage, errors, and `parent_tool_use_id`; native OTel was not verified. | Structured output adapter; zero/omitted usage remains unknown and OAuth/billing behavior remains partial. |
| OpenRouter | First-party material documents optional OTLP broadcast, metadata/generation records, provider attempts, requested/served model, native tokens, cost, latency, request IDs, and route/gateway state. | Represent OpenRouter as a gateway/route dimension only when inference already uses it. It is never a mandatory Observatory proxy. |
| Direct Anthropic/OpenAI/Google APIs | First-party APIs expose response IDs, usage, models, tools, errors, and request metadata; base SDK native OTLP was not established uniformly. | `ProviderResponseAdapter` accepts an already-completed response mapping, preserves provider-reported usage, and never creates or proxies an inference request. The caller owns latency/retry/session semantics; no repository integration is required by the Observatory baseline. |
| Direct xAI SDK | First-party SDK material documents optional console/OTLP exporters, OTel environment variables, model/usage/tool/error/timing fields, and exact cost ticks. Local SDK integration is not verified. | Candidate native OTel adapter with sensitive attributes disabled; cost remains provider-response evidence. |

Primary first-party sources inspected include:

- [Claude Code monitoring](https://code.claude.com/docs/en/monitoring-usage)
- [Codex monitoring and telemetry](https://developers.openai.com/codex/agent-approvals-security#monitoring-and-telemetry)
- [Gemini CLI telemetry](https://geminicli.com/docs/cli/telemetry/)
- [Cursor CLI output](https://docs.cursor.com/en/cli/reference/output-format)
- [Kimi Code hooks and sessions](https://moonshotai.github.io/kimi-code/en/customization/hooks)
- [OpenRouter OTLP broadcast](https://openrouter.ai/docs/guides/features/broadcast/otel-collector)
- [xAI SDK](https://github.com/xai-org/xai-sdk-python)

## Integration rule

No adapter is complete merely because a client emits a plausible JSON record. Its contract test must prove: no repository-local files, no provider endpoint mutation, bounded asynchronous delivery, redaction before persistence, stable event IDs, explicit usage provenance, and restoration/removal without clobbering user changes.

## Configuration safety rule

`observatory configure <client>` is a plan by default and returns a degraded exit code until applied. `--apply` is required for user-level client-file changes; `--force` is required to replace conflicting telemetry-only keys. `--remove` without `--apply` removes only Observatory state. Removal with `--apply` deletes only keys or marked blocks recorded as Observatory-managed and leaves later user edits intact. No command writes to a repository, provider credential store, base URL, proxy variable, or inference request path.
