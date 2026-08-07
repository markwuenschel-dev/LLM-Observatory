# Privacy and collection policy

The Observatory is metadata-first. The default policy stores event identity, timing, normalized dimensions, status, bounded numeric measurements, and provenance while excluding prompt/completion content, sensitive tool arguments/results, credentials, environment values, and raw filesystem paths.

Redaction runs before SQLite persistence, offline spooling, metrics projection, or an optional OpenTelemetry export. It is not a post-processing dashboard filter. The Collector also deletes common prompt/completion/message/tool-content attributes and replaces log bodies with a fixed redaction marker before fan-out to Tempo, Loki, or the normalizer, and has no debug exporter in the production pipeline. Sensitive keys and recognizable bearer/API-key patterns are replaced with a redaction marker; they are never sent to Grafana or used as provider credentials. Strings are bounded to prevent an accidental large payload from becoming telemetry storage. Provider-specific payload fields outside the maintained deny-list remain a residual risk and must be covered by canary tests before enabling richer capture.

Content capture is disabled by default. Enabling it is an explicit operator decision and records `provenance.content_capture = "enabled"`. Even when enabled, credentials and authorization material remain redacted and all values are length-bounded.

The Observatory derives project identity from Git without adding files or dependencies to observed repositories. Raw roots and worktree paths are removed by the default policy; stable project and worktree identifiers remain available for aggregation. Git remotes are sanitized to remove credentials, query strings, and fragments before persistence, and path-shaped project/worktree identifiers are pseudonymized at the same boundary.

Telemetry health is separate from inference health. A failed exporter, store, Collector, or Grafana service may reduce observability, but it must not be placed in a provider request path.
