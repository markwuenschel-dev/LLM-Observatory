"""Current client capability records and safe, global configuration plans.

The catalog is deliberately data-driven.  Native telemetry configuration is
only applied for clients whose first-party settings contract is known.  Other
clients still get a useful discovery/adapter plan; the Observatory never
rewrites an inference endpoint or adds a repository-local file.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import site
import subprocess
import sys
from typing import Any, Mapping
from urllib.parse import urlsplit

from .adapters.base import CapabilityRecord


LOCAL_OTLP_GRPC = "http://127.0.0.1:4317"
LOCAL_OTLP_HTTP = "http://127.0.0.1:4318"
HOOK_MARKER_START = "# BEGIN LLM Observatory managed hook telemetry"
HOOK_MARKER_END = "# END LLM Observatory managed hook telemetry"
CAPABILITY_CONTRACT_VERSION = "1"
CANONICAL_CAPABILITY_FIELDS = (
    "native_otel",
    "signals",
    "hooks_or_events",
    "subscription_telemetry",
    "authoritative_token_counts",
    "model_identity",
    "tool_calls",
    "session_identity",
    "agent_identity",
    "request_latency",
    "errors_retries",
    "global_configuration",
    "zero_repository_contamination",
    "inference_proxy",
)


def _canonical_capabilities(values: Mapping[str, str]) -> dict[str, str]:
    """Expose one stable vocabulary while retaining provider-specific extras."""

    aliases = {
        "authoritative_token_counts": ("authoritative_token_counts", "authoritative_usage"),
        "session_identity": ("session_identity", "session_and_agent_identity", "session_and_tool_identity"),
        "agent_identity": ("agent_identity", "session_and_agent_identity", "session_and_tool_identity"),
        "zero_repository_contamination": ("zero_repository_contamination",),
    }
    result = dict(values)
    for field_name in CANONICAL_CAPABILITY_FIELDS:
        if field_name in result:
            continue
        result[field_name] = next((str(values[name]) for name in aliases.get(field_name, (field_name,)) if name in values), "UNKNOWN")
    return result


@dataclass(frozen=True)
class ClientSpec:
    name: str
    provider: str
    confidence: str
    capabilities: Mapping[str, str]
    auth_modes: tuple[str, ...]
    evidence: tuple[str, ...]
    config_kind: str
    config_path_hint: str | None = None
    native_config: bool = False

    def capabilities_record(self, *, installed: bool | None = None, version_probe_status: str | None = None) -> CapabilityRecord:
        capabilities = _canonical_capabilities(self.capabilities)
        if installed is False:
            capabilities["installed"] = "NOT_INSTALLED"
        elif installed is True:
            capabilities["installed"] = "VERIFIED_LOCALLY" if version_probe_status in (None, "verified") else "INSTALLED_NOT_VERIFIED"
        return CapabilityRecord(
            provider=self.provider,
            client=self.name,
            confidence=self.confidence,
            capabilities=capabilities,
            auth_modes=self.auth_modes,
            evidence=self.evidence,
            last_verified=datetime.now(timezone.utc).date().isoformat(),
            contract_version=CAPABILITY_CONTRACT_VERSION,
        )


_COMMON = {
    # The baseline is repository-external, but not every client can be
    # configured globally without project-local hooks or rules.  Keep the
    # executable catalog conservative; provider-specific rows may strengthen
    # this only when their evidence supports it.
    "zero_repository_contamination": "PARTIAL",
    "inference_proxy": "MUST_NOT_BE_USED",
    "metadata_only_default": "SUPPORTED",
}


CLIENT_SPECS: dict[str, ClientSpec] = {
    "claude": ClientSpec(
        name="claude-code",
        provider="anthropic",
        confidence="PARTIAL",
        capabilities={
            **_COMMON,
            "native_otel": "PARTIAL",
            "signals": "metrics,logs,traces-beta",
            "hooks_or_events": "SUPPORTED",
            "subscription_telemetry": "SUPPORTED",
            "authoritative_usage": "SUPPORTED",
            "session_and_tool_identity": "SUPPORTED",
            "agent_identity": "PARTIAL",
            "global_configuration": "SUPPORTED",
        },
        auth_modes=("subscription", "api", "bedrock", "vertex"),
        evidence=(
            "https://code.claude.com/docs/en/monitoring-usage",
            "https://code.claude.com/docs/en/agent-sdk/observability",
        ),
        config_kind="claude-json",
        config_path_hint="~/.claude/settings.json",
        native_config=True,
    ),
    "codex": ClientSpec(
        name="codex",
        provider="openai",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={
            **_COMMON,
            "native_otel": "SUPPORTED",
            "signals": "logs,metrics,traces",
            "hooks_or_events": "SUPPORTED",
            "subscription_telemetry": "SUPPORTED",
            "authoritative_usage": "PARTIAL",
            "session_and_agent_identity": "PARTIAL",
            "global_configuration": "SUPPORTED",
        },
        auth_modes=("subscription", "api"),
        evidence=(
            "https://github.com/openai/codex/blob/main/codex-rs/core/config.schema.json",
            "https://developers.openai.com/codex/agent-approvals-security#monitoring-and-telemetry",
        ),
        config_kind="codex-toml",
        config_path_hint="~/.codex/config.toml",
        native_config=True,
    ),
    "gemini": ClientSpec(
        name="gemini-cli",
        provider="google",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={
            **_COMMON,
            "native_otel": "SUPPORTED",
            "signals": "logs,metrics,traces",
            "hooks_or_events": "SUPPORTED",
            "subscription_telemetry": "PARTIAL",
            "authoritative_usage": "PARTIAL",
            "session_and_agent_identity": "SUPPORTED",
            "global_configuration": "SUPPORTED",
        },
        auth_modes=("subscription", "api", "vertex"),
        evidence=(
            "https://geminicli.com/docs/cli/telemetry/",
            "https://geminicli.com/docs/reference/configuration/",
        ),
        config_kind="gemini-json",
        config_path_hint="~/.gemini/settings.json",
        native_config=True,
    ),
    "cursor": ClientSpec(
        name="cursor",
        provider="cursor",
        confidence="PARTIAL",
        capabilities={
            **_COMMON,
            "native_otel": "UNKNOWN",
            "signals": "structured-output/events",
            "hooks_or_events": "SUPPORTED",
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("subscription", "api"),
        evidence=(
            "https://docs.cursor.com/en/cli/reference/output-format",
            "https://docs.cursor.com/en/cli/overview",
            "Local installation was detected; structured stream events are supported but native OTel configuration was not verified.",
        ),
        config_kind="discovery-only",
        native_config=False,
    ),
    "kimi": ClientSpec(
        name="kimi",
        provider="moonshot",
        confidence="PARTIAL",
        capabilities={
            **_COMMON,
            "native_otel": "UNKNOWN",
            "signals": "stream-json/hooks",
            "hooks_or_events": "SUPPORTED",
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("subscription", "api"),
        evidence=(
            "https://moonshotai.github.io/kimi-code/en/reference/kimi-command",
            "https://moonshotai.github.io/kimi-code/en/customization/hooks",
            "Local installation was detected; stream-json and hooks are supported but native OTel configuration was not verified.",
        ),
        config_kind="kimi-toml-hook",
        config_path_hint="~/.kimi-code/config.toml",
        native_config=True,
    ),
    "grok": ClientSpec(
        name="grok",
        provider="xai",
        confidence="PARTIAL",
        capabilities={
            **_COMMON,
            "native_otel": "UNKNOWN",
            "signals": "structured-output/sessions",
            "hooks_or_events": "SUPPORTED",
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("api",),
        evidence=(
            "https://docs.x.ai/build/features/skills-plugins-marketplaces",
            "https://docs.x.ai/build/cli/reference",
            "Local capability research observed structured output, hooks, and session surfaces; native OTel was not verified.",
        ),
        config_kind="grok-toml-hook",
        config_path_hint="~/.grok/config.toml",
        native_config=True,
    ),
    "jsonl": ClientSpec(
        name="jsonl",
        provider="unknown",
        confidence="VERIFIED_LOCALLY",
        capabilities={
            "native_otel": "UNSUPPORTED",
            "signals": "jsonl",
            "hooks_or_events": "SUPPORTED",
            "subscription_telemetry": "UNKNOWN",
            "authoritative_token_counts": "UNKNOWN",
            "model_identity": "SUPPORTED_IF_REPORTED",
            "tool_calls": "SUPPORTED_IF_REPORTED",
            "session_identity": "SUPPORTED_IF_REPORTED",
            "agent_identity": "SUPPORTED_IF_REPORTED",
            "global_configuration": "SUPPORTED",
            "zero_repository_contamination": "SUPPORTED",
            "request_latency": "SUPPORTED_IF_REPORTED",
            "errors_retries": "SUPPORTED_IF_REPORTED",
            "inference_proxy": "MUST_NOT_BE_USED",
        },
        auth_modes=("unknown",),
        evidence=("bounded JSONL adapter and contract tests in this repository",),
        config_kind="adapter-only",
        native_config=False,
    ),
    "openrouter": ClientSpec(
        name="openrouter-api",
        provider="openrouter",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={
            **_COMMON,
            "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "native_otel": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "signals": "gateway-response/optional-otel",
            "authoritative_usage": "SUPPORTED",
            "route_dimension": "gateway=openrouter",
            "global_configuration": "UNSUPPORTED",
        },
        auth_modes=("api",),
        evidence=("OpenRouter is represented as a route/gateway dimension, never as a mandatory Observatory proxy.",),
        config_kind="adapter-only",
        native_config=False,
    ),
    "direct-openai": ClientSpec(
        name="direct-openai-api",
        provider="openai",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-anthropic": ClientSpec(
        name="direct-anthropic-api",
        provider="anthropic",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-google": ClientSpec(
        name="direct-google-api",
        provider="google",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api", "vertex"), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-xai": ClientSpec(
        name="direct-xai-api",
        provider="xai",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
}


# Keep the executable catalog on the same canonical vocabulary as
# docs/capability-matrix.yaml.  Provider-specific aliases may remain in the
# declarations above for compatibility, but every capability exposed to the
# CLI and doctor is normalized through this contract.
_CAPABILITY_MATRIX_FIELDS: dict[str, dict[str, str]] = {
    "claude": {
        "native_otel": "PARTIAL", "signals": "metrics,logs,traces-beta", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "VERIFIED_FIRST_PARTY", "authoritative_token_counts": "VERIFIED_FIRST_PARTY",
        "model_identity": "VERIFIED_FIRST_PARTY", "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "VERIFIED_FIRST_PARTY",
        "agent_identity": "PARTIAL", "request_latency": "VERIFIED_FIRST_PARTY", "errors_retries": "VERIFIED_FIRST_PARTY",
        "global_configuration": "VERIFIED_FIRST_PARTY", "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "codex": {
        "native_otel": "VERIFIED_FIRST_PARTY", "signals": "logs,metrics,traces", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "VERIFIED_FIRST_PARTY", "authoritative_token_counts": "PARTIAL", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "PARTIAL", "agent_identity": "PARTIAL",
        "request_latency": "VERIFIED_FIRST_PARTY", "errors_retries": "VERIFIED_FIRST_PARTY", "global_configuration": "VERIFIED_FIRST_PARTY",
        "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "gemini": {
        "native_otel": "VERIFIED_FIRST_PARTY", "signals": "logs,metrics,traces", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "PARTIAL", "authoritative_token_counts": "PARTIAL", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "VERIFIED_FIRST_PARTY", "agent_identity": "VERIFIED_FIRST_PARTY",
        "request_latency": "VERIFIED_FIRST_PARTY", "errors_retries": "VERIFIED_FIRST_PARTY", "global_configuration": "VERIFIED_FIRST_PARTY",
        "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "cursor": {
        "native_otel": "UNKNOWN", "signals": "structured-output/events", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "VERIFIED_FIRST_PARTY", "authoritative_token_counts": "PARTIAL", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "VERIFIED_FIRST_PARTY", "agent_identity": "UNKNOWN",
        "request_latency": "PARTIAL", "errors_retries": "PARTIAL", "global_configuration": "VERIFIED_FIRST_PARTY",
        "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "kimi": {
        "native_otel": "UNKNOWN", "signals": "stream-json/hooks", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "VERIFIED_FIRST_PARTY", "authoritative_token_counts": "PARTIAL", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "VERIFIED_FIRST_PARTY", "agent_identity": "VERIFIED_FIRST_PARTY",
        "request_latency": "PARTIAL", "errors_retries": "PARTIAL", "global_configuration": "VERIFIED_FIRST_PARTY",
        "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "grok": {
        "native_otel": "UNKNOWN", "signals": "structured-output/sessions", "hooks_or_events": "VERIFIED_FIRST_PARTY",
        "subscription_telemetry": "PARTIAL", "authoritative_token_counts": "PARTIAL", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "VERIFIED_FIRST_PARTY", "session_identity": "VERIFIED_FIRST_PARTY", "agent_identity": "PARTIAL",
        "request_latency": "PARTIAL", "errors_retries": "PARTIAL", "global_configuration": "PARTIAL",
        "zero_repository_contamination": "PARTIAL", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "jsonl": {
        "native_otel": "UNSUPPORTED", "signals": "jsonl", "hooks_or_events": "SUPPORTED", "subscription_telemetry": "UNKNOWN",
        "authoritative_token_counts": "UNKNOWN", "model_identity": "SUPPORTED_IF_REPORTED", "tool_calls": "SUPPORTED_IF_REPORTED",
        "session_identity": "SUPPORTED_IF_REPORTED", "agent_identity": "SUPPORTED_IF_REPORTED", "request_latency": "SUPPORTED_IF_REPORTED",
        "errors_retries": "SUPPORTED_IF_REPORTED", "global_configuration": "SUPPORTED", "zero_repository_contamination": "SUPPORTED",
        "inference_proxy": "MUST_NOT_BE_USED",
    },
    "openrouter": {
        "native_otel": "SUPPORTED_NOT_LOCALLY_VERIFIED", "signals": "gateway-response/optional-otel", "hooks_or_events": "SUPPORTED_NOT_LOCALLY_VERIFIED",
        "subscription_telemetry": "UNSUPPORTED", "authoritative_token_counts": "VERIFIED_FIRST_PARTY", "model_identity": "VERIFIED_FIRST_PARTY",
        "tool_calls": "SUPPORTED_NOT_LOCALLY_VERIFIED", "session_identity": "PARTIAL", "agent_identity": "PARTIAL",
        "request_latency": "VERIFIED_FIRST_PARTY", "errors_retries": "VERIFIED_FIRST_PARTY", "global_configuration": "SUPPORTED_NOT_LOCALLY_VERIFIED",
        "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "direct-openai": {
        "native_otel": "UNKNOWN", "signals": "response-envelope", "hooks_or_events": "PARTIAL", "subscription_telemetry": "UNSUPPORTED",
        "authoritative_token_counts": "VERIFIED_FIRST_PARTY", "model_identity": "VERIFIED_FIRST_PARTY", "tool_calls": "VERIFIED_FIRST_PARTY",
        "session_identity": "PARTIAL", "agent_identity": "PARTIAL", "request_latency": "SUPPORTED_IF_REPORTED", "errors_retries": "SUPPORTED_IF_REPORTED",
        "global_configuration": "SUPPORTED_NOT_LOCALLY_VERIFIED", "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "direct-anthropic": {
        "native_otel": "UNKNOWN", "signals": "response-envelope", "hooks_or_events": "PARTIAL", "subscription_telemetry": "UNSUPPORTED",
        "authoritative_token_counts": "VERIFIED_FIRST_PARTY", "model_identity": "VERIFIED_FIRST_PARTY", "tool_calls": "VERIFIED_FIRST_PARTY",
        "session_identity": "PARTIAL", "agent_identity": "UNKNOWN", "request_latency": "SUPPORTED_IF_REPORTED", "errors_retries": "SUPPORTED_IF_REPORTED",
        "global_configuration": "SUPPORTED_NOT_LOCALLY_VERIFIED", "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "direct-google": {
        "native_otel": "UNKNOWN", "signals": "response-envelope", "hooks_or_events": "PARTIAL", "subscription_telemetry": "UNSUPPORTED",
        "authoritative_token_counts": "VERIFIED_FIRST_PARTY", "model_identity": "VERIFIED_FIRST_PARTY", "tool_calls": "VERIFIED_FIRST_PARTY",
        "session_identity": "PARTIAL", "agent_identity": "UNKNOWN", "request_latency": "SUPPORTED_IF_REPORTED", "errors_retries": "SUPPORTED_IF_REPORTED",
        "global_configuration": "SUPPORTED_NOT_LOCALLY_VERIFIED", "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "inference_proxy": "MUST_NOT_BE_USED",
    },
    "direct-xai": {
        "native_otel": "SUPPORTED_NOT_LOCALLY_VERIFIED", "signals": "response-envelope", "hooks_or_events": "PARTIAL", "subscription_telemetry": "UNSUPPORTED",
        "authoritative_token_counts": "VERIFIED_FIRST_PARTY", "model_identity": "VERIFIED_FIRST_PARTY", "tool_calls": "VERIFIED_FIRST_PARTY",
        "session_identity": "PARTIAL", "agent_identity": "PARTIAL", "request_latency": "SUPPORTED_IF_REPORTED", "errors_retries": "SUPPORTED_IF_REPORTED",
        "global_configuration": "SUPPORTED_NOT_LOCALLY_VERIFIED", "zero_repository_contamination": "SUPPORTED_NOT_LOCALLY_VERIFIED", "inference_proxy": "MUST_NOT_BE_USED",
    },
}

for _client_key, _canonical_fields in _CAPABILITY_MATRIX_FIELDS.items():
    _spec = CLIENT_SPECS[_client_key]
    CLIENT_SPECS[_client_key] = replace(_spec, capabilities={**_spec.capabilities, **_canonical_fields})


ALIASES = {
    "claude-code": "claude",
    "anthropic": "claude",
    "gemini-cli": "gemini",
    "google": "gemini",
    "openai": "codex",
    "xai": "grok",
}


def normalize_client_name(name: str) -> str:
    normalized = name.strip().lower()
    if normalized == "all":
        return normalized
    return ALIASES.get(normalized, normalized)


def client_spec(name: str) -> ClientSpec:
    normalized = normalize_client_name(name)
    try:
        return CLIENT_SPECS[normalized]
    except KeyError as exc:
        raise ValueError(f"unknown client: {name}") from exc


def _home() -> Path:
    return Path(os.environ.get("USERPROFILE") or Path.home())


def config_path(spec: ClientSpec) -> Path | None:
    home = _home()
    if spec.config_kind == "codex-toml":
        return Path(os.environ.get("CODEX_HOME") or home / ".codex") / "config.toml"
    if spec.config_kind == "claude-json":
        return home / ".claude" / "settings.json"
    if spec.config_kind == "gemini-json":
        return home / ".gemini" / "settings.json"
    if spec.config_kind == "kimi-toml-hook":
        return home / ".kimi-code" / "config.toml"
    if spec.config_kind == "grok-toml-hook":
        return home / ".grok" / "config.toml"
    return None


def _executable_candidates(spec: ClientSpec) -> tuple[str, ...]:
    return {
        "claude-code": ("claude",),
        "codex": ("codex",),
        "gemini-cli": ("gemini",),
        "cursor": ("cursor", "cursor-agent"),
        "kimi": ("kimi",),
        "grok": ("grok",),
    }.get(spec.name, ())


def _probe_executable_version(executable: str | None) -> dict[str, Any]:
    """Run only the client's bounded version probe; never invoke inference."""

    if executable is None:
        return {"version": None, "version_probe_status": "not_installed"}
    try:
        result = subprocess.run(
            [executable, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return {"version": None, "version_probe_status": "timeout"}
    except PermissionError:
        return {"version": None, "version_probe_status": "blocked"}
    except OSError:
        return {"version": None, "version_probe_status": "unavailable"}
    output = (result.stdout or result.stderr or "").strip()
    version = next((line.strip() for line in output.splitlines() if line.strip()), None)
    if version:
        version = version[:160]
    return {
        "version": version,
        "version_probe_status": "verified" if result.returncode == 0 and version else "returned_no_version" if result.returncode == 0 else "failed",
    }


def discover_client(name: str) -> dict[str, Any]:
    spec = client_spec(name)
    executable = next((shutil.which(candidate) for candidate in _executable_candidates(spec) if shutil.which(candidate)), None)
    version_evidence = _probe_executable_version(executable)
    path = config_path(spec)
    return {
        "client": spec.name,
        "provider": spec.provider,
        "installed": executable is not None,
        "executable": executable,
        **version_evidence,
        "config_path": str(path) if path else None,
        "config_exists": bool(path and path.exists()),
        "capabilities": spec.capabilities_record(installed=executable is not None, version_probe_status=version_evidence.get("version_probe_status")).to_mapping(),
        "inference_proxy": False,
    }


def discovery(names: list[str] | None = None) -> list[dict[str, Any]]:
    selected = names or sorted(CLIENT_SPECS)
    return [discover_client(name) for name in selected]


def _claude_values(*, enable_traces: bool) -> dict[str, str]:
    values = {
        "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
        "OTEL_METRICS_EXPORTER": "otlp",
        "OTEL_LOGS_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
        "OTEL_EXPORTER_OTLP_ENDPOINT": _otlp_grpc_endpoint(),
        "OTEL_LOG_USER_PROMPTS": "0",
        "OTEL_LOG_TOOL_DETAILS": "0",
        "OTEL_LOG_TOOL_CONTENT": "0",
        "OTEL_LOG_RAW_API_BODIES": "0",
    }
    if enable_traces:
        values.update({"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1", "OTEL_TRACES_EXPORTER": "otlp"})
    return values


def _gemini_values(*, enable_traces: bool = False) -> dict[str, Any]:
    return {
        "enabled": True,
        "traces": enable_traces,
        "target": "local",
        "otlpEndpoint": _otlp_grpc_endpoint(),
        "otlpProtocol": "grpc",
        "logPrompts": False,
        "useCollector": True,
    }


def _configured_otlp_endpoint(environment_name: str, default: str) -> str:
    """Resolve an operator-selected local OTLP endpoint safely.

    The normal installation remains on the conventional loopback ports.  A
    disposable acceptance project can override those ports without stopping
    another local Observatory stack, but endpoint values are still restricted
    to credential-free HTTP(S) URLs.
    """

    value = os.environ.get(environment_name, "").strip()
    if not value:
        return default
    if len(value) > 512:
        raise ValueError(f"{environment_name} is too long")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{environment_name} must be a credential-free HTTP(S) endpoint")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{environment_name} must not include a query or fragment")
    return value.rstrip("/")


def _otlp_grpc_endpoint() -> str:
    return _configured_otlp_endpoint("OBSERVATORY_OTLP_GRPC_ENDPOINT", LOCAL_OTLP_GRPC)


def _otlp_http_endpoint() -> str:
    return _configured_otlp_endpoint("OBSERVATORY_OTLP_HTTP_ENDPOINT", LOCAL_OTLP_HTTP)


def _codex_block(*, enable_traces: bool) -> str:
    http_endpoint = _otlp_http_endpoint()
    trace_setting = (
        f'trace_exporter = {{ otlp-http = {{ endpoint = "{http_endpoint}/v1/traces", protocol = "json" }} }}\n'
        if enable_traces
        else 'trace_exporter = "none"\n'
    )
    return (
        "# BEGIN LLM Observatory managed telemetry\n"
        "[otel]\n"
        "environment = \"llm-observatory\"\n"
        f'exporter = {{ otlp-http = {{ endpoint = "{http_endpoint}/v1/logs", protocol = "json" }} }}\n'
        f'metrics_exporter = {{ otlp-http = {{ endpoint = "{http_endpoint}/v1/metrics", protocol = "json" }} }}\n'
        f"{trace_setting}"
        "log_user_prompt = false\n"
        "# END LLM Observatory managed telemetry\n"
    )


def _managed_block_hash(block: str) -> str:
    return hashlib.sha256(block.rstrip("\n").encode("utf-8")).hexdigest()


def _configuration_mode(spec: ClientSpec) -> str:
    if spec.config_kind in {"kimi-toml-hook", "grok-toml-hook"}:
        return "global-hook"
    if spec.native_config:
        return "native-otlp"
    return spec.config_kind


def _hook_command(client: str) -> str:
    launcher = shutil.which("observatory")
    if os.name == "nt":
        user_scripts = Path(site.getuserbase()) / f"Python{sys.version_info.major}{sys.version_info.minor}" / "Scripts" / "observatory.exe"
        if user_scripts.exists():
            launcher = str(user_scripts)
    executable = f'"{launcher}"' if launcher and any(character.isspace() for character in launcher) else launcher or "observatory"
    return f"{executable} hook --client {client} --quiet"


def _toml_string(value: str) -> str:
    """Encode a command as a TOML basic string without leaking path escapes."""

    return json.dumps(value, ensure_ascii=False)


def _hook_block(spec: ClientSpec) -> str:
    """Return a marked, user-level, observation-only hook configuration."""

    command = _hook_command(spec.name)
    if spec.config_kind == "kimi-toml-hook":
        return (
            f"{HOOK_MARKER_START}\n"
            "[[hooks]]\n"
            'event = "Notification"\n'
            f"command = {_toml_string(command)}\n"
            "timeout = 2\n"
            "[[hooks]]\n"
            'event = "Interrupt"\n'
            f"command = {_toml_string(command)}\n"
            "timeout = 2\n"
            "[[hooks]]\n"
            'event = "PreCompact"\n'
            f"command = {_toml_string(command)}\n"
            "timeout = 2\n"
            "[[hooks]]\n"
            'event = "PostCompact"\n'
            f"command = {_toml_string(command)}\n"
            "timeout = 2\n"
            f"{HOOK_MARKER_END}\n"
        )
    if spec.config_kind == "grok-toml-hook":
        events = ("SessionStart", "SessionEnd", "PostToolUse")
        body = [HOOK_MARKER_START]
        for event in events:
            body.extend(
                (
                    f"[[hooks.{event}]]",
                    f"[[hooks.{event}.hooks]]",
                    'type = "command"',
                    f"command = {_toml_string(command)}",
                    "timeout = 2",
                )
            )
        body.append(HOOK_MARKER_END)
        return "\n".join(body) + "\n"
    raise ValueError(f"client {spec.name} does not support a managed hook block")


def _contains_embedded_credentials(value: Any) -> bool:
    """Reject endpoint values that would make managed state secret-bearing."""

    if not isinstance(value, str) or not value.strip():
        return False
    text = value.strip()
    try:
        parsed = urlsplit(text)
    except ValueError:
        parsed = None
    if parsed is not None and (parsed.username or parsed.password):
        return True
    return bool(re.search(r"(?i)(?:bearer\s+|(?:api[_-]?key|access[_-]?token|client[_-]?secret|password|credential)\s*[=:])", text))


def plan_configuration(name: str, *, enable_traces: bool = False) -> dict[str, Any]:
    spec = client_spec(name)
    discovered = discover_client(name)
    path = config_path(spec)
    plan: dict[str, Any] = {
        "client": spec.name,
        "provider": spec.provider,
        "mode": _configuration_mode(spec),
        "inference_proxy": False,
        "config_path": str(path) if path else None,
        "config_exists": bool(path and path.exists()),
        "installed": discovered["installed"],
        "version": discovered.get("version"),
        "version_probe_status": discovered.get("version_probe_status"),
        "capabilities": spec.capabilities_record(installed=discovered["installed"], version_probe_status=discovered.get("version_probe_status")).to_mapping(),
        "changes": {},
        "apply_required": bool(spec.native_config),
        "supported": spec.native_config,
    }
    if spec.config_kind == "claude-json":
        plan["changes"] = {"env": _claude_values(enable_traces=enable_traces)}
    elif spec.config_kind == "gemini-json":
        plan["changes"] = {"telemetry": _gemini_values(enable_traces=enable_traces)}
    elif spec.config_kind == "codex-toml":
        plan["changes"] = {"toml_block": _codex_block(enable_traces=enable_traces)}
    elif spec.config_kind in {"kimi-toml-hook", "grok-toml-hook"}:
        plan["changes"] = {"toml_block": _hook_block(spec)}
    else:
        plan["warnings"] = ["No first-party global telemetry configuration contract is verified for this client; use the response/JSONL adapter or configure its native hooks separately."]
    return plan


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"configuration file must contain an object: {path}")
    return value


def _write_text_atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".observatory.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _apply_json(
    spec: ClientSpec,
    *,
    enable_traces: bool,
    force: bool,
    managed_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    path = config_path(spec)
    if path is None:
        raise ValueError(f"client {spec.name} does not have a JSON configuration path")
    current = _read_json_object(path)
    desired = _claude_values(enable_traces=enable_traces) if spec.config_kind == "claude-json" else _gemini_values(enable_traces=enable_traces)
    section_name = "env" if spec.config_kind == "claude-json" else "telemetry"
    section = current.get(section_name)
    if section is None:
        section = {}
    if not isinstance(section, dict):
        raise ValueError(f"{path} has a non-object {section_name} section")
    conflicts = {key: section[key] for key in desired if key in section and section[key] != desired[key]}
    sensitive_conflicts = [key for key, value in conflicts.items() if _contains_embedded_credentials(value)]
    if sensitive_conflicts:
        return {
            "changed": False,
            "conflicts": [f"{key} contains embedded credentials; refusing to persist or force-overwrite it" for key in sorted(sensitive_conflicts)],
            "path": str(path),
            "inference_proxy": False,
        }
    if conflicts and not force:
        return {"changed": False, "conflicts": sorted(conflicts), "path": str(path), "inference_proxy": False}
    prior_state = dict(managed_state or {})
    ownership: dict[str, dict[str, Any]] = {}
    for key in desired:
        prior = prior_state.get(key)
        if isinstance(prior, Mapping) and isinstance(prior.get("present"), bool):
            entry = {"present": prior["present"]}
            if prior["present"]:
                entry["value"] = prior.get("value")
            entry["managed"] = desired[key]
            ownership[key] = entry
        elif key in section:
            ownership[key] = {"present": True, "value": section[key], "managed": desired[key]}
        else:
            ownership[key] = {"present": False, "managed": desired[key]}
    for key, prior in prior_state.items():
        if key in ownership or not isinstance(prior, Mapping) or not isinstance(prior.get("present"), bool):
            continue
        ownership[key] = dict(prior)
    changed = False
    for key, value in desired.items():
        if section.get(key) != value:
            section[key] = value
            changed = True
    if current.get(section_name) != section:
        current[section_name] = section
        changed = True
    if changed:
        _write_json_atomic(path, current)
    return {
        "changed": changed,
        # A reviewed --force apply is an accepted overwrite, not an
        # unresolved conflict. Keep an explicit audit field for callers that
        # want to display what was replaced.
        "conflicts": [] if force else sorted(conflicts),
        "overwritten": sorted(conflicts) if force else [],
        "path": str(path),
        "managed_keys": sorted(ownership),
        "managed_state": ownership,
        "inference_proxy": False,
        "content_capture": False,
    }


def _apply_codex(*, enable_traces: bool, force: bool, managed_hash: str | None = None) -> dict[str, Any]:
    spec = CLIENT_SPECS["codex"]
    path = config_path(spec)
    assert path is not None
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker_start = "# BEGIN LLM Observatory managed telemetry"
    marker_end = "# END LLM Observatory managed telemetry"
    if marker_start in existing and marker_end not in existing:
        raise ValueError(f"incomplete Observatory block in {path}")
    block = _codex_block(enable_traces=enable_traces)
    if marker_start in existing:
        start_index = existing.index(marker_start)
        end_index = existing.index(marker_end, start_index) + len(marker_end)
        current_block = existing[start_index:end_index]
        expected_hash = managed_hash or _managed_block_hash(block)
        if _managed_block_hash(current_block) != expected_hash:
            return {
                "changed": False,
                "conflicts": ["managed Observatory block changed by user"],
                "path": str(path),
                "inference_proxy": False,
                "managed_block": True,
                "managed_keys": ["managed_block"],
            }
        before = existing[:start_index]
        after = existing[end_index:]
        new_text = before + block.rstrip("\n") + after
        changed = new_text != existing
    else:
        existing_otel_tables = any(
            line.strip() == "[otel]" or line.strip().startswith("[otel.")
            for line in existing.splitlines()
        )
        if existing_otel_tables:
            return {"changed": False, "conflicts": ["otel table already exists"], "path": str(path), "inference_proxy": False}
        separator = "\n" if existing and not existing.endswith("\n") else ""
        new_text = existing + separator + block
        changed = True
    if changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".observatory.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(new_text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    return {
        "changed": changed,
        "conflicts": [],
        "path": str(path),
        "managed_block": True,
        "managed_keys": ["managed_block"],
        "managed_hash": _managed_block_hash(block),
        "inference_proxy": False,
        "content_capture": False,
    }


def _apply_hook(spec: ClientSpec, *, managed_hash: str | None = None) -> dict[str, Any]:
    path = config_path(spec)
    if path is None:
        raise ValueError(f"client {spec.name} does not have a hook configuration path")
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if HOOK_MARKER_START in existing and HOOK_MARKER_END not in existing:
        raise ValueError(f"incomplete Observatory hook block in {path}")
    block = _hook_block(spec)
    if HOOK_MARKER_START in existing:
        start_index = existing.index(HOOK_MARKER_START)
        end_index = existing.index(HOOK_MARKER_END, start_index) + len(HOOK_MARKER_END)
        current_block = existing[start_index:end_index]
        expected_hash = managed_hash or _managed_block_hash(block)
        if _managed_block_hash(current_block) != expected_hash:
            return {
                "changed": False,
                "conflicts": ["managed Observatory hook block changed by user"],
                "path": str(path),
                "inference_proxy": False,
                "managed_block": True,
                "managed_keys": ["managed_block"],
            }
        before = existing[:start_index]
        after = existing[end_index:]
        new_text = before + block.rstrip("\n") + after
        changed = new_text != existing
    else:
        separator = "\n" if existing and not existing.endswith("\n") else ""
        new_text = existing + separator + block
        changed = True
    if changed:
        _write_text_atomic(path, new_text)
    return {
        "changed": changed,
        "conflicts": [],
        "path": str(path),
        "managed_block": True,
        "managed_keys": ["managed_block"],
        "managed_hash": _managed_block_hash(block),
        "inference_proxy": False,
        "content_capture": False,
    }


def apply_configuration(
    name: str,
    *,
    enable_traces: bool = False,
    force: bool = False,
    managed_hash: str | None = None,
    managed_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = client_spec(name)
    discovered = discover_client(name)
    if not spec.native_config:
        result = plan_configuration(name, enable_traces=enable_traces)
        result["applied"] = False
        return result
    result = (
        _apply_codex(enable_traces=enable_traces, force=force, managed_hash=managed_hash)
        if spec.config_kind == "codex-toml"
        else _apply_hook(spec, managed_hash=managed_hash)
        if spec.config_kind in {"kimi-toml-hook", "grok-toml-hook"}
        else _apply_json(spec, enable_traces=enable_traces, force=force, managed_state=managed_state)
    )
    result.update({
        "client": spec.name,
        "provider": spec.provider,
        "applied": not bool(result.get("conflicts")),
        "mode": _configuration_mode(spec),
        "version": discovered.get("version"),
        "version_probe_status": discovered.get("version_probe_status"),
        "capabilities": spec.capabilities_record(installed=discovered["installed"], version_probe_status=discovered.get("version_probe_status")).to_mapping(),
    })
    return result


def _remove_json(
    spec: ClientSpec,
    *,
    managed_keys: list[str] | None = None,
    managed_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    path = config_path(spec)
    if path is None or not path.exists():
        return {"changed": False, "removed": [], "path": str(path) if path else None, "inference_proxy": False}
    current = _read_json_object(path)
    section_name = "env" if spec.config_kind == "claude-json" else "telemetry"
    section = current.get(section_name)
    if not isinstance(section, dict):
        return {"changed": False, "removed": [], "path": str(path), "inference_proxy": False}
    if not managed_keys:
        return {"changed": False, "removed": [], "path": str(path), "inference_proxy": False}
    if not isinstance(managed_state, Mapping):
        return {
            "changed": False,
            "removed": [],
            "conflicts": ["managed JSON setting originals are unavailable; re-apply Observatory configuration before removal"],
            "path": str(path),
            "inference_proxy": False,
        }
    desired = _claude_values(enable_traces=True) if spec.config_kind == "claude-json" else _gemini_values()
    conflicts: list[str] = []
    for key in managed_keys:
        owner = managed_state.get(key)
        expected = owner.get("managed") if isinstance(owner, Mapping) else desired.get(key)
        if expected is None:
            conflicts.append(key)
        elif key in section and section[key] != expected:
            conflicts.append(key)
    if conflicts:
        return {
            "changed": False,
            "removed": [],
            "conflicts": sorted(conflicts),
            "path": str(path),
            "inference_proxy": False,
        }
    restored: list[str] = []
    removed: list[str] = []
    for key in managed_keys:
        owner = managed_state.get(key)
        if not isinstance(owner, Mapping) or not isinstance(owner.get("present"), bool):
            return {
                "changed": False,
                "removed": [],
                "conflicts": [f"managed original for {key} is unavailable; re-apply Observatory configuration before removal"],
                "path": str(path),
                "inference_proxy": False,
            }
        if not owner["present"]:
            if key in section:
                del section[key]
                removed.append(key)
        else:
            original = owner.get("value")
            if section.get(key) != original:
                section[key] = original
                restored.append(key)
    if removed or restored:
        if section:
            current[section_name] = section
        else:
            current.pop(section_name, None)
        _write_json_atomic(path, current)
    return {
        "changed": bool(removed or restored),
        "removed": sorted(removed),
        "restored": sorted(restored),
        "path": str(path),
        "inference_proxy": False,
    }


def remove_configuration(
    name: str,
    *,
    managed_keys: list[str] | None = None,
    managed_hash: str | None = None,
    managed_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = client_spec(name)
    if spec.config_kind in {"kimi-toml-hook", "grok-toml-hook"}:
        path = config_path(spec)
        if path is None or not path.exists() or not managed_keys or "managed_block" not in managed_keys:
            return {"changed": False, "removed": False, "path": str(path) if path else None, "inference_proxy": False}
        existing = path.read_text(encoding="utf-8")
        if HOOK_MARKER_START not in existing:
            return {"changed": False, "removed": False, "path": str(path), "inference_proxy": False}
        if HOOK_MARKER_END not in existing:
            raise ValueError(f"incomplete Observatory hook block in {path}")
        if not managed_hash:
            return {
                "changed": False,
                "removed": False,
                "conflicts": ["managed hook block hash is required before removal"],
                "path": str(path),
                "inference_proxy": False,
            }
        start_index = existing.index(HOOK_MARKER_START)
        end_index = existing.index(HOOK_MARKER_END, start_index) + len(HOOK_MARKER_END)
        current_block = existing[start_index:end_index]
        if _managed_block_hash(current_block) != managed_hash:
            return {
                "changed": False,
                "removed": False,
                "conflicts": ["managed Observatory hook block changed by user"],
                "path": str(path),
                "inference_proxy": False,
            }
        before = existing[:start_index]
        after = existing[end_index:]
        _write_text_atomic(path, before.rstrip() + ("\n" if after or before else "") + after.lstrip("\n"))
        return {"changed": True, "removed": True, "path": str(path), "inference_proxy": False}
    if spec.config_kind == "codex-toml":
        path = config_path(spec)
        if path is None or not path.exists() or not managed_keys or "managed_block" not in managed_keys:
            return {"changed": False, "removed": False, "path": str(path) if path else None, "inference_proxy": False}
        existing = path.read_text(encoding="utf-8")
        start = "# BEGIN LLM Observatory managed telemetry"
        end = "# END LLM Observatory managed telemetry"
        if start not in existing:
            return {"changed": False, "removed": False, "path": str(path), "inference_proxy": False}
        if end not in existing:
            raise ValueError(f"incomplete Observatory block in {path}")
        start_index = existing.index(start)
        end_index = existing.index(end, start_index) + len(end)
        current_block = existing[start_index:end_index]
        if not managed_hash:
            return {
                "changed": False,
                "removed": False,
                "conflicts": ["managed block hash is required before removal"],
                "path": str(path),
                "inference_proxy": False,
            }
        if _managed_block_hash(current_block) != managed_hash:
            return {
                "changed": False,
                "removed": False,
                "conflicts": ["managed Observatory block changed by user"],
                "path": str(path),
                "inference_proxy": False,
            }
        before = existing[:start_index]
        after = existing[end_index:]
        _write_text_atomic(path, before.rstrip() + ("\n" if after or before else "") + after.lstrip("\n"))
        return {"changed": True, "removed": True, "path": str(path), "inference_proxy": False}
    if spec.config_kind in {"claude-json", "gemini-json"}:
        return _remove_json(spec, managed_keys=managed_keys, managed_state=managed_state)
    return {"changed": False, "removed": False, "path": None, "inference_proxy": False}
