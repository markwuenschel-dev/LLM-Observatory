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
from typing import Any, Mapping, Sequence
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


CLAUDE_HOOK_EVENT = "SessionStart"
# Which skill or workflow ran is the one dimension the comparison surface needs
# and no client reports natively -- it exists only in a tool-call hook payload.
# Measured on this host, one hook invocation costs ~307 ms (median): ~30 ms
# interpreter start, ~128 ms imports, ~77 ms for the git subprocess in
# `resolve_project`, and the rest in POST and argument parsing. An unscoped
# PostToolUse hook therefore costs that on EVERY tool call -- 8,619 of them in a
# single observed session, about 44 minutes of added wall clock.
#
# The matcher is what makes the capture affordable: it fires only for the tools
# whose payloads `hooks._invoked_capability` can actually read, so the cost is
# paid a handful of times per session rather than thousands. Keep it in step
# with `_SKILL_TOOLS` / `_WORKFLOW_TOOLS` there.
CLAUDE_CAPABILITY_HOOK_EVENT = "PostToolUse"
CLAUDE_CAPABILITY_HOOK_MATCHER = "Skill|Workflow"
# Bounded so a telemetry hook can never delay a session start for long.
CLAUDE_HOOK_TIMEOUT_SECONDS = 5


def _claude_hook_handler() -> dict[str, Any]:
    return {
        "type": "command",
        "command": _hook_command("claude-code"),
        "timeout": CLAUDE_HOOK_TIMEOUT_SECONDS,
    }


def _is_observatory_claude_hook(handler: Any) -> bool:
    """Recognize our own hook without relying on external bookkeeping."""

    return (
        isinstance(handler, Mapping)
        and isinstance(handler.get("command"), str)
        and "hook --client claude-code" in handler["command"]
    )


def _claude_hook_groups(current: dict[str, Any], event: str = CLAUDE_HOOK_EVENT) -> list[Any]:
    hooks = current.get("hooks")
    if hooks is None:
        hooks = {}
    if not isinstance(hooks, dict):
        raise ValueError("settings.json has a non-object hooks section")
    groups = hooks.get(event)
    if groups is None:
        groups = []
    if not isinstance(groups, list):
        raise ValueError(f"settings.json has a non-array hooks.{event} section")
    current["hooks"] = hooks
    hooks[event] = groups
    return groups


def _apply_claude_hooks(current: dict[str, Any]) -> bool:
    """Install a user-level SessionStart hook that reports session -> project.

    Claude Code's OTel export carries a session id but no working directory, so
    native telemetry alone cannot say which repository a session belongs to. The
    hook runs once per session start, reports the resolved project alongside the
    same session id, and the store binds the two. It is configured at user level,
    so it applies to every repository without placing anything inside one.
    """

    changed = False
    for event, matcher in (
        (CLAUDE_HOOK_EVENT, ""),
        (CLAUDE_CAPABILITY_HOOK_EVENT, CLAUDE_CAPABILITY_HOOK_MATCHER),
    ):
        groups = _claude_hook_groups(current, event)
        desired = _claude_hook_handler()
        found = False
        for group in groups:
            if not isinstance(group, dict):
                continue
            handlers = group.get("hooks")
            if not isinstance(handlers, list):
                continue
            for index, handler in enumerate(handlers):
                if _is_observatory_claude_hook(handler):
                    found = True
                    if handler != desired:
                        handlers[index] = desired
                        changed = True
                    # An existing group whose matcher has drifted would silently
                    # widen or narrow what the hook fires on -- and widening is
                    # what makes it unaffordable.
                    if group.get("matcher") != matcher:
                        group["matcher"] = matcher
                        changed = True
        if not found:
            groups.append({"matcher": matcher, "hooks": [desired]})
            changed = True
    return changed


def _remove_claude_hooks(current: dict[str, Any]) -> bool:
    """Drop only the handlers we installed, leaving any user hooks intact."""

    hooks = current.get("hooks")
    if not isinstance(hooks, dict):
        return False
    changed = False
    # Both events, or `--remove` would leave the capability hook behind firing
    # on every Skill call with nothing listening.
    for event in (CLAUDE_HOOK_EVENT, CLAUDE_CAPABILITY_HOOK_EVENT):
        groups = hooks.get(event)
        if not isinstance(groups, list):
            continue
        event_changed = False
        surviving: list[Any] = []
        for group in groups:
            if not isinstance(group, dict):
                surviving.append(group)
                continue
            handlers = group.get("hooks")
            if not isinstance(handlers, list):
                surviving.append(group)
                continue
            kept = [handler for handler in handlers if not _is_observatory_claude_hook(handler)]
            if len(kept) != len(handlers):
                event_changed = True
            if kept:
                group["hooks"] = kept
                surviving.append(group)
            elif len(group) > 2 or set(group) - {"matcher", "hooks"}:
                group["hooks"] = kept
                surviving.append(group)
        if not event_changed:
            continue
        changed = True
        if surviving:
            hooks[event] = surviving
        else:
            hooks.pop(event, None)
    if not changed:
        return False
    if not hooks:
        current.pop("hooks", None)
    return True


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


CODEX_MARKER_START = "# BEGIN LLM Observatory managed telemetry"
CODEX_MARKER_END = "# END LLM Observatory managed telemetry"


def _unmanaged_observatory_hook(existing: str, client: str) -> bool:
    """True when the config already invokes our hook outside a managed block.

    Recognised by the command line rather than by any bookkeeping, so it holds
    for hooks written by hand, by an older version, or in a different TOML shape
    than the one we emit.
    """

    if not existing:
        return False
    marker = f"hook --client {client}"
    for line in existing.splitlines():
        if marker in line and "observatory" in line.casefold():
            return True
    return False


def configuration_drift(
    name: str,
    *,
    applied: bool,
    managed_keys: Sequence[str] | None = None,
    managed_hash: str | None = None,
) -> list[str]:
    """Report where the ownership record no longer matches what is on disk.

    Observatory records what it wrote so `--remove` can reverse exactly that and
    nothing else. That record is only trustworthy while it still describes the
    file. Found live: the manifest claimed a managed block for `grok` with a
    hash, while the config held unmarked hooks in a different format entirely --
    so `--remove --apply` would have found nothing to reverse and reported
    success, and `--apply` would have appended a second block, double-emitting
    every hook event.

    Nothing else detects this: `plan_configuration` is never given the manifest
    (`cli.py:2369-2398` passes it only to apply/remove), so the plan cannot
    compare its claim against the file.
    """

    if not applied:
        return []
    spec = client_spec(name)
    path = config_path(spec)
    if path is None:
        return []
    if not path.exists():
        return [f"{spec.name}: recorded as configured, but {path} no longer exists"]
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return [f"{spec.name}: recorded as configured, but {path} cannot be read ({exc})"]

    keys = list(managed_keys or [])
    problems: list[str] = []
    if "managed_block" in keys:
        hook_kinds = {"kimi-toml-hook", "grok-toml-hook"}
        start = HOOK_MARKER_START if spec.config_kind in hook_kinds else CODEX_MARKER_START
        end = HOOK_MARKER_END if spec.config_kind in hook_kinds else CODEX_MARKER_END
        if start not in text:
            problems.append(
                f"{spec.name}: recorded as owning a managed block in {path}, but no Observatory "
                "marker is present -- `--remove` cannot reverse it and `--apply` would add a "
                "second, duplicating its telemetry"
            )
        elif end not in text:
            problems.append(f"{spec.name}: the managed block in {path} is missing its END marker")
        elif managed_hash:
            block = text[text.index(start): text.index(end, text.index(start)) + len(end)]
            if _managed_block_hash(block) != managed_hash:
                problems.append(
                    f"{spec.name}: the managed block in {path} was edited after Observatory wrote it"
                )
    elif keys:
        section_name = "env" if spec.config_kind == "claude-json" else "telemetry"
        section = _read_json_object(path).get(section_name)
        section = section if isinstance(section, Mapping) else {}
        missing = [key for key in keys if key not in section]
        if missing:
            problems.append(
                f"{spec.name}: recorded as managing {len(keys)} {section_name} keys in {path}, "
                f"but {len(missing)} are gone ({', '.join(sorted(missing)[:4])}"
                f"{'...' if len(missing) > 4 else ''}) -- telemetry may be silently off"
            )
    return problems


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
    if spec.config_kind == "claude-json" and _apply_claude_hooks(current):
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
    drifted = False
    if marker_start in existing:
        start_index = existing.index(marker_start)
        end_index = existing.index(marker_end, start_index) + len(marker_end)
        current_block = existing[start_index:end_index]
        expected_hash = managed_hash or _managed_block_hash(block)
        drifted = _managed_block_hash(current_block) != expected_hash
        # `--force` is the documented way to replace a conflicting managed
        # value, and every JSON client honors it.
        if drifted and not force:
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
        "overwritten": ["managed_block"] if drifted else [],
        "inference_proxy": False,
        "content_capture": False,
    }


def _apply_hook(spec: ClientSpec, *, force: bool = False, managed_hash: str | None = None) -> dict[str, Any]:
    path = config_path(spec)
    if path is None:
        raise ValueError(f"client {spec.name} does not have a hook configuration path")
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if HOOK_MARKER_START in existing and HOOK_MARKER_END not in existing:
        raise ValueError(f"incomplete Observatory hook block in {path}")
    block = _hook_block(spec)
    drifted = False
    if HOOK_MARKER_START in existing:
        start_index = existing.index(HOOK_MARKER_START)
        end_index = existing.index(HOOK_MARKER_END, start_index) + len(HOOK_MARKER_END)
        current_block = existing[start_index:end_index]
        expected_hash = managed_hash or _managed_block_hash(block)
        drifted = _managed_block_hash(current_block) != expected_hash
        # `--force` is the documented way to replace a conflicting managed
        # value, and every JSON client honors it.
        if drifted and not force:
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
    elif _unmanaged_observatory_hook(existing, spec.name) and not force:
        # Observatory hooks are already here without our markers -- installed by
        # hand, or by a version that did not mark them. Appending a marked block
        # would leave the client invoking the hook twice per event, doubling
        # this client's telemetry against every other client it is compared
        # with. Found live on grok, whose config carries three unmarked
        # `observatory.exe hook --client grok` entries in a different TOML shape
        # from the one we write.
        return {
            "changed": False,
            "conflicts": ["unmarked Observatory hooks already present; applying would duplicate them"],
            "path": str(path),
            "inference_proxy": False,
            "managed_block": True,
            "managed_keys": ["managed_block"],
        }
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
        "overwritten": ["managed_block"] if drifted else [],
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
        else _apply_hook(spec, force=force, managed_hash=managed_hash)
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
    hooks_removed = _remove_claude_hooks(current) if spec.config_kind == "claude-json" else False
    if removed or restored or hooks_removed:
        if section:
            current[section_name] = section
        else:
            current.pop(section_name, None)
        _write_json_atomic(path, current)
    return {
        "changed": bool(removed or restored or hooks_removed),
        "removed": sorted(removed),
        "restored": sorted(restored),
        "hooks_removed": hooks_removed,
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
