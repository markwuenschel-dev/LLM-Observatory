"""Current client capability records and safe, global configuration plans.

The catalog is deliberately data-driven.  Native telemetry configuration is
only applied for clients whose first-party settings contract is known.  Other
clients still get a useful discovery/adapter plan; the Observatory never
rewrites an inference endpoint or adds a repository-local file.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Mapping

from .adapters.base import CapabilityRecord


LOCAL_OTLP_GRPC = "http://127.0.0.1:4317"
LOCAL_OTLP_HTTP = "http://127.0.0.1:4318"


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

    def capabilities_record(self, *, installed: bool | None = None) -> CapabilityRecord:
        capabilities = dict(self.capabilities)
        if installed is False:
            capabilities["installed"] = "NOT_INSTALLED"
        elif installed is True:
            capabilities["installed"] = "VERIFIED_LOCALLY"
        return CapabilityRecord(
            provider=self.provider,
            client=self.name,
            confidence=self.confidence,
            capabilities=capabilities,
            auth_modes=self.auth_modes,
            evidence=self.evidence,
            last_verified="2026-08-07",
        )


_COMMON = {
    "zero_repository_contamination": "SUPPORTED",
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
            "native_otel": "SUPPORTED",
            "signals": "metrics,logs,traces-beta",
            "subscription_telemetry": "SUPPORTED",
            "authoritative_usage": "SUPPORTED",
            "session_and_tool_identity": "SUPPORTED",
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
            "subscription_telemetry": "SUPPORTED",
            "authoritative_usage": "PARTIAL",
            "session_and_agent_identity": "PARTIAL",
            "global_configuration": "SUPPORTED",
        },
        auth_modes=("subscription", "api"),
        evidence=("https://learn.chatgpt.com/docs/config-file/config-reference#configtoml",),
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
            "subscription_telemetry": "PARTIAL",
            "authoritative_usage": "SUPPORTED",
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
            "signals": "structured-output/hooks",
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("subscription", "api"),
        evidence=("Local installation was detected; native OTel configuration was not verified.",),
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
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("subscription", "api"),
        evidence=("Local installation was detected; native OTel configuration was not verified.",),
        config_kind="discovery-only",
        native_config=False,
    ),
    "grok": ClientSpec(
        name="grok",
        provider="xai",
        confidence="PARTIAL",
        capabilities={
            **_COMMON,
            "native_otel": "UNKNOWN",
            "signals": "structured-output/sessions",
            "authoritative_usage": "PARTIAL",
            "model_identity": "SUPPORTED",
            "global_configuration": "PARTIAL",
        },
        auth_modes=("api",),
        evidence=("Local capability research observed structured output; native OTel was not verified.",),
        config_kind="discovery-only",
        native_config=False,
    ),
    "openrouter": ClientSpec(
        name="openrouter-api",
        provider="openrouter",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={
            **_COMMON,
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
        capabilities={**_COMMON, "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-anthropic": ClientSpec(
        name="direct-anthropic-api",
        provider="anthropic",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-google": ClientSpec(
        name="direct-google-api",
        provider="google",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api", "vertex"), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
    "direct-xai": ClientSpec(
        name="direct-xai-api",
        provider="xai",
        confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
        capabilities={**_COMMON, "native_otel": "CALLER_OWNED", "signals": "response-envelope", "authoritative_usage": "SUPPORTED", "global_configuration": "CALLER_OWNED"},
        auth_modes=("api",), evidence=("The caller-owned response adapter preserves provider usage without owning inference routing.",), config_kind="adapter-only", native_config=False,
    ),
}


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


def discover_client(name: str) -> dict[str, Any]:
    spec = client_spec(name)
    executable = next((shutil.which(candidate) for candidate in _executable_candidates(spec) if shutil.which(candidate)), None)
    path = config_path(spec)
    return {
        "client": spec.name,
        "provider": spec.provider,
        "installed": executable is not None,
        "executable": executable,
        "config_path": str(path) if path else None,
        "config_exists": bool(path and path.exists()),
        "capabilities": spec.capabilities_record(installed=executable is not None).to_mapping(),
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
        "OTEL_EXPORTER_OTLP_ENDPOINT": LOCAL_OTLP_GRPC,
        "OTEL_LOG_USER_PROMPTS": "0",
        "OTEL_LOG_TOOL_DETAILS": "0",
        "OTEL_LOG_TOOL_CONTENT": "0",
        "OTEL_LOG_RAW_API_BODIES": "0",
    }
    if enable_traces:
        values.update({"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1", "OTEL_TRACES_EXPORTER": "otlp"})
    return values


def _gemini_values() -> dict[str, Any]:
    return {
        "enabled": True,
        "traces": False,
        "target": "local",
        "otlpEndpoint": LOCAL_OTLP_GRPC,
        "otlpProtocol": "grpc",
        "logPrompts": False,
        "useCollector": True,
    }


def _codex_block(*, enable_traces: bool) -> str:
    trace_setting = (
        'trace_exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/traces", protocol = "json" } }\n'
        if enable_traces
        else 'trace_exporter = "none"\n'
    )
    return (
        "# BEGIN LLM Observatory managed telemetry\n"
        "[otel]\n"
        "environment = \"llm-observatory\"\n"
        'exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/logs", protocol = "json" } }\n'
        'metrics_exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/metrics", protocol = "json" } }\n'
        f"{trace_setting}"
        "log_user_prompt = false\n"
        "# END LLM Observatory managed telemetry\n"
    )


def _managed_block_hash(block: str) -> str:
    return hashlib.sha256(block.rstrip("\n").encode("utf-8")).hexdigest()


def plan_configuration(name: str, *, enable_traces: bool = False) -> dict[str, Any]:
    spec = client_spec(name)
    discovered = discover_client(name)
    path = config_path(spec)
    plan: dict[str, Any] = {
        "client": spec.name,
        "provider": spec.provider,
        "mode": "native-otlp" if spec.native_config else spec.config_kind,
        "inference_proxy": False,
        "config_path": str(path) if path else None,
        "config_exists": bool(path and path.exists()),
        "installed": discovered["installed"],
        "capabilities": spec.capabilities_record(installed=discovered["installed"]).to_mapping(),
        "changes": {},
        "apply_required": bool(spec.native_config),
        "supported": spec.native_config,
    }
    if spec.config_kind == "claude-json":
        plan["changes"] = {"env": _claude_values(enable_traces=enable_traces)}
    elif spec.config_kind == "gemini-json":
        plan["changes"] = {"telemetry": _gemini_values()}
    elif spec.config_kind == "codex-toml":
        plan["changes"] = {"toml_block": _codex_block(enable_traces=enable_traces)}
    else:
        plan["warnings"] = ["No first-party global OTLP configuration contract is verified for this client; use the response/JSONL adapter or configure its native hooks separately."]
    return plan


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"configuration file must contain an object: {path}")
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".observatory.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _apply_json(spec: ClientSpec, *, enable_traces: bool, force: bool) -> dict[str, Any]:
    path = config_path(spec)
    if path is None:
        raise ValueError(f"client {spec.name} does not have a JSON configuration path")
    current = _read_json_object(path)
    desired = _claude_values(enable_traces=enable_traces) if spec.config_kind == "claude-json" else _gemini_values()
    section_name = "env" if spec.config_kind == "claude-json" else "telemetry"
    section = current.get(section_name)
    if section is None:
        section = {}
    if not isinstance(section, dict):
        raise ValueError(f"{path} has a non-object {section_name} section")
    conflicts = {key: section[key] for key in desired if key in section and section[key] != desired[key]}
    if conflicts and not force:
        return {"changed": False, "conflicts": sorted(conflicts), "path": str(path), "inference_proxy": False}
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
        "conflicts": sorted(conflicts),
        "path": str(path),
        "managed_keys": sorted(desired),
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
        if existing_otel_tables and not force:
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


def apply_configuration(name: str, *, enable_traces: bool = False, force: bool = False, managed_hash: str | None = None) -> dict[str, Any]:
    spec = client_spec(name)
    discovered = discover_client(name)
    if not spec.native_config:
        result = plan_configuration(name, enable_traces=enable_traces)
        result["applied"] = False
        return result
    result = _apply_codex(enable_traces=enable_traces, force=force, managed_hash=managed_hash) if spec.config_kind == "codex-toml" else _apply_json(spec, enable_traces=enable_traces, force=force)
    result.update({
        "client": spec.name,
        "provider": spec.provider,
        "applied": not bool(result.get("conflicts")),
        "mode": "native-otlp",
        "capabilities": spec.capabilities_record(installed=discovered["installed"]).to_mapping(),
    })
    return result


def _remove_json(spec: ClientSpec, *, managed_keys: list[str] | None = None) -> dict[str, Any]:
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
    desired = _claude_values(enable_traces=True) if spec.config_kind == "claude-json" else _gemini_values()
    desired = {key: value for key, value in desired.items() if key in managed_keys}
    removed = [key for key, value in desired.items() if section.get(key) == value]
    for key in removed:
        del section[key]
    if removed:
        if section:
            current[section_name] = section
        else:
            current.pop(section_name, None)
        _write_json_atomic(path, current)
    return {"changed": bool(removed), "removed": sorted(removed), "path": str(path), "inference_proxy": False}


def remove_configuration(name: str, *, managed_keys: list[str] | None = None, managed_hash: str | None = None) -> dict[str, Any]:
    spec = client_spec(name)
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
        path.write_text(before.rstrip() + ("\n" if after or before else "") + after.lstrip("\n"), encoding="utf-8")
        return {"changed": True, "removed": True, "path": str(path), "inference_proxy": False}
    if spec.config_kind in {"claude-json", "gemini-json"}:
        return _remove_json(spec, managed_keys=managed_keys)
    return {"changed": False, "removed": False, "path": None, "inference_proxy": False}
