"""The host-side `observatory` lifecycle and ingestion CLI."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import socket
import shutil
import sqlite3
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.error import URLError
from urllib.request import Request, urlopen
import webbrowser

from .api import serve
from .clients import (
    CLIENT_SPECS,
    apply_configuration,
    client_spec,
    discovery,
    normalize_client_name,
    plan_configuration,
    remove_configuration,
)
from .clock import utc_now
from .contracts import ContractError, NormalizedEvent, canonical_json
from .intake import Intake
from .maintenance import backup_database, backup_state, inspect_backend_volume_capacity, purge_events, read_schema_versions, resolve_backend_volumes, restore_database, restore_state, schema_versions
from .privacy import PrivacyPolicy, redact_event
from .project import resolve_project
from .outcomes import git_outcome_snapshot, make_outcome_event, run_command_outcome
from .store import DEFAULT_MAX_DATABASE_BYTES, EventStore


EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_INITIALIZED = 3
EXIT_FAILED = 4
EXIT_DEGRADED = 5
EXIT_CONFLICT = 6
EXIT_UNSUPPORTED = 7
MAX_SPOOL_BYTES = 64 * 1024 * 1024
MAX_SPOOL_FILES = 128
MAX_INGEST_BYTES = 8 * 1024 * 1024
MAX_INGEST_RECORDS = 256
MAX_INGEST_ERRORS = 256
DEFAULT_OPERATION_TIMEOUT = 180.0
DEFAULT_MIN_FREE_BYTES = 1 * 1024 ** 3
DEFAULT_MAX_BACKEND_VOLUME_BYTES = 16 * 1024 ** 3
DEFAULT_COMPOSE_PROJECT = "llm-observatory"
DEFAULT_RETENTION = {
    "prometheus_days": 30,
    "prometheus_size": "8GB",
    "tempo_hours": 720,
    "loki_hours": 336,
    "normalized_events": "operator-managed",
}
DEFAULT_STORAGE = {
    "max_backend_volume_bytes": DEFAULT_MAX_BACKEND_VOLUME_BYTES,
    "min_free_bytes": DEFAULT_MIN_FREE_BYTES,
}

_COMPOSE_ROLLBACK_IMAGE_ID = re.compile(r"^(?:sha256:)?[0-9a-f]{12,64}$", re.IGNORECASE)


class SpoolFullError(RuntimeError):
    """The bounded offline telemetry spool cannot accept another batch."""


@dataclass(frozen=True)
class StatePaths:
    root: Path

    @property
    def config(self) -> Path:
        return self.root / "config.json"

    @property
    def database(self) -> Path:
        return self.root / "data" / "events.sqlite3"

    @property
    def spool(self) -> Path:
        return self.root / "spool"

    @property
    def secret(self) -> Path:
        return self.root / "secrets" / "grafana_admin_password"

    @property
    def compose_env(self) -> Path:
        return self.root / "compose.env"


def default_state_dir() -> Path:
    configured = os.environ.get("OBSERVATORY_STATE_DIR")
    if configured:
        return Path(configured).expanduser()
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or str(Path.home())
    return Path(base) / "LLM-Observatory"


def _paths(args: argparse.Namespace) -> StatePaths:
    return StatePaths(Path(args.state_dir).expanduser() if args.state_dir else default_state_dir())


def _is_loopback_host(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _read_api_token(path_value: str | None) -> str | None:
    """Read one operator-owned bearer token without echoing or persisting it."""

    if not path_value:
        return None
    path = Path(path_value).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"API token path is not a regular file: {path}")
    token = path.read_text(encoding="utf-8").strip()
    if not token or len(token) > 4096 or any(character.isspace() for character in token):
        raise ValueError("API token file must contain one non-empty token without whitespace")
    return token


def _load_config(paths: StatePaths) -> dict[str, Any] | None:
    if not paths.config.exists():
        return None
    try:
        value = json.loads(paths.config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid state configuration: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != "1.0":
        raise RuntimeError("unsupported state configuration schema")
    return value


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _retention_environment(retention: Mapping[str, Any] | None) -> dict[str, str]:
    values = dict(DEFAULT_RETENTION)
    if isinstance(retention, Mapping):
        values.update({key: value for key, value in retention.items() if value is not None})

    def positive_int(key: str) -> int:
        value = values.get(key)
        return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else int(DEFAULT_RETENTION[key])

    size = values.get("prometheus_size")
    return {
        "PROMETHEUS_RETENTION_TIME": f"{positive_int('prometheus_days')}d",
        "PROMETHEUS_RETENTION_SIZE": str(size) if isinstance(size, str) and size.strip() else str(DEFAULT_RETENTION["prometheus_size"]),
        "TEMPO_RETENTION": f"{positive_int('tempo_hours')}h",
        "LOKI_RETENTION": f"{positive_int('loki_hours')}h",
    }


def _storage_environment(storage: Mapping[str, Any] | None) -> dict[str, str]:
    values = dict(DEFAULT_STORAGE)
    if isinstance(storage, Mapping):
        values.update({key: value for key, value in storage.items() if value is not None})
    value = values.get("max_backend_volume_bytes")
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        value = DEFAULT_MAX_BACKEND_VOLUME_BYTES
    return {"OBSERVATORY_MAX_BACKEND_VOLUME_BYTES": str(value)}


def _reconcile_compose_env(
    paths: StatePaths,
    retention: Mapping[str, Any] | None = None,
    storage: Mapping[str, Any] | None = None,
) -> bool:
    """Keep Compose pointed at the same host state and retention policy used by the CLI."""

    desired = {
        "OBSERVATORY_STATE_DIR": paths.root.resolve().as_posix(),
        "OBSERVATORY_SECRET_FILE": paths.secret.resolve().as_posix(),
    }
    desired.update(_retention_environment(retention))
    desired.update(_storage_environment(storage))
    existing = paths.compose_env.read_text(encoding="utf-8") if paths.compose_env.exists() else ""
    lines = existing.splitlines()
    output: list[str] = []
    seen: set[str] = set()
    for line in lines:
        stripped = line.strip()
        key = stripped.split("=", 1)[0] if stripped and not stripped.startswith("#") and "=" in stripped else None
        if key not in desired:
            output.append(line)
            continue
        if key in seen:
            continue
        output.append(f"{key}={desired[key]}")
        seen.add(key)
    for key, value in desired.items():
        if key not in seen:
            output.append(f"{key}={value}")
    reconciled = "\n".join(output).rstrip("\n") + "\n"
    if reconciled == existing:
        return False
    paths.compose_env.parent.mkdir(parents=True, exist_ok=True)
    temporary = paths.compose_env.with_suffix(paths.compose_env.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(reconciled)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, paths.compose_env)
    return True


def _result(command: str, outcome: str, exit_code: int, *, data: dict[str, Any] | None = None, warnings: Iterable[str] = (), errors: Iterable[str] = ()) -> dict[str, Any]:
    return {
        "schema": "observatory.cli/v1",
        "command": command,
        "outcome": outcome,
        "exit_code": exit_code,
        "checks": [],
        "data": data or {},
        "warnings": list(warnings),
        "errors": list(errors),
    }


def _print_result(value: dict[str, Any], json_mode: bool) -> int:
    if json_mode:
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    else:
        print(f"{value['command']}: {value['outcome']} (exit {value['exit_code']})")
        for warning in value["warnings"]:
            print(f"warning: {warning}", file=sys.stderr)
        for error in value["errors"]:
            print(f"error: {error}", file=sys.stderr)
    return int(value["exit_code"])


def _probe_http(url: str, timeout: float = 0.5) -> tuple[bool, str]:
    try:
        request = Request(url, method="GET")
        with urlopen(request, timeout=timeout) as response:
            response.read(256)
            return 200 <= response.status < 400, f"HTTP {response.status}"
    except (OSError, URLError, ValueError) as exc:
        return False, str(exc)


def _path_key(value: str | Path) -> str:
    """Normalize a local path for comparison without reading or storing its contents."""

    return os.path.normcase(os.path.normpath(os.path.abspath(os.path.expanduser(str(value)))))


def _inspect_live_compose_state(expected_state: Path, *, timeout: float = 5.0) -> dict[str, Any]:
    """Read Docker's Compose labels and bind mounts without changing the stack."""

    project = DEFAULT_COMPOSE_PROJECT
    bounded_timeout = max(1.0, min(timeout, 5.0))
    format_template = (
        '{{.Names}}|{{.State}}|{{.Label "com.docker.compose.project.environment_file"}}|'
        '{{.Label "com.docker.compose.project.working_dir"}}|{{.Label "com.docker.compose.service"}}'
    )
    command = [
        "docker",
        "container",
        "ls",
        "--all",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--format",
        format_template,
    ]
    try:
        listed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=bounded_timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"project": project, "status": "unavailable", "stale": False, "detail": str(exc), "services": []}
    if listed.returncode != 0:
        detail = (listed.stderr or listed.stdout or "Docker container listing failed").strip()
        return {"project": project, "status": "unavailable", "stale": False, "detail": detail, "services": []}

    services: list[dict[str, str]] = []
    names: list[str] = []
    environment_paths: dict[str, dict[str, Any]] = {}
    working_directories: set[str] = set()
    for line in listed.stdout.splitlines():
        fields = line.split("|", 4)
        if len(fields) != 5 or not fields[0]:
            continue
        name, state, environment_file, working_directory, service = fields
        names.append(name)
        services.append({"name": name, "service": service or name, "state": state})
        if environment_file:
            key = _path_key(environment_file)
            environment_paths[key] = {
                "path": environment_file,
                "exists": Path(environment_file).exists(),
                "matches_state": key == _path_key(expected_state / "compose.env"),
            }
        if working_directory:
            working_directories.add(working_directory)

    environment_files = sorted(environment_paths.values(), key=lambda value: value["path"])
    stale_reasons: list[str] = []
    if environment_files and any(not value["exists"] for value in environment_files):
        stale_reasons.append("environment_file_missing")
    if environment_files and not any(value["matches_state"] for value in environment_files):
        stale_reasons.append("environment_file_mismatch")

    binds: list[dict[str, Any]] = []
    inspect_detail: str | None = None
    if names:
        inspect_command = ["docker", "container", "inspect", "--format", "{{json .Mounts}}", *names]
        try:
            inspected = subprocess.run(
                inspect_command,
                check=False,
                capture_output=True,
                text=True,
                timeout=bounded_timeout,
            )
            if inspected.returncode == 0:
                for name, line in zip(names, inspected.stdout.splitlines()):
                    try:
                        mounts = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(mounts, list):
                        continue
                    for mount in mounts:
                        if not isinstance(mount, dict) or mount.get("Destination") != "/var/lib/observatory":
                            continue
                        source = mount.get("Source")
                        if not isinstance(source, str) or not source:
                            continue
                        source_key = _path_key(source)
                        binds.append(
                            {
                                "container": name,
                                "destination": "/var/lib/observatory",
                                "source": source,
                                "exists": Path(source).exists(),
                                "matches_state": source_key == _path_key(expected_state),
                            }
                        )
            else:
                inspect_detail = (inspected.stderr or inspected.stdout or "Docker container inspection failed").strip()
        except (OSError, subprocess.TimeoutExpired) as exc:
            inspect_detail = str(exc)
    if binds and any(not value["matches_state"] for value in binds):
        stale_reasons.append("state_bind_mismatch")

    result: dict[str, Any] = {
        "project": project,
        "status": "present" if services else "absent",
        "stale": bool(stale_reasons),
        "stale_reasons": stale_reasons,
        "services": services,
        "environment_files": environment_files,
        "working_directories": sorted(working_directories),
        "binds": binds,
    }
    if inspect_detail:
        result["inspect_detail"] = inspect_detail
    return result


def _live_compose_warning(live_compose: Mapping[str, Any]) -> str | None:
    if not live_compose.get("stale"):
        return None
    project = live_compose.get("project", DEFAULT_COMPOSE_PROJECT)
    return (
        f"live Compose project {project} references stale generated state; "
        "run observatory install to reconcile the requested state before using lifecycle commands"
    )


def _wait_http(url: str, *, timeout: float) -> tuple[bool, str]:
    """Wait for a bounded readiness window after a lifecycle operation."""

    deadline = time.monotonic() + max(0.5, min(timeout, 30.0))
    last_detail = "readiness probe did not run"
    while True:
        ready, detail = _probe_http(url)
        if ready:
            return True, detail
        last_detail = detail
        if time.monotonic() >= deadline:
            return False, last_detail
        time.sleep(0.5)


def _read_only_integrity(path: Path) -> str:
    if not path.exists():
        return "missing"
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2.0)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _storage_bytes(path: Path) -> int:
    total = 0
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            total += candidate.stat().st_size
        except OSError:
            pass
    return total


def _configured_database_limit() -> int:
    raw = os.environ.get("OBSERVATORY_MAX_DATABASE_BYTES")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return DEFAULT_MAX_DATABASE_BYTES


def _configured_backend_volume_limit(storage: Mapping[str, Any] | None = None) -> int:
    raw = os.environ.get("OBSERVATORY_MAX_BACKEND_VOLUME_BYTES")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    if isinstance(storage, Mapping):
        value = storage.get("max_backend_volume_bytes")
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    return DEFAULT_MAX_BACKEND_VOLUME_BYTES


def _event_store(paths: StatePaths) -> EventStore:
    return EventStore(paths.database, max_bytes=_configured_database_limit())


def _command_install(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    try:
        existing = _load_config(paths)
    except RuntimeError as exc:
        return _result("install", "conflict", EXIT_CONFLICT, errors=[str(exc)])
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.database.parent.mkdir(parents=True, exist_ok=True)
    paths.spool.mkdir(parents=True, exist_ok=True)
    paths.secret.parent.mkdir(parents=True, exist_ok=True)
    for private_directory in (paths.root, paths.database.parent, paths.spool, paths.secret.parent):
        try:
            os.chmod(private_directory, 0o700)
        except OSError:
            # Windows ACLs govern these directories; Unix profiles still get
            # a private default when the filesystem supports chmod.
            pass
    secret_created = not paths.secret.exists() or paths.secret.stat().st_size == 0
    if secret_created:
        # The file is mounted into Linux containers even when the host is Windows.
        # Keep the mounted secret LF-only so Grafana does not receive a trailing CR.
        with paths.secret.open("w", encoding="utf-8", newline="\n") as secret_file:
            secret_file.write(secrets.token_urlsafe(32) + "\n")
    try:
        os.chmod(paths.secret, 0o600)
    except OSError:
        # Windows ACLs govern the file there; do not make install fail when
        # chmod is unsupported, while Unix profiles still get a private mode.
        pass
    compose_candidate = Path(args.compose_file).expanduser() if args.compose_file else Path.cwd() / "compose.yaml"
    configured_compose = str(compose_candidate.resolve()) if compose_candidate.exists() else (existing or {}).get("compose_file")
    config = existing or {
        "schema_version": "1.0",
        "privacy": {"content_capture": False, "store_raw_paths": False},
        "retention": dict(DEFAULT_RETENTION),
        "storage": dict(DEFAULT_STORAGE),
        "spool": {"max_bytes": MAX_SPOOL_BYTES, "max_files": MAX_SPOOL_FILES},
        "managed_clients": {},
        "created_at": utc_now().isoformat(),
    }
    changed = existing is None
    if existing is not None:
        if "retention" not in config:
            config["retention"] = dict(DEFAULT_RETENTION)
            changed = True
        elif isinstance(config["retention"], dict) and "prometheus_size" not in config["retention"]:
            config["retention"]["prometheus_size"] = DEFAULT_RETENTION["prometheus_size"]
            changed = True
        if "spool" not in config:
            config["spool"] = {"max_bytes": MAX_SPOOL_BYTES, "max_files": MAX_SPOOL_FILES}
            changed = True
        if not isinstance(config.get("storage"), dict):
            config["storage"] = dict(DEFAULT_STORAGE)
            changed = True
        else:
            for key, value in DEFAULT_STORAGE.items():
                if key not in config["storage"]:
                    config["storage"][key] = value
                    changed = True
    if configured_compose and config.get("compose_file") != configured_compose:
        config["compose_file"] = configured_compose
        changed = True
    env_changed = _reconcile_compose_env(paths, config.get("retention"), config.get("storage"))
    changed = changed or env_changed
    if not paths.config.exists() or changed:
        _write_json_atomic(paths.config, config)
    with _event_store(paths) as store:
        versions = schema_versions(store)
    demo_result = _seed_demo(paths, getattr(args, "demo_file", None)) if getattr(args, "demo", False) else None
    demo_failed = demo_result is not None and demo_result["outcome"] != "success"
    return _result(
        "install",
        "degraded" if demo_failed else "success",
        EXIT_DEGRADED if demo_failed else EXIT_OK,
        data={
            "state_dir": str(paths.root),
            "changed": changed,
            "secret_created": secret_created,
            "database": str(paths.database),
            "schema_versions": versions,
            "content_capture": False,
            "demo": demo_result["data"] if demo_result is not None else {"seeded": False},
        },
        errors=demo_result["errors"] if demo_result is not None else (),
    )


def _command_doctor(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    checks: list[dict[str, Any]] = []
    if not paths.config.exists():
        live_compose = _inspect_live_compose_state(paths.root)
        checks.append({
            "id": "compose.live_state",
            "status": "warn" if live_compose.get("stale") or live_compose.get("status") == "unavailable" else "pass",
            "blocking": False,
            **live_compose,
        })
        warnings = []
        live_warning = _live_compose_warning(live_compose)
        if live_warning:
            warnings.append(live_warning)
        result = _result(
            "doctor",
            "not_initialized",
            EXIT_NOT_INITIALIZED,
            data={"checks": checks, "state_dir": str(paths.root), "live_compose": live_compose},
            warnings=warnings,
            errors=["run observatory install first"],
        )
        result["checks"] = checks
        return result
    try:
        config = _load_config(paths)
    except RuntimeError as exc:
        return _result("doctor", "conflict", EXIT_CONFLICT, errors=[str(exc)])
    checks.append({"id": "state.config", "status": "pass", "blocking": True})
    checks.append({"id": "privacy.content_capture", "status": "pass" if config.get("privacy", {}).get("content_capture") is False else "warn", "blocking": True})
    checks.append({"id": "retention.policy", "status": "pass" if isinstance(config.get("retention"), dict) else "warn", "blocking": False})
    checks.append({"id": "storage.policy", "status": "pass" if isinstance(config.get("storage"), dict) else "warn", "blocking": False, "max_backend_volume_bytes": _configured_backend_volume_limit(config.get("storage"))})
    checks.append({"id": "spool.policy", "status": "pass" if isinstance(config.get("spool"), dict) else "warn", "blocking": False})
    checks.append({"id": "python.version", "status": "pass" if sys.version_info >= (3, 11) else "fail", "blocking": True, "value": sys.version.split()[0]})
    compose = _compose_path(args, paths)
    checks.append({"id": "compose.file", "status": "pass" if compose.exists() else "warn", "blocking": False, "path": str(compose)})
    checks.append({"id": "state.compose_env", "status": "pass" if paths.compose_env.exists() else "fail", "blocking": True})
    checks.append({"id": "state.grafana_secret", "status": "pass" if paths.secret.exists() else "fail", "blocking": True})
    checks.append({"id": "state.database", "status": "pass" if paths.database.exists() else "fail", "blocking": True})
    try:
        integrity = _read_only_integrity(paths.database)
    except (OSError, sqlite3.Error) as exc:
        integrity = str(exc)
    checks.append({"id": "state.database_integrity", "status": "pass" if integrity == "ok" else "fail", "blocking": True, "value": integrity})
    if paths.compose_env.exists():
        compose_env = paths.compose_env.read_text(encoding="utf-8")
        env_matches_state = f"OBSERVATORY_STATE_DIR={paths.root.resolve().as_posix()}" in compose_env
        env_matches_secret = f"OBSERVATORY_SECRET_FILE={paths.secret.resolve().as_posix()}" in compose_env
        checks.append({"id": "state.compose_env_alignment", "status": "pass" if env_matches_state and env_matches_secret else "fail", "blocking": True})
    live_compose = _inspect_live_compose_state(paths.root)
    checks.append({
        "id": "compose.live_state",
        "status": "warn" if live_compose.get("stale") or live_compose.get("status") == "unavailable" else "pass",
        "blocking": False,
        **live_compose,
    })
    warnings: list[str] = []
    live_warning = _live_compose_warning(live_compose)
    if live_warning:
        warnings.append(live_warning)
    try:
        free_bytes = shutil.disk_usage(paths.root).free
        free_gib = round(free_bytes / (1024 ** 3), 2)
        disk_status = "pass" if free_bytes >= DEFAULT_MIN_FREE_BYTES else "warn"
        checks.append({"id": "state.disk_free", "status": disk_status, "blocking": False, "free_gib": free_gib})
        if disk_status == "warn":
            warnings.append(f"state directory has only {free_gib} GiB free; retention and queue growth need operator attention")
    except OSError as exc:
        warnings.append(f"could not inspect state-disk capacity: {exc}")
    database_bytes = _storage_bytes(paths.database)
    database_limit = _configured_database_limit()
    database_ratio = database_bytes / database_limit
    database_status = "fail" if database_ratio >= 1 else "warn" if database_ratio >= 0.9 else "pass"
    checks.append({"id": "state.database_capacity", "status": database_status, "blocking": False, "bytes": database_bytes, "max_bytes": database_limit, "ratio": database_ratio})
    if database_status == "fail":
        warnings.append(f"normalized store is at its {database_limit}-byte budget; prune or back up before accepting more telemetry")
    elif database_status == "warn":
        warnings.append(f"normalized store is at {database_ratio:.1%} of its {database_limit}-byte budget")
    try:
        client_profiles = discovery()
        checks.append({
            "id": "clients.capability_catalog",
            "status": "pass",
            "blocking": False,
            "targets": [
                {
                    "client": profile.get("client"),
                    "provider": profile.get("provider"),
                    "installed": profile.get("installed"),
                    "version_probe_status": profile.get("version_probe_status"),
                    "confidence": profile.get("capabilities", {}).get("confidence") if isinstance(profile.get("capabilities"), dict) else None,
                }
                for profile in client_profiles
            ],
        })
    except (OSError, RuntimeError, ValueError) as exc:
        checks.append({"id": "clients.capability_catalog", "status": "warn", "blocking": False, "detail": str(exc)})
        warnings.append(f"client capability discovery is incomplete: {exc}")
    for service_id, url in (
        ("service.api", "http://127.0.0.1:8787/readyz"),
        ("service.collector", "http://127.0.0.1:13133/"),
        ("service.grafana", "http://127.0.0.1:3000/api/health"),
    ):
        ready, detail = _probe_http(url)
        checks.append({"id": service_id, "status": "pass" if ready else "warn", "blocking": False, "detail": detail})
        if not ready:
            warnings.append(f"{service_id} is not reachable at {url}")
    if not compose.exists():
        warnings.append(f"Compose file not found at {compose}; lifecycle commands are unavailable")
    if not paths.compose_env.exists() or not paths.secret.exists() or not paths.database.exists():
        warnings.append("generated Compose state is incomplete; run observatory install to reconcile it")
    try:
        compose_probe = subprocess.run(["docker", "compose", "version"], check=False, capture_output=True, text=True, timeout=5)
        compose_ready = compose_probe.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        compose_ready = False
    checks.append({"id": "docker.compose", "status": "pass" if compose_ready else "fail", "blocking": False})
    if not compose_ready:
        warnings.append("Docker Compose CLI is unavailable")
    try:
        engine_probe = subprocess.run(["docker", "info"], check=False, capture_output=True, text=True, timeout=5)
        engine_ready = engine_probe.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        engine_ready = False
    checks.append({"id": "docker.engine", "status": "pass" if engine_ready else "warn", "blocking": False})
    if not engine_ready:
        warnings.append("Docker engine is not reachable; start will remain degraded until Docker Desktop is running")
    if engine_ready and compose.exists() and paths.compose_env.exists():
        try:
            backend_volumes = resolve_backend_volumes(compose, paths.compose_env, timeout=10.0)
            capacity = inspect_backend_volume_capacity(
                backend_volumes,
                max_bytes=_configured_backend_volume_limit(config.get("storage")),
                timeout=10.0,
            )
            checks.append({"id": "backend.volume_capacity", "blocking": False, **capacity})
            if capacity["status"] == "fail":
                warnings.append("Observatory backend volumes exceed their configured soft budget; reduce retention or back up and prune")
            elif capacity["status"] == "warn":
                if capacity["missing"]:
                    warnings.append("one or more Observatory backend volumes are not created yet; capacity is not fully measured")
                elif capacity["ratio"] >= 0.9:
                    warnings.append("Observatory backend volumes are at or above 90% of their configured soft budget")
        except (OSError, RuntimeError, ValueError) as exc:
            checks.append({"id": "backend.volume_capacity", "status": "warn", "blocking": False, "detail": str(exc)})
            warnings.append(f"could not inspect Observatory backend-volume capacity: {exc}")
    else:
        checks.append({"id": "backend.volume_capacity", "status": "warn", "blocking": False, "detail": "Docker engine, Compose file, or generated Compose environment is unavailable"})
    service_status = {check["id"]: check["status"] for check in checks if check["id"].startswith("service.")}
    expected_ports = {
        8787: service_status.get("service.api") == "pass",
        3000: service_status.get("service.grafana") == "pass",
        13133: service_status.get("service.collector") == "pass",
        4317: service_status.get("service.collector") == "pass",
        4318: service_status.get("service.collector") == "pass",
    }
    for port in (8787, 3000, 4317, 4318, 13133):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.2)
            occupied = probe.connect_ex(("127.0.0.1", port)) == 0
        owned = occupied and expected_ports[port]
        checks.append({"id": f"port.{port}", "status": "pass" if not occupied or owned else "warn", "blocking": False, "detail": "expected Observatory listener" if owned else None})
        if occupied and not owned:
            warnings.append(f"loopback port {port} is already in use")
    result = _result("doctor", "success" if not warnings else "degraded", EXIT_OK if not warnings else EXIT_DEGRADED, data={"checks": checks, "state_dir": str(paths.root)}, warnings=warnings)
    result["checks"] = checks
    return result


def _read_jsonl(path: str | None) -> tuple[list[dict[str, Any]], list[str]]:
    stream = sys.stdin if not path or path == "-" else open(path, "r", encoding="utf-8", errors="replace")
    close = stream is not sys.stdin
    records: list[dict[str, Any]] = []
    errors: list[str] = []

    def add_error(message: str) -> None:
        if len(errors) < MAX_INGEST_ERRORS - 1:
            errors.append(message)
        elif len(errors) == MAX_INGEST_ERRORS - 1:
            errors.append(f"additional parse errors truncated at {MAX_INGEST_ERRORS} entries")
    total_bytes = 0
    try:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            total_bytes += len(line.encode("utf-8"))
            if total_bytes > MAX_INGEST_BYTES:
                raise ValueError(f"input exceeds {MAX_INGEST_BYTES} bytes")
            if len(records) >= MAX_INGEST_RECORDS:
                raise ValueError(f"input exceeds {MAX_INGEST_RECORDS} records")
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                detail = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
                add_error(f"line {line_number}: invalid JSON: {detail}")
                continue
            if not isinstance(value, dict):
                add_error(f"line {line_number}: record must be an object")
                continue
            records.append(value)
    finally:
        if close:
            stream.close()
    return records, errors


def _safe_records(
    records: Iterable[dict[str, Any]],
    *,
    project_path: str | Path | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    safe: list[dict[str, Any]] = []
    errors: list[str] = []
    project = resolve_project(project_path).to_mapping() if project_path is not None else None
    for index, record in enumerate(records):
        try:
            candidate = dict(record)
            if project is not None:
                existing_project = candidate.get("project") if isinstance(candidate.get("project"), Mapping) else {}
                merged_project = dict(project)
                for key, value in existing_project.items():
                    if value not in (None, ""):
                        merged_project[key] = value
                candidate["project"] = merged_project
            event = NormalizedEvent.from_mapping(candidate, received_at=utc_now())
            safe.append(redact_event(event).to_mapping())
        except (ContractError, ValueError) as exc:
            errors.append(f"record {index}: {exc}")
    return safe, errors


def _default_demo_file() -> Path | None:
    """Locate the repository-bundled metadata-only walkthrough fixture."""

    candidates = (
        Path(__file__).resolve().parents[2] / "examples" / "synthetic-events.jsonl",
        Path.cwd() / "examples" / "synthetic-events.jsonl",
    )
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _seed_demo(paths: StatePaths, file_value: str | None = None) -> dict[str, Any]:
    """Seed the explicit local walkthrough without contacting any provider."""

    if not paths.config.exists():
        return _result("demo", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    file_path = Path(file_value).expanduser() if file_value else _default_demo_file()
    if file_path is None:
        return _result(
            "demo",
            "failed",
            EXIT_FAILED,
            errors=["bundled demo fixture is unavailable; pass --file with a JSONL fixture"],
        )
    try:
        records, parse_errors = _read_jsonl(str(file_path))
        safe, validation_errors = _safe_records(records)
        errors = [*parse_errors, *validation_errors]
        with _event_store(paths) as store:
            result = Intake(store).ingest(safe)
    except (OSError, ValueError, RuntimeError) as exc:
        return _result("demo", "failed", EXIT_FAILED, data={"file": str(file_path)}, errors=[str(exc)])
    degraded = bool(errors or result.rejected or result.unavailable)
    return _result(
        "demo",
        "degraded" if degraded else "success",
        EXIT_DEGRADED if degraded else EXIT_OK,
        data={"mode": "offline", "file": str(file_path), "demo": True, **result.to_mapping()},
        errors=errors,
    )


def _spool(paths: StatePaths, records: Iterable[dict[str, Any]]) -> Path:
    paths.spool.mkdir(parents=True, exist_ok=True)
    existing_files = sorted(paths.spool.glob("*.jsonl"))
    current_bytes = sum(path.stat().st_size for path in existing_files if path.is_file())
    if len(existing_files) >= MAX_SPOOL_FILES:
        raise SpoolFullError(f"offline spool file limit reached ({MAX_SPOOL_FILES})")
    timestamp = utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    path = paths.spool / f"events-{timestamp}-{secrets.token_hex(4)}.jsonl"
    payload = "".join(canonical_json(record) + "\n" for record in records)
    if current_bytes + len(payload.encode("utf-8")) > MAX_SPOOL_BYTES:
        raise SpoolFullError(f"offline spool byte limit reached ({MAX_SPOOL_BYTES})")
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def _command_flush(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("flush", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    files = sorted(paths.spool.glob("*.jsonl"))[: args.max_files]
    if not files:
        return _result("flush", "success", EXIT_OK, data={"files": 0, "records": 0, "removed": 0, "mode": "offline" if args.offline else "api"})
    removed = 0
    records_sent = 0
    errors: list[str] = []
    for path in files:
        try:
            records, parse_errors = _read_jsonl(str(path))
            safe, rejected = _safe_records(records, project_path=getattr(args, "project_path", str(Path.cwd())))
            file_errors = [*parse_errors, *rejected]
            if file_errors:
                errors.extend([f"{path.name}: {item}" for item in file_errors])
            if not safe:
                break
            if args.offline:
                with _event_store(paths) as store:
                    intake = Intake(store)
                    result = intake.ingest(safe)
                if result.unavailable or result.rejected:
                    raise RuntimeError(
                        f"offline store did not accept the spool batch (rejected={result.rejected}, unavailable={result.unavailable}); spool file retained"
                    )
                records_sent += result.inserted + result.duplicate + result.conflict
            else:
                response = _post_events(args.url, safe)
                if response.get("outcome") != "accepted":
                    raise RuntimeError("flush endpoint did not accept the batch")
                rejected = int(response.get("rejected", 0) or 0)
                if rejected:
                    raise RuntimeError(f"flush endpoint rejected {rejected} records; spool file retained")
                records_sent += len(safe)
            if not file_errors:
                path.unlink()
                removed += 1
            else:
                # Keep malformed/invalid lines for operator repair.  Valid
                # siblings may still be replayed safely because event IDs are
                # idempotent on the next flush.
                break
        except (OSError, URLError, ValueError, RuntimeError) as exc:
            errors.append(f"{path.name}: {exc}")
            break
    outcome = "success" if not errors else "degraded"
    code = EXIT_OK if not errors else EXIT_DEGRADED
    return _result(
        "flush",
        outcome,
        code,
        data={"files": len(files), "records": records_sent, "removed": removed, "remaining": len(list(paths.spool.glob("*.jsonl"))), "mode": "offline" if args.offline else "api"},
        errors=errors,
    )


def _post_events(url: str, records: list[dict[str, Any]], timeout: float = 1.5) -> dict[str, Any]:
    body = json.dumps(records).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=timeout) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("ingest endpoint response must be an object")
    return dict(value)


def _require_accepted_response(response: Mapping[str, Any], operation: str) -> None:
    """Reject a 2xx response that did not durably accept every record."""

    outcome = response.get("outcome")
    rejected = int(response.get("rejected", 0) or 0)
    unavailable = int(response.get("unavailable", 0) or 0)
    if outcome != "accepted" or rejected or unavailable:
        raise ValueError(
            f"{operation} endpoint did not fully accept the batch "
            f"(outcome={outcome!r}, rejected={rejected}, unavailable={unavailable})"
        )


def _command_ingest(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("ingest", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        records, parse_errors = _read_jsonl(args.file)
        safe, errors = _safe_records(records, project_path=getattr(args, "project_path", str(Path.cwd())))
        errors = [*parse_errors, *errors]
    except (OSError, ValueError) as exc:
        return _result("ingest", "failed", EXIT_FAILED, errors=[str(exc)])
    if not safe:
        return _result("ingest", "degraded", EXIT_DEGRADED, data={"accepted": 0}, errors=errors)
    if args.offline:
        with _event_store(paths) as store:
            intake = Intake(store)
            result = intake.ingest(safe)
        degraded = bool(errors or result.rejected or result.unavailable)
        return _result("ingest", "success" if not degraded else "degraded", EXIT_OK if not degraded else EXIT_DEGRADED, data={**result.to_mapping(), "mode": "offline"}, errors=errors)
    try:
        response = _post_events(args.url, safe)
        response["mode"] = "api"
        response["preflight_rejected"] = len(errors)
        rejected = int(response.get("rejected", 0) or 0)
        unavailable = int(response.get("unavailable", 0) or 0)
        if response.get("outcome") != "accepted" or rejected or unavailable:
            return _result(
                "ingest",
                "degraded",
                EXIT_DEGRADED,
                data=response,
                errors=[*errors, f"ingest endpoint did not fully accept the batch (outcome={response.get('outcome')!r}, rejected={rejected}, unavailable={unavailable})"],
            )
        return _result("ingest", "success" if not errors else "degraded", EXIT_OK if not errors else EXIT_DEGRADED, data=response, errors=errors)
    except (OSError, URLError, ValueError) as exc:
        try:
            spool = _spool(paths, safe)
        except SpoolFullError as spool_exc:
            return _result("ingest", "degraded", EXIT_DEGRADED, data={"mode": "dropped", "count": len(safe), "telemetry_lost": True}, warnings=[f"API unavailable: {exc}", str(spool_exc)], errors=errors)
        return _result("ingest", "degraded", EXIT_DEGRADED, data={"mode": "spooled", "path": str(spool), "count": len(safe)}, warnings=[f"API unavailable: {exc}"], errors=errors)


def _command_record_outcome(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("record-outcome", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        event = make_outcome_event(
            args.kind,
            args.status,
            correlation_id=args.correlation_id,
            correlation_basis=args.correlation_basis,
            evidence_source=args.evidence_source,
            task_id=args.task_id,
            task_class=args.task_class,
            project=resolve_project(args.project_path),
            attributes={"task_class": args.task_class} if args.task_class else {},
        )
    except (OSError, ValueError, RuntimeError) as exc:
        return _result("record-outcome", "failed", EXIT_FAILED, errors=[str(exc)])
    event = redact_event(event)
    record = event.to_mapping()
    if args.offline:
        with _event_store(paths) as store:
            result = store.append(event)
        return _result("record-outcome", "success" if result.status == "inserted" else result.status, EXIT_OK if result.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": result.status})
    try:
        response = _post_events(args.url, [record])
        _require_accepted_response(response, "record-outcome")
        return _result("record-outcome", "success", EXIT_OK, data={"mode": "api", "event_id": event.event_id, "response": response})
    except (OSError, URLError, ValueError) as exc:
        try:
            spool = _spool(paths, [record])
        except SpoolFullError as spool_exc:
            return _result("record-outcome", "degraded", EXIT_DEGRADED, data={"mode": "dropped", "event_id": event.event_id, "telemetry_lost": True}, warnings=[f"API unavailable: {exc}", str(spool_exc)])
        return _result("record-outcome", "degraded", EXIT_DEGRADED, data={"mode": "spooled", "event_id": event.event_id, "path": str(spool)}, warnings=[f"API unavailable: {exc}"])


def _command_run_outcome(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("run-outcome", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        command_args = list(args.command_args)
        if command_args and command_args[0] == "--":
            command_args = command_args[1:]
        if not command_args:
            return _result("run-outcome", "failed", EXIT_USAGE, errors=["a command is required after --"])
        result = run_command_outcome(
            command_args,
            project_path=args.project_path,
            kind=args.kind,
            correlation_id=args.correlation_id,
            evidence_source=args.evidence_source,
            timeout_seconds=args.command_timeout,
        )
        event = redact_event(result.event)
    except (OSError, ValueError, RuntimeError) as exc:
        return _result("run-outcome", "failed", EXIT_FAILED, errors=[str(exc)])
    record = event.to_mapping()
    if args.offline:
        with _event_store(paths) as store:
            append = store.append(event)
        return _result("run-outcome", "success" if append.status in {"inserted", "duplicate"} else "conflict", EXIT_OK if append.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": append.status, "returncode": result.returncode, "duration_ms": result.duration_ms})
    try:
        response = _post_events(args.url, [record])
        _require_accepted_response(response, "run-outcome")
        return _result("run-outcome", "success", EXIT_OK, data={"mode": "api", "event_id": event.event_id, "response": response, "returncode": result.returncode, "duration_ms": result.duration_ms})
    except (OSError, URLError, ValueError) as exc:
        try:
            spool = _spool(paths, [record])
        except SpoolFullError as spool_exc:
            return _result("run-outcome", "degraded", EXIT_DEGRADED, data={"mode": "dropped", "event_id": event.event_id, "telemetry_lost": True, "returncode": result.returncode}, warnings=[f"API unavailable: {exc}", str(spool_exc)])
        return _result("run-outcome", "degraded", EXIT_DEGRADED, data={"mode": "spooled", "event_id": event.event_id, "path": str(spool), "returncode": result.returncode}, warnings=[f"API unavailable: {exc}"])


def _command_git_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("git-snapshot", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        event = git_outcome_snapshot(args.project_path, correlation_id=args.correlation_id)
    except (OSError, ValueError, RuntimeError) as exc:
        return _result("git-snapshot", "failed", EXIT_FAILED, errors=[str(exc)])
    event = redact_event(event)
    record = event.to_mapping()
    if args.offline:
        with _event_store(paths) as store:
            result = store.append(event)
        return _result("git-snapshot", "success" if result.status in {"inserted", "duplicate"} else "conflict", EXIT_OK if result.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": result.status})
    try:
        response = _post_events(args.url, [record])
        _require_accepted_response(response, "git-snapshot")
        return _result("git-snapshot", "success", EXIT_OK, data={"mode": "api", "event_id": event.event_id, "response": response})
    except (OSError, URLError, ValueError) as exc:
        try:
            spool = _spool(paths, [record])
        except SpoolFullError as spool_exc:
            return _result("git-snapshot", "degraded", EXIT_DEGRADED, data={"mode": "dropped", "event_id": event.event_id, "telemetry_lost": True}, warnings=[f"API unavailable: {exc}", str(spool_exc)])
        return _result("git-snapshot", "degraded", EXIT_DEGRADED, data={"mode": "spooled", "event_id": event.event_id, "path": str(spool)}, warnings=[f"API unavailable: {exc}"])


def _command_migrate(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("migrate", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        with _event_store(paths) as store:
            versions = schema_versions(store)
        return _result("migrate", "success", EXIT_OK, data={"database": str(paths.database), "schema_versions": versions})
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("migrate", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_retention(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("retention", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        config = _load_config(paths) or {}
        current = config.get("retention")
        if not isinstance(current, dict):
            return _result("retention", "conflict", EXIT_CONFLICT, errors=["state retention policy is invalid"])
        requested = {
            key: value
            for key, value in {
                "prometheus_days": args.prometheus_days,
                "tempo_hours": args.tempo_hours,
                "loki_hours": args.loki_hours,
            }.items()
            if value is not None
        }
        for key, value in requested.items():
            if value < 1:
                raise ValueError(f"{key} must be at least 1")
        updated = {**DEFAULT_RETENTION, **current, **requested}
        changed = updated != current
        env_changed = _reconcile_compose_env(paths, updated, config.get("storage"))
        if changed:
            config["retention"] = updated
            _write_json_atomic(paths.config, config)
        return _result("retention", "success", EXIT_OK, data={"retention": updated, "changed": changed or env_changed, "compose_env": str(paths.compose_env)})
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("retention", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_backup(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("backup", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        backend_volumes = None
        if args.backend_volumes:
            if not args.full_state:
                return _result("backup", "failed", EXIT_USAGE, errors=["--backend-volumes requires --full-state"])
            if not args.include_secret:
                return _result("backup", "failed", EXIT_USAGE, errors=["--backend-volumes requires --include-secret so Grafana state and its admin secret remain portable; encrypt the archive before storage"])
            _require_compose_stopped(args, paths)
            backend_volumes = _resolve_backend_volumes(args, paths)
        if args.full_state:
            result = backup_state(
                paths.root,
                args.target,
                include_secret=args.include_secret,
                overwrite=args.overwrite,
                backend_volumes=backend_volumes,
                docker_timeout=args.timeout,
            )
        else:
            if args.include_secret:
                return _result("backup", "failed", EXIT_USAGE, errors=["--include-secret requires --full-state"])
            result = backup_database(paths.database, args.target, overwrite=args.overwrite)
        return _result("backup", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("backup", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_restore(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("restore", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    if args.backend_volumes and not args.full_state:
        return _result("restore", "failed", EXIT_USAGE, errors=["--backend-volumes requires --full-state"])
    api_reachable, api_detail = _probe_http(args.api_health_url, timeout=min(args.timeout, 2.0))
    if api_reachable:
        return _result(
            "restore",
            "conflict",
            EXIT_CONFLICT,
            errors=[f"refusing live restore while the Observatory API is reachable ({api_detail}); stop the stack first"],
        )
    if args.full_state:
        pre_restore = None
        try:
            backend_volumes = None
            target_compose_file = _compose_path(args, paths).expanduser().resolve()
            if args.backend_volumes:
                _require_compose_stopped(args, paths)
                backend_volumes = _resolve_backend_volumes(args, paths)
            if args.overwrite and paths.database.exists():
                pre_restore = backup_database(paths.database, paths.root / "backups" / f"pre-restore-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}.sqlite3")
            result = restore_state(
                args.source,
                paths.root,
                overwrite=args.overwrite,
                restore_secret=args.restore_secret,
                backend_volumes=backend_volumes,
                docker_timeout=args.timeout,
            )
            restored_config = _load_config(paths) or {}
            restored_config["compose_file"] = str(target_compose_file)
            _reconcile_compose_env(paths, restored_config.get("retention"), restored_config.get("storage"))
            _write_json_atomic(paths.config, restored_config)
            result["portable_paths_rebased"] = True
            result["compose_file"] = str(target_compose_file)
            if pre_restore is not None:
                result["pre_restore_backup"] = pre_restore
            return _result("restore", "success", EXIT_OK, data=result)
        except (OSError, RuntimeError, ValueError) as exc:
            return _result("restore", "failed", EXIT_FAILED, errors=[str(exc)])
    temporary: Path | None = None
    try:
        pre_restore = None
        if args.overwrite and paths.database.exists():
            pre_restore = backup_database(paths.database, paths.root / "backups" / f"pre-restore-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}.sqlite3")
        temporary = paths.database.with_name(f".{paths.database.name}.restore-{secrets.token_hex(6)}.tmp")
        result = restore_database(args.source, temporary, overwrite=False)
        if result.get("integrity") != "ok":
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"restored database failed integrity check: {result.get('integrity')}")
        if paths.database.exists() and not args.overwrite:
            temporary.unlink(missing_ok=True)
            raise FileExistsError(f"restore target exists; pass --overwrite explicitly: {paths.database}")
        os.replace(temporary, paths.database)
        if pre_restore is not None:
            result["pre_restore_backup"] = pre_restore
        return _result("restore", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        return _result("restore", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_prune(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("prune", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        with _event_store(paths) as store:
            result = purge_events(store, before=args.before, event_ids=args.event_id, confirm=args.confirm)
        return _result("prune", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("prune", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_resolve_project(args: argparse.Namespace) -> dict[str, Any]:
    return _result("resolve-project", "success", EXIT_OK, data=resolve_project(args.path).to_mapping())


def _compose_path(args: argparse.Namespace, paths: StatePaths) -> Path:
    if getattr(args, "compose_file", None):
        return Path(args.compose_file).expanduser()
    try:
        config = _load_config(paths)
    except RuntimeError:
        config = None
    configured = config.get("compose_file") if isinstance(config, dict) else None
    if isinstance(configured, str) and configured.strip():
        return Path(configured).expanduser()
    return Path.cwd() / "compose.yaml"


def _resolve_backend_volumes(args: argparse.Namespace, paths: StatePaths) -> dict[str, str]:
    compose = _compose_path(args, paths)
    if not compose.exists():
        raise FileNotFoundError(f"Compose file not found at {compose}")
    if not paths.compose_env.exists():
        raise FileNotFoundError(f"state environment not found at {paths.compose_env}; run observatory install")
    return resolve_backend_volumes(
        compose,
        paths.compose_env,
        project_name=getattr(args, "project_name", None),
        timeout=args.timeout,
    )


def _start_backend_capacity(paths: StatePaths, args: argparse.Namespace) -> dict[str, Any] | None:
    """Return the backend-volume budget check used by ``start``.

    Missing Docker state is deliberately non-blocking here: Compose's own
    readiness result remains the source of truth for an unavailable engine,
    while an already-over-budget installed stack is refused before it can
    create more telemetry.
    """

    if not paths.config.exists() or not paths.compose_env.exists():
        return None
    compose = _compose_path(args, paths)
    if not compose.exists():
        return None
    try:
        config = _load_config(paths) or {}
        volume_names = resolve_backend_volumes(compose, paths.compose_env, timeout=min(args.timeout, 10.0))
        return inspect_backend_volume_capacity(
            volume_names,
            max_bytes=_configured_backend_volume_limit(config.get("storage")),
            timeout=min(args.timeout, 10.0),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return {"status": "warn", "detail": str(exc)}


def _require_compose_stopped(args: argparse.Namespace, paths: StatePaths) -> None:
    """Refuse volume snapshots/restores while any Compose service is running."""

    compose = _compose_path(args, paths)
    if not compose.exists():
        raise FileNotFoundError(f"Compose file not found at {compose}")
    if not paths.compose_env.exists():
        raise FileNotFoundError(f"state environment not found at {paths.compose_env}; run observatory install")
    command = ["docker", "compose"]
    project_name = getattr(args, "project_name", None)
    if project_name:
        command.extend(["--project-name", project_name])
    command.extend(["--env-file", str(paths.compose_env), "-f", str(compose), "ps", "--status", "running", "--services"])
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=args.timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not inspect Compose service state: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "Compose service inspection failed").strip()
        raise RuntimeError(detail)
    services = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if services:
        raise RuntimeError(f"backend volume operation requires the Compose stack to be stopped; running services: {', '.join(services)}")


def _compose(args: argparse.Namespace, operation: str) -> tuple[int, str]:
    try:
        command = _compose_command(args, operation.split())
    except FileNotFoundError as exc:
        return EXIT_NOT_INITIALIZED, str(exc)
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=args.timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return EXIT_DEGRADED, f"Docker unavailable: {exc}"
    if result.returncode != 0:
        return EXIT_DEGRADED, (result.stderr or result.stdout).strip() or "Docker operation failed"
    return EXIT_OK, (result.stdout or "completed").strip()


def _compose_command(args: argparse.Namespace, operation: Sequence[str]) -> list[str]:
    """Build a Compose command using the installed state, without shell parsing."""

    paths = _paths(args)
    compose = _compose_path(args, paths)
    if not compose.exists():
        raise FileNotFoundError(f"Compose file not found at {compose}")
    if not paths.compose_env.exists():
        raise FileNotFoundError(f"state environment not found at {paths.compose_env}; run observatory install")
    return ["docker", "compose", "--env-file", str(paths.compose_env), "-f", str(compose), *operation]


def _snapshot_compose_images(args: argparse.Namespace) -> list[dict[str, str]]:
    """Capture the image IDs and references used by the current Compose containers.

    Image IDs are important here: a tag can be overwritten by ``pull`` or a local
    build, while the old ID can still be re-tagged for a bounded rollback.
    """

    command = _compose_command(args, ("images", "--format", "json"))
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=args.timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not snapshot current Compose images: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "Compose image inspection failed").strip()
        raise RuntimeError(detail)
    raw = (result.stdout or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Be tolerant of Compose versions that emit one JSON object per line.
        rows = []
        for line in raw.splitlines():
            if line.strip():
                rows.append(json.loads(line))
        parsed = rows
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raise RuntimeError("Compose image inspection returned an unexpected JSON shape")

    snapshot: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in parsed:
        if not isinstance(row, dict):
            raise RuntimeError("Compose image inspection returned a non-object row")
        image_id = row.get("ID") or row.get("Id")
        repository = row.get("Repository")
        tag = row.get("Tag")
        if not isinstance(image_id, str) or not _COMPOSE_ROLLBACK_IMAGE_ID.fullmatch(image_id):
            raise RuntimeError("Compose image inspection returned an invalid image ID")
        if not isinstance(repository, str) or not repository.strip() or any(ch.isspace() for ch in repository):
            raise RuntimeError("Compose image inspection returned an invalid repository")
        if tag is None or tag == "<none>" or not str(tag).strip():
            reference = repository
        else:
            tag_text = str(tag)
            if any(ch.isspace() for ch in tag_text) or ":" in tag_text or "/" in tag_text:
                raise RuntimeError("Compose image inspection returned an invalid tag")
            reference = f"{repository}:{tag_text}"
        key = (image_id, reference)
        if key not in seen:
            snapshot.append({"id": image_id, "reference": reference})
            seen.add(key)
    return snapshot


def _restore_compose_images(snapshot: Sequence[Mapping[str, str]], *, timeout: float) -> dict[str, Any]:
    """Re-tag captured image IDs so Compose can recreate the prior stack."""

    if not snapshot:
        return {"status": "unavailable", "reason": "no existing Compose image set was found"}
    restored: list[str] = []
    for item in snapshot:
        image_id = item.get("id")
        reference = item.get("reference")
        if not isinstance(image_id, str) or not _COMPOSE_ROLLBACK_IMAGE_ID.fullmatch(image_id):
            raise RuntimeError("refusing to restore an invalid saved image ID")
        if not isinstance(reference, str) or not reference or any(ch.isspace() for ch in reference):
            raise RuntimeError("refusing to restore an invalid saved image reference")
        try:
            result = subprocess.run(
                ["docker", "image", "tag", image_id, reference],
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"could not restore image tag {reference}: {exc}") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "docker image tag failed").strip()
            raise RuntimeError(f"could not restore image tag {reference}: {detail}")
        restored.append(reference)
    return {"status": "success", "restored": restored}


def _restore_update_database(paths: StatePaths, backup: Mapping[str, Any]) -> dict[str, Any]:
    """Restore the pre-update SQLite image through a temporary validated copy."""

    source = Path(str(backup["target"])).expanduser().resolve(strict=True)
    target = paths.database.expanduser().resolve(strict=False)
    temporary = target.with_name(f".{target.name}.rollback-{secrets.token_hex(8)}.tmp")
    try:
        restored = restore_database(source, temporary, overwrite=False)
        for sidecar in (Path(f"{target}-wal"), Path(f"{target}-shm")):
            sidecar.unlink(missing_ok=True)
        os.replace(temporary, target)
        return {"status": "success", "integrity": restored["integrity"], "target": str(target)}
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _rollback_update(args: argparse.Namespace, paths: StatePaths, backup: Mapping[str, Any], snapshot: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    """Attempt a bounded image/database rollback after an update failure."""

    rollback: dict[str, Any] = {"status": "incomplete", "steps": {}}
    down_code, down_detail = _compose(args, "down --remove-orphans")
    rollback["steps"]["stack_stop"] = {"status": "success" if down_code == EXIT_OK else "failed", "detail": down_detail}

    try:
        rollback["steps"]["images"] = _restore_compose_images(snapshot, timeout=args.timeout)
    except (OSError, RuntimeError, ValueError) as exc:
        rollback["steps"]["images"] = {"status": "failed", "detail": str(exc)}

    try:
        rollback["steps"]["database"] = _restore_update_database(paths, backup)
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        rollback["steps"]["database"] = {"status": "failed", "detail": str(exc)}

    up_code, up_detail = _compose(args, "up -d --no-build --wait")
    rollback["steps"]["stack_restart"] = {"status": "success" if up_code == EXIT_OK else "failed", "detail": up_detail}
    if up_code == EXIT_OK:
        readiness: dict[str, str] = {}
        ready = True
        for name, url in (
            ("api", "http://127.0.0.1:8787/readyz"),
            ("collector", "http://127.0.0.1:13133/"),
            ("grafana", "http://127.0.0.1:3000/api/health"),
        ):
            service_ready, detail = _wait_http(url, timeout=args.timeout)
            readiness[name] = detail
            ready = ready and service_ready
        rollback["steps"]["readiness"] = {"status": "success" if ready else "failed", "details": readiness}

    image_status = rollback["steps"]["images"].get("status")
    database_status = rollback["steps"]["database"].get("status")
    stack_status = rollback["steps"]["stack_restart"].get("status")
    readiness_status = rollback["steps"].get("readiness", {}).get("status", "failed")
    if all(status == "success" for status in (image_status, database_status, stack_status, readiness_status)):
        rollback["status"] = "success"
    return rollback


def _command_lifecycle(args: argparse.Namespace, command: str, operation: str) -> dict[str, Any]:
    code, message = _compose(args, operation)
    return _result(command, "success" if code == EXIT_OK else "degraded", code, data={"message": message})


def _command_start(args: argparse.Namespace) -> dict[str, Any]:
    # A lifecycle restart must reuse the local image set. Rebuilding here
    # makes recovery depend on registry/network availability; explicit image
    # refreshes belong to `update --pull`.
    paths = _paths(args)
    capacity = _start_backend_capacity(paths, args)
    if capacity is not None and capacity.get("status") == "fail":
        return _result(
            "start",
            "degraded",
            EXIT_DEGRADED,
            data={"capacity": capacity},
            warnings=["Observatory backend volumes exceed their configured budget; reduce retention or back up and prune before starting"],
        )
    code, message = _compose(args, "up -d --wait")
    if code != EXIT_OK:
        return _result("start", "degraded", code, data={"message": message, "capacity": capacity})
    readiness: dict[str, str] = {}
    for name, url in (
        ("api", getattr(args, "api_url", "http://127.0.0.1:8787/readyz")),
        ("collector", getattr(args, "collector_url", "http://127.0.0.1:13133/")),
        ("grafana", getattr(args, "grafana_url", "http://127.0.0.1:3000/api/health")),
    ):
        ready, detail = _wait_http(url, timeout=args.timeout)
        readiness[name] = detail
        if not ready:
            return _result("start", "degraded", EXIT_DEGRADED, data={"message": message, "readiness": readiness, "capacity": capacity}, warnings=[f"{name} did not become ready: {detail}"])
    return _result("start", "success", EXIT_OK, data={"message": message, "readiness": readiness, "capacity": capacity})


def _command_update(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("update", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])

    try:
        versions = read_schema_versions(paths.database)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("update", "failed", EXIT_FAILED, errors=[str(exc)])
    if args.check:
        return _result("update", "success", EXIT_OK, data={"check": True, "schema_versions": versions, "changed": False, "image_pull": "not_requested"})
    if not args.pull:
        return _result("update", "degraded", EXIT_DEGRADED, data={"check": False, "schema_versions": versions, "changed": False, "image_pull": "not_requested"}, warnings=["local migrations are current; pass --pull to request an explicit Compose image update"])
    backup_target = paths.root / "backups" / f"pre-update-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}.sqlite3"
    try:
        pre_update_backup = backup_database(paths.database, backup_target)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result(
            "update",
            "failed",
            EXIT_FAILED,
            data={"schema_versions": versions, "changed": False, "image_pull": "not_started"},
            errors=[f"refusing image update because the pre-update database backup failed: {exc}"],
        )
    try:
        image_snapshot = _snapshot_compose_images(args)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result(
            "update",
            "failed",
            EXIT_FAILED,
            data={"schema_versions": versions, "changed": False, "image_pull": "not_started", "backup": pre_update_backup},
            errors=[f"refusing image update because the current Compose image set could not be captured: {exc}"],
        )
    backup = pre_update_backup
    image_data = {"count": len(image_snapshot), "references": [item["reference"] for item in image_snapshot]}
    code, message = _compose(args, "pull --ignore-buildable")
    if code != EXIT_OK:
        rollback = _rollback_update(args, paths, backup, image_snapshot)
        outcome = "degraded" if rollback["status"] == "success" else "failed"
        return _result(
            "update",
            outcome,
            EXIT_DEGRADED if outcome == "degraded" else EXIT_FAILED,
            data={
                "schema_versions": versions,
                "changed": False,
                "image_pull": "failed",
                "backup": backup,
                "image_snapshot": image_data,
                "message": message,
                "rollback": rollback,
            },
            warnings=[f"Compose image pull failed; rollback status: {rollback['status']}; pre-update backup retained at {backup['target']}"],
        )
    start_code, start_message = _compose(args, "up -d --build --wait")
    if start_code != EXIT_OK:
        rollback = _rollback_update(args, paths, backup, image_snapshot)
        outcome = "degraded" if rollback["status"] == "success" else "failed"
        return _result("update", outcome, EXIT_DEGRADED if outcome == "degraded" else EXIT_FAILED, data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "image_snapshot": image_data, "message": start_message, "rollback": rollback}, warnings=[f"updated Compose stack failed to start; rollback status: {rollback['status']}; pre-update backup retained at {backup['target']}"])
    ready, detail = _wait_http("http://127.0.0.1:8787/readyz", timeout=args.timeout)
    if not ready:
        rollback = _rollback_update(args, paths, backup, image_snapshot)
        outcome = "degraded" if rollback["status"] == "success" else "failed"
        return _result("update", outcome, EXIT_DEGRADED if outcome == "degraded" else EXIT_FAILED, data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "image_snapshot": image_data, "message": start_message, "readiness": detail, "rollback": rollback}, warnings=[f"updated Compose stack did not pass API readiness; rollback status: {rollback['status']}; pre-update backup retained at {backup['target']}"])
    service_readiness = {"api": detail}
    for name, url in (("collector", "http://127.0.0.1:13133/"), ("grafana", "http://127.0.0.1:3000/api/health")):
        service_ready, service_detail = _wait_http(url, timeout=args.timeout)
        service_readiness[name] = service_detail
        if not service_ready:
            rollback = _rollback_update(args, paths, backup, image_snapshot)
            outcome = "degraded" if rollback["status"] == "success" else "failed"
            return _result("update", outcome, EXIT_DEGRADED if outcome == "degraded" else EXIT_FAILED, data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "image_snapshot": image_data, "message": start_message, "readiness": detail, "service_readiness": service_readiness, "rollback": rollback}, warnings=[f"updated Compose stack did not pass {name} readiness; rollback status: {rollback['status']}; pre-update backup retained at {backup['target']}"])
    try:
        versions = read_schema_versions(paths.database)
    except (OSError, RuntimeError, ValueError) as exc:
        rollback = _rollback_update(args, paths, backup, image_snapshot)
        outcome = "degraded" if rollback["status"] == "success" else "failed"
        return _result("update", outcome, EXIT_DEGRADED if outcome == "degraded" else EXIT_FAILED, data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "image_snapshot": image_data, "message": start_message, "readiness": detail, "service_readiness": service_readiness, "rollback": rollback}, warnings=[f"updated stack passed service readiness but migration state could not be read: {exc}; rollback status: {rollback['status']}; pre-update backup retained at {backup['target']}"])
    return _result("update", "success", EXIT_OK, data={"schema_versions": versions, "changed": True, "image_pull": "complete", "backup": backup, "image_snapshot": image_data, "message": start_message, "readiness": detail, "service_readiness": service_readiness, "rollback": {"status": "available" if image_snapshot else "unavailable", "image_count": len(image_snapshot)}})


def _command_status(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    state_initialized = paths.config.exists()
    live_compose = _inspect_live_compose_state(paths.root)
    state = {
        "status": "ready" if state_initialized else "missing",
        "path": str(paths.root),
    }
    dashboard_ready, dashboard_detail = _probe_http(args.grafana_url, timeout=args.timeout)
    dashboard = {"status": "ready" if dashboard_ready else "unavailable", "detail": dashboard_detail, "url": args.grafana_url}
    collector_ready, collector_detail = _probe_http(args.collector_url, timeout=args.timeout)
    collector = {"status": "ready" if collector_ready else "unavailable", "detail": collector_detail, "url": args.collector_url}
    try:
        request = Request(args.url, method="GET")
        with urlopen(request, timeout=args.timeout) as response:
            health = json.loads(response.read().decode("utf-8"))
        warnings = []
        live_warning = _live_compose_warning(live_compose)
        if live_warning:
            warnings.append(live_warning)
        if not dashboard_ready:
            warnings.append(f"Grafana is not reachable at {args.grafana_url}")
        if not collector_ready:
            warnings.append(f"OTel Collector is not reachable at {args.collector_url}")
        if not state_initialized:
            warnings.append(f"local Observatory state is missing at {paths.root}; run install before relying on lifecycle or offline maintenance")
        ready = dashboard_ready and collector_ready
        telemetry_ready = isinstance(health, dict) and health.get("status") == "ok"
        if not telemetry_ready:
            warnings.append("normalizer health is degraded; inference remains unmanaged/no-proxy")
        ready = dashboard_ready and collector_ready and telemetry_ready
        overall_ready = ready and state_initialized
        return _result("status", "success" if overall_ready else "degraded", EXIT_OK if overall_ready else EXIT_DEGRADED, data={"observatory": "ready" if ready else "degraded", "state": state, "dashboard": dashboard, "collector": collector, "telemetry": health, "live_compose": live_compose, "inference_path": "unmanaged/no-proxy"}, warnings=warnings)
    except (OSError, URLError, ValueError) as exc:
        warnings = [str(exc)]
        live_warning = _live_compose_warning(live_compose)
        if live_warning:
            warnings.append(live_warning)
        if not state_initialized:
            warnings.append(f"local Observatory state is missing at {paths.root}; run install before relying on lifecycle or offline maintenance")
        return _result("status", "degraded", EXIT_DEGRADED, data={"observatory": "unavailable", "state": state, "dashboard": dashboard, "collector": collector, "live_compose": live_compose, "inference_path": "unmanaged/no-proxy"}, warnings=warnings)


def _command_configure(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("configure", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        config = _load_config(paths) or {}
    except RuntimeError as exc:
        return _result("configure", "conflict", EXIT_CONFLICT, errors=[str(exc)])
    clients = dict(config.get("managed_clients", {}))
    requested = [item for item in (args.client.split(",") if args.client == "all" else [args.client]) if item]
    names: list[str] = []
    try:
        for item in requested:
            normalized = normalize_client_name(item)
            if normalized == "all":
                # The executable catalog is the source of truth for the
                # supported target set.  Do not maintain a second list here.
                names.extend(sorted(CLIENT_SPECS))
            else:
                client_spec(normalized)
                names.append(normalized)
    except ValueError as exc:
        return _result("configure", "unsupported", EXIT_UNSUPPORTED, errors=[str(exc)])
    names = list(dict.fromkeys(names))

    if args.remove:
        results: dict[str, Any] = {}
        changed = False
        exit_code = EXIT_OK
        for name in names:
            record = clients.get(name, {}) if isinstance(clients.get(name), dict) else {}
            if args.apply:
                managed_keys = record.get("managed_keys") if isinstance(record.get("managed_keys"), list) else None
                managed_hash = record.get("managed_hash") if isinstance(record.get("managed_hash"), str) else None
                managed_state = record.get("managed_state") if isinstance(record.get("managed_state"), dict) else None
                result = remove_configuration(name, managed_keys=managed_keys, managed_hash=managed_hash, managed_state=managed_state)
                changed = changed or bool(result.get("changed"))
            else:
                result = {"changed": False, "removed": False, "inference_proxy": False, "mode": "state-only"}
            results[name] = result
            if result.get("conflicts"):
                exit_code = max(exit_code, EXIT_CONFLICT)
            else:
                clients.pop(name, None)
        config["managed_clients"] = clients
        _write_json_atomic(paths.config, config)
        warning = [] if args.apply else ["Only Observatory state was removed; use --apply to remove matching managed keys from a client configuration."]
        outcome = "conflict" if exit_code == EXIT_CONFLICT else "success"
        return _result("configure", outcome, exit_code, data={"clients": results, "changed": changed, "removed": exit_code == EXIT_OK, "inference_proxy": False}, warnings=warning)

    results = {}
    warnings: list[str] = []
    exit_code = EXIT_OK
    for name in names:
        record = clients.get(name, {}) if isinstance(clients.get(name), dict) else {}
        prior_managed_hash = record.get("managed_hash") if isinstance(record.get("managed_hash"), str) else None
        prior_managed_state = record.get("managed_state") if isinstance(record.get("managed_state"), dict) else None
        if args.apply:
            result = apply_configuration(
                name,
                enable_traces=args.traces,
                force=args.force,
                managed_hash=prior_managed_hash,
                managed_state=prior_managed_state,
            )
            if result.get("conflicts"):
                exit_code = max(exit_code, EXIT_CONFLICT)
                warnings.append(f"{name}: existing client telemetry keys were not overwritten; use --force only after reviewing the conflict")
            elif not result.get("applied", False):
                exit_code = max(exit_code, EXIT_DEGRADED)
                warnings.extend(result.get("warnings", []))
        else:
            result = plan_configuration(name, enable_traces=args.traces)
            result["applied"] = False
            exit_code = max(exit_code, EXIT_DEGRADED)
            if result.get("supported"):
                warnings.append(f"{name}: plan only; pass --apply to update the user-level client configuration")
            else:
                warnings.extend(result.get("warnings", []))
        results[name] = result
        managed_keys = result.get("managed_keys") if isinstance(result.get("managed_keys"), list) else record.get("managed_keys", [])
        managed_state = result.get("managed_state") if isinstance(result.get("managed_state"), dict) else prior_managed_state
        clients[name] = {
            "mode": result.get("mode", "discovery-only"),
            "config_path": result.get("path") or result.get("config_path"),
            "managed_keys": managed_keys,
            "managed_state": managed_state or {},
            "managed_hash": result.get("managed_hash") or prior_managed_hash,
            "applied": bool(result.get("applied", False)),
            "inference_proxy": False,
            "content_capture": False,
            "capability_confidence": result.get("capabilities", {}).get("confidence") if isinstance(result.get("capabilities"), dict) else None,
        }
    config["managed_clients"] = clients
    _write_json_atomic(paths.config, config)
    outcome = "success" if exit_code == EXIT_OK else "conflict" if exit_code == EXIT_CONFLICT else "partial"
    return _result("configure", outcome, exit_code, data={"clients": results, "client": names[0] if len(names) == 1 else "all", "inference_proxy": False}, warnings=warnings)


def _command_uninstall(args: argparse.Namespace) -> dict[str, Any]:
    """Stop Observatory and optionally restore its owned client settings."""

    paths = _paths(args)
    if not paths.config.exists():
        return _result("uninstall", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    if args.delete_state and not args.apply:
        return _result("uninstall", "failed", EXIT_USAGE, errors=["--delete-state requires --apply so managed client settings are restored first"])
    try:
        config = _load_config(paths) or {}
    except RuntimeError as exc:
        return _result("uninstall", "conflict", EXIT_CONFLICT, errors=[str(exc)])

    managed_clients = config.get("managed_clients", {})
    managed_clients = managed_clients if isinstance(managed_clients, dict) else {}
    client_results: dict[str, Any] = {}
    exit_code = EXIT_OK
    if args.apply:
        for name, record in sorted(managed_clients.items()):
            record = record if isinstance(record, dict) else {}
            result = remove_configuration(
                name,
                managed_keys=record.get("managed_keys") if isinstance(record.get("managed_keys"), list) else None,
                managed_hash=record.get("managed_hash") if isinstance(record.get("managed_hash"), str) else None,
                managed_state=record.get("managed_state") if isinstance(record.get("managed_state"), dict) else None,
            )
            client_results[name] = result
            if result.get("conflicts"):
                exit_code = max(exit_code, EXIT_CONFLICT)
    else:
        client_results = {
            name: {"changed": False, "removed": False, "mode": "state-only", "inference_proxy": False}
            for name in sorted(managed_clients)
        }

    operation = "down --remove-orphans"
    if args.remove_volumes:
        operation += " --volumes"
    compose_code, compose_message = _compose(args, operation)
    if compose_code != EXIT_OK:
        return _result(
            "uninstall",
            "degraded",
            compose_code,
            data={"clients": client_results, "stack": "not_removed", "inference_proxy": False},
            errors=[compose_message],
        )

    if args.apply and exit_code == EXIT_OK:
        config["managed_clients"] = {}
        if args.delete_state:
            resolved = paths.root.resolve()
            home = Path.home().resolve()
            cwd = Path.cwd().resolve()
            if (
                resolved == Path(resolved.anchor)
                or resolved == home
                or home.is_relative_to(resolved)
                or cwd.is_relative_to(resolved)
                or resolved == cwd
            ):
                return _result("uninstall", "failed", EXIT_FAILED, data={"clients": client_results, "stack": "removed"}, errors=[f"refusing to delete unsafe state directory: {resolved}"])
            shutil.rmtree(resolved)
            state = "deleted"
        else:
            _write_json_atomic(paths.config, config)
            state = "retained"
    else:
        state = "retained"

    warnings = []
    if not args.apply and managed_clients:
        warnings.append("stack removed but managed client settings were retained; rerun uninstall --apply to restore them")
    outcome = "success" if exit_code == EXIT_OK else "conflict"
    return _result(
        "uninstall",
        outcome,
        exit_code,
        data={"clients": client_results, "stack": "removed", "state": state, "inference_proxy": False},
        warnings=warnings,
    )


def _command_open(args: argparse.Namespace) -> dict[str, Any]:
    url = args.url
    if args.print_url:
        return _result("open", "success", EXIT_OK, data={"url": url})
    opened = webbrowser.open(url)
    return _result("open", "success" if opened else "degraded", EXIT_OK if opened else EXIT_DEGRADED, data={"url": url, "opened": opened})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="observatory")
    parser.add_argument("--json", action="store_true", dest="json_mode")
    parser.add_argument("--state-dir")
    parser.add_argument("--timeout", type=float, default=DEFAULT_OPERATION_TIMEOUT)
    sub = parser.add_subparsers(dest="command", required=True)
    install = sub.add_parser("install")
    install.add_argument("--compose-file")
    install.add_argument("--demo", action="store_true", help="seed the bundled metadata-only walkthrough fixture")
    install.add_argument("--demo-file", help="use an explicit JSONL walkthrough fixture with --demo")
    doctor = sub.add_parser("doctor")
    doctor.add_argument("--compose-file")
    start = sub.add_parser("start")
    start.add_argument("--compose-file")
    start.add_argument("--api-url", default="http://127.0.0.1:8787/readyz")
    start.add_argument("--collector-url", default="http://127.0.0.1:13133/")
    start.add_argument("--grafana-url", default="http://127.0.0.1:3000/api/health")
    stop = sub.add_parser("stop")
    stop.add_argument("--compose-file")
    uninstall = sub.add_parser("uninstall")
    uninstall.add_argument("--compose-file")
    uninstall.add_argument("--apply", action="store_true", help="restore Observatory-owned client telemetry settings")
    uninstall.add_argument("--delete-state", action="store_true", help="delete the exact installed state directory after cleanup")
    uninstall.add_argument("--remove-volumes", action="store_true", help="remove Compose volumes after stopping the stack")
    status = sub.add_parser("status")
    status.add_argument("--url", default="http://127.0.0.1:8787/healthz")
    status.add_argument("--grafana-url", default="http://127.0.0.1:3000/api/health")
    status.add_argument("--collector-url", default="http://127.0.0.1:13133/")
    configure = sub.add_parser("configure")
    configure.add_argument("client")
    configure.add_argument("--remove", action="store_true")
    configure.add_argument("--apply", action="store_true", help="write verified global client telemetry settings")
    configure.add_argument("--force", action="store_true", help="overwrite conflicting telemetry-only keys after review")
    configure.add_argument("--traces", action="store_true", help="enable client trace export where the client marks it supported")
    open_parser = sub.add_parser("open")
    open_parser.add_argument("--url", default="http://127.0.0.1:3000/d/global-observatory/global-observatory")
    open_parser.add_argument("--print-url", action="store_true")
    demo = sub.add_parser("demo", help="seed the bundled metadata-only walkthrough fixture")
    demo.add_argument("--file", help="use an explicit JSONL walkthrough fixture")
    update = sub.add_parser("update")
    update.add_argument("--compose-file")
    update.add_argument("--check", action="store_true")
    update.add_argument("--pull", action="store_true", help="pull pinned Compose images and recreate services")
    ingest = sub.add_parser("ingest")
    ingest.add_argument("--file", default="-")
    ingest.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    ingest.add_argument("--offline", action="store_true")
    ingest.add_argument("--project-path", default=str(Path.cwd()), help="project directory used when records omit project identity")
    flush = sub.add_parser("flush")
    flush.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    flush.add_argument("--offline", action="store_true", help="replay the bounded spool into the local SQLite store")
    flush.add_argument("--max-files", type=int, default=16)
    flush.add_argument("--project-path", default=str(Path.cwd()), help="project directory used when spooled records omit project identity")
    outcome = sub.add_parser("record-outcome")
    outcome.add_argument("--kind", required=True)
    outcome.add_argument("--status", required=True)
    outcome.add_argument("--correlation-id")
    outcome.add_argument("--correlation-basis")
    outcome.add_argument("--evidence-source", default="operator")
    outcome.add_argument("--task-id")
    outcome.add_argument("--task-class")
    outcome.add_argument("--project-path", default=str(Path.cwd()))
    outcome.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    outcome.add_argument("--offline", action="store_true")
    run_outcome = sub.add_parser("run-outcome")
    run_outcome.add_argument("--kind", required=True)
    run_outcome.add_argument("--correlation-id")
    run_outcome.add_argument("--evidence-source", default="local-command")
    run_outcome.add_argument("--project-path", default=str(Path.cwd()))
    run_outcome.add_argument("--command-timeout", type=float, default=900.0)
    run_outcome.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    run_outcome.add_argument("--offline", action="store_true")
    run_outcome.add_argument("command_args", nargs=argparse.REMAINDER)
    snapshot = sub.add_parser("git-snapshot")
    snapshot.add_argument("--correlation-id")
    snapshot.add_argument("--project-path", default=str(Path.cwd()))
    snapshot.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    snapshot.add_argument("--offline", action="store_true")
    migrate = sub.add_parser("migrate")
    retention = sub.add_parser("retention")
    retention.add_argument("--prometheus-days", type=int)
    retention.add_argument("--tempo-hours", type=int)
    retention.add_argument("--loki-hours", type=int)
    backup = sub.add_parser("backup")
    backup.add_argument("target")
    backup.add_argument("--compose-file")
    backup.add_argument("--project-name", help="Compose project name when it differs from the installed default")
    backup.add_argument("--full-state", action="store_true", help="archive host-owned state and bounded spool")
    backup.add_argument("--backend-volumes", action="store_true", help="also archive all five stopped Compose backend volumes")
    backup.add_argument("--include-secret", action="store_true", help="include the Grafana secret in a full-state archive")
    backup.add_argument("--overwrite", action="store_true", help="replace an existing archive target")
    restore = sub.add_parser("restore")
    restore.add_argument("source")
    restore.add_argument("--compose-file")
    restore.add_argument("--project-name", help="Compose project name when it differs from the installed default")
    restore.add_argument("--full-state", action="store_true", help="restore a host-state archive")
    restore.add_argument("--backend-volumes", action="store_true", help="also restore all archived Compose backend volumes")
    restore.add_argument("--restore-secret", action="store_true", help="restore the archived Grafana secret")
    restore.add_argument("--overwrite", action="store_true")
    restore.add_argument("--api-health-url", default="http://127.0.0.1:8787/healthz", help="health endpoint for the target API; restore refuses to proceed when it is reachable")
    prune = sub.add_parser("prune")
    prune.add_argument("--before")
    prune.add_argument("--event-id", action="append", default=[])
    prune.add_argument("--confirm", action="store_true")
    resolve = sub.add_parser("resolve-project")
    resolve.add_argument("path")
    api = sub.add_parser("run-api")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8787)
    api.add_argument("--max-database-bytes", type=int, default=DEFAULT_MAX_DATABASE_BYTES)
    api.add_argument("--allow-remote", action="store_true", help="explicitly allow a non-loopback bind")
    api.add_argument("--allow-insecure-remote", action="store_true", help="explicitly allow a non-loopback bind without a bearer token; use only behind a trusted private network")
    api.add_argument("--auth-token-file", help="read a bearer token from an operator-owned file")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "install":
            value = _command_install(args)
        elif args.command == "doctor":
            value = _command_doctor(args)
        elif args.command == "ingest":
            value = _command_ingest(args)
        elif args.command == "flush":
            if args.max_files < 1 or args.max_files > MAX_SPOOL_FILES:
                value = _result("flush", "failed", EXIT_USAGE, errors=[f"max-files must be between 1 and {MAX_SPOOL_FILES}"])
            else:
                value = _command_flush(args)
        elif args.command == "record-outcome":
            value = _command_record_outcome(args)
        elif args.command == "run-outcome":
            if not args.command_args:
                value = _result("run-outcome", "failed", EXIT_USAGE, errors=["a command is required after --"])
            else:
                value = _command_run_outcome(args)
        elif args.command == "git-snapshot":
            value = _command_git_snapshot(args)
        elif args.command == "migrate":
            value = _command_migrate(args)
        elif args.command == "retention":
            value = _command_retention(args)
        elif args.command == "backup":
            value = _command_backup(args)
        elif args.command == "restore":
            value = _command_restore(args)
        elif args.command == "prune":
            value = _command_prune(args)
        elif args.command == "resolve-project":
            value = _command_resolve_project(args)
        elif args.command == "start":
            value = _command_start(args)
        elif args.command == "stop":
            value = _command_lifecycle(args, "stop", "stop")
        elif args.command == "uninstall":
            value = _command_uninstall(args)
        elif args.command == "update":
            value = _command_update(args)
        elif args.command == "status":
            value = _command_status(args)
        elif args.command == "configure":
            value = _command_configure(args)
        elif args.command == "open":
            value = _command_open(args)
        elif args.command == "demo":
            value = _seed_demo(_paths(args), args.file)
        elif args.command == "run-api":
            auth_token = _read_api_token(args.auth_token_file)
            if not _is_loopback_host(args.host) and not args.allow_remote:
                value = _result("run-api", "failed", EXIT_USAGE, errors=["non-loopback API binds require explicit --allow-remote review"])
            elif not _is_loopback_host(args.host) and auth_token is None and not args.allow_insecure_remote:
                value = _result("run-api", "failed", EXIT_USAGE, errors=["non-loopback API binds require --auth-token-file, or an explicit --allow-insecure-remote review"])
            else:
                serve(args.host, args.port, _paths(args).database, max_database_bytes=args.max_database_bytes, auth_token=auth_token)
                value = _result("run-api", "success", EXIT_OK)
        else:
            value = _result(args.command, "failed", EXIT_USAGE, errors=["unsupported command"])
    except (OSError, RuntimeError, ValueError) as exc:
        value = _result(args.command, "failed", EXIT_FAILED, errors=[str(exc)])
    return _print_result(value, args.json_mode)


if __name__ == "__main__":
    raise SystemExit(main())
