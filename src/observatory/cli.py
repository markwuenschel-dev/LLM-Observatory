"""The host-side `observatory` lifecycle and ingestion CLI."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
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
    apply_configuration,
    client_spec,
    normalize_client_name,
    plan_configuration,
    remove_configuration,
)
from .clock import utc_now
from .contracts import ContractError, NormalizedEvent, canonical_json
from .intake import Intake
from .maintenance import backup_database, purge_events, restore_database, schema_versions
from .privacy import PrivacyPolicy, redact_event
from .project import resolve_project
from .outcomes import git_outcome_snapshot, make_outcome_event, run_command_outcome
from .store import EventStore


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
DEFAULT_RETENTION = {
    "prometheus_days": 30,
    "prometheus_size": "8GB",
    "tempo_hours": 720,
    "loki_hours": 336,
    "normalized_events": "operator-managed",
}


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


def _reconcile_compose_env(paths: StatePaths, retention: Mapping[str, Any] | None = None) -> bool:
    """Keep Compose pointed at the same host state and retention policy used by the CLI."""

    desired = {
        "OBSERVATORY_STATE_DIR": paths.root.resolve().as_posix(),
        "OBSERVATORY_SECRET_FILE": paths.secret.resolve().as_posix(),
    }
    desired.update(_retention_environment(retention))
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
        paths.secret.write_text(secrets.token_urlsafe(32) + "\n", encoding="utf-8")
    try:
        os.chmod(paths.secret, 0o600)
    except OSError:
        # Windows ACLs govern the file there; do not make install fail when
        # chmod is unsupported, while Unix profiles still get a private mode.
        pass
    config = existing or {
        "schema_version": "1.0",
        "privacy": {"content_capture": False, "store_raw_paths": False},
        "retention": dict(DEFAULT_RETENTION),
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
    env_changed = _reconcile_compose_env(paths, config.get("retention"))
    changed = changed or env_changed
    if not paths.config.exists() or changed:
        _write_json_atomic(paths.config, config)
    with EventStore(paths.database) as store:
        versions = schema_versions(store)
    return _result("install", "success", EXIT_OK, data={"state_dir": str(paths.root), "changed": changed, "secret_created": secret_created, "database": str(paths.database), "schema_versions": versions, "content_capture": False})


def _command_doctor(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    checks: list[dict[str, Any]] = []
    if not paths.config.exists():
        return _result("doctor", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        config = _load_config(paths)
    except RuntimeError as exc:
        return _result("doctor", "conflict", EXIT_CONFLICT, errors=[str(exc)])
    checks.append({"id": "state.config", "status": "pass", "blocking": True})
    checks.append({"id": "privacy.content_capture", "status": "pass" if config.get("privacy", {}).get("content_capture") is False else "warn", "blocking": True})
    checks.append({"id": "retention.policy", "status": "pass" if isinstance(config.get("retention"), dict) else "warn", "blocking": False})
    checks.append({"id": "spool.policy", "status": "pass" if isinstance(config.get("spool"), dict) else "warn", "blocking": False})
    checks.append({"id": "python.version", "status": "pass" if sys.version_info >= (3, 11) else "fail", "blocking": True, "value": sys.version.split()[0]})
    compose = Path(args.compose_file) if args.compose_file else Path.cwd() / "compose.yaml"
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
    try:
        free_bytes = shutil.disk_usage(paths.root).free
        free_gib = round(free_bytes / (1024 ** 3), 2)
        disk_status = "pass" if free_bytes >= 1 * 1024 ** 3 else "warn"
        checks.append({"id": "state.disk_free", "status": disk_status, "blocking": False, "free_gib": free_gib})
        if disk_status == "warn":
            warnings = [f"state directory has only {free_gib} GiB free; retention and queue growth need operator attention"]
        else:
            warnings = []
    except OSError as exc:
        warnings = [f"could not inspect state-disk capacity: {exc}"]
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
    for port in (8787, 3000, 4317, 4318, 13133):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.2)
            occupied = probe.connect_ex(("127.0.0.1", port)) == 0
        checks.append({"id": f"port.{port}", "status": "warn" if occupied else "pass", "blocking": False})
        if occupied:
            warnings.append(f"loopback port {port} is already in use")
    result = _result("doctor", "success" if not warnings else "degraded", EXIT_OK if not warnings else EXIT_DEGRADED, data={"checks": checks, "state_dir": str(paths.root)}, warnings=warnings)
    result["checks"] = checks
    return result


def _read_jsonl(path: str | None) -> list[dict[str, Any]]:
    stream = sys.stdin if not path or path == "-" else open(path, "r", encoding="utf-8")
    close = stream is not sys.stdin
    records: list[dict[str, Any]] = []
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
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {line_number}: invalid JSON: {exc.msg}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"line {line_number}: record must be an object")
            records.append(value)
    finally:
        if close:
            stream.close()
    return records


def _safe_records(records: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    safe: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, record in enumerate(records):
        try:
            event = NormalizedEvent.from_mapping(record, received_at=utc_now())
            safe.append(redact_event(event).to_mapping())
        except (ContractError, ValueError) as exc:
            errors.append(f"record {index}: {exc}")
    return safe, errors


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
            records = _read_jsonl(str(path))
            safe, rejected = _safe_records(records)
            if rejected:
                errors.extend([f"{path.name}: {item}" for item in rejected])
                break
            if args.offline:
                with EventStore(paths.database) as store:
                    intake = Intake(store)
                    result = intake.ingest(safe)
                records_sent += result.inserted + result.duplicate + result.conflict
            else:
                response = _post_events(args.url, safe)
                if response.get("outcome") != "accepted":
                    raise RuntimeError("flush endpoint did not accept the batch")
                rejected = int(response.get("rejected", 0) or 0)
                if rejected:
                    raise RuntimeError(f"flush endpoint rejected {rejected} records; spool file retained")
                records_sent += len(safe)
            path.unlink()
            removed += 1
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
        return json.loads(response.read().decode("utf-8"))


def _command_ingest(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("ingest", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        records = _read_jsonl(args.file)
        safe, errors = _safe_records(records)
    except (OSError, ValueError) as exc:
        return _result("ingest", "failed", EXIT_FAILED, errors=[str(exc)])
    if not safe:
        return _result("ingest", "degraded", EXIT_DEGRADED, data={"accepted": 0}, errors=errors)
    if args.offline:
        with EventStore(paths.database) as store:
            intake = Intake(store)
            result = intake.ingest(safe)
        return _result("ingest", "success" if not errors else "degraded", EXIT_OK if not errors else EXIT_DEGRADED, data={**result.to_mapping(), "mode": "offline"}, errors=errors)
    try:
        response = _post_events(args.url, safe)
        response["mode"] = "api"
        response["preflight_rejected"] = len(errors)
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
        with EventStore(paths.database) as store:
            result = store.append(event)
        return _result("record-outcome", "success" if result.status == "inserted" else result.status, EXIT_OK if result.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": result.status})
    try:
        response = _post_events(args.url, [record])
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
        with EventStore(paths.database) as store:
            append = store.append(event)
        return _result("run-outcome", "success" if append.status in {"inserted", "duplicate"} else "conflict", EXIT_OK if append.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": append.status, "returncode": result.returncode, "duration_ms": result.duration_ms})
    try:
        response = _post_events(args.url, [record])
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
        with EventStore(paths.database) as store:
            result = store.append(event)
        return _result("git-snapshot", "success" if result.status in {"inserted", "duplicate"} else "conflict", EXIT_OK if result.status in {"inserted", "duplicate"} else EXIT_CONFLICT, data={"mode": "offline", "event_id": event.event_id, "append": result.status})
    try:
        response = _post_events(args.url, [record])
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
        with EventStore(paths.database) as store:
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
        env_changed = _reconcile_compose_env(paths, updated)
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
        result = backup_database(paths.database, args.target)
        return _result("backup", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("backup", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_restore(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("restore", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        result = restore_database(args.source, paths.database, overwrite=args.overwrite)
        return _result("restore", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("restore", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_prune(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("prune", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        with EventStore(paths.database) as store:
            result = purge_events(store, before=args.before, event_ids=args.event_id, confirm=args.confirm)
        return _result("prune", "success", EXIT_OK, data=result)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("prune", "failed", EXIT_FAILED, errors=[str(exc)])


def _command_resolve_project(args: argparse.Namespace) -> dict[str, Any]:
    return _result("resolve-project", "success", EXIT_OK, data=resolve_project(args.path).to_mapping())


def _compose(args: argparse.Namespace, operation: str) -> tuple[int, str]:
    compose = Path(args.compose_file) if args.compose_file else Path.cwd() / "compose.yaml"
    if not compose.exists():
        return EXIT_NOT_INITIALIZED, f"Compose file not found at {compose}"
    paths = _paths(args)
    if not paths.compose_env.exists():
        return EXIT_NOT_INITIALIZED, f"state environment not found at {paths.compose_env}; run observatory install"
    command = ["docker", "compose", "--env-file", str(paths.compose_env), "-f", str(compose), *operation.split()]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=args.timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return EXIT_DEGRADED, f"Docker unavailable: {exc}"
    if result.returncode != 0:
        return EXIT_DEGRADED, (result.stderr or result.stdout).strip() or "Docker operation failed"
    return EXIT_OK, (result.stdout or "completed").strip()


def _command_lifecycle(args: argparse.Namespace, command: str, operation: str) -> dict[str, Any]:
    code, message = _compose(args, operation)
    return _result(command, "success" if code == EXIT_OK else "degraded", code, data={"message": message})


def _command_update(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("update", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    try:
        with EventStore(paths.database) as store:
            versions = schema_versions(store)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result("update", "failed", EXIT_FAILED, errors=[str(exc)])
    if args.check:
        return _result("update", "success", EXIT_OK, data={"check": True, "schema_versions": versions, "changed": False, "image_pull": "not_requested"})
    if not args.pull:
        return _result("update", "degraded", EXIT_DEGRADED, data={"check": False, "schema_versions": versions, "changed": False, "image_pull": "not_requested"}, warnings=["local migrations are current; pass --pull to request an explicit Compose image update"])
    backup_target = paths.root / "backups" / f"pre-update-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}.sqlite3"
    try:
        backup = backup_database(paths.database, backup_target)
    except (OSError, RuntimeError, ValueError) as exc:
        return _result(
            "update",
            "failed",
            EXIT_FAILED,
            data={"schema_versions": versions, "changed": False, "image_pull": "not_started"},
            errors=[f"refusing image update because the pre-update database backup failed: {exc}"],
        )
    code, message = _compose(args, "pull")
    if code != EXIT_OK:
        return _result("update", "degraded", EXIT_DEGRADED, data={"schema_versions": versions, "changed": False, "image_pull": "failed", "backup": backup}, warnings=[message, f"pre-update backup retained at {backup['target']}"])
    start_code, start_message = _compose(args, "up -d")
    if start_code != EXIT_OK:
        return _result("update", "degraded", EXIT_DEGRADED, data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "message": start_message}, warnings=[f"pre-update backup retained at {backup['target']}"])
    ready, detail = _wait_http("http://127.0.0.1:8787/readyz", timeout=args.timeout)
    if not ready:
        return _result(
            "update",
            "degraded",
            EXIT_DEGRADED,
            data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "message": start_message, "readiness": detail},
            warnings=[f"updated Compose stack did not pass API readiness; pre-update backup retained at {backup['target']}"],
        )
    service_readiness = {"api": detail}
    for name, url in (("collector", "http://127.0.0.1:13133/"), ("grafana", "http://127.0.0.1:3000/api/health")):
        service_ready, service_detail = _wait_http(url, timeout=args.timeout)
        service_readiness[name] = service_detail
        if not service_ready:
            return _result(
                "update",
                "degraded",
                EXIT_DEGRADED,
                data={"schema_versions": versions, "changed": False, "image_pull": "complete", "backup": backup, "message": start_message, "readiness": detail, "service_readiness": service_readiness},
                warnings=[f"updated Compose stack did not pass {name} readiness; pre-update backup retained at {backup['target']}"],
            )
    return _result("update", "success", EXIT_OK, data={"schema_versions": versions, "changed": True, "image_pull": "complete", "backup": backup, "message": start_message, "readiness": detail, "service_readiness": service_readiness})


def _command_status(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths(args)
    if not paths.config.exists():
        return _result("status", "not_initialized", EXIT_NOT_INITIALIZED, errors=["run observatory install first"])
    dashboard_ready, dashboard_detail = _probe_http(args.grafana_url, timeout=args.timeout)
    dashboard = {"status": "ready" if dashboard_ready else "unavailable", "detail": dashboard_detail, "url": args.grafana_url}
    collector_ready, collector_detail = _probe_http(args.collector_url, timeout=args.timeout)
    collector = {"status": "ready" if collector_ready else "unavailable", "detail": collector_detail, "url": args.collector_url}
    try:
        request = Request(args.url, method="GET")
        with urlopen(request, timeout=args.timeout) as response:
            health = json.loads(response.read().decode("utf-8"))
        warnings = []
        if not dashboard_ready:
            warnings.append(f"Grafana is not reachable at {args.grafana_url}")
        if not collector_ready:
            warnings.append(f"OTel Collector is not reachable at {args.collector_url}")
        ready = dashboard_ready and collector_ready
        return _result("status", "success" if ready else "degraded", EXIT_OK if ready else EXIT_DEGRADED, data={"observatory": "ready", "dashboard": dashboard, "collector": collector, "telemetry": health, "inference_path": "unmanaged/no-proxy"}, warnings=warnings)
    except (OSError, URLError, ValueError) as exc:
        return _result("status", "degraded", EXIT_DEGRADED, data={"observatory": "unavailable", "dashboard": dashboard, "collector": collector, "inference_path": "unmanaged/no-proxy"}, warnings=[str(exc)])


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
                names.extend(sorted({key for key in ("claude", "codex", "gemini", "cursor", "kimi", "grok", "openrouter", "direct-openai", "direct-anthropic", "direct-google", "direct-xai")}))
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
                result = remove_configuration(name, managed_keys=managed_keys, managed_hash=managed_hash)
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
        if args.apply:
            result = apply_configuration(name, enable_traces=args.traces, force=args.force, managed_hash=prior_managed_hash)
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
        clients[name] = {
            "mode": result.get("mode", "discovery-only"),
            "config_path": result.get("path") or result.get("config_path"),
            "managed_keys": managed_keys,
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
    parser.add_argument("--timeout", type=float, default=10.0)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("install")
    doctor = sub.add_parser("doctor")
    doctor.add_argument("--compose-file")
    start = sub.add_parser("start")
    start.add_argument("--compose-file")
    stop = sub.add_parser("stop")
    stop.add_argument("--compose-file")
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
    update = sub.add_parser("update")
    update.add_argument("--compose-file")
    update.add_argument("--check", action="store_true")
    update.add_argument("--pull", action="store_true", help="pull pinned Compose images and recreate services")
    ingest = sub.add_parser("ingest")
    ingest.add_argument("--file", default="-")
    ingest.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    ingest.add_argument("--offline", action="store_true")
    flush = sub.add_parser("flush")
    flush.add_argument("--url", default="http://127.0.0.1:8787/v1/events")
    flush.add_argument("--offline", action="store_true", help="replay the bounded spool into the local SQLite store")
    flush.add_argument("--max-files", type=int, default=16)
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
    restore = sub.add_parser("restore")
    restore.add_argument("source")
    restore.add_argument("--overwrite", action="store_true")
    prune = sub.add_parser("prune")
    prune.add_argument("--before")
    prune.add_argument("--event-id", action="append", default=[])
    prune.add_argument("--confirm", action="store_true")
    resolve = sub.add_parser("resolve-project")
    resolve.add_argument("path")
    api = sub.add_parser("run-api")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8787)
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
            value = _command_lifecycle(args, "start", "up -d")
        elif args.command == "stop":
            value = _command_lifecycle(args, "stop", "stop")
        elif args.command == "update":
            value = _command_update(args)
        elif args.command == "status":
            value = _command_status(args)
        elif args.command == "configure":
            value = _command_configure(args)
        elif args.command == "open":
            value = _command_open(args)
        elif args.command == "run-api":
            serve(args.host, args.port, _paths(args).database)
            value = _result("run-api", "success", EXIT_OK)
        else:
            value = _result(args.command, "failed", EXIT_USAGE, errors=["unsupported command"])
    except (OSError, RuntimeError, ValueError) as exc:
        value = _result(args.command, "failed", EXIT_FAILED, errors=[str(exc)])
    return _print_result(value, args.json_mode)


if __name__ == "__main__":
    raise SystemExit(main())
