"""Explicit backup, restore, migration, and audited retention operations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import uuid
import zipfile
from typing import Any, Callable, Iterable, Mapping

from .contracts import ContractError, ensure_utc
from .store import EventStore


_EVIDENCE_TRIGGERS = (
    "prevent_ingest_ledger_update",
    "prevent_ingest_ledger_delete",
    "prevent_measurement_facts_update",
    "prevent_measurement_facts_delete",
    "prevent_outcome_events_update",
    "prevent_outcome_events_delete",
    "prevent_attribution_edges_update",
    "prevent_attribution_edges_delete",
)

_EVIDENCE_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_update BEFORE UPDATE ON ingest_ledger BEGIN SELECT RAISE(ABORT, 'ingest_ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_delete BEFORE DELETE ON ingest_ledger BEGIN SELECT RAISE(ABORT, 'ingest_ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_update BEFORE UPDATE ON measurement_facts BEGIN SELECT RAISE(ABORT, 'measurement_facts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_delete BEFORE DELETE ON measurement_facts BEGIN SELECT RAISE(ABORT, 'measurement_facts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_update BEFORE UPDATE ON outcome_events BEGIN SELECT RAISE(ABORT, 'outcome_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_delete BEFORE DELETE ON outcome_events BEGIN SELECT RAISE(ABORT, 'outcome_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_update BEFORE UPDATE ON attribution_edges BEGIN SELECT RAISE(ABORT, 'attribution_edges is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_delete BEFORE DELETE ON attribution_edges BEGIN SELECT RAISE(ABORT, 'attribution_edges is append-only'); END;
"""


def _restore_evidence_triggers(connection: sqlite3.Connection) -> None:
    for statement in _EVIDENCE_TRIGGER_SQL.strip().splitlines():
        if statement.strip():
            connection.execute(statement)


def schema_versions(store: EventStore) -> list[str]:
    rows = store.connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return [str(row["version"]) for row in rows]


def read_schema_versions(path: str | Path) -> list[str]:
    """Read migration state through SQLite's read-only URI without migrating it."""

    database = Path(path).expanduser().resolve(strict=True)
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        rows = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    return [str(row[0]) for row in rows]


def backup_database(source: str | Path, target: str | Path, *, overwrite: bool = False) -> dict[str, Any]:
    source_path = Path(source).expanduser().resolve(strict=True)
    target_path = Path(target).expanduser().resolve(strict=False)
    if source_path == target_path:
        raise ValueError("backup target must differ from source")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"backup target exists; pass overwrite explicitly: {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source_path)) as source_connection:
        with closing(sqlite3.connect(target_path)) as target_connection:
            source_connection.backup(target_connection)
            target_connection.commit()
    return {
        "source": str(source_path),
        "target": str(target_path),
        "bytes": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        "integrity": _integrity(target_path),
    }


def restore_database(source: str | Path, target: str | Path, *, overwrite: bool = False) -> dict[str, Any]:
    source_path = Path(source).expanduser().resolve(strict=True)
    target_path = Path(target).expanduser().resolve(strict=False)
    if source_path == target_path:
        raise ValueError("restore source and target must differ")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"restore target exists; pass overwrite explicitly: {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source_path)) as source_connection:
        with closing(sqlite3.connect(target_path)) as target_connection:
            source_connection.backup(target_connection)
            target_connection.commit()
    return {
        "source": str(source_path),
        "target": str(target_path),
        "bytes": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        "integrity": _integrity(target_path),
    }


def _integrity(path: Path) -> str:
    with closing(sqlite3.connect(path)) as connection:
        value = connection.execute("PRAGMA integrity_check").fetchone()[0]
    return str(value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


_STATE_BACKUP_SCHEMA = "observatory.state-backup/v1"
_MAX_STATE_ARCHIVE_BYTES = 8 * 1024 * 1024 * 1024
_MAX_STATE_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
_MAX_STATE_UNCOMPRESSED_BYTES = 8 * 1024 * 1024 * 1024
_MAX_STATE_ARCHIVE_MEMBERS = 4096
BACKEND_VOLUME_KEYS = (
    "otel-queue",
    "tempo-data",
    "loki-data",
    "prometheus-data",
    "grafana-data",
)
_DOCKER_VOLUME_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DOCKER_SIZE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>B|kB|MB|GB|TB|KiB|MiB|GiB|TiB)?\s*$", re.IGNORECASE)
_BACKEND_ARCHIVE_ROOT = "backend-volumes"
_SECRET_FILES = (
    "secrets/grafana_admin_password",
    "secrets/observatory_api_token",
    "secrets/observatory_api_authorization",
)


def _copy_zip_member_bounded(archive: zipfile.ZipFile, member: str, destination: Path, *, maximum: int) -> int:
    info = archive.getinfo(member)
    if info.file_size < 0 or info.file_size > maximum:
        raise ValueError(f"state backup member exceeds the {maximum}-byte extraction limit: {member}")
    copied = 0
    with archive.open(info, mode="r") as source_handle, destination.open("wb") as destination_handle:
        while True:
            chunk = source_handle.read(1_048_576)
            if not chunk:
                break
            copied += len(chunk)
            if copied > maximum:
                raise ValueError(f"state backup member expands beyond the {maximum}-byte extraction limit: {member}")
            destination_handle.write(chunk)
    return copied


def _archive_member_is_safe(name: str) -> bool:
    path = Path(name)
    return bool(name) and not path.is_absolute() and all(part not in {"", ".", ".."} for part in path.parts)


def _atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.restore-{uuid.uuid4().hex}.tmp")
    try:
        with source.open("rb") as source_handle, temporary.open("wb") as target_handle:
            shutil.copyfileobj(source_handle, target_handle, length=1_048_576)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _docker_result(
    command: list[str],
    *,
    timeout: float,
    runner: Callable[..., Any] | None = None,
) -> Any:
    execute = runner or subprocess.run
    try:
        result = execute(command, check=False, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"Docker command failed to run: {' '.join(command[:4])}: {exc}") from exc
    if getattr(result, "returncode", 1) != 0:
        detail = (getattr(result, "stderr", "") or getattr(result, "stdout", "") or "command failed").strip()
        raise RuntimeError(f"Docker command failed ({getattr(result, 'returncode', 1)}): {detail}")
    return result


def _validated_backend_volume_map(volume_names: Mapping[str, str]) -> dict[str, str]:
    if set(volume_names) != set(BACKEND_VOLUME_KEYS):
        missing = sorted(set(BACKEND_VOLUME_KEYS) - set(volume_names))
        extra = sorted(set(volume_names) - set(BACKEND_VOLUME_KEYS))
        raise ValueError(f"backend volume map must contain exactly the Compose volumes; missing={missing}, extra={extra}")
    result: dict[str, str] = {}
    for logical, actual in volume_names.items():
        if not isinstance(actual, str) or not _DOCKER_VOLUME_NAME.fullmatch(actual):
            raise ValueError(f"unsafe Docker volume name for {logical}: {actual!r}")
        result[logical] = actual
    return result


def resolve_backend_volumes(
    compose_file: str | Path,
    env_file: str | Path,
    *,
    project_name: str | None = None,
    timeout: float = 30.0,
    runner: Callable[..., Any] | None = None,
) -> dict[str, str]:
    """Resolve logical Compose volume keys to the actual Docker volume names."""

    compose_path = Path(compose_file).expanduser().resolve(strict=True)
    env_path = Path(env_file).expanduser().resolve(strict=True)
    command = ["docker", "compose"]
    if project_name:
        if not _DOCKER_VOLUME_NAME.fullmatch(project_name):
            raise ValueError(f"unsafe Compose project name: {project_name!r}")
        command.extend(["--project-name", project_name])
    command.extend(["--env-file", str(env_path), "-f", str(compose_path), "config", "--format", "json"])
    result = _docker_result(command, timeout=timeout, runner=runner)
    try:
        rendered = json.loads((getattr(result, "stdout", "") or "").strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError("docker compose config did not return JSON") from exc
    if not isinstance(rendered, dict) or not isinstance(rendered.get("volumes"), dict):
        raise RuntimeError("docker compose config did not expose a volume map")
    configured = rendered["volumes"]
    default_project = rendered.get("name") or project_name
    resolved: dict[str, str] = {}
    for logical in BACKEND_VOLUME_KEYS:
        entry = configured.get(logical)
        actual = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(actual, str) or not actual.strip():
            if not isinstance(default_project, str) or not default_project.strip():
                raise RuntimeError(f"Compose did not resolve a Docker name for volume {logical}")
            actual = f"{default_project}_{logical}"
        resolved[logical] = actual
    return _validated_backend_volume_map(resolved)


def _docker_size_bytes(value: Any) -> int:
    """Convert Docker's human-readable disk-usage value into bytes."""

    if isinstance(value, bool):
        raise ValueError(f"invalid Docker size: {value!r}")
    if isinstance(value, (int, float)):
        if value < 0:
            raise ValueError(f"invalid Docker size: {value!r}")
        return int(value)
    if not isinstance(value, str):
        raise ValueError(f"invalid Docker size: {value!r}")
    match = _DOCKER_SIZE.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid Docker size: {value!r}")
    unit = (match.group("unit") or "B").casefold()
    multipliers = {
        "b": 1,
        "kb": 1_000,
        "mb": 1_000_000,
        "gb": 1_000_000_000,
        "tb": 1_000_000_000_000,
        "kib": 1_024,
        "mib": 1_048_576,
        "gib": 1_073_741_824,
        "tib": 1_099_511_627_776,
    }
    return int(float(match.group("value")) * multipliers[unit])


def inspect_backend_volume_capacity(
    volume_names: Mapping[str, str],
    *,
    max_bytes: int,
    timeout: float = 30.0,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Inspect only the resolved Observatory volumes against a soft total budget.

    Docker reports usage for every local volume. The exact resolved names are
    used here so unrelated projects and disposable test volumes cannot affect
    the result. This is an operator check, not a Docker quota: retention and
    the Collector queue cap remain the enforcement mechanisms.
    """

    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    validated = _validated_backend_volume_map(volume_names)
    result = _docker_result(
        ["docker", "system", "df", "-v", "--format", "{{json .}}"],
        timeout=timeout,
        runner=runner,
    )
    try:
        report = json.loads((getattr(result, "stdout", "") or "").strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError("docker system df did not return JSON") from exc
    if not isinstance(report, dict) or not isinstance(report.get("Volumes"), list):
        raise RuntimeError("docker system df did not expose verbose volume usage")
    by_name = {
        item.get("Name"): item
        for item in report["Volumes"]
        if isinstance(item, dict) and isinstance(item.get("Name"), str)
    }
    volumes: list[dict[str, Any]] = []
    for logical in BACKEND_VOLUME_KEYS:
        name = validated[logical]
        item = by_name.get(name)
        if item is None:
            volumes.append({"logical": logical, "name": name, "present": False, "bytes": 0, "reported_size": None})
            continue
        reported_size = item.get("Size", "0B")
        try:
            size_bytes = _docker_size_bytes(reported_size)
        except ValueError as exc:
            raise RuntimeError(f"Docker returned an invalid size for volume {name}: {reported_size!r}") from exc
        volumes.append({
            "logical": logical,
            "name": name,
            "present": True,
            "bytes": size_bytes,
            "reported_size": reported_size,
            "links": item.get("Links"),
        })
    total_bytes = sum(int(item["bytes"]) for item in volumes)
    missing = [item["logical"] for item in volumes if not item["present"]]
    ratio = total_bytes / max_bytes
    status = "fail" if total_bytes > max_bytes else "warn" if missing or ratio >= 0.9 else "pass"
    return {
        "status": status,
        "budget_enforced_by": "start_guard_retention_and_queue_caps",
        "max_bytes": max_bytes,
        "bytes": total_bytes,
        "ratio": ratio,
        "missing": missing,
        "volumes": volumes,
    }


def _volume_member_is_safe(name: str) -> bool:
    normalized = name.replace("\\", "/")
    if not normalized or normalized.startswith("/") or ":" in normalized.split("/", 1)[0]:
        return False
    if normalized.rstrip("/") == ".":
        return True
    parts = [part for part in normalized.split("/") if part not in {"", "."}]
    return bool(parts) and ".." not in parts


def _validate_volume_archive(path: Path) -> None:
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            for member in archive.getmembers():
                if not _volume_member_is_safe(member.name):
                    raise ValueError(f"unsafe member in backend volume archive: {member.name!r}")
                if member.issym() or member.islnk() or member.isdev():
                    raise ValueError(f"links and device nodes are not allowed in backend volume archive: {member.name!r}")
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"invalid backend volume archive: {path}") from exc


def _docker_volume_exists(
    volume_name: str,
    *,
    timeout: float,
    runner: Callable[..., Any] | None = None,
) -> bool:
    execute = runner or subprocess.run
    try:
        result = execute(
            ["docker", "volume", "inspect", volume_name],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not inspect Docker volume {volume_name}: {exc}") from exc
    if getattr(result, "returncode", 1) == 0:
        return True
    detail = (getattr(result, "stderr", "") or "").strip()
    if "no such volume" in detail.casefold() or "not found" in detail.casefold():
        return False
    raise RuntimeError(f"could not inspect Docker volume {volume_name}: {detail or 'volume inspect failed'}")


def _export_backend_volume(
    logical: str,
    volume_name: str,
    destination: Path,
    *,
    docker_image: str,
    timeout: float,
    runner: Callable[..., Any] | None = None,
) -> None:
    if not _docker_volume_exists(volume_name, timeout=timeout, runner=runner):
        raise FileNotFoundError(f"Docker volume does not exist: {volume_name}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent.resolve().as_posix()
    command = [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--mount",
        f"type=volume,source={volume_name},target=/source,readonly",
        "--mount",
        f"type=bind,source={staging},target=/backup",
        docker_image,
        "sh",
        "-c",
        f"tar -czf /backup/{logical}.tar.gz -C /source .",
    ]
    _docker_result(command, timeout=timeout, runner=runner)
    if not destination.exists():
        raise RuntimeError(f"Docker did not create the backend volume archive for {logical}")
    _validate_volume_archive(destination)


def _restore_backend_volume(
    logical: str,
    volume_name: str,
    source: Path,
    *,
    overwrite: bool,
    docker_image: str,
    timeout: float,
    runner: Callable[..., Any] | None = None,
) -> None:
    exists = _docker_volume_exists(volume_name, timeout=timeout, runner=runner)
    if exists and not overwrite:
        raise FileExistsError(f"Docker volume exists; pass overwrite explicitly: {volume_name}")
    if not exists:
        _docker_result(["docker", "volume", "create", volume_name], timeout=timeout, runner=runner)
    staging = source.parent.resolve().as_posix()
    command = [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--mount",
        f"type=volume,source={volume_name},target=/target",
        "--mount",
        f"type=bind,source={staging},target=/backup,readonly",
        docker_image,
        "sh",
        "-c",
        f"for item in /target/* /target/.[!.]* /target/..?*; do if [ -e \"$item\" ] || [ -L \"$item\" ]; then rm -rf \"$item\"; fi; done; tar -xzf /backup/{logical}.tar.gz -C /target",
    ]
    _docker_result(command, timeout=timeout, runner=runner)


def _remove_backend_volume(
    volume_name: str,
    *,
    timeout: float,
    runner: Callable[..., Any] | None = None,
) -> None:
    """Remove a volume created by a failed restore transaction."""

    if _docker_volume_exists(volume_name, timeout=timeout, runner=runner):
        _docker_result(["docker", "volume", "rm", volume_name], timeout=timeout, runner=runner)


def _backend_volume_manifest(
    backend_volumes: Mapping[str, str],
    staged_files: Mapping[str, Path],
) -> dict[str, dict[str, Any]]:
    return {
        logical: {
            "docker_volume": backend_volumes[logical],
            "archive_member": f"{_BACKEND_ARCHIVE_ROOT}/{logical}.tar.gz",
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for logical, path in sorted(staged_files.items())
    }


def backup_state(
    source_root: str | Path,
    target: str | Path,
    *,
    include_secret: bool = False,
    overwrite: bool = False,
    backend_volumes: Mapping[str, str] | None = None,
    docker_image: str = "alpine:3.22.1",
    docker_timeout: float = 300.0,
    docker_runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Create a portable, checksum-verified archive of Observatory state.

    Host state is always included.  Docker backend volumes are included only
    when their exact resolved names are supplied explicitly; this keeps the
    ordinary host-state backup non-disruptive while allowing a stopped-stack
    disaster-recovery bundle to cover the complete Compose persistence set.
    """

    root = Path(source_root).expanduser().resolve(strict=True)
    database = root / "data" / "events.sqlite3"
    config = root / "config.json"
    if not database.exists() or not config.exists():
        raise FileNotFoundError(f"installed Observatory state is incomplete under {root}")
    target_path = Path(target).expanduser().resolve(strict=False)
    try:
        target_path.relative_to(root)
    except ValueError:
        pass
    else:
        raise ValueError("full-state backup target must be outside the state directory")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"backup target exists; pass overwrite explicitly: {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    validated_backend_volumes = _validated_backend_volume_map(backend_volumes) if backend_volumes is not None else None

    with tempfile.TemporaryDirectory(prefix=".observatory-state-backup-", dir=str(target_path.parent)) as temporary_dir:
        temporary_root = Path(temporary_dir)
        snapshot = temporary_root / "events.sqlite3"
        database_result = backup_database(database, snapshot)
        files: list[tuple[Path, str]] = [(snapshot, "data/events.sqlite3"), (config, "config.json")]
        compose_env = root / "compose.env"
        if compose_env.exists():
            files.append((compose_env, "compose.env"))
        for spool_file in sorted((root / "spool").glob("*.jsonl")) if (root / "spool").exists() else ():
            if spool_file.is_file():
                files.append((spool_file, f"spool/{spool_file.name}"))
        managed_secrets_included = False
        if include_secret:
            for member in _SECRET_FILES:
                secret = root / member
                if secret.exists():
                    files.append((secret, member))
                    managed_secrets_included = True

        staged_backend: dict[str, Path] = {}
        if validated_backend_volumes is not None:
            for logical, volume_name in validated_backend_volumes.items():
                destination = temporary_root / f"{logical}.tar.gz"
                _export_backend_volume(
                    logical,
                    volume_name,
                    destination,
                    docker_image=docker_image,
                    timeout=docker_timeout,
                    runner=docker_runner,
                )
                staged_backend[logical] = destination

        manifest_files: dict[str, dict[str, Any]] = {}
        for source, member in files:
            manifest_files[member] = {"bytes": source.stat().st_size, "sha256": _sha256(source)}
        backend_manifest = _backend_volume_manifest(validated_backend_volumes, staged_backend) if validated_backend_volumes is not None else {}
        manifest = {
            "schema": _STATE_BACKUP_SCHEMA,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "scope": "host_state_and_backend_volumes" if validated_backend_volumes is not None else "host_state_only",
            "docker_named_volumes": "included" if validated_backend_volumes is not None else "excluded",
            "secret_included": managed_secrets_included,
            "encryption": "operator-managed",
            "files": manifest_files,
            "backend_volumes": backend_manifest,
        }
        temporary_archive = target_path.with_name(f".{target_path.name}.backup-{uuid.uuid4().hex}.tmp")
        try:
            with zipfile.ZipFile(temporary_archive, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
                for source, member in files:
                    archive.write(source, arcname=member)
                for logical, source in sorted(staged_backend.items()):
                    archive.write(source, arcname=f"{_BACKEND_ARCHIVE_ROOT}/{logical}.tar.gz")
                archive.writestr("manifest.json", json.dumps(manifest, sort_keys=True, separators=(",", ":")))
            os.replace(temporary_archive, target_path)
        finally:
            temporary_archive.unlink(missing_ok=True)
    return {
        "schema": _STATE_BACKUP_SCHEMA,
        "target": str(target_path),
        "bytes": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        "database": database_result,
        "files": sorted(manifest_files),
        "secret_included": bool(manifest["secret_included"]),
        "docker_named_volumes": manifest["docker_named_volumes"],
        "backend_volumes": sorted(backend_manifest),
        "encryption": "operator-managed",
    }


def restore_state(
    source: str | Path,
    target_root: str | Path,
    *,
    overwrite: bool = False,
    restore_secret: bool = False,
    backend_volumes: Mapping[str, str] | None = None,
    docker_image: str = "alpine:3.22.1",
    docker_timeout: float = 300.0,
    docker_runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Validate and restore a host-state archive into an installed state root.

    A volume-bearing archive must be restored with an explicit resolved
    backend-volume map.  The target stack must be stopped by the caller so
    the Docker volumes are not concurrently mutated.
    """

    source_path = Path(source).expanduser().resolve(strict=True)
    root = Path(target_root).expanduser().resolve(strict=True)
    if source_path.stat().st_size > _MAX_STATE_ARCHIVE_BYTES:
        raise ValueError(f"state backup archive exceeds the {_MAX_STATE_ARCHIVE_BYTES}-byte limit")
    try:
        archive = zipfile.ZipFile(source_path, mode="r")
    except zipfile.BadZipFile as exc:
        raise ValueError(f"invalid state backup archive: {source_path}") from exc
    with archive:
        archive_infos = archive.infolist()
        if len(archive_infos) > _MAX_STATE_ARCHIVE_MEMBERS:
            raise ValueError(f"state backup contains more than {_MAX_STATE_ARCHIVE_MEMBERS} archive members")
        if any(info.file_size < 0 or info.file_size > _MAX_STATE_MEMBER_BYTES for info in archive_infos):
            raise ValueError(f"state backup contains a member larger than the {_MAX_STATE_MEMBER_BYTES}-byte limit")
        if sum(info.file_size for info in archive_infos) > _MAX_STATE_UNCOMPRESSED_BYTES:
            raise ValueError(f"state backup expands beyond the {_MAX_STATE_UNCOMPRESSED_BYTES}-byte limit")
        try:
            manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("state backup manifest is missing or invalid") from exc
        if not isinstance(manifest, dict) or manifest.get("schema") != _STATE_BACKUP_SCHEMA:
            raise ValueError("unsupported state backup schema")
        files = manifest.get("files")
        if not isinstance(files, dict) or "data/events.sqlite3" not in files or "config.json" not in files:
            raise ValueError("state backup is missing required host-state files")
        backend_manifest = manifest.get("backend_volumes", {})
        if not isinstance(backend_manifest, dict):
            raise ValueError("state backup backend volume manifest is malformed")
        if backend_manifest and backend_volumes is None:
            raise ValueError("state backup contains Docker named volumes; pass --backend-volumes to restore them")
        if backend_manifest and not manifest.get("secret_included"):
            raise ValueError("backend-volume restore requires the Grafana secret; create the backup with --include-secret and encrypt it for storage")
        if manifest.get("secret_included") and not restore_secret:
            raise ValueError("this backup includes managed secrets; pass --restore-secret explicitly")
        validated_backend_volumes = _validated_backend_volume_map(backend_volumes) if backend_volumes is not None else None
        if validated_backend_volumes is not None and set(backend_manifest) != set(validated_backend_volumes):
            raise ValueError("restore backend volume map does not match the archive volume set")
        names = set(archive.namelist())
        expected_names = {"manifest.json", *files}
        expected_names.update(
            metadata.get("archive_member")
            for metadata in backend_manifest.values()
            if isinstance(metadata, dict) and isinstance(metadata.get("archive_member"), str)
        )
        unexpected_names = sorted(names - expected_names)
        if unexpected_names:
            raise ValueError(f"state backup contains unexpected archive members: {unexpected_names}")
        staged: Path
        with tempfile.TemporaryDirectory(prefix=".observatory-state-restore-", dir=str(root.parent)) as temporary_dir:
            staged = Path(temporary_dir)
            expected_uncompressed = 0
            for member, metadata in files.items():
                if not isinstance(member, str) or not _archive_member_is_safe(member) or member == "manifest.json":
                    raise ValueError(f"unsafe state backup member: {member!r}")
                if member not in names or not isinstance(metadata, dict):
                    raise ValueError(f"state backup member is missing or malformed: {member}")
                declared_bytes = metadata.get("bytes")
                if not isinstance(declared_bytes, int) or declared_bytes < 0 or declared_bytes > _MAX_STATE_MEMBER_BYTES:
                    raise ValueError(f"state backup member byte declaration is invalid: {member}")
                expected_uncompressed += declared_bytes
                destination = staged / member
                destination.parent.mkdir(parents=True, exist_ok=True)
                copied = _copy_zip_member_bounded(archive, member, destination, maximum=_MAX_STATE_MEMBER_BYTES)
                if copied != declared_bytes or _sha256(destination) != metadata.get("sha256"):
                    raise ValueError(f"state backup checksum mismatch: {member}")
            if expected_uncompressed > _MAX_STATE_UNCOMPRESSED_BYTES:
                raise ValueError(f"state backup expands beyond the {_MAX_STATE_UNCOMPRESSED_BYTES}-byte limit")
            database = staged / "data/events.sqlite3"
            if _integrity(database) != "ok":
                raise ValueError("state backup database failed integrity check")
            staged_backend: dict[str, Path] = {}
            for logical, metadata in sorted(backend_manifest.items()):
                if not isinstance(logical, str) or logical not in BACKEND_VOLUME_KEYS or not isinstance(metadata, dict):
                    raise ValueError(f"state backup backend volume entry is malformed: {logical!r}")
                member = metadata.get("archive_member")
                if not isinstance(member, str) or not _archive_member_is_safe(member) or not member.startswith(f"{_BACKEND_ARCHIVE_ROOT}/"):
                    raise ValueError(f"unsafe backend volume archive member: {member!r}")
                if member not in names:
                    raise ValueError(f"state backup backend volume member is missing: {member}")
                declared_bytes = metadata.get("bytes")
                if not isinstance(declared_bytes, int) or declared_bytes < 0 or declared_bytes > _MAX_STATE_MEMBER_BYTES:
                    raise ValueError(f"state backup backend volume byte declaration is invalid: {logical}")
                expected_uncompressed += declared_bytes
                if expected_uncompressed > _MAX_STATE_UNCOMPRESSED_BYTES:
                    raise ValueError(f"state backup expands beyond the {_MAX_STATE_UNCOMPRESSED_BYTES}-byte limit")
                destination = staged / member
                destination.parent.mkdir(parents=True, exist_ok=True)
                copied = _copy_zip_member_bounded(archive, member, destination, maximum=_MAX_STATE_MEMBER_BYTES)
                if copied != declared_bytes or _sha256(destination) != metadata.get("sha256"):
                    raise ValueError(f"state backup checksum mismatch: {member}")
                _validate_volume_archive(destination)
                staged_backend[logical] = destination
            existing_database = root / "data" / "events.sqlite3"
            if existing_database.exists() and not overwrite:
                raise FileExistsError(f"restore target exists; pass overwrite explicitly: {existing_database}")
            existing_files: dict[str, Path] = {}
            missing_files: set[str] = set()
            for member in files:
                if member.startswith("secrets/") and not restore_secret:
                    continue
                raw_target = root / member
                target = raw_target.resolve(strict=False)
                if not target.is_relative_to(root):
                    raise ValueError(f"restore target escapes the installed state directory: {member}")
                if raw_target.exists() or raw_target.is_symlink():
                    if raw_target.is_symlink() or not raw_target.is_file():
                        raise ValueError(f"restore target is not a regular file: {raw_target}")
                    previous = staged / "pre-restore-files" / member
                    previous.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(raw_target, previous)
                    existing_files[member] = previous
                else:
                    missing_files.add(member)
            initial_volume_exists: dict[str, bool] = {}
            if validated_backend_volumes is not None:
                for logical, volume_name in validated_backend_volumes.items():
                    exists = _docker_volume_exists(volume_name, timeout=docker_timeout, runner=docker_runner)
                    initial_volume_exists[logical] = exists
                    if exists and not overwrite:
                        raise FileExistsError(f"Docker volume exists; pass overwrite explicitly: {volume_name}")
                    if exists:
                        previous = staged / "pre-restore-backend" / f"{logical}.tar.gz"
                        _export_backend_volume(
                            logical,
                            volume_name,
                            previous,
                            docker_image=docker_image,
                            timeout=docker_timeout,
                            runner=docker_runner,
                        )
            restored: list[str] = []
            skipped: list[str] = []
            restored_backend: list[str] = []
            try:
                for member in sorted(files):
                    if member.startswith("secrets/") and not restore_secret:
                        skipped.append(member)
                        continue
                    _atomic_copy(staged / member, root / member)
                    restored.append(member)
                if validated_backend_volumes is not None:
                    for logical, volume_name in validated_backend_volumes.items():
                        _restore_backend_volume(
                            logical,
                            volume_name,
                            staged_backend[logical],
                            overwrite=overwrite,
                            docker_image=docker_image,
                            timeout=docker_timeout,
                            runner=docker_runner,
                        )
                        restored_backend.append(logical)
            except Exception as exc:
                rollback_errors: list[str] = []
                for member, previous in existing_files.items():
                    try:
                        _atomic_copy(previous, root / member)
                    except Exception as rollback_exc:  # pragma: no cover - defensive failure reporting
                        rollback_errors.append(f"host file {member}: {rollback_exc}")
                for member in missing_files:
                    target = root / member
                    try:
                        if target.is_file() or target.is_symlink():
                            target.unlink()
                    except Exception as rollback_exc:  # pragma: no cover - defensive failure reporting
                        rollback_errors.append(f"new host file {member}: {rollback_exc}")
                if validated_backend_volumes is not None:
                    for logical, volume_name in validated_backend_volumes.items():
                        try:
                            if initial_volume_exists.get(logical):
                                previous = staged / "pre-restore-backend" / f"{logical}.tar.gz"
                                _restore_backend_volume(
                                    logical,
                                    volume_name,
                                    previous,
                                    overwrite=True,
                                    docker_image=docker_image,
                                    timeout=docker_timeout,
                                    runner=docker_runner,
                                )
                            else:
                                _remove_backend_volume(volume_name, timeout=docker_timeout, runner=docker_runner)
                        except Exception as rollback_exc:  # pragma: no cover - defensive failure reporting
                            rollback_errors.append(f"backend volume {logical}: {rollback_exc}")
                if rollback_errors:
                    raise RuntimeError(f"state restore failed and rollback was incomplete: {rollback_errors}") from exc
                raise RuntimeError(f"state restore rolled back after failure: {exc}") from exc
    return {
        "schema": _STATE_BACKUP_SCHEMA,
        "source": str(source_path),
        "target_root": str(root),
        "restored": restored,
        "skipped": skipped,
        "secret_restored": any(member in restored for member in _SECRET_FILES),
        "restored_backend_volumes": restored_backend,
        "docker_named_volumes": "included" if validated_backend_volumes is not None else "excluded",
    }


def purge_events(
    store: EventStore,
    *,
    before: str | None = None,
    event_ids: Iterable[str] = (),
    confirm: bool = False,
    compact: bool | None = None,
) -> dict[str, Any]:
    """Physically delete selected telemetry with an append-only audit record.

    ``compact`` controls the post-delete rewrite: True always compacts (an
    operator running `prune` expects the space back), False never does, and the
    default defers to a size threshold so a scheduled sweep does not rewrite a
    multi-GB store to reclaim a handful of rows.

    The application append path cannot invoke this function.  Callers must
    explicitly confirm, and the immutable evidence triggers are removed only
    inside the same SQLite transaction as the deletion and immediately
    recreated before commit.
    """

    if not confirm:
        raise ValueError("purge requires explicit confirm=True")
    ids = [value.strip() for value in event_ids if isinstance(value, str) and value.strip()]
    clauses: list[str] = []
    params: list[str] = []
    if before is not None:
        try:
            before = ensure_utc(before, "before").isoformat()
        except ContractError as exc:
            raise ValueError(str(exc)) from exc
        clauses.append("observed_at < ?")
        params.append(before)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        clauses.append(f"event_id IN ({placeholders})")
        params.extend(ids)
    if not clauses:
        raise ValueError("purge requires before or at least one event id")
    # The id list must be chunked here too: `prune --event-id` with more ids
    # than SQLite's parameter ceiling raised OperationalError, which is not one
    # of the exception types the callers catch, so it surfaced as a traceback.
    selected: list[str] = []
    if before is not None:
        rows = store.connection.execute(
            "SELECT event_id FROM events WHERE observed_at < ? ORDER BY event_id", (before,)
        ).fetchall()
        selected.extend(str(row["event_id"]) for row in rows)
    for offset in range(0, len(ids), PURGE_CHUNK_SIZE):
        chunk = ids[offset : offset + PURGE_CHUNK_SIZE]
        placeholders = ",".join("?" for _ in chunk)
        rows = store.connection.execute(
            f"SELECT event_id FROM events WHERE event_id IN ({placeholders})", chunk
        ).fetchall()
        selected.extend(str(row["event_id"]) for row in rows)
    selected = sorted(set(selected))
    action_id = f"maintenance:{uuid.uuid4()}"
    requested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    selector = json.dumps({"before": before, "event_ids": ids}, sort_keys=True)
    try:
        with store.connection:
            store.connection.execute(
                "INSERT INTO maintenance_actions(action_id, action_type, selector_json, requested_at, outcome, affected_events, detail) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (action_id, "purge", selector, requested_at, "started", len(selected), None),
            )
            for trigger in _EVIDENCE_TRIGGERS:
                store.connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            # SQLite binds at most SQLITE_LIMIT_VARIABLE_NUMBER (32766)
            # parameters per statement, and the edge delete uses two per event.
            # An unchunked delete therefore raised OperationalError above ~16k
            # events -- which sqlite3 raises outside the OSError/RuntimeError/
            # ValueError the callers catch, so a size-driven retention run ended
            # in a raw traceback rather than a result.
            for offset in range(0, len(selected), PURGE_CHUNK_SIZE):
                chunk = selected[offset : offset + PURGE_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk)
                store.connection.execute(
                    f"DELETE FROM attribution_edges WHERE child_event_id IN ({placeholders}) OR parent_event_id IN ({placeholders})",
                    (*chunk, *chunk),
                )
                for table, column in (
                    ("measurement_facts", "event_id"),
                    ("outcome_events", "event_id"),
                    ("ingest_ledger", "event_id"),
                    ("event_conflicts", "event_id"),
                    ("events", "event_id"),
                ):
                    store.connection.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", chunk)
            _restore_evidence_triggers(store.connection)
            completed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            store.connection.execute(
                "INSERT INTO maintenance_actions(action_id, action_type, selector_json, requested_at, completed_at, outcome, affected_events, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (f"{action_id}:completed", "purge", selector, requested_at, completed_at, "completed", len(selected), "explicit operator retention/deletion"),
            )
    except Exception:
        # DDL is transactional on SQLite, but restore the guards defensively
        # before handing the error to the caller.
        _restore_evidence_triggers(store.connection)
        raise
    compaction: dict[str, Any] = {"status": "not_needed"}
    # Compaction rewrites the whole database, so it must not run for a handful
    # of rows: on a multi-GB store that is minutes of writer lock in exchange
    # for nothing, and intake -- which uses a short busy timeout so it never
    # blocks a client -- reports unavailable throughout.
    if selected and (compact is False or (compact is None and len(selected) < MIN_COMPACTION_EVENTS)):
        # A DELETE without a checkpoint leaves the freed pages in the WAL, and
        # `storage_bytes()` counts the WAL -- so a successful purge could report
        # the store as LARGER than before, keep intake over its budget, and make
        # retention recompute the same excess forever. Checkpoint even when the
        # full rewrite is deliberately skipped.
        reason = "compaction disabled by caller" if compact is False else (
            f"{len(selected)} events is below the compaction threshold"
        )
        try:
            # The PRAGMA returns (busy, log_pages, checkpointed); busy=1 means it
            # truncated nothing, and it does not raise. Reporting a hardcoded
            # True made a failed checkpoint indistinguishable from a real one --
            # and a reader on the dashboard plane is the documented steady state.
            row = store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            busy = int(row[0]) if row is not None else 1
            compaction = {"status": "skipped", "reason": reason, "checkpointed": busy == 0}
            if busy:
                compaction["checkpoint_blocked_by_reader"] = True
        except sqlite3.Error as exc:
            compaction = {"status": "skipped", "reason": reason, "checkpointed": False, "error": str(exc)}
    elif selected:
        try:
            checkpoint = store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint is not None and int(checkpoint[0]) != 0:
                compaction = {"status": "deferred", "reason": "SQLite WAL checkpoint is busy"}
            else:
                store.connection.execute("VACUUM")
                # VACUUM rebuilds the database through the WAL, so the sidecar
                # transiently holds a copy of the whole store. `storage_bytes()`
                # counts the WAL, so without a second checkpoint the measured
                # size roughly doubles immediately after a purge -- and near the
                # budget that makes the capacity guard reject intake, an outage
                # caused by the very operation meant to prevent one.
                store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                compaction = {"status": "completed"}
        except sqlite3.Error as exc:
            compaction = {"status": "deferred", "reason": str(exc)}
    return {
        "action_id": action_id,
        "outcome": "completed",
        "affected_events": len(selected),
        "selector": json.loads(selector),
        "compaction": compaction,
    }


DEFAULT_RETENTION_TARGET_RATIO = 0.8
# Below this, compacting costs more than the space it reclaims.
MIN_COMPACTION_EVENTS = 5000
# Half the SQLite parameter ceiling, because the edge delete binds two per event.
PURGE_CHUNK_SIZE = 8000


def _measure_store_bytes(store: EventStore) -> dict[str, Any]:
    """Measure the store the way a deletion decision has to see it.

    ``storage_bytes()`` counts the ``-wal`` sidecar, and ``store.py`` documents
    that a passive autocheckpoint cannot reset the WAL while the dashboard read
    plane holds readers -- so the inflated reading is the steady state, not an
    edge case. Sizing a purge from it deletes telemetry that never needed
    deleting, unattended, every 15 minutes.

    Two things happen here. The WAL is folded back into the database first, so
    the space an earlier purge freed is actually returned before anything else
    is considered for deletion; and the PRAGMA's real return value is read
    rather than assumed, because a TRUNCATE checkpoint legitimately fails while
    a reader is open. The size policy then decides on the page image
    (``page_count * page_size``) -- the durable volume, and the only part of the
    footprint that deleting rows can move -- so a sidecar that could not be
    checkpointed can never push retention into destroying telemetry.
    """

    checkpointed = False
    try:
        # (busy, log_pages, checkpointed_pages); busy=1 means the log was not
        # reset, and the PRAGMA reports that rather than raising.
        row = store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        checkpointed = row is not None and int(row[0]) == 0
    except sqlite3.Error:
        checkpointed = False
    page_count = int(store.connection.execute("PRAGMA page_count").fetchone()[0])
    page_size = int(store.connection.execute("PRAGMA page_size").fetchone()[0])
    return {
        "durable_bytes": page_count * page_size,
        "on_disk_bytes": store.storage_bytes(),
        "checkpointed": checkpointed,
    }


def _sidecar_blocker(measurement: Mapping[str, Any], max_bytes: int) -> str | None:
    """Report a footprint only a checkpoint -- never a delete -- can bring down."""

    if measurement["checkpointed"] or int(measurement["on_disk_bytes"]) <= max_bytes:
        return None
    return (
        f"the store holds {measurement['durable_bytes']} bytes of data but occupies "
        f"{measurement['on_disk_bytes']} bytes on disk against a {max_bytes}-byte budget: the WAL "
        "sidecar could not be checkpointed because a reader is open, and deleting events cannot "
        "reclaim it. Let the read plane drain and re-run."
    )


def _retention_report(
    *,
    outcome: str,
    blocker: str | None,
    deleted_events: int,
    bytes_before: int,
    bytes_after: int,
    max_bytes: int,
    target_bytes: int,
    cutoff: str | None,
    reasons: list[str],
    rows_remaining: int,
    action_id: str | None,
    compaction: Any,
    measurement: Mapping[str, Any],
) -> dict[str, Any]:
    """One shape for every exit, so a caller can index the report blind."""

    return {
        "outcome": outcome,
        "blocker": blocker,
        "deleted_events": deleted_events,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "max_bytes": max_bytes,
        "target_bytes": target_bytes,
        "ratio_before": bytes_before / max_bytes,
        "ratio_after": bytes_after / max_bytes,
        "cutoff": cutoff,
        "reasons": reasons,
        "rows_remaining": rows_remaining,
        "action_id": action_id,
        "compaction": compaction,
        "measurement": dict(measurement),
    }


def enforce_normalized_retention(
    store: EventStore,
    *,
    max_bytes: int,
    target_ratio: float = DEFAULT_RETENTION_TARGET_RATIO,
    max_age_days: int | None = None,
) -> dict[str, Any]:
    """Bound the normalized store by age and size through the audited purge path.

    Retention is deliberately a maintenance operation rather than an append-path
    side effect: intake must never delete telemetry in order to accept more of
    it.  The caller selects the policy; this function only decides which
    ``observed_at`` cutoff satisfies it and delegates the deletion, WAL
    checkpoint, and ``maintenance_actions`` audit record to :func:`purge_events`.

    Returns a report even when nothing needs deleting so operators and health
    checks can distinguish "policy satisfied" from "policy never ran".
    """

    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    if not isinstance(target_ratio, (int, float)) or isinstance(target_ratio, bool):
        raise ValueError("target_ratio must be a number")
    if not 0 < float(target_ratio) <= 1:
        raise ValueError("target_ratio must be greater than 0 and at most 1")
    if max_age_days is not None:
        if not isinstance(max_age_days, int) or isinstance(max_age_days, bool) or max_age_days < 1:
            raise ValueError("max_age_days must be a positive integer")

    # Never `store.storage_bytes()` directly: it counts the WAL sidecar, and a
    # purge sized from an uncheckpointed reading deletes telemetry no policy
    # asked to delete.
    measurement = _measure_store_bytes(store)
    started_bytes = int(measurement["durable_bytes"])
    target_bytes = int(max_bytes * float(target_ratio))
    total_rows = int(store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    reasons: list[str] = []
    cutoffs: list[str] = []

    if max_age_days is not None:
        age_cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
        expired = int(
            store.connection.execute(
                "SELECT COUNT(*) FROM events WHERE observed_at < ?", (age_cutoff,)
            ).fetchone()[0]
        )
        if expired:
            cutoffs.append(age_cutoff)
            reasons.append(f"age:{max_age_days}d")

    size_cutoff: str | None = None
    if started_bytes > target_bytes and total_rows > 0:
        excess = started_bytes - target_bytes
        bytes_per_row = started_bytes / total_rows
        rows_to_delete = min(total_rows, max(1, math.ceil(excess / bytes_per_row)))
        row = store.connection.execute(
            "SELECT observed_at FROM events ORDER BY observed_at LIMIT 1 OFFSET ?",
            (rows_to_delete,),
        ).fetchone()
        # Falling off the end means the policy asks for the whole table; use the
        # newest timestamp so at least the historical rows go and the store is
        # never emptied without the operator seeing `rows_remaining: 0`.
        size_cutoff = str(row[0]) if row is not None else str(
            store.connection.execute("SELECT MAX(observed_at) FROM events").fetchone()[0]
        )
        cutoffs.append(size_cutoff)
        reasons.append(f"size:{started_bytes}>{target_bytes}")

    if not cutoffs:
        return _retention_report(
            outcome="satisfied",
            blocker=_sidecar_blocker(measurement, max_bytes),
            deleted_events=0,
            bytes_before=started_bytes,
            bytes_after=started_bytes,
            max_bytes=max_bytes,
            target_bytes=target_bytes,
            cutoff=None,
            reasons=[],
            rows_remaining=total_rows,
            action_id=None,
            compaction={"status": "not_needed"},
            measurement=measurement,
        )

    # Whichever policy demands the most deletion wins; both are upper bounds on
    # what may remain, so the later cutoff satisfies each of them.
    cutoff = max(cutoffs)
    purged = purge_events(store, before=cutoff, confirm=True)
    ended = _measure_store_bytes(store)
    ended_bytes = int(ended["durable_bytes"])
    remaining = int(store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    deleted = int(purged.get("affected_events", 0) or 0)
    # Rows deleted is not the question the operator asked. The policy is a size
    # budget, so the only honest test of "enforced" is whether the store now
    # fits inside it: deriving success from the delete count reported a store
    # 20x over budget as enforced while intake stayed refused.
    if not deleted:
        # `before` is strict and the size cutoff falls back to MAX(observed_at),
        # so a store whose candidate events share one timestamp matches nothing.
        outcome = "ineffective"
        blocker = (
            "retention selected a cutoff but deleted nothing: every candidate event shares "
            "that timestamp, so this policy cannot free space. Widen the age policy or prune "
            "explicitly by event id."
        )
    elif ended_bytes > target_bytes:
        outcome = "ineffective"
        blocker = (
            f"retention deleted {deleted} events and the store is still {ended_bytes} bytes "
            f"against a {target_bytes}-byte target ({ended_bytes / target_bytes:.1f}x over). "
            "Deleting rows frees pages inside the database file without returning them to the "
            "file system, so the budget is met only once a compaction rewrites the store: run "
            "`observatory purge --before <cutoff> --confirm`, or raise the budget."
        )
    else:
        outcome = "enforced"
        blocker = _sidecar_blocker(ended, max_bytes)
    return _retention_report(
        outcome=outcome,
        blocker=blocker,
        deleted_events=deleted,
        bytes_before=started_bytes,
        bytes_after=ended_bytes,
        max_bytes=max_bytes,
        target_bytes=target_bytes,
        cutoff=cutoff,
        reasons=reasons,
        rows_remaining=remaining,
        action_id=purged.get("action_id"),
        compaction=purged.get("compaction"),
        measurement=ended,
    )
