"""Clock helpers kept in one module so tests can use deterministic time."""

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(timezone.utc)

