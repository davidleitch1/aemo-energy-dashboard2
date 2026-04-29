"""DuckDB connection helper for the mobile API.

Opens a fresh read-only connection per request. DuckDB's MVCC isolates
each connection at a snapshot from the time it was opened, so a long-lived
cached connection wouldn't see new writes from the AEMO data collector.
Per-request open is ~1ms overhead and always sees the latest data.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

_DEFAULT_DB = "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb"

# DuckDB stores AEMO data as naive datetimes representing NEM time (AEST = UTC+10).
NEM_TZ = timezone(timedelta(hours=10))


def get_db_path() -> str:
    return os.environ.get("AEMO_DUCKDB_PATH", _DEFAULT_DB)


def get_connection() -> duckdb.DuckDBPyConnection:
    """Open a fresh read-only connection. Caller is responsible for closing it."""
    path = get_db_path()
    if not Path(path).exists():
        raise RuntimeError(f"DuckDB file not found at {path}")
    return duckdb.connect(path, read_only=True)


def nem_naive_to_utc(dt: datetime) -> datetime:
    """Tag a NEM-time naive datetime as UTC+10, then convert to UTC."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=NEM_TZ).astimezone(timezone.utc)


def utc_to_nem_naive(dt: datetime) -> datetime:
    """Convert any datetime to NEM-time naive, suitable for DB query parameters."""
    if dt.tzinfo is None:
        # Treat as already-NEM-naive
        return dt
    return dt.astimezone(NEM_TZ).replace(tzinfo=None)


def reset_connection_for_tests() -> None:
    """No-op now that we don't cache. Kept for test compatibility."""
    pass
