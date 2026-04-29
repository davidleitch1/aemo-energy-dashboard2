"""DuckDB connection helper for the mobile API.

Holds a single read-only connection per process. The dashboard's Panel app
keeps its own connection — we don't share state with it.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

_DEFAULT_DB = "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb"


def get_db_path() -> str:
    return os.environ.get("AEMO_DUCKDB_PATH", _DEFAULT_DB)


_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Lazy-initialised read-only DuckDB connection."""
    global _conn
    if _conn is None:
        path = get_db_path()
        if not Path(path).exists():
            raise RuntimeError(f"DuckDB file not found at {path}")
        _conn = duckdb.connect(path, read_only=True)
    return _conn


def reset_connection_for_tests() -> None:
    """Clear the cached connection — used by tests that override $AEMO_DUCKDB_PATH."""
    global _conn
    if _conn is not None:
        _conn.close()
    _conn = None
