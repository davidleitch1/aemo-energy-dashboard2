"""Shared fixtures for API tests."""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Point the API at a small in-repo DuckDB fixture so tests don't depend on
# the production data file. Must be set BEFORE the api package is imported.
_FIXTURE = Path(__file__).parent / "fixtures" / "test.duckdb"
if _FIXTURE.exists():
    os.environ["AEMO_DUCKDB_PATH"] = str(_FIXTURE)


@pytest.fixture(scope="session")
def app():
    # Dev-mode auth (any non-empty token accepted) for the default fixture
    os.environ.pop("API_TOKENS_FILE", None)
    from aemo_dashboard.api.main import app as fastapi_app
    return fastapi_app


@pytest.fixture(scope="session")
def client(app):
    return TestClient(app)


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer test-token"}
