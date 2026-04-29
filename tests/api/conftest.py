"""Shared fixtures for API tests."""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Point the API at small in-repo fixtures so tests don't depend on
# production data files. Must be set BEFORE the api package is imported.
_FIXTURES = Path(__file__).parent / 'fixtures'
_DUCKDB = _FIXTURES / 'test.duckdb'
if _DUCKDB.exists():
    os.environ['AEMO_DUCKDB_PATH'] = str(_DUCKDB)
# PASA reads parquet files from AEMO_DATA_PATH (one dir, not a single file).
os.environ['AEMO_DATA_PATH'] = str(_FIXTURES)


@pytest.fixture(scope='session')
def app():
    os.environ.pop('API_TOKENS_FILE', None)
    from aemo_dashboard.api.main import app as fastapi_app
    return fastapi_app


@pytest.fixture(scope='session')
def client(app):
    return TestClient(app)


@pytest.fixture
def auth_headers():
    return {'Authorization': 'Bearer test-token'}
