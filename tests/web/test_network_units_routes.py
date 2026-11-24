"""Route checks for the Units sort (live read-only DBs)."""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _active(text, key, value):
    return bool(re.search(rf'class="pill active"[^>]*hx-get="[^"]*{key}={value}(&|")', text))


@pytest.mark.parametrize("sort", ["pct", "gwh", "value"])
def test_units_sort_pills(client, sort):
    r = client.get(f"/network/units?sort={sort}&region=NSW1&fuel=wind&range=1y")
    assert r.status_code == 200
    assert "Rank by" in r.text and _active(r.text, "sort", sort)
    for label in ("% of intervals held back", "GWh curtailed while constrained", "Value, $m"):
        assert f">{label}</button>" in r.text
    assert 'class="subtab-link active" href="/network/units"' in r.text
    assert r.text.count("▼") == 1


def test_units_sort_default_and_invalid(client):
    assert _active(client.get("/network/units").text, "sort", "pct")
    assert _active(client.get("/network/units?sort=bogus").text, "sort", "pct")


def test_units_sort_carried_by_other_pills(client):
    t = client.get("/network/units?sort=gwh").text
    for label in ("NSW", "Wind", "30D"):
        assert re.search(rf'hx-get="[^"]*sort=gwh[^"]*"[^>]*>{label}</button>', t), label


def test_units_sort_htmx(client):
    r = client.get("/network/units?sort=value", headers={"HX-Request": "true"})
    assert r.status_code == 200 and "<!doctype" not in r.text.lower()
