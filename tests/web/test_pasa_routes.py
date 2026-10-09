"""Route-level checks for the PASA tab (live parquet stores, read-only)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def test_pasa_redirects_to_now(client):
    r = client.get("/pasa", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/pasa/now"


def test_pasa_now_renders(client):
    r = client.get("/pasa/now")
    assert r.status_code == 200
    for text in ("Now &amp; 7 days", "Units out now", "ST-PASA run",
                 "MW out over the next 7 days", "Data: AEMO ST-PASA, MT-PASA"):
        assert text in r.text or text.replace("&amp;", "&") in r.text


@pytest.mark.parametrize("region", ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"])
def test_pasa_now_regions(client, region):
    assert client.get(f"/pasa/now?region={region}").status_code == 200


@pytest.mark.parametrize("sub", ["extended", "slippage", "transmission"])
def test_pasa_placeholders(client, sub):
    r = client.get(f"/pasa/{sub}")
    assert r.status_code == 200


def test_pasa_unknown_sub_404(client):
    assert client.get("/pasa/nope").status_code == 404


def test_today_outage_tile(client):
    r = client.get("/tile/outages-summary")
    assert r.status_code == 200
    assert "Generator outages" in r.text


def test_pasa_slippage_renders(client):
    r = client.get("/pasa/slippage")
    assert r.status_code == 200
    assert "Return-date changes" in r.text and "Return-date path" in r.text


@pytest.mark.parametrize("region", ["NSW1", "QLD1", "TAS1"])
def test_pasa_slippage_regions(client, region):
    assert client.get(f"/pasa/slippage?region={region}").status_code == 200


def test_pasa_now_mothballed_footnote(client):
    r = client.get("/pasa/now")
    assert "not counted" in r.text
