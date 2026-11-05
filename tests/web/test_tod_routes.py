"""Route-level checks for /batteries subtabs and the /demand tab (live DB)."""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _subnav_ok(text: str, active: str) -> None:
    assert 'class="subtab-nav"' in text
    assert 'href="/batteries/fleet"' in text and 'href="/batteries/tod"' in text
    assert f'class="subtab-link active" href="/batteries/{active}"' in text


def test_batteries_default_has_subtab_nav(client):
    r = client.get("/batteries")
    assert r.status_code == 200
    _subnav_ok(r.text, "fleet")
    assert "Batteries" in r.text


def test_batteries_fleet_alias(client):
    r = client.get("/batteries/fleet?range=30d")
    assert r.status_code == 200
    _subnav_ok(r.text, "fleet")
    assert 'class="pill-bar-label">Metric' in r.text


def test_batteries_tod_renders(client):
    r = client.get("/batteries/tod")
    assert r.status_code == 200
    _subnav_ok(r.text, "tod")
    assert '<div class="btod">' in r.text
    assert r.text.count('class="pill active"') >= 2   # region + period
    for label in ("NEM", "NSW", "SA", "7D", "YTD"):
        assert f">{label}</button>" in r.text


def test_batteries_tod_params(client):
    r = client.get("/batteries/tod?region=SA1&range=7d")
    assert r.status_code == 200
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*region=SA1', r.text)
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*range=7d', r.text)


def test_batteries_tod_invalid_params_fall_back(client):
    r = client.get("/batteries/tod?region=XXX&range=bogus")
    assert r.status_code == 200
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*region=NEM', r.text)
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*range=30d', r.text)


def test_htmx_returns_body_only(client):
    for url in ("/batteries", "/batteries/tod", "/demand"):
        r = client.get(url, headers={"HX-Request": "true"})
        assert r.status_code == 200
        assert "<!doctype" not in r.text.lower()
        full = client.get(url)
        assert "<!doctype" in full.text.lower()


def test_top_nav_has_demand_after_prices(client):
    r = client.get("/batteries")
    nav = r.text[r.text.index('class="tab-nav"'):]
    assert ">Demand</a>" in nav
    assert nav.index(">Prices</a>") < nav.index(">Demand</a>") < nav.index(">Batteries</a>")


def test_demand_defaults(client):
    r = client.get("/demand")
    assert r.status_code == 200
    assert '<div class="dtod">' in r.text
    for label in ("Operational", "Total (incl. rooftop)", "+ Battery charging",
                  "+ Pump load", "Storage", "Demand", "TAS"):
        assert label in r.text


def test_demand_explicit_params(client):
    r = client.get("/demand?region=TAS1&range=ytd&base=total&chg=0&pump=1&dis=1")
    assert r.status_code == 200
    assert '<div class="dtod">' in r.text


def test_demand_invalid_params_fall_back(client):
    r = client.get("/demand?region=ZZ&range=nope&base=junk&chg=x")
    assert r.status_code == 200
    assert '<div class="dtod">' in r.text


def _pill_url(html: str, text: str) -> str:
    m = re.search(r'hx-get="([^"]*)"[^>]*>' + re.escape(text) + "</button>", html)
    assert m, text
    return m.group(1)


def test_demand_last_base_cannot_be_deselected(client):
    r = client.get("/demand?base=op")
    url = _pill_url(r.text, "Operational")
    assert "base=op" in url and "total" not in url.split("base=")[1].split("&")[0]
    assert re.search(r'class="pill active" hx-get="[^"]*"[^>]*>Operational<', r.text)


def test_demand_base_toggles_and_carries_params(client):
    r = client.get("/demand?region=VIC1&range=90d&base=op&chg=0&pump=1&dis=0")
    url = _pill_url(r.text, "Total (incl. rooftop)")
    assert "base=op%2Ctotal" in url
    assert "region=VIC1" in url and "range=90d" in url and "pump=1" in url
    url = _pill_url(r.text, "+ Pump load")
    assert "pump=0" in url and "region=VIC1" in url
    url = _pill_url(r.text, "+ Battery charging")
    assert "chg=1" in url
