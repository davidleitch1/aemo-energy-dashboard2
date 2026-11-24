"""Route-level checks for the /network tab (live read-only DBs)."""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


SUBTABS = ("hotspots", "causes", "units", "now")


def _subnav_ok(text: str, active: str) -> None:
    assert 'class="subtab-nav"' in text
    for s in SUBTABS:
        assert f'href="/network/{s}"' in text
    assert f'class="subtab-link active" href="/network/{active}"' in text
    assert text.count('subtab-link active') == 1


def _active_pill(text: str, key: str, value: str) -> bool:
    return bool(re.search(
        rf'class="pill active"[^>]*hx-get="[^"]*{key}={value}(&|")', text))


ROUTES = [("/network", "hotspots"), ("/network/hotspots", "hotspots"),
          ("/network/causes", "causes"), ("/network/units", "units"),
          ("/network/now", "now")]


@pytest.mark.parametrize("url,active", ROUTES)
def test_route_renders_with_subnav(client, url, active):
    r = client.get(url)
    assert r.status_code == 200
    assert "<!doctype" in r.text.lower()
    _subnav_ok(r.text, active)
    assert '<div class="netw">' in r.text
    assert "Source: AEMO" in r.text and "exposure" in r.text


def test_top_nav_has_network_after_pasa(client):
    r = client.get("/network")
    nav = r.text[r.text.index('class="tab-nav"'):]
    assert nav.index(">PASA</a>") < nav.index(">Network</a>")


@pytest.mark.parametrize("url", ["/network", "/network/causes", "/network/units",
                                 "/network/now"])
def test_htmx_partial_has_no_shell(client, url):
    r = client.get(url, headers={"HX-Request": "true"})
    assert r.status_code == 200
    assert "<!doctype" not in r.text.lower()
    assert 'class="subtab-nav"' in r.text


def test_hotspots_markers(client):
    r = client.get("/network/hotspots")
    assert "<table>" in r.text and "Plotly.newPlot" in r.text
    assert "/network/area?id=" in r.text
    for label in ("NEM", "NSW", "TAS", "30D", "90D", "YTD", "1Y", "All"):
        assert f">{label}</button>" in r.text
    assert _active_pill(r.text, "region", "NEM") and _active_pill(r.text, "range", "90d")


def test_hotspots_non_default(client):
    r = client.get("/network/hotspots?region=SA1&range=30d")
    assert r.status_code == 200
    assert _active_pill(r.text, "region", "SA1") and _active_pill(r.text, "range", "30d")


def test_causes_markers(client):
    r = client.get("/network/causes?region=VIC1&range=1y")
    assert r.status_code == 200
    assert r.text.count("Plotly.newPlot") == 4
    assert _active_pill(r.text, "region", "VIC1")


def test_units_markers_and_fuel(client):
    r = client.get("/network/units?fuel=battery&range=all")
    assert r.status_code == 200
    _subnav_ok(r.text, "units")
    assert "charge-limited" in r.text and "/network/unit?duid=" in r.text
    assert _active_pill(r.text, "fuel", "battery")


def test_now_markers(client):
    r = client.get("/network/now")
    assert "Latest dispatch interval" in r.text
    assert "/pasa/transmission" in r.text
    assert "description pending" in r.text or "AEMO description" in r.text


def test_area_page_from_live_link(client):
    from urllib.parse import unquote
    page = client.get("/network/hotspots?range=30d").text
    aid = unquote(re.search(r'/network/area\?id=([^&"]+)&', page).group(1))
    r = client.get("/network/area", params={"id": aid, "range": "30d"})
    assert r.status_code == 200
    _subnav_ok(r.text, "hotspots")
    assert "Constraints that bound" in r.text and "Plotly.newPlot" in r.text
    assert "Units behind it" in r.text and "Associated outages" in r.text


def test_unit_page_from_live_link(client):
    from urllib.parse import unquote
    page = client.get("/network/units?range=90d").text
    duid = unquote(re.search(r'/network/unit\?duid=([^&"]+)&', page).group(1))
    r = client.get("/network/unit", params={"duid": duid, "range": "90d"})
    assert r.status_code == 200
    _subnav_ok(r.text, "units")
    assert r.text.count("Plotly.newPlot") == 2
    assert "Constraints behind it" in r.text


def test_unknown_area_and_unit_are_placeholders(client):
    r = client.get("/network/area?id=NO_SUCH_AREA")
    assert r.status_code == 200 and 'class="placeholder"' in r.text
    _subnav_ok(r.text, "hotspots")
    r = client.get("/network/unit?duid=NO_SUCH_DUID")
    assert r.status_code == 200 and 'class="placeholder"' in r.text
    r = client.get("/network/area")
    assert r.status_code == 200 and 'class="placeholder"' in r.text


@pytest.mark.parametrize("url", ["/network/hotspots", "/network/causes",
                                 "/network/units"])
def test_invalid_params_fall_back(client, url):
    r = client.get(f"{url}?region=XXX&range=bogus&fuel=nope")
    assert r.status_code == 200
    assert _active_pill(r.text, "region", "NEM") and _active_pill(r.text, "range", "90d")
    if url.endswith("units"):
        assert _active_pill(r.text, "fuel", "all")


def test_invalid_range_on_detail_pages(client):
    r = client.get("/network/area?id=X&range=bogus")
    assert r.status_code == 200 and _active_pill(r.text, "range", "90d")
    r = client.get("/network/unit?duid=X&range=bogus")
    assert r.status_code == 200 and _active_pill(r.text, "range", "90d")


def test_no_banned_words_on_pages(client):
    banned = re.compile(r"\b(significant\w*|substantial\w*|notabl\w+|clearly|really|"
                        r"genuine\w*|robust\w*|honest\w*)\b", re.I)
    for url in ("/network", "/network/causes", "/network/units", "/network/now"):
        text = re.sub(r"<script>.*?</script>", "", client.get(url).text, flags=re.S)
        assert not banned.search(re.sub(r"<[^>]+>", " ", text.split('id="tab-body"')[1])), url


def test_every_table_is_in_a_scrolling_container(client):
    for url in ("/network", "/network/causes", "/network/units", "/network/now",
                "/network/units?fuel=battery"):
        t = client.get(url).text
        assert t.count("<table>") >= 1
        assert t.count("<table>") == t.count('<div class="tbl-wrap"><table>'), url
    assert "overflow-x:auto" in client.get("/network").text


def test_cause_key_on_hotspots_and_causes(client):
    for url in ("/network", "/network/causes"):
        t = client.get(url).text
        # live data carries five causes since 25 Nov 2026; legacy three-cause data shows the single-plant label
        assert "Network as built:" in t and ("Stability limit on a plant:" in t or "Limit on a single plant:" in t)
        assert "Unit-specific limit" not in t.split('id="tab-body"')[1]
