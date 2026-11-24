"""Route checks for /network/map (live read-only DBs)."""
from __future__ import annotations

import re
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app

ORDER = ["hotspots", "map", "causes", "units", "now"]


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _subnav(text):
    nav = text[text.index('class="subtab-nav"'):]
    nav = nav[:nav.index("</nav>")]
    return re.findall(r'href="/network/(\w+)"', nav)


def test_map_renders_with_active_subtab(client):
    r = client.get("/network/map")
    assert r.status_code == 200
    assert 'class="subtab-link active" href="/network/map"' in r.text
    assert _subnav(r.text) == ORDER
    assert 'id="netw-map-' in r.text and "Plotly.newPlot" in r.text
    for label in ("NEM", "NSW", "TAS", "30D", "90D", "YTD", "1Y", ">All<", ">Now<"):
        assert label in r.text
    assert "% of intervals held back" in r.text


@pytest.mark.parametrize("url", [
    "/network/map?range=now",
    "/network/map?region=QLD1&range=1y&measure=gwh",
    "/network/map?area=" + quote("N>>NIL_060_051"),
    "/network/map?area=NO_SUCH_AREA",
    "/network/map?region=ZZ&range=bogus&measure=nope&area=",
])
def test_map_variants_200(client, url):
    r = client.get(url)
    assert r.status_code == 200 and 'id="netw-map-' in r.text


def test_map_invalid_params_fall_back(client):
    r = client.get("/network/map?region=ZZ&range=bogus&measure=nope")
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*region=NEM', r.text)
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*range=90d', r.text)
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*measure=pct', r.text)


def test_map_gwh_pill_active(client):
    r = client.get("/network/map?measure=gwh")
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*measure=gwh', r.text)


def test_map_now_pill_active(client):
    r = client.get("/network/map?range=now")
    assert re.search(r'class="pill active"[^>]*hx-get="[^"]*range=now', r.text)


def test_selected_area_line(client):
    r = client.get("/network/map?area=" + quote("N>>NIL_060_051"))
    if "Clear selection" in r.text:
        assert "/network/area?id=" in r.text


def test_map_htmx_partial(client):
    r = client.get("/network/map", headers={"HX-Request": "true"})
    assert r.status_code == 200 and "<!doctype" not in r.text.lower()
    assert 'class="subtab-nav"' in r.text


@pytest.mark.parametrize("url", ["/network", "/network/map", "/network/causes",
                                 "/network/units", "/network/now"])
def test_subtab_order_on_every_network_page(client, url):
    assert _subnav(client.get(url).text) == ORDER


def test_map_banned_words(client):
    banned = re.compile(r"\b(significant\w*|substantial\w*|notabl\w+|clearly|really|"
                        r"genuine\w*|robust\w*|honest\w*)\b", re.I)
    t = re.sub(r"<script>.*?</script>", "", client.get("/network/map").text, flags=re.S)
    assert not banned.search(re.sub(r"<[^>]+>", " ", t.split('id="tab-body"')[1]))
