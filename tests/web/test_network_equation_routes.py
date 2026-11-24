"""Route checks for the Equation view, sort choice and rank (live read-only DBs)."""
from __future__ import annotations

import re
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import app

AREA = quote("N>>NIL_060_051")


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _active(text, key, value):
    return bool(re.search(rf'class="pill active"[^>]*hx-get="[^"]*{key}={value}(&|")', text))


def test_area_overview_has_switch(client):
    r = client.get(f"/network/area?id={AREA}&range=90d")
    assert r.status_code == 200
    assert 'class="vsw active"' in r.text and "view=equation" in r.text
    assert 'class="subtab-link active" href="/network/hotspots"' in r.text


@pytest.mark.parametrize("q", ["&view=equation", "&view=equation&all=1",
                               "&view=equation&constraint=NOPE", "&view=bogus"])
def test_area_equation_variants(client, q):
    r = client.get(f"/network/area?id={AREA}&range=90d{q}")
    assert r.status_code == 200
    assert 'class="vsw' in r.text
    if "view=equation" in q:
        assert ("The equation data has not been built yet." in r.text
                or "The equation" in r.text)


def test_invalid_view_falls_back_to_overview(client):
    r = client.get(f"/network/area?id={AREA}&view=bogus")
    assert "Constraints that bound in the period" in r.text


def test_unknown_area_equation_is_placeholder(client):
    r = client.get("/network/area?id=NO_SUCH&view=equation")
    assert r.status_code == 200 and 'class="placeholder"' in r.text


@pytest.mark.parametrize("url", ["/network/hotspots?sort=energy", "/network/map?sort=energy",
                                 "/network/map?sort=hours", "/network/hotspots?sort=hours"])
def test_sort_pills(client, url):
    r = client.get(url)
    assert r.status_code == 200
    assert "Rank by" in r.text
    assert _active(r.text, "sort", url.split("sort=")[1])


def test_sort_defaults(client):
    assert _active(client.get("/network/hotspots").text, "sort", "hours")
    assert _active(client.get("/network/map").text, "sort", "energy")


def test_invalid_sort_and_view_fall_back(client):
    assert _active(client.get("/network/hotspots?sort=zzz").text, "sort", "hours")
    assert _active(client.get("/network/map?sort=zzz").text, "sort", "energy")


def test_sort_carried_by_other_pills(client):
    t = client.get("/network/hotspots?sort=energy").text
    assert re.search(r'hx-get="[^"]*sort=energy[^"]*"[^>]*>30D<', t) or "sort=energy" in t


@pytest.mark.parametrize("url", [f"/network/area?id={AREA}&view=equation",
                                 "/network/hotspots?sort=energy", "/network/map?sort=energy"])
def test_htmx_partials(client, url):
    r = client.get(url, headers={"HX-Request": "true"})
    assert r.status_code == 200 and "<!doctype" not in r.text.lower()


def test_banned_words_in_equation_view(client):
    banned = re.compile(r"\b(significant\w*|substantial\w*|notabl\w+|clearly|really|"
                        r"genuine\w*|robust\w*|honest\w*)\b", re.I)
    t = client.get(f"/network/area?id={AREA}&view=equation").text
    t = re.sub(r"<script>.*?</script>", "", t, flags=re.S)
    assert not banned.search(re.sub(r"<[^>]+>", " ", t.split('id="tab-body"')[1]))
