"""Unit tests for the Network map (web/network.py) with tmp DBs and a tiny geo file."""
from __future__ import annotations

import json
import math

import duckdb
import pandas as pd
import pytest

from aemo_dashboard.web import network
from test_network import dbs, make_summary  # noqa: F401  (fixtures and builders)


def _line(name, lon, lat, role="protected", kv=330):
    return {"role": role, "kind": "line", "name": name, "kv": kv, "source": "auto",
            "parts": [[[lon, lat], [lon + 0.5, lat + 0.2]]]}


GEO = {
    "built": "2026-11-25", "attribution": "Network © Test Agency (CC BY 4.0).",
    "units": {"WF1": [149.0, -33.0, "x"], "BT1": [149.1, -33.1, "x"],
              "SF1": [150.0, -26.0, "x"], "TINY": [149.0, -33.0, "x"]},
    "area_lines": {"A1": [_line("Alpha to Beta", 149.0, -33.0)],
                   "A2": [_line("Ref only", 150.0, -26.0, role="reference")]},
    "area_status": {"A1": "mapped", "A2": "single plant", "A3": "no line: no named line"},
    "backbone": {"500": [[[149.0, -33.0], [150.0, -34.0]]], "330": [], "275": [], "220": []},
}


@pytest.fixture
def geo(tmp_path, monkeypatch):
    path = tmp_path / "geo.json"
    path.write_text(json.dumps(GEO))
    monkeypatch.setattr(network, "GEO_PATH", str(path))
    monkeypatch.setattr(network, "_geo_cache", {})
    return path


def _extra(tmp_path, sql):
    c = duckdb.connect(str(tmp_path / "s.duckdb"))
    c.execute(sql)
    c.close()


# ---- geo file ------------------------------------------------------------------

def test_missing_geo_file_gives_placeholder(dbs, tmp_path, monkeypatch):
    monkeypatch.setattr(network, "GEO_PATH", str(tmp_path / "absent.json"))
    monkeypatch.setattr(network, "_geo_cache", {})
    assert network.load_geo() is None
    assert 'class="placeholder"' in network.map_html("NEM", "90d", "pct", None)


def test_unreadable_geo_file_gives_placeholder(dbs, tmp_path, monkeypatch):
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    monkeypatch.setattr(network, "GEO_PATH", str(bad))
    monkeypatch.setattr(network, "_geo_cache", {})
    assert network.load_geo() is None
    assert 'class="placeholder"' in network.map_html("NEM", "90d", "pct", None)


def test_geo_cache_follows_mtime(geo, monkeypatch):
    import os
    g1 = network.load_geo()
    assert network.load_geo() is g1                       # cached
    new = dict(GEO, built="2027-01-01")
    geo.write_text(json.dumps(new))
    os.utime(geo, (1_900_000_000, 1_900_000_000))
    assert network.load_geo()["built"] == "2027-01-01"


# ---- selection and colours -----------------------------------------------------

def _many_areas(tmp_path):
    """12 areas B01..B12 in NSW (B01 most hours) plus QLD areas Q1; all with lines."""
    geo_lines = {}
    for i in range(1, 13):
        aid = f"B{i:02d}"
        _extra(tmp_path, f"INSERT INTO area VALUES ('{aid}','Area {aid}','NSW','Thermal',1,1,10,0)")
        _extra(tmp_path, f"INSERT INTO area_daily VALUES (DATE '2026-11-03','{aid}',{600 - i * 10},"
                         f"{600 - i * 10},0,0,0,0)")
        geo_lines[aid] = [_line(f"Line {aid}", 148 + i * 0.1, -33)]
    _extra(tmp_path, "INSERT INTO area VALUES ('Q1','Q area','QLD','Thermal',1,1,10,0)")
    _extra(tmp_path, "INSERT INTO area_daily VALUES (DATE '2026-11-03','Q1',900,900,0,0,0,0)")
    geo_lines["Q1"] = [_line("Q line", 150, -26)]
    # one NSW area with the most hours but only a reference feature
    _extra(tmp_path, "INSERT INTO area VALUES ('REF','Ref area','NSW','Thermal',1,1,10,0)")
    _extra(tmp_path, "INSERT INTO area_daily VALUES (DATE '2026-11-03','REF',1000,1000,0,0,0,0)")
    geo_lines["REF"] = [_line("only ref", 150, -30, role="reference")]
    return geo_lines


def test_top_line_areas_respects_region_period_and_features(dbs, geo):
    lines = _many_areas(dbs)
    g = dict(GEO, area_lines=dict(GEO["area_lines"], **lines))
    p = network.get_period("30d")
    nsw = network.top_line_areas(network.hotspot_areas(p, "NSW1"), g)
    assert len(nsw) == 10
    assert "REF" not in set(nsw["area_id"]) and "Q1" not in set(nsw["area_id"])
    assert list(nsw["hours"]) == sorted(nsw["hours"], reverse=True)
    assert nsw.iloc[0]["area_id"] == "B01"
    nem = network.top_line_areas(network.hotspot_areas(p, "NEM"), g)
    assert nem.iloc[0]["area_id"] == "Q1"
    # a period that excludes the rows gives only what is inside it
    p_ytd = network.get_period("ytd")
    assert len(network.top_line_areas(network.hotspot_areas(p_ytd, "NSW1"), g)) == 10


def test_ramp_colours_are_monotonic_in_hours():
    cols = network.ramp_colors([10, 50, 100, 400, 800])
    lum = [network._luminance(c) for c in cols]
    assert lum == sorted(lum, reverse=True)
    assert cols[0].upper() == "#ECCB60" and cols[-1].upper() == "#6C201C"
    assert network.ramp_colors([5]) == [network.ramp_colors([5, 9])[-1]]
    assert network.ramp_colors([]) == []


# ---- units ---------------------------------------------------------------------

def test_units_without_coordinates_counted_not_drawn(dbs, geo):
    c = duckdb.connect(str(dbs / "a.duckdb"))
    c.execute("INSERT INTO duid_mapping VALUES ('NOCOORD','NSW1','No coords','o',50.0,NULL,'Wind')")
    c.close()
    df, missing = network.unit_frame(network.load_geo(), {"WF1": 12.0})
    assert missing == 1 and "NOCOORD" not in set(df["duid"])
    assert "TINY" not in set(df["duid"])                  # under 5 MW hidden
    assert df.set_index("duid").loc["WF1", "value"] == 12.0
    assert df.set_index("duid").loc["BT1", "value"] == 0.0


# ---- view arithmetic ------------------------------------------------------------

def test_bbox_view_single_point_and_500km_span():
    lon, lat, z = network.bbox_view([(149.0, -33.0)])
    assert (lon, lat) == (149.0, -33.0) and 8 <= z <= 10
    lon, lat, z = network.bbox_view([(149.0, -31.0), (149.0, -35.5)])   # about 500 km
    assert lat == pytest.approx(-33.25) and 5 <= z <= 7
    lon, lat, z = network.bbox_view([])
    assert 3 <= z <= 4


# ---- the model --------------------------------------------------------------------

def _trace_names(m):
    return [t.name for t in m["fig"].data]


def test_build_map_default(dbs, geo):
    m = network.build_map("NEM", "30d", "pct", None)
    names = _trace_names(m)
    assert any(n and n.startswith("500 kV") for n in names)
    ref = [t for t in m["fig"].data if t.name and t.name.endswith("kV")]
    assert all(t.visible == "legendonly" for t in ref)
    assert m["fig"].layout.map.style == "carto-positron"
    assert m["n_missing"] == 0
    assert [r["area_id"] for r in m["side"]][0] == "A1"
    assert m["side"][0]["tag"] == "line"


def test_unknown_area_falls_back_to_unselected(dbs, geo):
    m = network.build_map("NEM", "30d", "pct", "NO_SUCH")
    assert m["selected"] is None
    m2 = network.build_map("NEM", "30d", "pct", "A1")
    assert m2["selected"]["area_id"] == "A1" and m2["zoom"] > 5
    units = [t for t in m2["fig"].data if t.name == "Units"][0]
    ops = list(units.marker.opacity)
    assert 0.25 in ops and 1 in ops or 1.0 in ops


def test_gwh_measure_colour_scale(dbs, geo):
    m = network.build_map("NEM", "30d", "gwh", None)
    units = [t for t in m["fig"].data if t.name == "Units"][0]
    assert units.marker.cmin == 0 and units.marker.cmax >= 3
    assert "GWh" in units.marker.colorbar.title.text
    m = network.build_map("NEM", "30d", "pct", None)
    units = [t for t in m["fig"].data if t.name == "Units"][0]
    assert units.marker.cmax == 50


def test_halo_for_single_plant_area(dbs, geo):
    m = network.build_map("NEM", "30d", "pct", None)
    halos = [t for t in m["fig"].data if t.name == "Single-plant limits"]
    assert len(halos) == 1 and list(halos[0].lon) == [150.0]    # SF1 under A2


def test_now_mode_without_adjustment_rows(dbs, geo, tmp_path):
    c = duckdb.connect(str(dbs / "c.duckdb"))
    c.execute("DELETE FROM local_price5")
    c.close()
    m = network.build_map("NEM", "now", "pct", None)
    units = [t for t in m["fig"].data if t.name == "Units"][0]
    assert units.marker.cmax == 1000 and max(units.marker.color) == 0
    assert "Local price adjustment" in units.marker.colorbar.title.text
    assert 'id="netw-map-' in network.map_html("NEM", "now", "pct", None)


def test_now_mode_colours_binding_areas(dbs, geo):
    m = network.build_map("NEM", "now", "pct", None)
    lines = [t for t in m["fig"].data if t.name == "Alpha to Beta (1)"]
    assert lines and lines[0].line.color == "#AF3029"      # C1 binds, area A1


def test_map_html_note_and_markers(dbs, geo):
    h = network.map_html("NEM", "30d", "pct", None)
    assert ("A dot shows where generation is held back; a coloured line shows the limit "
            "AEMO names. They can be far apart.") in h
    assert "Network © Test Agency (CC BY 4.0)." in h
    assert "Source: AEMO" in h and "plotly_click" in h and "netw-map-" in h
    assert "not drawn" in h


def test_status_words(geo):
    g = network.load_geo()
    assert "Alpha to Beta" in network.status_words(g, "A1")
    assert network.status_words(g, "A2") == "A limit on a single plant"
    assert network.status_words(g, "A3") == "No line is drawn: no named line"
    assert network.area_tag(g, "A1") == "line" and network.area_tag(g, "A3") == "no line"
    assert network.area_tag(g, "ZZ") == "not mapped"
