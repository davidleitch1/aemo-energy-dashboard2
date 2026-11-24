"""Five-cause display (with three-cause legacy data) and the Units sort."""
from __future__ import annotations

import re

import duckdb
import pytest

from aemo_dashboard.web import network
from test_network import dbs, make_summary  # noqa: F401

KEY_TEXT = {
    "Network as built": "the limit binds with every line and transformer in service. "
                        "AEMO marks these “Out = Nil”.",
    "Equipment outage": "the limit applies only while a named line or transformer is out "
                        "of service.",
    "Stability limit on a plant": "AEMO caps one wind or solar farm to keep the local grid "
        "stable. Most are system strength limits, which depend on how many coal, gas or hydro "
        "units and synchronous condensers are running nearby; some guard against the plant "
        "being cut off from the grid.",
    "Commissioning": "a new plant held back while it is tested. This is not congestion.",
    "Other cap on a plant": "a cap AEMO applies to one plant for another reason, such as a "
        "discretionary limit or a unit held at zero.",
    "Limit on a single plant": "AEMO caps one wind or solar farm. Most are system strength "
        "limits, which depend on how many coal, gas or hydro units and synchronous "
        "condensers are running nearby.",
}


def _x(path, *sqls, db="s"):
    c = duckdb.connect(str(path / f"{db}.duckdb"))
    for s in sqls:
        c.execute(s)
    c.close()


@pytest.fixture
def five(dbs):
    """The fixture summary DB moved to five causes."""
    _x(dbs,
       "ALTER TABLE area_daily ADD COLUMN n_plant_stability BIGINT",
       "ALTER TABLE area_daily ADD COLUMN n_commissioning BIGINT",
       "ALTER TABLE area_daily ADD COLUMN n_other_cap BIGINT",
       "UPDATE area_daily SET n_plant_stability = 0, n_commissioning = 0, n_other_cap = 0",
       "UPDATE area_daily SET n_plant_stability = 3, n_commissioning = 2, n_other_cap = 1 "
       "WHERE n_unit_specific = 6",
       "UPDATE cause_daily SET cause = 'Plant stability limit' WHERE cause = 'Unit-specific limit'",
       "INSERT INTO cause_daily VALUES (DATE '2026-11-05','QLD','Commissioning','Other',12)",
       "INSERT INTO cause_daily VALUES (DATE '2026-11-05','NSW','Other plant cap','Other',6)",
       "UPDATE constraint_meta SET cause = 'Plant stability limit' WHERE constraintid = 'C2'",
       "UPDATE constraint_meta SET cause = 'Commissioning' WHERE constraintid = 'C1'")
    return dbs


def _cbar_titles(h):
    return re.findall(r'<div class="cbar" title="([^"]*)">', h)


# ---- colours, labels, key --------------------------------------------------------------

def test_labels_and_colours():
    d, c = network.CAUSE_DISPLAY, network.CAUSE_COLORS
    assert d["Plant stability limit"] == "Stability limit on a plant"
    assert d["Commissioning"] == "Commissioning" and d["Other plant cap"] == "Other cap on a plant"
    assert d["Unit-specific limit"] == "Limit on a single plant"
    assert c["Plant stability limit"] == "#5E409D" and c["Commissioning"] == "#24837B"
    assert c["Other plant cap"] == "#A02F6F" and c["Unit-specific limit"] == "#5E409D"
    assert c["System normal"] == "#205EA6" and c["Outage"] == "#BC5215"


def test_key_text_exact_for_five_and_legacy():
    k5 = network.cause_key(network.FIVE_CAUSES)
    for name in ("Network as built", "Equipment outage", "Stability limit on a plant",
                 "Commissioning", "Other cap on a plant"):
        assert f"<b>{name}:</b> {KEY_TEXT[name]}" in k5
    assert "Limit on a single plant" not in k5
    k3 = network.cause_key(network.LEGACY_CAUSES)
    assert f"<b>Limit on a single plant:</b> {KEY_TEXT['Limit on a single plant']}" in k3
    assert "Commissioning" not in k3


def test_shares_sum_to_100_five_and_tiles():
    s = network.shares([18, 12, 3, 2, 1])
    assert sum(s) == pytest.approx(100.0)
    t = network.tile_shares_five(18, 12, 3, 2, 1)
    assert len(t) == 4 and sum(t) == pytest.approx(100.0)
    assert t[3] == pytest.approx(3 / 36 * 100)           # commissioning + other caps
    assert network.shares([0, 0, 0]) == [0.0, 0.0, 0.0]


# ---- legacy data keeps three causes --------------------------------------------------------

def test_legacy_hotspots_three_segments_and_labels(dbs):
    h = network.hotspots_html("NEM", "30d")
    assert "Network as built share" in h and "Equipment outage share" in h
    assert "Single-plant limit share" in h
    assert "Stability limits on plants" not in h
    t = [x for x in _cbar_titles(h) if "Network as built" in x][0]
    assert t.count("%") == 3 and "Limit on a single plant" in t
    assert "Limit on a single plant:" in h


# ---- five causes -----------------------------------------------------------------------------

def test_five_cause_hotspots(five):
    h = network.hotspots_html("NEM", "30d")
    for lab in ("Network as built share", "Equipment outage share",
                "Stability limits on plants", "Commissioning and other caps"):
        assert lab in h
    assert "Single-plant limit share" not in h
    t = [x for x in _cbar_titles(h) if "Network as built" in x][0]
    assert t.count("%") == 5
    for lab in ("Stability limit on a plant", "Commissioning", "Other cap on a plant"):
        assert lab in t
    assert h.count("background:#24837B") >= 1 and h.count("background:#A02F6F") >= 1
    for name in ("Stability limit on a plant", "Commissioning", "Other cap on a plant"):
        assert f"<b>{name}:</b> {KEY_TEXT[name]}" in h
    assert "Unit-specific" not in h and "Limit on a single plant" not in h


def test_hotspot_areas_five_columns(five):
    df = network.hotspot_areas(network.get_period("30d"), "NEM")
    a1 = df[df["area_id"] == "A1"].iloc[0]
    assert (a1["ps"], a1["co"], a1["oc"]) == (3, 2, 1) and a1["us"] == 6


def test_five_cause_causes_page(five):
    t = network.cause_totals(network.get_period("30d"), "NEM")
    assert t["causes"] == network.FIVE_CAUSES
    assert t["by_cause"]["Commissioning"] == 12 and t["by_cause"]["Other plant cap"] == 6
    h = network.causes_html("NEM", "30d")
    assert h.count("Plotly.newPlot") == 4
    for lab in ("Commissioning", "Other cap on a plant", "Stability limit on a plant"):
        assert lab in h
    assert "Unit-specific" not in h


def test_legacy_causes_page_unchanged(dbs):
    t = network.cause_totals(network.get_period("30d"), "NEM")
    assert t["causes"] == network.LEGACY_CAUSES
    assert "Other cap on a plant" not in network.causes_html("NEM", "30d")


def test_five_cause_area_page(five):
    h = network.area_html("A2", "30d")
    assert "Stability limit on a plant" in h             # constraint table, C2
    h1 = network.area_html("A1", "30d")
    assert "Commissioning" in h1                          # constraint C1 cause and chart
    assert h1.count("Plotly.newPlot") == 1
    assert "Stability limits on plants" in h1 and "Commissioning and other caps" in h1


def test_five_cause_now_table(five):
    h = network.now_html()
    assert "Commissioning" in h


# ---- Units sort ------------------------------------------------------------------------------------

def _units_case(dbs):
    rows = [("BIGC", 10, 0, 50000.0, 0.0, 1_000_000.0), ("VALU", 10, 0, 5000.0, 0.0, 9_000_000.0)]
    for duid, dis, chg, mwh, oth, val in rows:
        _x(dbs, f"INSERT INTO unit_daily VALUES (DATE '2026-11-03','{duid}',{dis},{chg},{mwh},"
                f"{oth},{val},0,NULL)")
        _x(dbs, f"INSERT INTO duid_mapping VALUES ('{duid}','NSW1','{duid} site','o',100.0,"
                f"NULL,'Wind')", db="a")


def test_units_sort_orders_differ(dbs):
    _units_case(dbs)
    p = network.get_period("30d")
    by = lambda s: list(network.unit_league(p, "NEM", "all", sort=s)["duid"])
    assert by("pct")[0] == "WF1"                    # 5% of intervals
    assert by("gwh")[0] == "BIGC"                   # 50 GWh
    assert by("value")[0] == "VALU"                 # $9m
    assert by("pct") != by("gwh") != by("value")
    assert list(network.unit_league(p, "NEM", "all")["duid"]) == by("pct")   # default


def test_units_sort_normalise():
    assert network.normalise_unit_sort("gwh") == "gwh"
    assert network.normalise_unit_sort("value") == "value"
    assert network.normalise_unit_sort("zzz") == "pct" and network.normalise_unit_sort("") == "pct"


def test_units_html_heading_and_marked_header(dbs):
    _units_case(dbs)
    h = network.units_html("NEM", "all", "30d", sort="gwh")
    assert "units by GWh curtailed while constrained" in h
    assert h.index("BIGC site") < h.index("Windy")
    assert re.search(r'<th class="r sorted">GWh curtailed while constrained ▼</th>', h)
    assert h.count("▼") == 1
    h = network.units_html("NEM", "all", "30d", sort="value")
    assert "units by value of curtailment" in h
    assert re.search(r'<th class="r sorted">\$m ▼</th>', h)
    h = network.units_html("NEM", "all", "30d")
    assert "units by share of intervals limited" in h
    assert re.search(r'<th class="r sorted">% intervals limited ▼</th>', h)
    for col in ("Region", "Fuel", "MW", "GWh other curtailment", "Median local price adj. $/MWh",
                "Most frequent constraint area"):
        assert col in h
