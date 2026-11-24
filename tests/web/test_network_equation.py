"""Tests for the Equation view, sort choice and rank movement (web/network.py)."""
from __future__ import annotations

import duckdb
import pytest

from aemo_dashboard.web import network
from test_network import dbs, make_summary  # noqa: F401
from test_network_map import GEO, _line, geo  # noqa: F401

NOT_BUILT = "The equation data has not been built yet."


def _x(path, *sqls):
    c = duckdb.connect(str(path / "s.duckdb"))
    for s in sqls:
        c.execute(s)
    c.close()


def add_equation(path, *, scale=1.0, ctype="<=", n_fb=0, ops=False, n_units=15, n_ic=1,
                 stale=False, note=True, version_rule="exact"):
    """New-style tables for area A1 (constraints C1 lead, C1B) in the tmp summary DB."""
    _x(path,
       "ALTER TABLE constraint_daily ADD COLUMN constrained_mwh DOUBLE",
       "ALTER TABLE constraint_daily ADD COLUMN constrained_value DOUBLE",
       "UPDATE constraint_daily SET constrained_mwh = n_intervals * 10.0, constrained_value = 1000",
       "ALTER TABLE area ADD COLUMN lead_constraintid VARCHAR",
       "UPDATE area SET lead_constraintid = 'C1' WHERE area_id = 'A1'",
       "CREATE TABLE constraint_equation(constraintid VARCHAR, side VARCHAR, ord BIGINT, "
       "kind VARCHAR, spd_id VARCHAR, spd_type VARCHAR, factor DOUBLE, operation VARCHAR, "
       "defaultvalue DOUBLE, duid VARCHAR, site VARCHAR, fuel VARCHAR, capacity_mw DOUBLE, "
       "aemo_description VARCHAR, reading VARCHAR, is_feedback BOOLEAN)",
       "CREATE TABLE constraint_equation_meta(constraintid VARCHAR, effectivedate TIMESTAMP, "
       "versionno BIGINT, version_rule VARCHAR, constrainttype VARCHAR, scale DOUBLE, "
       "n_lhs_units BIGINT, n_lhs_interconnectors BIGINT, n_feedback BIGINT, "
       "has_operations BOOLEAN, fingerprint VARCHAR, rhs_p10 DOUBLE, rhs_med DOUBLE, "
       "rhs_p90 DOUBLE, lhs_med DOUBLE, n_binding_intervals BIGINT)",
       "CREATE TABLE constraint_hour(constraintid VARCHAR, hour BIGINT, n_intervals BIGINT)",
       "CREATE TABLE area_note(area_id VARCHAR, constraintid VARCHAR, note VARCHAR, "
       "written_effectivedate TIMESTAMP, written_fingerprint VARCHAR, "
       "current_fingerprint VARCHAR, is_stale BOOLEAN)")
    c = duckdb.connect(str(path / "s.duckdb"))
    for i in range(n_units):
        f = 1.0 - i * 0.05 if i % 4 else -(1.0 - i * 0.05)       # mixed signs, all distinct |w|
        c.execute("INSERT INTO constraint_equation VALUES ('C1','lhs',?,'unit',?,'T',?,NULL,NULL,"
                  "?,?,?,?,NULL,?,false)",
                  [i, f"CP{i}", f, "WF1" if i == 0 else f"U{i}", f"Site {i}",
                   "Wind", 350.0 + i, f"reading {i}"])
    for j in range(n_ic):
        c.execute("INSERT INTO constraint_equation VALUES ('C1','lhs',100,'interconnector',"
                  "'V-SA','I',0.5,NULL,NULL,NULL,NULL,NULL,NULL,'Heywood','Each MW adds 0.5',false)")
    c.execute("INSERT INTO constraint_equation VALUES ('C1','rhs',1,'rating','A_RATING','E',1.0,"
              "NULL,1200.0,NULL,NULL,NULL,NULL,NULL,'The 15-minute rating',false)")
    c.execute("INSERT INTO constraint_equation VALUES ('C1','rhs',2,'measurement','MEAS_1','A',"
              "-0.989,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Minus 0.989 × a live measurement',false)")
    c.execute("INSERT INTO constraint_equation VALUES ('C1','rhs',3,'unit','FB_1','T',1.0,NULL,"
              "NULL,'WF1',NULL,NULL,NULL,NULL,'feedback term',true)")
    c.execute("INSERT INTO constraint_equation_meta VALUES ('C1','2026-09-01',3,?,?,?,?,?,?,?,"
              "'fp2',900,1230,1500,1100,5000)",
              [version_rule, ctype, scale, n_units, n_ic, n_fb, ops])
    for h in range(24):
        c.execute("INSERT INTO constraint_hour VALUES ('C1',?,?)", [h, 100 if h == 12 else 10])
    if note:
        c.execute("INSERT INTO area_note VALUES ('A1','C1','This limit protects the Alpha line.',"
                  "'2026-09-01','fp1','fp2',?)", [stale])
    c.close()


# ---- number formatting -----------------------------------------------------------

def test_weight_formatting_never_strips_integer_zeros():
    assert network.fw(350) == "350"
    assert network.fw(1230) == "1,230"
    assert network.fw(0.989) == "0.989"
    assert network.fw(1) == "1"
    assert network.fw(1.0004) == "1"
    assert network.fw(0.5) == "0.5"
    assert network.fw(-0.25) == "−0.25"
    assert network.fw(100) == "100"
    assert network.fw(float("nan")) == "–"


# ---- degrade cleanly -----------------------------------------------------------------

def test_equation_view_without_new_tables(dbs):
    h = network.area_html("A1", "30d", view="equation")
    assert NOT_BUILT in h
    assert "Overview" in h and "Equation" in h            # the switch is still there
    assert "Constraints that bound in the period" in network.area_html("A1", "30d", view="overview")


def test_equation_view_with_meta_but_not_equation_table(dbs):
    _x(dbs, "CREATE TABLE constraint_equation_meta(constraintid VARCHAR)")
    assert NOT_BUILT in network.area_html("A1", "30d", view="equation")


def test_overview_is_unchanged_apart_from_the_switch(dbs):
    h = network.area_html("A1", "30d")
    assert "Constraints that bound in the period" in h and "Associated outages" in h
    assert 'class="vsw active"' in h and "view=equation" in h


def test_sort_and_rank_degrade_without_new_tables(dbs):
    h = network.hotspots_html("NEM", "30d")
    assert "12-month rank" not in h and "rising" not in h
    assert network.load_ranks() is None


# ---- equation view content ---------------------------------------------------------------

def test_equation_block_wording_scale_and_feedback():
    meta = dict(constrainttype="<=", scale=1.0, n_lhs_units=15, n_lhs_interconnectors=1,
                n_feedback=0)
    h = network.equation_block(meta)
    assert "weighted output of 15 generators and batteries" in h
    assert "+ weighted flow on 1 interconnector<" in h or "weighted flow on 1 interconnector" in h
    assert "interconnectors" not in h.split("Right side")[0]
    assert "≤" in h
    assert "( the limit − measured flows − margin )" in h
    assert " × (" not in h and "current output" not in h        # scale 1, K = 0
    meta.update(scale=0.857, n_feedback=6, constrainttype=">=", n_lhs_interconnectors=2)
    h = network.equation_block(meta)
    assert "0.857 × ( the limit − measured flows − margin )" in h
    assert "+ the same weighted sum at current output (6 terms)" in h
    assert "≥" in h and "weighted flow on 2 interconnectors" in h
    meta.update(constrainttype="=", n_lhs_interconnectors=0)
    h = network.equation_block(meta)
    assert "interconnector" not in h and "=" in h


def test_equation_view_renders_sections(dbs):
    add_equation(dbs)
    h = network.area_html("A1", "30d", view="equation")
    assert NOT_BUILT not in h
    for s in ("In plain English", "The equation", "Left side: what AEMO can change",
              "Right side: the limit", "When it binds, by hour of day"):
        assert s in h
    assert "This limit protects the Alpha line." in h
    assert "Awaiting review" not in h and "awaiting review" not in h
    assert "1,230" in h and "(middle value)" in h and "900" in h and "1,500" in h
    assert 'class="mono"' in h and ">C1<" in h
    assert "Network as built" in h                        # cause display label
    assert h.count('class="hbar"') == 24 or h.count("<span title=") >= 24
    assert "the exact version" in h
    assert "GENERICCONSTRAINTRHS" in h and "EMSMASTER" in h
    assert "version 3" in h and "2026-09-01" in h


def test_has_operations_sentence(dbs):
    add_equation(dbs, ops=True)
    assert "short sequence of operations" in network.area_html("A1", "30d", view="equation")


def test_earlier_version_rule_words(dbs):
    add_equation(dbs, version_rule="earlier")
    h = network.area_html("A1", "30d", view="equation")
    assert "an earlier version; AEMO's archive does not yet hold the current one" in h


def test_top_12_by_absolute_weight_and_show_all(dbs):
    add_equation(dbs)
    p = network.get_period("30d")
    rows = network.lhs_rows("C1", p)
    ab = [abs(w) for w in rows[rows["kind"] == "unit"]["factor"]]
    assert ab == sorted(ab, reverse=True)
    h = network.area_html("A1", "30d", view="equation")
    assert h.count("Site ") == 12 and "3 more" in h and "all=1" in h
    assert "Heywood" in h                                  # interconnector rows follow
    h_all = network.area_html("A1", "30d", view="equation", show_all=True)
    assert h_all.count("Site ") == 15 and "3 more" not in h_all
    # the largest |weight| is unit 0 (1.0 or -1.0): it must be in the 12
    assert "Site 0<" in h
    # integers keep their zeros in the MW column
    assert ">350<" in h


def test_lhs_counts_sentence(dbs):
    add_equation(dbs)
    h = network.area_html("A1", "30d", view="equation")
    assert "units add to the left side and" in h and "reduce it" in h
    assert "The 12 largest weights are shown, of 15" in h


def test_unit_gwh_column_uses_unit_daily(dbs):
    add_equation(dbs)
    h = network.area_html("A1", "30d", view="equation")
    # WF1 holds 3.0 GWh in the fixture's unit_daily over the period
    assert ">3.0<" in h


def test_right_side_rows_exclude_feedback_and_keep_order(dbs):
    add_equation(dbs, n_fb=1)
    h = network.area_html("A1", "30d", view="equation")
    assert h.index("A_RATING") < h.index("MEAS_1") and "FB_1" not in h
    assert "−0.989" in h
    assert "also repeats 1 of the left-side terms" in h


def test_stale_note_banner(dbs):
    add_equation(dbs, stale=True)
    h = network.area_html("A1", "30d", view="equation")
    assert ("Written for the version effective 2026-09-01. AEMO has since changed this "
            "equation's structure; the paragraph is awaiting review.") in h
    assert h.index("awaiting review") < h.index("This limit protects the Alpha line.")


def test_no_note_shows_nothing(dbs):
    add_equation(dbs, note=False)
    h = network.area_html("A1", "30d", view="equation")
    assert "In plain English" not in h


def test_note_only_when_selected_constraint_is_the_notes_constraint(dbs):
    add_equation(dbs)
    h = network.area_html("A1", "30d", view="equation", constraint="C1B")
    assert "This limit protects the Alpha line." not in h


def test_picker_orders_by_energy_and_falls_back(dbs):
    add_equation(dbs)
    # C1B has energy 120 (12 intervals x 10) and C1 has 240: C1 first
    p = network.get_period("30d")
    pk = network.constraint_picker("A1", p)
    assert list(pk["constraintid"]) == ["C1", "C1B"]
    h = network.area_html("A1", "30d", view="equation", constraint="C2")   # other area
    assert 'class="mono">C1<' in h or ">C1<" in h
    h2 = network.area_html("A1", "30d", view="equation", constraint="NOPE")
    assert ">C1<" in h2
    h3 = network.area_html("A1", "30d", view="equation", constraint="C1B")
    assert "No equation is stored for this constraint." in h3


def test_picker_default_is_lead_even_if_lower_in_order(dbs):
    add_equation(dbs)
    _x(dbs, "UPDATE area SET lead_constraintid = 'C1B' WHERE area_id = 'A1'")
    h = network.area_html("A1", "30d", view="equation")
    assert "No equation is stored for this constraint." in h      # C1B is selected


# ---- sort choice --------------------------------------------------------------------------

def _sort_case(dbs):
    """X: many hours, little energy. Y: few hours, much energy."""
    for aid, n, mwh in (("X", 900, 10.0), ("Y", 60, 90000.0)):
        _x(dbs, f"INSERT INTO area VALUES ('{aid}','Area {aid}','NSW','Thermal',1,1,10,0)",
           f"INSERT INTO area_daily VALUES (DATE '2026-11-04','{aid}',{n},{n},0,0,{mwh},1.0)")


def test_energy_sort_differs_from_hours_sort(dbs):
    _sort_case(dbs)
    p = network.get_period("30d")
    by_h = list(network.hotspot_areas(p, "NSW1", sort="hours")["area_id"])
    by_e = list(network.hotspot_areas(p, "NSW1", sort="energy")["area_id"])
    assert by_h[0] == "X" and by_e[0] == "Y" and by_h != by_e
    assert list(network.hotspot_areas(p, "NSW1")["area_id"]) == by_h      # default hours
    assert network.normalise_sort("bogus") == "hours" and network.normalise_sort("energy") == "energy"


def test_hotspots_html_follows_sort_and_shows_both(dbs):
    _sort_case(dbs)
    h = network.hotspots_html("NSW1", "30d", sort="energy")
    assert "by energy held back" in h and "by binding hours" not in h
    assert h.index("Area Y") < h.index("Area X")
    h = network.hotspots_html("NSW1", "30d")
    assert "by binding hours" in h and h.index("Area X") < h.index("Area Y")
    assert "GWh curtailed" in h and ">Hours<" in h


def test_map_top_lines_and_side_list_follow_sort(dbs, geo):
    _sort_case(dbs)
    g = dict(GEO, area_lines=dict(GEO["area_lines"], X=[_line("LX", 148, -33)],
                                  Y=[_line("LY", 149, -34)]))
    import json
    geo.write_text(json.dumps(g))
    import os
    os.utime(geo, (1_900_000_100, 1_900_000_100))
    me = network.build_map("NSW1", "30d", "pct", None, sort="energy")
    mh = network.build_map("NSW1", "30d", "pct", None, sort="hours")
    ids = lambda m: [r["area_id"] for r in m["side"]]
    assert ids(me) == ["Y", "A1", "X"]            # energy: 90 GWh, 1.5 GWh, 0.01 GWh
    assert ids(mh) == ["X", "Y", "A1"]            # hours: 75 h, 5 h, 3 h
    line_names = lambda m: [t.name for t in m["fig"].data if t.name in ("Area X", "Area Y")]
    assert line_names(me) == ["Area Y", "Area X"] and line_names(mh) == ["Area X", "Area Y"]
    # ramp: the first-ranked area is the darkest under either key
    col = lambda m, n: [t.line.color for t in m["fig"].data if t.name == n][0]
    assert col(me, "Area Y") == "#6C201C" and col(mh, "Area X") == "#6C201C"
    assert "by energy held back" in network.map_html("NSW1", "30d", "pct", None, sort="energy")
    assert "by binding hours" in network.map_html("NSW1", "30d", "pct", None, sort="hours")
    html = network.map_html("NSW1", "30d", "pct", None, sort="energy")
    assert "/network/area?id=Y&amp;range=30d&amp;view=equation" in html


# ---- rank and movement -------------------------------------------------------------------------

def test_rank_movement_symbols():
    assert network.rank_movement(5, 8) == "▲3"
    assert network.rank_movement(8, 5) == "▼3"
    assert network.rank_movement(5, 5) == "–"
    assert network.rank_movement(5, None) == "new"
    assert network.rank_movement(None, 3) == "–"


def _ranks(dbs, rows):
    _x(dbs, "CREATE TABLE area_rank(\"window\" VARCHAR, area_id VARCHAR, gwh DOUBLE, "
            "hours DOUBLE, rank_energy BIGINT, rank_hours BIGINT)")
    c = duckdb.connect(str(dbs / "s.duckdb"))
    for r in rows:
        c.execute("INSERT INTO area_rank VALUES (?,?,?,?,?,?)", list(r))
    c.close()


def test_rising_rule(dbs):
    _ranks(dbs, [("90d", "A1", 5, 5, 1, 1), ("12m", "A1", 5, 5, 1, 1),            # steady
                 ("90d", "A2", 5, 5, 3, 3), ("12m", "A2", 5, 5, 9, 9),            # rising
                 ("90d", "NEWB", 5, 5, 4, 4),                                     # no 12m row: rising
                 ("90d", "OUT", 5, 5, 6, 6), ("12m", "OUT", 5, 5, 20, 20)])       # not in 90d top 5
    ranks = network.load_ranks()
    assert network.rising_areas(ranks) == {"A2", "NEWB"}


def test_hotspots_rank_column_and_tag(dbs):
    _ranks(dbs, [("12m", "A1", 1, 1, 2, 2), ("12m_prev", "A1", 1, 1, 5, 5),
                 ("12m", "A2", 1, 1, 9, 9), ("12m_prev", "A2", 1, 1, 4, 4),
                 ("90d", "A2", 1, 1, 3, 3)])
    h = network.hotspots_html("NEM", "30d")
    assert "12-month rank" in h and "▲3" in h and "▼5" in h
    assert "rising" in h
    assert "movement" in h.lower()
    h2 = network.hotspots_html("NEM", "30d", sort="hours")
    assert "12-month rank" in h2


def test_rank_by_chosen_measure(dbs):
    _ranks(dbs, [("12m", "A1", 1, 1, 7, 2), ("12m_prev", "A1", 1, 1, 7, 4)])
    he = network.hotspots_html("NEM", "30d", sort="energy")
    hh = network.hotspots_html("NEM", "30d", sort="hours")
    assert "▲2" in hh and "▲2" not in he


def test_new_label_for_area_missing_from_prev_window(dbs):
    _ranks(dbs, [("12m", "A1", 1, 1, 2, 2)])
    assert ">new<" in network.hotspots_html("NEM", "30d")


def test_map_side_list_rising_tag(dbs, geo):
    _ranks(dbs, [("90d", "A1", 1, 1, 2, 2), ("12m", "A1", 1, 1, 9, 9)])
    h = network.map_html("NEM", "30d", "pct", None)
    assert ">rising<" in h
