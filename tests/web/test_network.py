"""Unit tests for web/network.py against tiny DuckDB files built in tmp."""
from __future__ import annotations

from datetime import date, datetime

import duckdb
import pandas as pd
import pytest

from aemo_dashboard.web import network

DATA_FROM = datetime(2024, 7, 1, 0, 5)
DATA_THROUGH = datetime(2026, 11, 24, 14, 35)


def _days(start: str, end: str):
    return pd.date_range(start, end, freq="D").date


def make_summary(path, optional: bool = False, through=DATA_THROUGH):
    c = duckdb.connect(str(path))
    c.execute("CREATE TABLE meta(built_at TIMESTAMP, data_from TIMESTAMP, "
              "data_through TIMESTAMP, n_binding_rows BIGINT, build_seconds DOUBLE)")
    c.execute("INSERT INTO meta VALUES (?, ?, ?, 1, 1.0)",
              [datetime(2026, 11, 24, 15, 33), DATA_FROM, through])
    c.execute("CREATE TABLE intervals_daily(day DATE, n_intervals BIGINT)")
    for d in _days("2024-07-01", "2026-11-23"):
        c.execute("INSERT INTO intervals_daily VALUES (?, 288)", [d])
    extra = (", units_mw DOUBLE, top_constraintid VARCHAR, description VARCHAR"
             if optional else "")
    c.execute("CREATE TABLE area(area_id VARCHAR, name VARCHAR, region VARCHAR, "
              "limittype VARCHAR, n_constraints BIGINT, n_units BIGINT, "
              f"weighted_mw DOUBLE, missing_capacity_units BIGINT{extra})")
    rows = [("A1", "Alpha to Beta (1)", "NSW", "Thermal", 2, 3, 500.0, 0),
            ("A2", "Gamma to Delta (2)", "QLD", "Voltage Stability", 1, 1, 80.0, 0)]
    for r in rows:
        if optional:
            r = r + (900.0 if r[0] == "A1" else 90.0, "C1" if r[0] == "A1" else "C2",
                     "Alpha line thermal limit" if r[0] == "A1" else None)
        c.execute(f"INSERT INTO area VALUES ({','.join('?' * len(r))})", list(r))
    c.execute("CREATE TABLE area_units(area_id VARCHAR, duid VARCHAR, site VARCHAR, "
              "fuel VARCHAR, region VARCHAR, capacity_mw DOUBLE, max_factor DOUBLE, "
              "direction VARCHAR)" if not optional else
              "CREATE TABLE area_units(area_id VARCHAR, duid VARCHAR, site VARCHAR, "
              "fuel VARCHAR, region VARCHAR, capacity_mw DOUBLE, max_factor DOUBLE, "
              "direction VARCHAR, max_rel_weight DOUBLE)")
    for u in [("A1", "WF1", "Windy", "Wind", "NSW1", 300.0, 1.0, "discharge"),
              ("A1", "BT1", "Battery One", "Battery Storage", "NSW1", 100.0, 1.0, "discharge"),
              ("A2", "SF1", "Sunny", "Solar", "QLD1", 80.0, 0.5, "discharge")]:
        u = u + ((0.9,) if optional else ())
        c.execute(f"INSERT INTO area_units VALUES ({','.join('?' * len(u))})", list(u))
    c.execute("CREATE TABLE area_daily(day DATE, area_id VARCHAR, n_intervals BIGINT, "
              "n_sysnormal BIGINT, n_outage BIGINT, n_unit_specific BIGINT, "
              "constrained_mwh DOUBLE, constrained_value DOUBLE)")
    ad = [("2026-11-01", "A1", 24, 12, 12, 0, 1000.0, 50000.0),
          ("2026-11-10", "A1", 12, 6, 0, 6, 500.0, 30000.0),
          ("2026-11-05", "A2", 6, 6, 0, 0, 0.0, 0.0),
          ("2026-01-05", "A2", 60, 60, 0, 0, 0.0, 0.0)]
    for r in ad:
        c.execute("INSERT INTO area_daily VALUES (?,?,?,?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE constraint_meta(constraintid VARCHAR, class VARCHAR, "
              "region VARCHAR, description VARCHAR, limittype VARCHAR, impact VARCHAR, "
              "constrainttype VARCHAR, cause VARCHAR, element VARCHAR, area_id VARCHAR, "
              "definition_rule VARCHAR)")
    for r in [("C1", "network", "NSW", "Alpha-Beta line", "Thermal", "x", "<=",
               "System normal", "e", "A1", "exact"),
              ("C1B", "network", "NSW", "", "Thermal", "x", "<=",
               "Outage", "e", "A1", "none"),
              ("C2", "network", "QLD", "Gamma-Delta", "Voltage Stability", "x", "<=",
               "System normal", "e", "A2", "exact")]:
        c.execute("INSERT INTO constraint_meta VALUES (?,?,?,?,?,?,?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE constraint_daily(day DATE, constraintid VARCHAR, "
              "n_intervals BIGINT, median_abs_mv DOUBLE)")
    for r in [("2026-11-01", "C1", 12, 5.0), ("2026-11-01", "C1B", 12, 5.0),
              ("2026-11-10", "C1", 12, 5.0), ("2026-11-05", "C2", 6, 1.0)]:
        c.execute("INSERT INTO constraint_daily VALUES (?,?,?,?)", list(r))
    c.execute("CREATE TABLE cause_daily(day DATE, region VARCHAR, cause VARCHAR, "
              "limittype VARCHAR, n_constraint_intervals BIGINT)")
    for r in [("2026-11-01", "NSW", "System normal", "Thermal", 120),
              ("2026-11-01", "NSW", "Outage", "Thermal", 60),
              ("2026-11-05", "QLD", "Unit-specific limit", "Other", 24)]:
        c.execute("INSERT INTO cause_daily VALUES (?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE unit_daily(day DATE, duid VARCHAR, n_discharge_limited BIGINT, "
              "n_charge_limited BIGINT, constrained_mwh DOUBLE, other_curtailed_mwh DOUBLE, "
              "constrained_value DOUBLE, n_local_price BIGINT, median_local_price_adj DOUBLE)")
    for r in [("2026-11-01", "WF1", 288, 0, 2000.0, 100.0, 90000.0, 288, -50.0),
              ("2026-11-02", "WF1", 144, 0, 1000.0, 0.0, 45000.0, 144, -20.0),
              ("2026-11-01", "BT1", 100, 40, 0.0, 0.0, 0.0, 0, None),
              ("2026-11-02", "SF1", 10, 0, 5.0, 1.0, 100.0, 0, None),
              ("2026-11-02", "TINY", 400, 0, 1.0, 0.0, 10.0, 0, None)]:
        c.execute("INSERT INTO unit_daily VALUES (?,?,?,?,?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE unit_constraint_daily(day DATE, duid VARCHAR, "
              "constraintid VARCHAR, direction VARCHAR, n_intervals BIGINT)")
    for r in [("2026-11-01", "WF1", "C1", "discharge", 288),
              ("2026-11-02", "WF1", "C1", "discharge", 144),
              ("2026-11-01", "BT1", "C1", "discharge", 100),
              ("2026-11-01", "BT1", "C1B", "charge", 40)]:
        c.execute("INSERT INTO unit_constraint_daily VALUES (?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE unit_hour_month(month DATE, duid VARCHAR, hour BIGINT, "
              "direction VARCHAR, n_intervals BIGINT)")
    for r in [("2026-11-01", "BT1", 18, "discharge", 90), ("2026-11-01", "BT1", 3, "charge", 30),
              ("2026-05-01", "BT1", 18, "discharge", 500)]:
        c.execute("INSERT INTO unit_hour_month VALUES (?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE outage_area(outageid BIGINT, genconsetid VARCHAR, "
              "area_id VARCHAR, substationid VARCHAR, equipmenttype VARCHAR, "
              "equipmentid VARCHAR, starttime TIMESTAMP, endtime TIMESTAMP, "
              "outagestatuscode VARCHAR, reason VARCHAR, n_binding_intervals BIGINT)")
    for r in [(1, "G", "A1", "SUB1", "LINE", "L1", "2026-11-01 07:00", "2026-11-05 17:00",
               "COMPLETE", "maintenance", 120),
              (1, "G", "A1", "SUB2", "LINE", "L1", "2026-11-01 07:00", "2026-11-05 17:00",
               "COMPLETE", "maintenance", 120),
              (2, "G", "A1", "SUB3", "LINE", "L9", "2025-01-01 07:00", "2025-01-05 17:00",
               "COMPLETE", "old", 500),
              (3, "G", "A1", "SUB4", "LINE", "L7", "2026-11-02 07:00", "2026-11-03 17:00",
               "WDRAWN", "never happened", 999)]:
        c.execute("INSERT INTO outage_area VALUES (?,?,?,?,?,?,?,?,?,?,?)", list(r))
    c.close()


def make_aemo(path):
    c = duckdb.connect(str(path))
    c.execute('CREATE TABLE duid_mapping(duid VARCHAR, region VARCHAR, "site name" VARCHAR, '
              "owner VARCHAR, capacity_mw DOUBLE, storage_mwh DOUBLE, fuel VARCHAR)")
    for r in [("WF1", "NSW1", "Windy", "o", 300.0, None, "Wind"),
              ("BT1", "NSW1", "Battery One", "o", 100.0, 200.0, "Battery Storage"),
              ("SF1", "QLD1", "Sunny", "o", 80.0, None, "Solar"),
              ("TINY", "NSW1", "Tiny", "o", 2.0, None, "Wind")]:
        c.execute("INSERT INTO duid_mapping VALUES (?,?,?,?,?,?,?)", list(r))
    c.close()


def make_constraints(path, ts=datetime(2026, 11, 24, 14, 35)):
    c = duckdb.connect(str(path))
    c.execute("CREATE TABLE constraint_binding5(settlementdate TIMESTAMP, constraintid VARCHAR, "
              "intervention BIGINT, rhs DOUBLE, marginalvalue DOUBLE, violationdegree DOUBLE, "
              "lhs DOUBLE)")
    for r in [(ts, "C1", 0, 1.0, 55.0, 0.0, 1.0), (ts, "F_X", 0, 1.0, 5.0, 0.0, 1.0),
              (ts, "#Q", 0, 1.0, 5.0, 0.0, 1.0), (ts, "$U", 0, 1.0, 5.0, 0.0, 1.0),
              (ts, "C2", 1, 1.0, 5.0, 0.0, 1.0), (ts, "C1B", 0, 1.0, 0.0, 0.0, 1.0),
              (ts, "NEWC", 0, 1.0, -12.0, 0.0, 1.0),
              (ts, "BIGMV", 0, 1.0, -26912000.0, 0.0, 1.0)]:
        c.execute("INSERT INTO constraint_binding5 VALUES (?,?,?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE local_price5(settlementdate TIMESTAMP, duid VARCHAR, "
              "local_price_adjustment DOUBLE, locally_constrained BIGINT)")
    for r in [(ts, "WF1", -300.0, 1), (ts, "BT1", -10.0, 1), (ts, "UNKNOWN", -999.0, 1)]:
        c.execute("INSERT INTO local_price5 VALUES (?,?,?,?)", list(r))
    c.execute("CREATE TABLE network_outagedetail(outageid BIGINT, substationid VARCHAR, "
              "equipmenttype VARCHAR, equipmentid VARCHAR, starttime TIMESTAMP, "
              "endtime TIMESTAMP, outagestatuscode VARCHAR, reason VARCHAR, "
              "actual_starttime TIMESTAMP, actual_endtime TIMESTAMP)")
    n = "2026-11-24 14:35"
    for r in [(10, "SUBA", "LINE", "LA", "2026-11-24 07:00", "2026-11-24 17:00", "PTP", "work", None, None),
              (10, "SUBB", "LINE", "LA", "2026-11-24 07:00", "2026-11-24 17:00", "PTP", "work", None, None),
              (11, "SUBC", "LINE", "LC", "2026-11-23 07:00", "2026-11-25 17:00", "WDRAWN", "x", None, None),
              (12, "SUBD", "LINE", "LD", "2026-11-23 07:00", "2026-11-25 17:00", "COMPLETE", "x", None, None),
              (13, "SUBE", "LINE", "LE", "2026-12-23 07:00", "2026-12-25 17:00", "PTP", "future", None, None),
              (14, "SUBF", "LINE", "LF", "2026-11-20 07:00", "2026-11-26 17:00", "PTP", "older", None, None),
              (15, "SUBG", "N/A", "N/A", "2026-11-24 09:00", "2026-11-24 17:00", "PTP", "blank equipment", None, None),
              (16, "SUBH", "LINE", "", "2026-11-24 10:00", "2026-11-24 17:00", "PTP", "blank id", None, None)]:
        c.execute("INSERT INTO network_outagedetail VALUES (?,?,?,?,?,?,?,?,?,?)", list(r))
    c.execute("CREATE TABLE network_outageconstraintset(outageid BIGINT, genconsetid VARCHAR, "
              "startinterval TIMESTAMP, endinterval TIMESTAMP)")
    for r in [(14, "N-SET_A", "2026-11-20 07:00", "2026-11-26 17:00"),
              (14, "N-SET_B", "2026-11-20 07:00", "2026-11-26 17:00"),
              (10, "N-OLD", "2026-01-01 00:00", "2026-01-02 00:00")]:   # not current
        c.execute("INSERT INTO network_outageconstraintset VALUES (?,?,?,?)", list(r))
    c.close()


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    make_summary(tmp_path / "s.duckdb")
    make_aemo(tmp_path / "a.duckdb")
    make_constraints(tmp_path / "c.duckdb")
    monkeypatch.setattr(network, "SUMMARY_DB", str(tmp_path / "s.duckdb"))
    monkeypatch.setattr(network, "AEMO_DB", str(tmp_path / "a.duckdb"))
    monkeypatch.setattr(network, "CONSTRAINTS_DB", str(tmp_path / "c.duckdb"))
    monkeypatch.setattr(network, "_nem_now",
                        lambda: pd.Timestamp("2026-11-24 14:40"))
    return tmp_path


# ---- period window arithmetic ----------------------------------------------

def test_last_complete_day_mid_day():
    assert network.last_complete_day(pd.Timestamp("2026-11-24 14:35")) == date(2026, 11, 23)


def test_last_complete_day_at_midnight():
    # interval ending 00:00 on the 25th is the last interval of the 24th
    assert network.last_complete_day(pd.Timestamp("2026-11-25 00:00")) == date(2026, 11, 24)


def test_period_30d_is_30_days_ending_last_complete_day():
    p = network.period_window("30d", pd.Timestamp(DATA_FROM), pd.Timestamp(DATA_THROUGH))
    assert (p.end, p.start) == (date(2026, 11, 23), date(2026, 10, 25))
    assert (p.end - p.start).days + 1 == 30


def test_period_ytd_1y_all_and_default():
    f, t = pd.Timestamp(DATA_FROM), pd.Timestamp(DATA_THROUGH)
    assert network.period_window("ytd", f, t).start == date(2026, 1, 1)
    assert network.period_window("1y", f, t).start == date(2025, 11, 24)
    assert network.period_window("all", f, t).start == date(2024, 7, 1)
    assert network.period_window("bogus", f, t) == network.period_window("90d", f, t)


def test_period_clipped_to_data_from():
    p = network.period_window("1y", pd.Timestamp("2026-06-01 00:05"),
                              pd.Timestamp("2026-11-24 14:35"))
    assert p.start == date(2026, 6, 1)


# ---- region / range normalising ---------------------------------------------

def test_region_normalising_and_labels():
    assert network.normalise_region("NSW1") == "NSW1"
    assert network.normalise_region("XXX") == "NEM"
    assert network.area_region("NEM") is None
    assert network.area_region("QLD1") == "QLD"
    assert network.normalise_range("bogus") == "90d"
    assert network.normalise_range("all") == "all"
    assert network.normalise_fuel("nope") == "all"


def test_cause_shares_sum_to_100():
    s = network.cause_shares(12, 12, 0)
    assert sum(s) == pytest.approx(100.0) and s == (50.0, 50.0, 0.0)
    s = network.cause_shares(1, 1, 1)
    assert sum(s) == pytest.approx(100.0)
    assert network.cause_shares(0, 0, 0) == (0.0, 0.0, 0.0)


# ---- hotspots ----------------------------------------------------------------

def test_hotspot_hours_and_percentages(dbs):
    p = network.get_period("30d")
    df = network.hotspot_areas(p, "NEM")
    assert list(df["area_id"]) == ["A1", "A2"]          # ranked by hours
    a1 = df.iloc[0]
    assert a1["hours"] == pytest.approx(36 / 12)
    assert a1["pct_period"] == pytest.approx(36 / (30 * 288) * 100)
    assert a1["sn"] + a1["ou"] + a1["us"] == 36
    assert a1["units_mw"] == 500.0                       # falls back to weighted_mw
    assert df.iloc[1]["hours"] == pytest.approx(0.5)     # the Jan row is outside 30d


def test_hotspot_region_filter(dbs):
    p = network.get_period("30d")
    assert list(network.hotspot_areas(p, "NSW1")["area_id"]) == ["A1"]
    assert list(network.hotspot_areas(p, "QLD1")["area_id"]) == ["A2"]
    assert network.hotspot_areas(p, "TAS1").empty


def test_hotspot_period_longer_includes_old_rows(dbs):
    df = network.hotspot_areas(network.get_period("ytd"), "QLD1")
    assert df.iloc[0]["hours"] == pytest.approx(66 / 12)


def test_hotspots_html_renders(dbs):
    h = network.hotspots_html("NEM", "30d")
    assert '<div class="netw">' in h
    assert "Alpha to Beta (1)" in h and "/network/area?id=A1" in h
    assert "Plotly.newPlot" in h
    assert '<td class="r">3</td>' in h                    # hours of A1, no decimals


def test_hotspots_region_with_no_areas_gives_placeholder(dbs):
    h = network.hotspots_html("TAS1", "30d")
    assert 'class="placeholder"' in h


def test_optional_area_columns_used_when_present(tmp_path, monkeypatch):
    make_summary(tmp_path / "s.duckdb", optional=True)
    monkeypatch.setattr(network, "SUMMARY_DB", str(tmp_path / "s.duckdb"))
    df = network.hotspot_areas(network.get_period("30d"), "NEM")
    assert df.iloc[0]["units_mw"] == 900.0
    h = network.area_html("A1", "30d")
    assert "Alpha line thermal limit" in h
    assert "0.9" in h                                     # max_rel_weight


def test_missing_optional_columns_do_not_break_pages(dbs):
    assert "Alpha to Beta (1)" in network.area_html("A1", "30d")
    assert "Alpha-Beta line" in network.area_html("A1", "30d")   # description fallback


def test_empty_period_gives_placeholder_not_exception(tmp_path, monkeypatch):
    # data_through before the first interval: no complete day exists
    make_summary(tmp_path / "s.duckdb", through=datetime(2024, 7, 1, 0, 5))
    monkeypatch.setattr(network, "SUMMARY_DB", str(tmp_path / "s.duckdb"))
    for h in (network.hotspots_html("NEM", "30d"), network.causes_html("NEM", "30d")):
        assert 'class="placeholder"' in h


def test_missing_database_gives_placeholder(tmp_path, monkeypatch):
    monkeypatch.setattr(network, "SUMMARY_DB", str(tmp_path / "absent.duckdb"))
    assert 'class="placeholder"' in network.hotspots_html("NEM", "30d")


# ---- area page ---------------------------------------------------------------

def test_area_page_contents(dbs):
    h = network.area_html("A1", "30d")
    assert "Windy" in h and "/network/unit?duid=WF1" in h
    assert "C1B" in h and "description pending" in h       # definition_rule none
    assert "maintenance" in h                              # outage in period
    assert "never happened" not in h                       # withdrawn
    assert ">old<" not in h                                # outside period


def test_area_unknown_id_placeholder(dbs):
    h = network.area_html("NOPE", "30d")
    assert 'class="placeholder"' in h and "NOPE" in h


def test_area_id_is_escaped(dbs):
    assert "<script>x" not in network.area_html("<script>x", "30d")


# ---- causes ------------------------------------------------------------------

def test_cause_totals(dbs):
    t = network.cause_totals(network.get_period("30d"), "NEM")
    assert t["by_cause"]["System normal"] == 120
    assert t["by_cause"]["Outage"] == 60
    t = network.cause_totals(network.get_period("30d"), "QLD1")
    assert t["by_cause"]["Unit-specific limit"] == 24 and t["by_cause"]["Outage"] == 0


def test_causes_html_outage_table(dbs):
    h = network.causes_html("NEM", "30d")
    assert "maintenance" in h and "Alpha to Beta (1)" in h
    assert h.count("Plotly.newPlot") == 4


# ---- units -------------------------------------------------------------------

def test_unit_league_rates_and_filters(dbs):
    p = network.get_period("30d")
    df = network.unit_league(p, "NEM", "all")
    assert "TINY" not in set(df["duid"])                   # < 5 MW hidden
    wf = df[df["duid"] == "WF1"].iloc[0]
    assert wf["pct_dis"] == pytest.approx(432 / (30 * 288) * 100)
    assert wf["gwh_con"] == pytest.approx(3.0)
    assert wf["value_m"] == pytest.approx(0.135)
    assert wf["median_adj"] == pytest.approx(-50.0)   # weighted by n_local_price: -50 (288) vs -20 (144) -> median -50
    assert df.iloc[0]["duid"] == "WF1"                     # ranked by share
    assert list(network.unit_league(p, "NEM", "battery")["duid"]) == ["BT1"]
    assert list(network.unit_league(p, "QLD1", "all")["duid"]) == ["SF1"]
    assert list(network.unit_league(p, "NEM", "wind")["duid"]) == ["WF1"]


def test_units_html(dbs):
    h = network.units_html("NEM", "battery", "30d")
    assert "Battery One" in h and "/network/unit?duid=BT1" in h
    assert "charge-limited" in h


def test_unit_page_and_unknown(dbs):
    h = network.unit_html("BT1", "30d")
    assert "Battery One" in h and "Plotly.newPlot" in h and "/network/area?id=A1" in h
    assert 'class="placeholder"' in network.unit_html("ZZZ", "30d")


def test_unit_hour_profile_uses_months_overlapping_period(dbs):
    p = network.get_period("30d")        # 25 Oct - 23 Nov: Oct and Nov months only
    prof = network.unit_hour_profile("BT1", p)
    row = prof[prof["hour"] == 18].iloc[0]
    assert row["discharge"] == pytest.approx(90 / (network.days_in_months(p) * 12) * 100)
    assert prof["discharge"].max() == pytest.approx(row["discharge"])   # May row (500) excluded


# ---- now ---------------------------------------------------------------------

def test_now_page(dbs):
    h = network.now_html()
    assert "age 5 min" in h or "5 min" in h
    assert "C1" in h and "Alpha to Beta (1)" in h and "55" in h
    assert "NEWC" in h and "description pending" in h       # unknown to meta
    assert "F_X" not in h and "#Q" not in h and "$U" not in h
    assert ">C2<" not in h                                  # intervention <> 0
    assert "C1B" not in h                                   # marginal value 0
    assert "/pasa/transmission" in h
    # unit table: largest |adj| first, left join keeps unknown duid
    assert h.index("Windy") < h.index("Battery One")
    assert "UNKNOWN" in h


def test_now_outage_rule(dbs):
    df = network.outages_in_progress(pd.Timestamp("2026-11-24 14:40"))
    # not withdrawn / complete / future / blank equipment (15, 16)
    assert set(df["outageid"]) == {10, 14}
    # the outage that invoked a constraint set comes first, though it started earlier
    assert list(df["outageid"]) == [14, 10]
    assert df.attrs["total"] == 2
    row = df[df["outageid"] == 10].iloc[0]
    assert "SUBA" in row["substation"] and "SUBB" in row["substation"]
    assert df.iloc[0]["constraint_set"] == "N-SET_A, N-SET_B"
    assert pd.isna(row["constraint_set"])                  # N-OLD is not current


def test_now_outage_cap_and_total(dbs):
    df = network.outages_in_progress(pd.Timestamp("2026-11-24 14:40"), limit=1)
    assert len(df) == 1 and df.attrs["total"] == 2


def test_now_violation_penalty_and_floor_sentence(dbs):
    h = network.now_html()
    assert "at violation penalty" in h and "26,912,000" not in h
    assert "−$1,000 market floor" in h and "not a measure of economic cost" in h
    assert "N-SET_A, N-SET_B" in h and "Constraint set" in h
    assert "blank equipment" not in h and "2 in progress" in h


# ---- display labels, key and number formats -------------------------------------

def test_number_formats():
    assert network.fhours(809.6) == "810" and network.fhours(1438.7) == "1,439"
    assert network.fgwh(448.2) == "448" and network.fgwh(16.43) == "16.4"
    assert network.fgwh(99.96) == "100.0" or network.fgwh(99.96) == "100"
    assert network.fpct(9.87) == "9.9%" and network.fpct(37.5) == "38%"
    assert network.fnum(11954.4) == "11,954"


def test_cause_labels_displayed_not_stored_names(dbs):
    for h in (network.hotspots_html("NEM", "30d"), network.causes_html("NEM", "30d"),
              network.area_html("A1", "30d")):
        assert "Network as built" in h and "Equipment outage" in h
        assert "Limit on a single plant" in h
        assert "Unit-specific" not in h and "System normal" not in h
        assert "Single-plant limit share" in h or "causes" in h


def test_cause_key_text(dbs):
    for h in (network.hotspots_html("NEM", "30d"), network.causes_html("NEM", "30d")):
        assert ("Network as built:</b> the limit binds with every line and transformer in "
                "service. AEMO marks these “Out = Nil”.") in h
        assert ("Equipment outage:</b> the limit applies only while a named line or "
                "transformer is out of service.") in h
        assert ("Limit on a single plant:</b> AEMO caps one wind or solar farm. Most are "
                "system strength limits, which depend on how many coal, gas or hydro units "
                "and synchronous condensers are running nearby.") in h
    h = network.hotspots_html("NEM", "30d")
    assert "Network as built share" in h and "Equipment outage share" in h
    assert "Hours in period" not in h


def _extra(tmp_path, sql):
    c = duckdb.connect(str(tmp_path / "s.duckdb"))
    c.execute(sql)
    c.close()


def test_hotspots_no_unit_terms_show_interconnector(dbs):
    _extra(dbs, "INSERT INTO area VALUES ('A3','Link area','SA','Thermal',1,0,0,0)")
    _extra(dbs, "INSERT INTO area_daily VALUES (DATE '2026-11-03','A3',48,48,0,0,0,0)")
    h = network.hotspots_html("NEM", "30d")
    assert 'colspan="2"' in h and ">interconnector<" in h
    assert "MW in constraints" in h and "Units in constraints" in h


def test_hotspots_uses_interconnectors_column(tmp_path, monkeypatch):
    make_summary(tmp_path / "s.duckdb", optional=True)
    monkeypatch.setattr(network, "SUMMARY_DB", str(tmp_path / "s.duckdb"))
    _extra(tmp_path, "ALTER TABLE area ADD COLUMN interconnectors VARCHAR")
    _extra(tmp_path, "INSERT INTO area VALUES ('A3','Link area','SA','Thermal',1,0,0,0,0,"
                     "'C3',NULL,'Heywood')")
    _extra(tmp_path, "INSERT INTO area_daily VALUES (DATE '2026-11-03','A3',48,48,0,0,0,0)")
    assert ">Heywood<" in network.hotspots_html("NEM", "30d")


def test_unit_constraints_capped_at_15(dbs):
    for i in range(20):
        _extra(dbs, f"INSERT INTO unit_constraint_daily VALUES (DATE '2026-11-03','BT1',"
                    f"'X{i:02d}','discharge',{10 + i})")
    h = network.unit_html("BT1", "30d")
    assert h.count(">X") == 13                 # 15 shown, two of them are C1 and C1B
    assert "7 others also included this unit" in h
    assert 'class="wrap260"' in h


def test_area_constraints_capped_at_25(dbs):
    for i in range(30):
        _extra(dbs, f"INSERT INTO constraint_meta VALUES ('Y{i:02d}','network','NSW','d','Thermal',"
                    f"'x','<=','Outage','e','A1','exact')")
        _extra(dbs, f"INSERT INTO constraint_daily VALUES (DATE '2026-11-03','Y{i:02d}',{12 + i},1.0)")
    h = network.area_html("A1", "30d")
    assert "7 others also bound" in h and h.count(">Y") == 24
    assert "Most frequent constraint:" in h and 'class="mono"' in h
