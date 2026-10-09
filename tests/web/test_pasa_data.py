"""Tests for the pure-pandas PASA outage logic behind the PASA tab and the
Today outage tile.

Definitions under test: MW out = max(0, capacity - PASA availability); a unit
is out when MW out >= threshold; scheduled fuels only; outage type and longer
return from MT-PASA unit state.
"""
from __future__ import annotations

import pandas as pd
import pytest

from aemo_dashboard.web import pasa_data as pd_mod

NOW = pd.Timestamp("2026-10-10 12:00")


def _units():
    return pd.DataFrame({
        "duid": ["COAL1", "GAS1", "HYD1", "WIND1", "BATT1", "PUMP1", "OTH1"],
        "site_name": ["Coal Stn", "Gas Stn", "Hydro Stn", "Wind Farm", "Big Batt",
                      "Pump", "Other"],
        "region": ["QLD1", "NSW1", "TAS1", "SA1", "VIC1", "NSW1", "NSW1"],
        "fuel": ["Coal", "CCGT", "Water", "Wind", "Battery Storage", None, "Other"],
        "capacity_mw": [700.0, 300.0, 100.0, 200.0, 150.0, 80.0, 90.0],
    })


def _st(rows):
    """rows: (duid, interval offset in half-hours from NOW, maxavail, pasaavail, recall)."""
    return pd.DataFrame([{
        "RUN_DATETIME": pd.Timestamp("2026-10-10 11:30"),
        "DUID": d, "INTERVAL_DATETIME": NOW + pd.Timedelta(minutes=30 * k),
        "GENERATION_MAX_AVAILABILITY": mx, "GENERATION_PASA_AVAILABILITY": pa,
        "GENERATION_RECALL_PERIOD": rc,
    } for d, k, mx, pa, rc in rows])


def test_economic_shutdown_is_not_out():
    st = _st([("COAL1", 0, 0.0, 700.0, 0.0)])
    out = pd_mod.current_outages(st, _units(), threshold=50, now=NOW)
    assert out.empty


def test_full_outage_is_out_at_capacity():
    st = _st([("COAL1", 0, 0.0, 0.0, 24000.0)])
    out = pd_mod.current_outages(st, _units(), threshold=50, now=NOW)
    assert list(out["duid"]) == ["COAL1"]
    assert out.iloc[0]["mw_out"] == 700.0
    assert out.iloc[0]["available_mw"] == 0.0


def test_partial_derating():
    st = _st([("GAS1", 0, 220.0, 220.0, 0.0)])
    out = pd_mod.current_outages(st, _units(), threshold=50, now=NOW)
    assert out.iloc[0]["mw_out"] == 80.0


def test_threshold_applies_per_unit():
    st = _st([("GAS1", 0, 270.0, 270.0, 0.0), ("COAL1", 0, 600.0, 600.0, 0.0)])
    out = pd_mod.current_outages(st, _units(), threshold=50, now=NOW)
    assert list(out["duid"]) == ["COAL1"]
    out = pd_mod.current_outages(st, _units(), threshold=20, now=NOW)
    assert set(out["duid"]) == {"COAL1", "GAS1"}


def test_availability_above_capacity_is_zero_out():
    st = _st([("HYD1", 0, 120.0, 120.0, 0.0)])
    assert pd_mod.current_outages(st, _units(), 50, now=NOW).empty


def test_excluded_fuels_and_null_fuel():
    st = _st([(d, 0, 0.0, 0.0, 0.0) for d in
              ["WIND1", "BATT1", "PUMP1", "OTH1", "HYD1"]])
    out = pd_mod.current_outages(st, _units(), threshold=50, now=NOW)
    assert list(out["duid"]) == ["HYD1"]


def test_fuel_display_mapping():
    st = _st([("GAS1", 0, 0.0, 0.0, 0.0), ("HYD1", 0, 0.0, 0.0, 0.0)])
    out = pd_mod.current_outages(st, _units(), 50, now=NOW).set_index("duid")
    assert out.loc["GAS1", "fuel"] == "Gas"
    assert out.loc["HYD1", "fuel"] == "Hydro"


def test_now_is_first_interval_at_or_after_now():
    st = _st([("COAL1", -2, 0.0, 0.0, 0.0), ("COAL1", 0, 0.0, 700.0, 0.0)])
    assert pd_mod.current_outages(st, _units(), 50, now=NOW).empty


def test_now_falls_back_to_latest_interval():
    st = _st([("COAL1", -4, 0.0, 0.0, 0.0), ("COAL1", -2, 0.0, 0.0, 0.0)])
    out = pd_mod.current_outages(st, _units(), 50, now=NOW)
    assert list(out["duid"]) == ["COAL1"]


def test_latest_run_wins_per_interval():
    st = _st([("COAL1", 0, 0.0, 0.0, 0.0)])
    older = st.copy()
    older["RUN_DATETIME"] = pd.Timestamp("2026-10-10 10:00")
    older["GENERATION_PASA_AVAILABILITY"] = 700.0
    out = pd_mod.current_outages(pd.concat([older, st]), _units(), 50, now=NOW)
    assert list(out["duid"]) == ["COAL1"]


def test_timeseries_by_fuel_and_region():
    st = _st([("COAL1", 0, 0, 0.0, 0), ("COAL1", 1, 0, 0.0, 0),
              ("GAS1", 0, 0, 0.0, 0), ("GAS1", 1, 0, 300.0, 0)])
    ts = pd_mod.outage_timeseries(st, _units(), 50, now=NOW)
    assert ts.loc[NOW, "Coal"] == 700.0 and ts.loc[NOW, "Gas"] == 300.0
    assert ts.loc[NOW + pd.Timedelta(minutes=30), "Gas"] == 0.0
    qld = pd_mod.outage_timeseries(st, _units(), 50, region="QLD1", now=NOW)
    assert "Gas" not in qld.columns or qld["Gas"].sum() == 0


def test_timeseries_excludes_past_intervals():
    st = _st([("COAL1", -2, 0, 0.0, 0), ("COAL1", 0, 0, 0.0, 0)])
    ts = pd_mod.outage_timeseries(st, _units(), 50, now=NOW)
    assert ts.index.min() == NOW


def test_return_within_week():
    st = _st([("COAL1", 0, 0, 0.0, 0), ("COAL1", 1, 0, 0.0, 0),
              ("COAL1", 2, 0, 700.0, 0), ("COAL1", 3, 0, 700.0, 0),
              ("GAS1", 0, 0, 0.0, 0), ("GAS1", 1, 0, 0.0, 0)])
    tbl = pd_mod.outage_table(st, _units(), None, 50, now=NOW).set_index("duid")
    assert tbl.loc["COAL1", "back_within_week"] == NOW + pd.Timedelta(minutes=60)
    assert pd.isna(tbl.loc["GAS1", "back_within_week"])


def _mt(rows):
    return pd.DataFrame([{
        "PUBLISH_DATETIME": pd.Timestamp("2026-10-10 09:00"),
        "DAY": pd.Timestamp("2026-10-10") + pd.Timedelta(days=k),
        "DUID": d, "PASAAVAILABILITY": 0, "PASAUNITSTATE": s,
    } for d, k, s in rows])


@pytest.mark.parametrize("state,expected", [
    ("OUTAGEPLANBASIC", "Planned"), ("DERATINGPLANEXTEND", "Planned"),
    ("OUTAGEUNPLANMAINT", "Unplanned"), ("DERATINGUNPLANFORCED", "Unplanned"),
    ("NODERATINGS", "Not in MT-PASA"), ("", "Not in MT-PASA"),
])
def test_mtpasa_type_mapping(state, expected):
    assert pd_mod.outage_type(state) == expected


def test_mtpasa_return_and_type_in_table():
    st = _st([("COAL1", 0, 0, 0.0, 0), ("GAS1", 0, 0, 0.0, 0)])
    mt = _mt([("COAL1", 0, "OUTAGEUNPLANMAINT"), ("COAL1", 1, "OUTAGEUNPLANMAINT"),
              ("COAL1", 2, "NODERATINGS"), ("GAS1", 0, "NODERATINGS")])
    tbl = pd_mod.outage_table(st, _units(), mt, 50, now=NOW).set_index("duid")
    assert tbl.loc["COAL1", "type"] == "Unplanned"
    assert tbl.loc["COAL1", "raw_state"] == "OUTAGEUNPLANMAINT"
    assert tbl.loc["COAL1", "mtpasa_return"] == pd.Timestamp("2026-10-12")
    assert tbl.loc["GAS1", "type"] == "Not in MT-PASA"
    assert pd.isna(tbl.loc["GAS1", "mtpasa_return"])


def test_mtpasa_newest_publish_wins():
    old = _mt([("COAL1", 0, "NODERATINGS")])
    old["PUBLISH_DATETIME"] = pd.Timestamp("2026-10-01 09:00")
    new = _mt([("COAL1", 0, "OUTAGEPLANBASIC"), ("COAL1", 3, "NODERATINGS")])
    st = _st([("COAL1", 0, 0, 0.0, 0)])
    tbl = pd_mod.outage_table(st, _units(), pd.concat([old, new]), 50,
                              now=NOW).set_index("duid")
    assert tbl.loc["COAL1", "type"] == "Planned"
    assert tbl.loc["COAL1", "mtpasa_return"] == pd.Timestamp("2026-10-13")


def test_table_sorted_by_mw_out_and_columns():
    st = _st([("GAS1", 0, 0, 0.0, 0), ("COAL1", 0, 0, 0.0, 0)])
    tbl = pd_mod.outage_table(st, _units(), None, 50, now=NOW)
    assert list(tbl["duid"]) == ["COAL1", "GAS1"]
    for c in ["site_name", "region", "fuel", "capacity_mw", "available_mw",
              "mw_out", "type", "back_within_week", "mtpasa_return", "recall_h"]:
        assert c in tbl.columns


def test_mtpasa_without_state_when_mtpasa_none():
    st = _st([("COAL1", 0, 0, 0.0, 0)])
    tbl = pd_mod.outage_table(st, _units(), None, 50, now=NOW)
    assert tbl.iloc[0]["type"] == "Not in MT-PASA"
