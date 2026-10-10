"""PD-PASA splice and supply-impact strip (pure pandas)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from aemo_dashboard.web import pasa_data as pd_mod

NOW = pd.Timestamp("2026-10-10 19:00")
PD_RUN = pd.Timestamp("2026-10-10 19:00")
ST_RUN = pd.Timestamp("2026-10-10 04:00")


def _units():
    return pd.DataFrame({
        "duid": ["COAL1", "COAL2", "GAS1", "HYD1", "COALQ"],
        "site_name": ["C1", "C2", "G1", "H1", "CQ"],
        "region": ["NSW1", "NSW1", "NSW1", "TAS1", "QLD1"],
        "fuel": ["Coal", "Coal", "CCGT", "Water", "Coal"],
        "capacity_mw": [700.0, 700.0, 300.0, 100.0, 1000.0],
    })


def _frame(run, rows):
    """rows: (duid, interval, pasa_avail)."""
    return pd.DataFrame([{
        "RUN_DATETIME": run, "DUID": d, "INTERVAL_DATETIME": pd.Timestamp(iv),
        "GENERATION_MAX_AVAILABILITY": a, "GENERATION_PASA_AVAILABILITY": a,
        "GENERATION_RECALL_PERIOD": 0.0,
    } for d, iv, a in rows])


def _pd(rows):
    return _frame(PD_RUN, rows)


def _st(rows):
    return _frame(ST_RUN, rows)


def test_combine_uses_pd_inside_its_range_and_st_after():
    pdf = _pd([("COAL1", "2026-10-10 19:00", 0.0),
               ("COAL1", "2026-10-10 19:30", 0.0)])
    st = _st([("COAL1", "2026-10-10 19:00", 700.0),   # stale, inside PD range
              ("COAL1", "2026-10-10 19:30", 700.0),
              ("COAL1", "2026-10-10 20:00", 650.0)])  # after PD
    out = pd_mod.combine_pasa(pdf, st)
    assert set(out["SOURCE"]) == {"PD", "ST"}
    assert len(out) == 3
    inside = out[out["INTERVAL_DATETIME"] <= pdf["INTERVAL_DATETIME"].max()]
    assert set(inside["SOURCE"]) == {"PD"}
    assert (inside["GENERATION_PASA_AVAILABILITY"] == 0.0).all()
    after = out[out["INTERVAL_DATETIME"] > pdf["INTERVAL_DATETIME"].max()]
    assert list(after["SOURCE"]) == ["ST"]


def test_combine_empty_pd_returns_st_with_source():
    st = _st([("COAL1", "2026-10-10 19:00", 700.0)])
    out = pd_mod.combine_pasa(pd.DataFrame(), st)
    assert list(out["SOURCE"]) == ["ST"]
    assert len(out) == 1


def test_combine_keeps_only_latest_pd_run():
    old = _frame(pd.Timestamp("2026-10-10 18:30"),
                 [("COAL1", "2026-10-10 19:00", 700.0)])
    new = _pd([("COAL1", "2026-10-10 19:00", 0.0)])
    out = pd_mod.combine_pasa(pd.concat([old, new]), pd.DataFrame())
    assert len(out) == 1 and out["GENERATION_PASA_AVAILABILITY"].iloc[0] == 0.0


def test_joined_one_row_per_duid_interval_after_splice():
    pdf = _pd([("COAL1", "2026-10-10 19:00", 0.0),
               ("COAL1", "2026-10-10 19:30", 0.0)])
    st = _st([("COAL1", "2026-10-10 19:30", 700.0),
              ("COAL1", "2026-10-10 20:00", 700.0)])
    j = pd_mod._joined(pd_mod.combine_pasa(pdf, st), _units())
    assert not j.duplicated(["DUID", "INTERVAL_DATETIME"]).any()
    row = j[j["INTERVAL_DATETIME"] == pd.Timestamp("2026-10-10 19:30")]
    assert row["mw_out"].iloc[0] == 700.0   # PD value, not the stale ST value


def test_now_interval_comes_from_pd_not_next_trading_day():
    pdf = _pd([("COAL1", "2026-10-10 19:00", 0.0),
               ("COAL1", "2026-10-10 19:30", 0.0)])
    st = _st([("COAL1", "2026-10-11 04:00", 700.0),
              ("COAL1", "2026-10-11 04:30", 700.0)])
    comb = pd_mod.combine_pasa(pdf, st)
    cur = pd_mod.current_outages(comb, _units(), now=pd.Timestamp("2026-10-10 18:51"))
    assert cur["interval"].iloc[0] == pd.Timestamp("2026-10-10 19:00")
    assert list(cur["duid"]) == ["COAL1"]
    # ST alone would put "now" at the next trading day
    cur_st = pd_mod.current_outages(st, _units(), now=pd.Timestamp("2026-10-10 18:51"))
    assert cur_st.empty or cur_st["interval"].iloc[0] == pd.Timestamp("2026-10-11 04:00")


# --- supply_impact ---------------------------------------------------------

def _cur():
    return pd.DataFrame({
        "duid": ["COAL1", "GAS1", "HYD1", "COALQ"],
        "site_name": ["C1", "G1", "H1", "CQ"],
        "region": ["NSW1", "NSW1", "TAS1", "QLD1"],
        "fuel": ["Coal", "Gas", "Hydro", "Coal"],
        "capacity_mw": [700.0, 300.0, 100.0, 1000.0],
        "available_mw": [0.0, 100.0, 0.0, 500.0],
        "mw_out": [700.0, 200.0, 100.0, 500.0],
        "recall_h": 0.0, "interval": NOW,
    })


DEM = {"NSW1": 8000.0, "QLD1": 6000.0, "VIC1": 4000.0, "SA1": 1000.0, "TAS1": 1000.0}
PRI = {"NSW1": 100.0, "QLD1": 50.0, "VIC1": 200.0, "SA1": 300.0, "TAS1": 0.0}


def test_supply_impact_rows_and_values():
    si = pd_mod.supply_impact(_cur(), _units(), DEM, PRI)
    assert list(si.index) == ["NSW1", "QLD1", "VIC1", "SA1", "TAS1", "NEM"]
    nsw = si.loc["NSW1"]
    assert nsw["coal_capacity_mw"] == 1400.0
    assert nsw["coal_out_mw"] == 700.0
    assert nsw["coal_pct"] == pytest.approx(50.0)
    assert nsw["sched_out_mw"] == 900.0
    assert nsw["demand_mw"] == 8000.0 and nsw["price"] == 100.0
    assert nsw["out_pct_demand"] == pytest.approx(900 / 8000 * 100)
    assert nsw["out_Coal"] == 700.0 and nsw["out_Gas"] == 200.0 and nsw["out_Hydro"] == 0.0


def test_supply_impact_region_without_coal_is_nan():
    si = pd_mod.supply_impact(_cur(), _units(), DEM, PRI)
    for r in ("SA1", "VIC1", "TAS1"):
        assert si.loc[r, "coal_capacity_mw"] == 0.0
        assert np.isnan(si.loc[r, "coal_pct"])
    assert si.loc["TAS1", "sched_out_mw"] == 100.0


def test_supply_impact_nem_totals_and_weighted_price():
    si = pd_mod.supply_impact(_cur(), _units(), DEM, PRI)
    nem = si.loc["NEM"]
    assert nem["coal_capacity_mw"] == 2400.0
    assert nem["coal_out_mw"] == 1200.0
    assert nem["coal_pct"] == pytest.approx(50.0)
    assert nem["sched_out_mw"] == 1500.0
    assert nem["demand_mw"] == 20000.0
    want = (8000*100 + 6000*50 + 4000*200 + 1000*300 + 1000*0) / 20000
    assert nem["price"] == pytest.approx(want)    # not the simple mean (130)
    assert nem["out_pct_demand"] == pytest.approx(1500 / 20000 * 100)


def test_supply_impact_missing_demand_or_price_gives_nan():
    si = pd_mod.supply_impact(_cur(), _units(), {}, {})
    assert np.isnan(si.loc["NEM", "demand_mw"]) and np.isnan(si.loc["NEM", "price"])
    assert si.loc["NEM", "sched_out_mw"] == 1500.0
    assert np.isnan(si.loc["NEM", "out_pct_demand"])


def test_supply_impact_empty_cur():
    si = pd_mod.supply_impact(_cur().iloc[0:0], _units(), DEM, PRI)
    assert si.loc["NEM", "coal_out_mw"] == 0.0
    assert si.loc["NEM", "coal_capacity_mw"] == 2400.0
