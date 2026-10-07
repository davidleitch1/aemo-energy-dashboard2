"""Batteries tab: Cap MW is nameplate power, summed up the tree.

Util % keeps its own denominator, storage_mwh / 24 (one full discharge a day
= 100%), so it reads as cycles per day; Cap MW must not show that number.
"""
from __future__ import annotations

import pandas as pd

from aemo_dashboard.web.app import _battery_agg_node


def _rows():
    return pd.DataFrame({
        "duid": ["B1", "B2"],
        "capacity_mw": [100.0, 50.0],
        "storage_mwh": [200.0, 200.0],
        "discharge_mwh": [200.0 * 10, 200.0 * 5],   # 10 and 5 full cycles
        "charge_mwh": [2400.0, 1200.0],
        "discharge_rev": [200_000.0, 100_000.0],
        "charge_cost": [50_000.0, 25_000.0],
    })


def test_cap_mw_is_summed_nameplate():
    node = _battery_agg_node(_rows(), hours=240.0, label="R", kind="region",
                             ctx={})
    assert node["cap_mw"] == 150
    assert node["storage_mwh"] == 400


def test_cap_mw_duid_level_is_nameplate():
    one = _rows().iloc[[0]]
    node = _battery_agg_node(one, hours=240.0, label="B1", kind="duid",
                             ctx={})
    assert node["cap_mw"] == 100


def test_util_is_cycles_per_day_on_storage():
    # 3,000 MWh discharged from 400 MWh over 10 days = 0.75 cycles/day.
    node = _battery_agg_node(_rows(), hours=240.0, label="R", kind="region",
                             ctx={})
    assert node["util"] == 75


def test_new_battery_is_weighted_by_time_in_window():
    # B1 reports for the whole 10-day window; B2 (same size) only the last
    # 5 days. Storage-days = 200*10 + 200*5 = 3,000 MWh-days.
    rows = _rows().assign(n_intervals=[480, 240])
    node = _battery_agg_node(rows, hours=240.0, label="R", kind="region",
                             ctx={})
    # 3,000 MWh discharged / 3,000 storage-days = 1.0 cycle/day.
    assert node["util"] == 100
    # Net revenue 225,000 over 3,000/365 storage-MWh-years.
    assert node["spread_per_mwh_yr"] == round(225_000 / (3000 / 365))
    # Cap and storage columns still show the installed fleet.
    assert node["storage_mwh"] == 400
