"""Pump loads (fuel NULL in duid_mapping) are consumption, not generation.

They report pumping as positive SCADA MW, so any query that sums
scadavalue by fuel must leave them out of the renewable gauge's total.
"""
from __future__ import annotations

import duckdb

from aemo_dashboard.api.routers.gauges import _load_renewable


def _conn():
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE scada5 (settlementdate TIMESTAMP, duid VARCHAR, scadavalue DOUBLE)")
    c.execute('CREATE TABLE duid_mapping (region VARCHAR, "site name" VARCHAR, owner VARCHAR, '
              "duid VARCHAR, capacity_mw DOUBLE, storage_mwh DOUBLE, fuel VARCHAR)")
    c.execute("CREATE TABLE rooftop30 (settlementdate TIMESTAMP, regionid VARCHAR, power DOUBLE)")
    ts = "2026-10-08 12:00:00"
    c.execute("""INSERT INTO duid_mapping VALUES
        ('NSW1','Hydro','x','HYD1',100,0,'Water'),
        ('NSW1','Coal','x','COAL1',100,0,'Coal'),
        ('NSW1','Pump','x','PUMP9',0,0,NULL)""")
    c.execute(f"""INSERT INTO scada5 VALUES
        ('{ts}','HYD1',100), ('{ts}','COAL1',300), ('{ts}','PUMP9',200)""")
    return c


def test_pump_load_not_in_renewable_total():
    out = _load_renewable(_conn())
    # 100 hydro of 400 generation = 25%; with the pump counted it would be 100/600.
    assert out["hydro_pct"] == 25.0
