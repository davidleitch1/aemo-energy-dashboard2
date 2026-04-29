"""Shape tests for GET /v1/gauges/today — the Today tab 3-gauge carousel feed.

Definitions match the existing Panel dashboard's nem_dash_tab.py:
  - demand     = SUM(demand30.demand) + SUM(rooftop30.power) across 5 mainland regions
  - renewable  = (Hydro + Wind + Solar + Rooftop) / total_gen at latest 5-min row,
                 excluding storage and transmission. Biomass is omitted from
                 the displayed breakdown (matches stacked-gauge convention).
  - battery    = SUM(bdu5.bdu_energy_storage) across mainland (TAS is NaN);
                 capacity = 30-day rolling max of that same sum.
"""
from __future__ import annotations


def test_gauges_returns_200_with_auth(client, auth_headers):
    resp = client.get("/v1/gauges/today", headers=auth_headers)
    assert resp.status_code == 200


def test_gauges_returns_401_without_auth(client):
    resp = client.get("/v1/gauges/today")
    assert resp.status_code == 401


def test_gauges_top_level_shape(client, auth_headers):
    body = client.get("/v1/gauges/today", headers=auth_headers).json()
    assert "data" in body
    assert "meta" in body
    assert "as_of" in body["meta"]
    for key in ("demand", "renewable_share", "battery_soc"):
        assert key in body["data"], f"missing data.{key}"


def test_demand_block_shape(client, auth_headers):
    demand = client.get("/v1/gauges/today", headers=auth_headers).json()["data"]["demand"]
    for key in ("current_mw", "alltime_record_mw", "alltime_min_mw",
                "hour_record_mw", "current_hour", "forecast_peak_mw", "as_of"):
        assert key in demand, f"missing demand.{key}"
    # current_mw/record/min are numeric; forecast_peak_mw can be null if no predispatch.
    assert isinstance(demand["current_mw"], (int, float))
    assert isinstance(demand["alltime_record_mw"], (int, float))
    assert isinstance(demand["alltime_min_mw"], (int, float))
    assert isinstance(demand["hour_record_mw"], (int, float))
    assert isinstance(demand["current_hour"], int)
    assert demand["forecast_peak_mw"] is None or isinstance(demand["forecast_peak_mw"], (int, float))


def test_renewable_block_shape(client, auth_headers):
    ren = client.get("/v1/gauges/today", headers=auth_headers).json()["data"]["renewable_share"]
    for key in ("renewable_pct", "hydro_pct", "wind_pct", "solar_pct", "rooftop_pct", "as_of"):
        assert key in ren, f"missing renewable_share.{key}"
    # Stacked-gauge invariant: renewable_pct == hydro+wind+solar+rooftop (within 0.1pp).
    summed = ren["hydro_pct"] + ren["wind_pct"] + ren["solar_pct"] + ren["rooftop_pct"]
    assert abs(ren["renewable_pct"] - summed) < 0.1
    # Each component in [0, 100].
    for k in ("renewable_pct", "hydro_pct", "wind_pct", "solar_pct", "rooftop_pct"):
        assert 0 <= ren[k] <= 100, f"{k}={ren[k]} out of range"


def test_battery_block_shape(client, auth_headers):
    bat = client.get("/v1/gauges/today", headers=auth_headers).json()["data"]["battery_soc"]
    for key in ("stored_mwh", "stored_1h_ago_mwh", "capacity_mwh", "as_of"):
        assert key in bat, f"missing battery_soc.{key}"
    assert isinstance(bat["stored_mwh"], (int, float))
    assert isinstance(bat["stored_1h_ago_mwh"], (int, float))
    assert isinstance(bat["capacity_mwh"], (int, float))
    # Stored should not exceed the 30-day rolling max it's compared against.
    assert bat["stored_mwh"] <= bat["capacity_mwh"] + 1e-6
