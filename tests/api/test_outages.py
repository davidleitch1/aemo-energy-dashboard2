"""Shape tests for GET /v1/pasa/generator-outages.

Replicates the Today-tab generator outage panel from nem_dash_tab.py
(via pasa_tab.create_generator_fuel_summary). Reads ST-PASA parquet,
filters to >=50 MW reduction over the next 48h, joins to duid_info
for fuel + region.
"""
from __future__ import annotations


def test_outages_returns_200_with_auth(client, auth_headers):
    resp = client.get("/v1/pasa/generator-outages", headers=auth_headers)
    assert resp.status_code == 200


def test_outages_returns_401_without_auth(client):
    resp = client.get("/v1/pasa/generator-outages")
    assert resp.status_code == 401


def test_outages_top_level_shape(client, auth_headers):
    body = client.get("/v1/pasa/generator-outages", headers=auth_headers).json()
    assert "data" in body and "meta" in body
    for key in ("fuels", "max_fuel_mw", "total_mw", "as_of"):
        assert key in body["data"], f"missing data.{key}"


def test_outages_fuel_row_shape(client, auth_headers):
    body = client.get("/v1/pasa/generator-outages", headers=auth_headers).json()
    fuels = body["data"]["fuels"]
    assert isinstance(fuels, list)
    if fuels:
        row = fuels[0]
        for key in ("fuel", "total_mw", "units"):
            assert key in row, f"missing fuel row.{key}"
        assert isinstance(row["units"], list)
        if row["units"]:
            unit = row["units"][0]
            for key in ("duid", "mw", "region"):
                assert key in unit, f"missing unit.{key}"
            assert isinstance(unit["mw"], (int, float))


def test_outages_sorted_descending_by_total_mw(client, auth_headers):
    body = client.get("/v1/pasa/generator-outages", headers=auth_headers).json()
    fuels = body["data"]["fuels"]
    if len(fuels) >= 2:
        totals = [f["total_mw"] for f in fuels]
        assert totals == sorted(totals, reverse=True), "fuels must be sorted by total_mw desc"


def test_outages_max_fuel_matches_first_row(client, auth_headers):
    body = client.get("/v1/pasa/generator-outages", headers=auth_headers).json()
    fuels = body["data"]["fuels"]
    if fuels:
        assert body["data"]["max_fuel_mw"] == fuels[0]["total_mw"]
