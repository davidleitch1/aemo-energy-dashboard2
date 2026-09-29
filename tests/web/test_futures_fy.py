"""Tests for financial-year contracts in the Futures › single-contract chart.

An Australian financial year FYyyyy runs July (yyyy-1) to June yyyy, so its
strip price is the mean of Q3 and Q4 of yyyy-1 and Q1 and Q2 of yyyy. A quarter
that has finished delivery stops trading; its last settlement is carried
forward so the FY line continues through the year.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import (
    app,
    _fy_contract_average,
    _futures_fy_available,
    _load_futures_df,
    _parse_futures_contracts,
)


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _synthetic():
    idx = pd.to_datetime(["2026-06-07", "2026-06-14", "2026-06-21",
                          "2026-06-28", "2026-07-05"])
    df = pd.DataFrame({
        "NSW 2026 Q3": [80, 82, 84, 86, np.nan],   # expired after 28 Jun
        "NSW 2026 Q4": [70, 70, 70, 70, 70],
        "NSW 2027 Q1": [60, 60, 60, 60, 60],
        "NSW 2027 Q2": [np.nan, 90, 90, 90, 90],   # lists a week late
    }, index=idx)
    return df, _parse_futures_contracts(df.columns)["NSW"]


def test_fy_average_is_mean_of_the_four_quarters():
    df, rc = _synthetic()
    s = _fy_contract_average(df, rc, 2027)
    assert s.loc["2026-06-14"] == pytest.approx((82 + 70 + 60 + 90) / 4)


def test_fy_average_nan_before_all_quarters_list():
    df, rc = _synthetic()
    s = _fy_contract_average(df, rc, 2027)
    assert np.isnan(s.loc["2026-06-07"])


def test_fy_average_carries_expired_quarter_forward():
    df, rc = _synthetic()
    s = _fy_contract_average(df, rc, 2027)
    assert s.loc["2026-07-05"] == pytest.approx((86 + 70 + 60 + 90) / 4)


def test_fy_average_missing_quarter_column_gives_nan():
    df, rc = _synthetic()
    s = _fy_contract_average(df, rc, 2028)
    assert s.isna().all()


def test_fy_available_needs_all_four_quarter_columns():
    df, _ = _synthetic()
    contracts = _parse_futures_contracts(df.columns)
    assert _futures_fy_available(contracts) == [2027]


def test_live_data_offers_fy_options():
    contracts = _parse_futures_contracts(_load_futures_df().columns)
    fys = _futures_fy_available(contracts)
    assert fys, "expected at least one complete financial year"
    assert fys == sorted(fys)


def test_route_fy_contract_renders(client):
    fy = _futures_fy_available(
        _parse_futures_contracts(_load_futures_df().columns))[-1]
    resp = client.get(f"/futures?region=NSW&contract=FY{fy}")
    assert resp.status_code == 200
    body = resp.text
    assert f'<option value="FY{fy}" selected>' in body
    assert f"FY{fy} base load" in body
    assert "plot-single-" in body


def test_dropdown_lists_quarters_and_financial_years(client):
    body = client.get("/futures?region=NSW").text
    assert '<optgroup label="Financial years">' in body
    assert '<optgroup label="Quarters">' in body


def test_quarter_contract_still_works(client):
    resp = client.get("/futures?region=NSW&contract=2027-1")
    assert resp.status_code == 200
    assert '<option value="2027-1" selected>' in resp.text
    assert "2027 Q1 base load" in resp.text


def test_bad_fy_falls_back_to_default(client):
    resp = client.get("/futures?region=NSW&contract=FY1990")
    assert resp.status_code == 200
    assert 'value="FY1990" selected' not in resp.text


def test_fy_average_stops_when_last_quarter_stops_trading():
    df, rc = _synthetic()
    df.loc[pd.Timestamp("2026-07-12")] = [np.nan, np.nan, np.nan, np.nan]
    s = _fy_contract_average(df, rc, 2027)
    assert np.isnan(s.loc["2026-07-12"])
