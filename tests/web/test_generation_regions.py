"""Tests for the Generation mix › Compare regions subtab.

Runs against the live read-only DuckDB the app already uses (aemo_readonly
.duckdb on .71) — no fixtures, matching how the rest of this app's routes
are exercised. `pyproject.toml`'s `[tool.pytest.ini_options] pythonpath =
["src"]` puts this repo's own `src/` ahead of the installed (different
checkout's) `aemo_dashboard` package on sys.path, so the import below
resolves to *this* repo's app.py.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from aemo_dashboard.web.app import (
    app,
    q,
    _regional_mix_energy,
)


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


# ----------------------------------------------------------------------------
# Route-level tests
# ----------------------------------------------------------------------------

def test_regions_subtab_returns_200_with_expected_content(client):
    resp = client.get("/generation-mix/regions?range=7d")
    assert resp.status_code == 200
    body = resp.text
    assert "Compare regions" in body
    assert "plot-genregions-abs-" in body
    assert "plot-genregions-share-" in body


def test_regions_subtab_hides_region_pills(client):
    resp = client.get("/generation-mix/regions?range=7d")
    assert resp.status_code == 200
    body = resp.text
    # The region pill bar renders a `pill-bar-label` span reading "Region".
    # The regions subtab shows every region on the chart, so it must not
    # render this selector.
    assert 'pill-bar-label">Region<' not in body


@pytest.mark.parametrize("sub", ["stack", "yr-on-yr", "tod", "trends", "transmission"])
def test_existing_generation_mix_subtabs_still_work(client, sub):
    resp = client.get(f"/generation-mix/{sub}?range=7d")
    assert resp.status_code == 200


# ----------------------------------------------------------------------------
# _regional_mix_energy tests
# ----------------------------------------------------------------------------

def _last_7d_window():
    now_ts = pd.Timestamp(pd.Timestamp.now())
    # Match the app's NEM-time convention loosely — exact tz handling is
    # covered by _range_window; here we just need a stable 7-day window.
    from aemo_dashboard.web.app import NEM_TZ
    from datetime import datetime, timedelta
    now_nem = pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))
    return now_nem - timedelta(days=7), now_nem


def test_regional_mix_energy_last_7_days_shape_and_values():
    s_ts, e_ts = _last_7d_window()
    df = _regional_mix_energy(s_ts, e_ts)

    assert len(df.index) == 5
    assert set(df.index) == {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}

    assert not df.isna().any().any()
    assert (df.values >= 0).all()

    assert "Rooftop Solar" in df.columns
    assert (df["Rooftop Solar"] > 0).all()


def test_regional_mix_energy_nem_average_gw_sane():
    s_ts, e_ts = _last_7d_window()
    df = _regional_mix_energy(s_ts, e_ts)
    total_gwh = df.values.sum()
    avg_gw = total_gwh / 168.0
    assert 15.0 <= avg_gw <= 35.0


def test_regional_mix_energy_qld_rooftop_not_inflated_by_subregions():
    """2024-03-01 → 2024-03-08: rooftop30 historically also carried
    QLDC/QLDN/QLDS sub-region rows. QLD1's value must come only from
    rows where regionid='QLD1', not a sum across QLD1+QLDC+QLDN+QLDS."""
    s_ts = pd.Timestamp("2024-03-01")
    e_ts = pd.Timestamp("2024-03-08")

    df = _regional_mix_energy(s_ts, e_ts)
    assert "QLD1" in df.index
    assert "Rooftop Solar" in df.columns

    direct = q(
        """
        SELECT SUM(GREATEST(power, 0)) * 0.5 / 1000.0 AS gwh
          FROM rooftop30
         WHERE regionid = 'QLD1'
           AND settlementdate >= ? AND settlementdate < ?
        """,
        [s_ts, e_ts],
    )
    expected = float(direct["gwh"].iloc[0])
    actual = float(df.loc["QLD1", "Rooftop Solar"])
    assert actual == pytest.approx(expected, rel=1e-6)

    # And the naive (buggy) sum across all QLD-prefixed regionids must be
    # strictly larger, confirming the guard actually matters for this window.
    inflated = q(
        """
        SELECT SUM(GREATEST(power, 0)) * 0.5 / 1000.0 AS gwh
          FROM rooftop30
         WHERE regionid IN ('QLD1', 'QLDC', 'QLDN', 'QLDS')
           AND settlementdate >= ? AND settlementdate < ?
        """,
        [s_ts, e_ts],
    )
    inflated_val = float(inflated["gwh"].iloc[0])
    assert inflated_val > expected


def test_prices_compare_regions_constant_not_shadowed():
    """Prices > Period Compare relies on COMPARE_REGIONS containing NEM; the
    Compare-regions subtab must not redefine that module-level name."""
    from aemo_dashboard.web import app as app_module
    assert "NEM" in app_module.COMPARE_REGIONS


def test_renewable_shares_vre_excludes_hydro():
    import pandas as pd
    from aemo_dashboard.web.app import _renewable_shares
    mix = pd.DataFrame(
        {"Wind": [20.0], "Solar": [10.0], "Rooftop Solar": [10.0],
         "Hydro": [10.0], "Battery": [5.0], "Gas": [5.0], "Coal": [40.0]},
        index=["NSW1"])
    out = _renewable_shares(mix)
    assert abs(out.loc["NSW1", "vre"] - 40.0) < 1e-9
    assert abs(out.loc["NSW1", "re"] - 50.0) < 1e-9


def test_renewable_shares_missing_columns():
    import pandas as pd
    from aemo_dashboard.web.app import _renewable_shares
    mix = pd.DataFrame({"Wind": [30.0], "Gas": [70.0]}, index=["SA1"])
    out = _renewable_shares(mix)
    assert abs(out.loc["SA1", "vre"] - 30.0) < 1e-9
    assert abs(out.loc["SA1", "re"] - 30.0) < 1e-9


def test_compare_fuel_order_puts_renewables_first():
    from aemo_dashboard.web.app import COMPARE_FUEL_ORDER
    assert COMPARE_FUEL_ORDER[:4] == ["Wind", "Solar", "Rooftop Solar", "Hydro"]


def test_share_chart_has_re_labels():
    from fastapi.testclient import TestClient
    from aemo_dashboard.web.app import app
    r = TestClient(app).get("/generation-mix/regions?range=7d")
    assert r.status_code == 200
    assert "VRE " in r.text and "RE " in r.text
