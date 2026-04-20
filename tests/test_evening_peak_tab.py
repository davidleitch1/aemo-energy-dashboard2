"""Tests for the Evening peak tab integration in the AEMO dashboard."""

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

os.environ.setdefault("USE_DUCKDB", "true")
os.environ.setdefault(
    "AEMO_DUCKDB_PATH",
    "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb",
)


def test_evening_peak_package_imports():
    from aemo_dashboard.evening_peak import create_evening_peak_tab
    assert callable(create_evening_peak_tab)


def test_evening_peak_is_third_tab():
    from aemo_dashboard.generation.gen_dash import EnergyDashboard

    dashboard = EnergyDashboard()
    dashboard.create_dashboard()

    assert hasattr(dashboard, "_tab_names"), "dashboard did not capture _tab_names"
    assert len(dashboard._tab_names) >= 3, f"expected >=3 tabs, got {dashboard._tab_names}"
    assert dashboard._tab_names[0] == "Today"
    assert dashboard._tab_names[1] == "Generation mix"
    assert dashboard._tab_names[2] == "Evening peak"
    assert 2 in dashboard._tab_creators, "no creator registered for tab index 2"


def test_create_evening_peak_tab_returns_panel_layout():
    import panel as pn
    from aemo_dashboard.generation.gen_dash import EnergyDashboard

    dashboard = EnergyDashboard()
    dashboard.create_dashboard()

    tab = dashboard._create_evening_peak_tab()
    assert tab is not None

    if isinstance(tab, pn.Column) and len(tab) == 2:
        header = tab[0]
        if isinstance(header, pn.pane.Markdown) and "Error loading tab" in (header.object or ""):
            pytest.fail(f"tab built in error-fallback mode: {tab[1].object}")

    assert isinstance(tab, pn.viewable.Viewable), f"tab is not a Panel viewable: {type(tab)}"


def test_get_evening_data_returns_expected_shape():
    import pandas as pd
    from aemo_dashboard.evening_peak.evening_analysis import (
        get_evening_data,
        get_latest_data_date,
    )

    end = get_latest_data_date()
    start = (end - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
    end_exclusive = (end + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    avg_by_time, avg_price_by_time, stats = get_evening_data(start, end_exclusive, "NEM")

    assert isinstance(avg_by_time, pd.DataFrame)
    assert len(avg_by_time) > 0, "no rows returned for 7-day NEM window"
    assert isinstance(avg_price_by_time, pd.Series)
    assert len(avg_price_by_time) > 0

    for key in ("total", "battery", "rooftop", "net_imports", "price", "fuel_averages"):
        assert key in stats, f"missing stats key: {key}"
    assert isinstance(stats["fuel_averages"], dict)
    assert stats["total"] > 0
