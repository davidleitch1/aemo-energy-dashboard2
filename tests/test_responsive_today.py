"""
Phase 1: Today Tab responsive tests.

Covers: nem_dash_tab.py, generation_overview.py, price_components_hvplot.py,
        price_components.py, renewable_gauge.py, daily_summary.py
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import plotly.graph_objects as go

pytestmark = pytest.mark.responsive


# ---------------------------------------------------------------------------
# generation_overview.py — HoloViews → Plotly conversion
# ---------------------------------------------------------------------------

class TestGenerationOverview:
    """generation_overview.py must return Plotly figures, not HoloViews."""

    def test_no_holoviews_import(self):
        """generation_overview.py must not import holoviews."""
        import importlib
        import inspect
        mod = importlib.import_module("aemo_dashboard.nem_dash.generation_overview")
        source = inspect.getsource(mod)
        assert "import holoviews" not in source, "Must not import holoviews"
        assert "import hvplot" not in source, "Must not import hvplot"

    def test_no_hardcoded_width_1000(self):
        """No width=1000 in plot opts."""
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.generation_overview")
        )
        lines = source.split("\n")
        violations = [
            f"Line {i}: {l.strip()[:80]}"
            for i, l in enumerate(lines, 1)
            if "width=1000" in l and not l.strip().startswith("#")
        ]
        assert not violations, f"Hardcoded width=1000:\n" + "\n".join(violations)

    def test_no_sizing_mode_fixed(self):
        """No sizing_mode='fixed' on plot panes."""
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.generation_overview")
        )
        lines = [l for l in source.split("\n")
                 if "sizing_mode='fixed'" in l and not l.strip().startswith("#")]
        assert len(lines) == 0, (
            f"sizing_mode='fixed' must be removed:\n" + "\n".join(l.strip() for l in lines)
        )

    def test_no_inline_width_css(self):
        """No inline style width:1000px."""
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.generation_overview")
        )
        assert "width:1000px" not in source and "width: 1000px" not in source, (
            "Inline CSS width:1000px must be removed"
        )


# ---------------------------------------------------------------------------
# price_components_hvplot.py — HoloViews → Plotly conversion
# ---------------------------------------------------------------------------

class TestPriceComponentsHvplot:
    """price_components_hvplot.py must use Plotly, not HoloViews."""

    def test_no_holoviews_import(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.price_components_hvplot")
        )
        assert "import holoviews" not in source, "Must not import holoviews"
        assert "import hvplot" not in source, "Must not import hvplot"

    def test_no_pn_pane_holoviews(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.price_components_hvplot")
        )
        assert "pn.pane.HoloViews" not in source, "Must use pn.pane.Plotly"

    def test_no_sizing_mode_fixed(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.price_components_hvplot")
        )
        # Count real occurrences (not in comments)
        lines = [l for l in source.split("\n") if "sizing_mode='fixed'" in l and not l.strip().startswith("#")]
        assert len(lines) == 0, f"sizing_mode='fixed' found:\n" + "\n".join(l.strip() for l in lines)


# ---------------------------------------------------------------------------
# nem_dash_tab.py — Gauge layout responsiveness
# ---------------------------------------------------------------------------

class TestNemDashTab:
    """Today tab gauges must be in a responsive flexbox, not fixed layout."""

    def test_no_excessive_sizing_mode_fixed(self):
        """At most 5 sizing_mode='fixed' allowed (gauge/chart panes with Matplotlib only)."""
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.nem_dash_tab")
        )
        lines = [l for l in source.split("\n")
                 if "sizing_mode='fixed'" in l and not l.strip().startswith("#")]
        assert len(lines) <= 5, (
            f"Found {len(lines)} sizing_mode='fixed' — max 5 allowed (gauge panes).\n"
            + "\n".join(l.strip()[:80] for l in lines[:8])
        )

    def test_tables_have_scroll_wrapper(self):
        """HTML tables must be wrapped in overflow-x: auto."""
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.nem_dash_tab")
        )
        # Count <table and count overflow-x: auto — should be >= table count
        table_count = source.count("<table")
        scroll_count = source.count("overflow-x") + source.count("responsive-table")
        if table_count > 0:
            assert scroll_count >= table_count, (
                f"Found {table_count} <table> but only {scroll_count} scroll wrappers"
            )


# ---------------------------------------------------------------------------
# renewable_gauge.py — No inline CSS pixel widths
# ---------------------------------------------------------------------------

class TestRenewableGauge:
    """Gauge must not use hardcoded CSS pixel width."""

    def test_no_inline_width_px(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.renewable_gauge")
        )
        # Allow width in Plotly figure layout (that's fine), but not in HTML inline style
        lines = source.split("\n")
        violations = [
            f"Line {i}: {l.strip()[:80]}"
            for i, l in enumerate(lines, 1)
            if "width:400px" in l.replace(" ", "") and "style" in l.lower()
        ]
        assert not violations, f"Inline CSS width:400px:\n" + "\n".join(violations)


# ---------------------------------------------------------------------------
# price_components.py — sizing_mode fixes
# ---------------------------------------------------------------------------

class TestPriceComponents:
    """price_components.py must not use sizing_mode='fixed'."""

    def test_no_sizing_mode_fixed(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.price_components")
        )
        lines = [l for l in source.split("\n")
                 if "sizing_mode='fixed'" in l and not l.strip().startswith("#")]
        assert len(lines) == 0, f"sizing_mode='fixed' found:\n" + "\n".join(l.strip() for l in lines)


# ---------------------------------------------------------------------------
# daily_summary.py — table wrappers
# ---------------------------------------------------------------------------

class TestDailySummary:
    """daily_summary.py tables must have scroll wrappers."""

    def test_tables_have_scroll_wrapper(self):
        import importlib, inspect
        source = inspect.getsource(
            importlib.import_module("aemo_dashboard.nem_dash.daily_summary")
        )
        table_count = source.count("<table")
        scroll_count = source.count("overflow-x") + source.count("responsive-table")
        if table_count > 0:
            assert scroll_count >= table_count, (
                f"Found {table_count} <table> but only {scroll_count} scroll wrappers"
            )
