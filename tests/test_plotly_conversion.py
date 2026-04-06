"""
Tests for HoloViews → Plotly conversion of utilization and transmission plots.

These tests verify that:
1. create_utilization_plot() returns a Plotly go.Figure (not HoloViews)
2. create_transmission_plot() returns a Plotly go.Figure (not HoloViews)
3. Both panes are initialized as pn.pane.Plotly (not pn.pane.HoloViews)
4. update_plot() does not call get_root() (removed Bokeh hack)
5. Empty/error states return None (not hv.Text)
6. Flexoki theme is applied correctly
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, PropertyMock
import plotly.graph_objects as go


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_utilization_data():
    """Create mock capacity utilization DataFrame (datetime index, fuel columns 0-100)."""
    dates = pd.date_range("2026-04-05 00:00", periods=48, freq="30min")
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "Coal": rng.uniform(40, 90, len(dates)),
            "Solar": rng.uniform(0, 60, len(dates)),
            "Wind": rng.uniform(10, 70, len(dates)),
        },
        index=pd.Index(dates, name="settlementdate"),
    )


def _make_transmission_df():
    """Create mock transmission DataFrame matching AEMO schema."""
    dates = pd.date_range("2026-04-05 00:00", periods=48, freq="30min")
    rows = []
    for dt in dates:
        rows.append({
            "settlementdate": dt,
            "interconnectorid": "NSW1-QLD1",
            "meteredmwflow": np.random.uniform(-300, 500),
            "exportlimit": 600.0,
            "importlimit": 1000.0,
        })
        rows.append({
            "settlementdate": dt,
            "interconnectorid": "VIC1-NSW1",
            "meteredmwflow": np.random.uniform(-200, 800),
            "exportlimit": 800.0,
            "importlimit": 1200.0,
        })
    return pd.DataFrame(rows)


FUEL_COLORS = {
    "Coal": "#403E3C",
    "Solar": "#AD8301",
    "Wind": "#24837B",
    "Gas": "#BC5215",
    "Hydro": "#205EA6",
    "Battery": "#5E409D",
}


def _make_dashboard_stub():
    """Create a minimal EnergyDashboard-like object without full init."""
    stub = MagicMock()
    stub.region = "NSW1"
    stub.time_range = "1"
    stub._get_time_range_display = MagicMock(return_value="Last 24 Hours")
    stub.get_fuel_colors = MagicMock(return_value=FUEL_COLORS)
    stub.transmission_df = _make_transmission_df()
    stub.load_transmission_data = MagicMock()
    # Bind the real static method so Plotly gets valid rgba strings
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    stub._hex_to_rgba = EnergyDashboard._hex_to_rgba
    stub._get_effective_date_range = MagicMock(
        return_value=(datetime(2026, 4, 5), datetime(2026, 4, 6))
    )
    return stub


# ---------------------------------------------------------------------------
# Utilization plot tests
# ---------------------------------------------------------------------------

class TestUtilizationPlotly:
    """Tests for create_utilization_plot returning Plotly figures."""

    def test_returns_plotly_figure(self):
        """Utilization plot must return a go.Figure, not a HoloViews object."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.calculate_capacity_utilization = MagicMock(return_value=_make_utilization_data())

        result = EnergyDashboard.create_utilization_plot(stub)

        assert isinstance(result, go.Figure), (
            f"Expected go.Figure, got {type(result).__module__}.{type(result).__name__}"
        )

    def test_flexoki_theme_applied(self):
        """Utilization plot must use Flexoki paper background."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard, FLEXOKI_PAPER

        stub = _make_dashboard_stub()
        stub.calculate_capacity_utilization = MagicMock(return_value=_make_utilization_data())

        fig = EnergyDashboard.create_utilization_plot(stub)

        assert fig.layout.paper_bgcolor == FLEXOKI_PAPER
        assert fig.layout.plot_bgcolor == FLEXOKI_PAPER

    def test_has_traces_per_fuel(self):
        """Utilization plot must have one Scatter trace per fuel type."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.calculate_capacity_utilization = MagicMock(return_value=_make_utilization_data())

        fig = EnergyDashboard.create_utilization_plot(stub)

        trace_names = {t.name for t in fig.data}
        assert "Coal" in trace_names
        assert "Solar" in trace_names
        assert "Wind" in trace_names
        for trace in fig.data:
            assert isinstance(trace, go.Scatter)

    def test_yaxis_range_0_100(self):
        """Y-axis must be 0-100 for percentage utilization."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.calculate_capacity_utilization = MagicMock(return_value=_make_utilization_data())

        fig = EnergyDashboard.create_utilization_plot(stub)

        assert fig.layout.yaxis.range == [0, 100] or fig.layout.yaxis.range == (0, 100)

    def test_empty_data_returns_none(self):
        """Empty utilization data must return None (not hv.Text)."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.calculate_capacity_utilization = MagicMock(return_value=pd.DataFrame())

        result = EnergyDashboard.create_utilization_plot(stub)

        assert result is None


# ---------------------------------------------------------------------------
# Transmission plot tests
# ---------------------------------------------------------------------------

class TestTransmissionPlotly:
    """Tests for create_transmission_plot returning Plotly figures."""

    def test_returns_plotly_figure(self):
        """Transmission plot must return a go.Figure, not a HoloViews object."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()

        result = EnergyDashboard.create_transmission_plot(stub)

        assert isinstance(result, go.Figure), (
            f"Expected go.Figure, got {type(result).__module__}.{type(result).__name__}"
        )

    def test_flexoki_theme_applied(self):
        """Transmission plot must use Flexoki paper background."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard, FLEXOKI_PAPER

        stub = _make_dashboard_stub()

        fig = EnergyDashboard.create_transmission_plot(stub)

        assert fig.layout.paper_bgcolor == FLEXOKI_PAPER
        assert fig.layout.plot_bgcolor == FLEXOKI_PAPER

    def test_has_flow_traces(self):
        """Transmission plot must have Scatter traces for flow lines."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()

        fig = EnergyDashboard.create_transmission_plot(stub)

        # Should have traces for NSW1-QLD1 and VIC1-NSW1
        trace_names = [t.name for t in fig.data if t.name]
        assert any("NSW1-QLD1" in n for n in trace_names), f"Missing NSW1-QLD1 trace. Traces: {trace_names}"
        assert any("VIC1-NSW1" in n for n in trace_names), f"Missing VIC1-NSW1 trace. Traces: {trace_names}"

    def test_has_zero_line(self):
        """Transmission plot must have a horizontal zero line."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()

        fig = EnergyDashboard.create_transmission_plot(stub)

        # Check for zero line via shapes or yaxis zeroline
        has_zero = (
            fig.layout.yaxis.zeroline is True
            or any(
                getattr(s, "y0", None) == 0 and getattr(s, "y1", None) == 0
                for s in (fig.layout.shapes or [])
            )
        )
        assert has_zero, "Transmission plot missing zero line"

    def test_nem_returns_none(self):
        """NEM region has no interconnector view — must return None."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.region = "NEM"

        result = EnergyDashboard.create_transmission_plot(stub)

        assert result is None

    def test_empty_data_returns_none(self):
        """No transmission data must return None."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.transmission_df = None

        result = EnergyDashboard.create_transmission_plot(stub)

        assert result is None

    def test_hover_includes_key_fields(self):
        """Hover templates must include MW and % fields."""
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()

        fig = EnergyDashboard.create_transmission_plot(stub)

        # Find a flow trace (one with a hovertemplate)
        flow_traces = [t for t in fig.data if t.hovertemplate and "MW" in t.hovertemplate]
        assert len(flow_traces) > 0, "No flow traces with MW in hovertemplate"
        assert any("%" in t.hovertemplate for t in flow_traces), "No flow traces with % in hovertemplate"


# ---------------------------------------------------------------------------
# Pane type tests
# ---------------------------------------------------------------------------

class TestPaneTypes:
    """Tests that panes use Plotly, not HoloViews."""

    def test_utilization_pane_is_plotly(self):
        """utilization_pane must be pn.pane.Plotly after init."""
        import panel as pn
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.create_plot = MagicMock(return_value=pn.Column())
        stub.create_utilization_plot = MagicMock(return_value=go.Figure())
        stub.create_transmission_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_tod_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_summary_table = MagicMock(return_value=pn.pane.HTML(""))
        stub.utilization_pane = None
        stub.transmission_pane = None
        stub.plot_pane = None
        stub.generation_tod_pane = None
        stub.summary_table_pane = None

        EnergyDashboard._initialize_panes(stub)

        assert isinstance(stub.utilization_pane, pn.pane.Plotly), (
            f"Expected pn.pane.Plotly, got {type(stub.utilization_pane)}"
        )

    def test_transmission_pane_is_plotly(self):
        """transmission_pane must be pn.pane.Plotly after init."""
        import panel as pn
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        stub.create_plot = MagicMock(return_value=pn.Column())
        stub.create_utilization_plot = MagicMock(return_value=go.Figure())
        stub.create_transmission_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_tod_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_summary_table = MagicMock(return_value=pn.pane.HTML(""))
        stub.utilization_pane = None
        stub.transmission_pane = None
        stub.plot_pane = None
        stub.generation_tod_pane = None
        stub.summary_table_pane = None

        EnergyDashboard._initialize_panes(stub)

        assert isinstance(stub.transmission_pane, pn.pane.Plotly), (
            f"Expected pn.pane.Plotly, got {type(stub.transmission_pane)}"
        )


# ---------------------------------------------------------------------------
# update_plot tests
# ---------------------------------------------------------------------------

class TestUpdatePlotNoGetRoot:
    """Verify that update_plot() no longer uses get_root() Bokeh hack."""

    def test_no_get_root_called(self):
        """update_plot must not call get_root() on any pane."""
        import panel as pn
        from aemo_dashboard.generation.gen_dash import EnergyDashboard

        stub = _make_dashboard_stub()
        # Set up panes as Plotly
        stub.plot_pane = pn.Column()
        stub.utilization_pane = pn.pane.Plotly(go.Figure())
        stub.transmission_pane = pn.pane.Plotly(go.Figure())
        stub.generation_tod_pane = pn.pane.Plotly(go.Figure())
        stub.summary_table_pane = pn.Column()
        stub.header_section = pn.pane.HTML("")

        stub.create_plot = MagicMock(return_value=pn.pane.HTML(""))
        stub.create_utilization_plot = MagicMock(return_value=go.Figure())
        stub.create_transmission_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_tod_plot = MagicMock(return_value=go.Figure())
        stub.create_generation_summary_table = MagicMock(return_value=pn.pane.HTML(""))

        # Spy on get_root
        stub.transmission_pane.get_root = MagicMock()
        stub.utilization_pane.get_root = MagicMock()

        EnergyDashboard.update_plot(stub)

        stub.transmission_pane.get_root.assert_not_called()
        stub.utilization_pane.get_root.assert_not_called()
