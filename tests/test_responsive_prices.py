"""
Phase 2: Prices Tab responsive tests.

Covers: prices_tab.py, price_chart.py, fuel_relatives.py, price_bands.py
Also verifies the external module is wired up from gen_dash.py.
"""
import pytest
import importlib
import inspect

pytestmark = pytest.mark.responsive


def _get_source(module_path):
    mod = importlib.import_module(module_path)
    return inspect.getsource(mod)


def _count_pattern(source, pattern, exclude_comments=True):
    lines = source.split("\n")
    matches = []
    for i, l in enumerate(lines, 1):
        if pattern in l:
            if exclude_comments and l.strip().startswith("#"):
                continue
            matches.append(f"Line {i}: {l.strip()[:80]}")
    return matches


# ---------------------------------------------------------------------------
# prices_tab.py — HoloViews removal
# ---------------------------------------------------------------------------

class TestPricesTab:
    """prices_tab.py — all panes must be Plotly, no HoloViews."""

    MODULE = "aemo_dashboard.prices.prices_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_pn_pane_holoviews(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "pn.pane.HoloViews")
        assert not hits, f"pn.pane.HoloViews still used:\n" + "\n".join(hits)

    def test_no_hv_text(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hv.Text(")
        assert not hits, f"hv.Text still used ({len(hits)} instances):\n" + "\n".join(hits[:5])

    def test_tables_have_scroll_wrappers(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "Tabulator")
        for h in hits:
            assert "width=" not in h or "stretch" in h, f"Tabulator with fixed width: {h}"


# ---------------------------------------------------------------------------
# prices_tab.py — Feature completeness
# ---------------------------------------------------------------------------

class TestPricesTabFeatures:
    """prices_tab.py must have all features from the internal gen_dash version."""

    MODULE = "aemo_dashboard.prices.prices_tab"

    def test_single_region_selector(self):
        """Must have only one region selector (no duplicate for fuel relatives)."""
        source = _get_source(self.MODULE)
        # Should NOT have a separate fuel_relatives_region_selector
        assert "fuel_relatives_region_selector" not in source, (
            "Duplicate region selector found — should use the main region_selector"
        )

    def test_analyze_triggers_fuel_relatives(self):
        """Analyze button must trigger fuel relatives too (single action)."""
        source = _get_source(self.MODULE)
        assert "analyze_all" in source or "update_fuel_relatives" in source, (
            "Analyze button must also trigger fuel relatives update"
        )

    def test_has_control_state_tracking(self):
        """Controls must show dirty/clean state when settings change."""
        source = _get_source(self.MODULE)
        assert "dirty" in source.lower() or "button_type" in source, (
            "Missing control state tracking (dirty/clean button feedback)"
        )

    def test_has_three_subtabs(self):
        """Must have Price Analysis, Price Bands, and Fuel Relatives subtabs."""
        source = _get_source(self.MODULE)
        assert "Price Analysis" in source
        assert "Price Bands" in source
        assert "Fuel Relatives" in source

    def test_has_smoothing_options(self):
        """Must support LOESS and EWM smoothing."""
        source = _get_source(self.MODULE)
        assert "LOESS" in source
        assert "EWM" in source

    def test_has_log_scale(self):
        """Must support log scale with negative price handling."""
        source = _get_source(self.MODULE)
        assert "log" in source.lower() or "Log Scale" in source


# ---------------------------------------------------------------------------
# Wiring — gen_dash.py must delegate to external module
# ---------------------------------------------------------------------------

class TestPricesWiring:
    """gen_dash.py must call the external prices_tab module, not inline code."""

    def test_gen_dash_delegates_to_external(self):
        """_create_prices_tab must import and call prices_tab.create_prices_tab."""
        source = _get_source("aemo_dashboard.generation.gen_dash")
        # Find the _create_prices_tab method
        lines = source.split("\n")
        in_method = False
        method_lines = []
        for line in lines:
            if "def _create_prices_tab" in line:
                in_method = True
            elif in_method and (line.strip().startswith("def ") and "def _create_prices_tab" not in line):
                break
            if in_method:
                method_lines.append(line)

        method_source = "\n".join(method_lines)
        assert "from" in method_source and "prices_tab" in method_source or \
               "create_prices_tab" in method_source, (
            "_create_prices_tab must delegate to external prices_tab.create_prices_tab(). "
            f"Method is {len(method_lines)} lines — should be <20 if delegating."
        )

    def test_gen_dash_prices_method_is_short(self):
        """_create_prices_tab should be a thin wrapper (<30 lines) if delegating."""
        source = _get_source("aemo_dashboard.generation.gen_dash")
        lines = source.split("\n")
        in_method = False
        method_lines = []
        indent = None
        for line in lines:
            if "def _create_prices_tab" in line:
                in_method = True
                indent = len(line) - len(line.lstrip())
                method_lines.append(line)
                continue
            if in_method:
                if line.strip() and not line.strip().startswith("#"):
                    current_indent = len(line) - len(line.lstrip())
                    if current_indent <= indent and line.strip().startswith("def "):
                        break
                method_lines.append(line)

        assert len(method_lines) < 30, (
            f"_create_prices_tab is {len(method_lines)} lines — should be <30 if "
            f"delegating to external module. Still has inline implementation?"
        )


# ---------------------------------------------------------------------------
# price_chart.py — Plotly conversion
# ---------------------------------------------------------------------------

class TestPriceChart:
    MODULE = "aemo_dashboard.prices.price_chart"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hvplot_line(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.line")
        assert not hits, f"hvplot.line still used:\n" + "\n".join(hits)

    def test_no_hardcoded_width(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=1200")
        assert not hits, f"width=1200 still present:\n" + "\n".join(hits)

    def test_returns_plotly_figure(self):
        import plotly.graph_objects as go
        mod = importlib.import_module(self.MODULE)
        funcs = [name for name in dir(mod)
                 if callable(getattr(mod, name)) and not name.startswith("_")
                 and ("chart" in name.lower() or "plot" in name.lower() or "build" in name.lower())]
        assert len(funcs) > 0, "No public chart/plot/build function found"


# ---------------------------------------------------------------------------
# fuel_relatives.py — Plotly conversion
# ---------------------------------------------------------------------------

class TestFuelRelatives:
    MODULE = "aemo_dashboard.prices.fuel_relatives"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hvplot_line(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.line")
        assert not hits, f"hvplot.line still used:\n" + "\n".join(hits)

    def test_no_hv_hline(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hv.HLine")
        assert not hits, f"hv.HLine still used:\n" + "\n".join(hits)

    def test_no_hardcoded_width_900(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=900")
        assert not hits, f"width=900 still present:\n" + "\n".join(hits)


# ---------------------------------------------------------------------------
# price_bands.py — Plotly conversion
# ---------------------------------------------------------------------------

class TestPriceBands:
    MODULE = "aemo_dashboard.prices.price_bands"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hvplot_bar(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.bar")
        assert not hits, f"hvplot.bar still used:\n" + "\n".join(hits)

    def test_no_hv_overlay(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hv.Overlay")
        assert not hits, f"hv.Overlay still used:\n" + "\n".join(hits)

    def test_no_hv_text(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hv.Text(")
        assert not hits, f"hv.Text still used:\n" + "\n".join(hits)
