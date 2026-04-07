"""
Phase 5: Remaining tabs responsive tests.

Covers: curtailment, station analysis, insights, pasa, gas, futures, spot_prices
"""
import pytest
import importlib
import inspect

pytestmark = pytest.mark.responsive


def _get_source(module_path):
    mod = importlib.import_module(module_path)
    return inspect.getsource(mod)


def _count_pattern(source, pattern):
    lines = source.split("\n")
    return [
        f"Line {i}: {l.strip()[:80]}"
        for i, l in enumerate(lines, 1)
        if pattern in l and not l.strip().startswith("#")
    ]


# ---------------------------------------------------------------------------
# curtailment_tab.py
# ---------------------------------------------------------------------------

class TestCurtailmentTab:
    MODULE = "aemo_dashboard.curtailment.curtailment_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hvplot_area(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.area")
        assert not hits, f"hvplot.area still used:\n" + "\n".join(hits)

    def test_no_hvplot_line(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.line")
        assert not hits, f"hvplot.line still used:\n" + "\n".join(hits)

    def test_no_hardcoded_width_900(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=900")
        assert not hits, f"width=900:\n" + "\n".join(hits)

    def test_no_hardcoded_width_800(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=800")
        assert not hits, f"width=800:\n" + "\n".join(hits)


# ---------------------------------------------------------------------------
# station_analysis_ui.py
# ---------------------------------------------------------------------------

class TestStationAnalysis:
    MODULE = "aemo_dashboard.station.station_analysis_ui"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hardcoded_width_1000(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=1000")
        assert not hits, f"width=1000:\n" + "\n".join(hits)


# ---------------------------------------------------------------------------
# insights_tab.py (Pivot Table)
# ---------------------------------------------------------------------------

class TestInsightsTab:
    MODULE = "aemo_dashboard.insights.insights_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_pn_pane_holoviews(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "pn.pane.HoloViews")
        assert not hits, f"pn.pane.HoloViews:\n" + "\n".join(hits)

    def test_no_hvplot_scatter(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.scatter")
        assert not hits, f"hvplot.scatter:\n" + "\n".join(hits)

    def test_no_hvplot_area(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.area")
        assert not hits, f"hvplot.area:\n" + "\n".join(hits)

    def test_no_hardcoded_width_1000(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=1000")
        assert not hits, f"width=1000:\n" + "\n".join(hits)


# ---------------------------------------------------------------------------
# pasa_tab.py
# ---------------------------------------------------------------------------

class TestPasaTab:
    MODULE = "aemo_dashboard.pasa.pasa_tab"

    def test_tables_have_scroll_wrappers(self):
        source = _get_source(self.MODULE)
        table_count = source.count("<table")
        scroll_count = source.count("overflow-x") + source.count("responsive-table")
        if table_count > 0:
            assert scroll_count >= table_count, (
                f"Found {table_count} <table> but only {scroll_count} scroll wrappers"
            )


# ---------------------------------------------------------------------------
# gas/sttm_tab.py — already Plotly, just widget widths
# ---------------------------------------------------------------------------

class TestGasTab:
    MODULE = "aemo_dashboard.gas.sttm_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source


# ---------------------------------------------------------------------------
# futures/futures_tab.py — already Plotly, just widget widths + responsiveness
# ---------------------------------------------------------------------------

class TestFuturesTab:
    MODULE = "aemo_dashboard.futures.futures_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source

    def test_plotly_autosize(self):
        """All figures must have autosize=True."""
        source = _get_source(self.MODULE)
        assert "autosize=True" in source or "autosize = True" in source, (
            "Plotly figures must have autosize=True"
        )


# ---------------------------------------------------------------------------
# spot_prices/display_spot.py
# ---------------------------------------------------------------------------

class TestSpotPrices:
    MODULE = "aemo_dashboard.spot_prices.display_spot"

    def test_no_sizing_mode_fixed(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "sizing_mode='fixed'")
        assert not hits, f"sizing_mode='fixed' ({len(hits)}):\n" + "\n".join(hits[:5])

    def test_no_inline_width_450(self):
        source = _get_source(self.MODULE)
        hits = [l for l in _count_pattern(source, "width:450px") if "style" in l.lower()]
        assert not hits, f"Inline CSS width:450px:\n" + "\n".join(hits)
