"""
Phase 3: Generation Mix Tab responsive tests.

Covers remaining HoloViews in gen_dash.py (price analysis sub-tab).
The utilization/transmission plots were already converted to Plotly.
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


class TestGenDashHoloViewsRemoval:
    """gen_dash.py must have zero remaining HoloViews panes."""

    MODULE = "aemo_dashboard.generation.gen_dash"

    def test_no_pn_pane_holoviews(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "pn.pane.HoloViews")
        assert not hits, f"pn.pane.HoloViews still used ({len(hits)}):\n" + "\n".join(hits)

    def test_no_hv_text(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hv.Text(")
        assert not hits, f"hv.Text still used ({len(hits)}):\n" + "\n".join(hits[:5])

    def test_no_hvplot_line_in_prices(self):
        """The price analysis hvplot.line calls must be converted."""
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.line")
        assert not hits, f"hvplot.line still used ({len(hits)}):\n" + "\n".join(hits)

    def test_no_hvplot_area(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.area")
        assert not hits, f"hvplot.area still used ({len(hits)}):\n" + "\n".join(hits)


class TestGenDashWidths:
    """gen_dash.py must not have hardcoded widths in plot calls."""

    MODULE = "aemo_dashboard.generation.gen_dash"

    def test_no_width_1200_in_opts(self):
        source = _get_source(self.MODULE)
        lines = source.split("\n")
        violations = []
        for i, l in enumerate(lines, 1):
            if "width=1200" in l and not l.strip().startswith("#"):
                # Allow in CSS/HTML strings, not in .opts() or hvplot
                context = l.strip()
                if ".opts(" in context or "hvplot" in context:
                    violations.append(f"Line {i}: {context[:80]}")
        assert not violations, f"width=1200 in plot calls:\n" + "\n".join(violations)

    def test_no_css_max_width_550(self):
        """The hardcoded max-width: 550px CSS must be removed or made responsive."""
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "max-width: 550px")
        assert not hits, f"Hardcoded max-width: 550px:\n" + "\n".join(hits)

    def test_no_css_max_width_620(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "max-width: 620px")
        assert not hits, f"Hardcoded max-width: 620px:\n" + "\n".join(hits)

    def test_generation_tab_panes_stretch_width(self):
        """The generation tab pane containers should use stretch_width, not max_width=1250."""
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "max_width=1250")
        assert not hits, f"max_width=1250 constrains responsiveness:\n" + "\n".join(hits)
