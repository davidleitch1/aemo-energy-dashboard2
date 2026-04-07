"""
Phase 4: Trends (Penetration) Tab responsive tests.

penetration_tab.py has the heaviest hvplot usage — 13+ hvplot.line calls.
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


class TestPenetrationTab:
    """penetration_tab.py — all 13+ hvplot.line must be Plotly."""

    MODULE = "aemo_dashboard.penetration.penetration_tab"

    def test_no_holoviews_import(self):
        source = _get_source(self.MODULE)
        assert "import holoviews" not in source
        assert "import hvplot" not in source

    def test_no_hvplot_line(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "hvplot.line")
        assert not hits, f"hvplot.line still used ({len(hits)}):\n" + "\n".join(hits[:5])

    def test_no_pn_pane_holoviews(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "pn.pane.HoloViews")
        assert not hits, f"pn.pane.HoloViews still used:\n" + "\n".join(hits)

    def test_no_hardcoded_width_700(self):
        source = _get_source(self.MODULE)
        hits = _count_pattern(source, "width=700")
        assert not hits, f"width=700 still present ({len(hits)}):\n" + "\n".join(hits[:5])
