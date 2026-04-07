"""
Phase 0: CSS Foundation tests.

Verify that global CSS prevents overflow and establishes responsive defaults.
"""
import pytest
import re

pytestmark = pytest.mark.responsive


class TestCSSFoundation:
    """The raw_css block in gen_dash.py must include responsive rules."""

    def _get_raw_css(self):
        from aemo_dashboard.generation import gen_dash
        import panel as pn
        return "\n".join(pn.config.raw_css)

    def test_body_overflow_x_hidden(self):
        css = self._get_raw_css()
        assert "overflow-x" in css, "body must have overflow-x rule"

    def test_box_sizing_border_box(self):
        css = self._get_raw_css()
        assert "box-sizing" in css and "border-box" in css, (
            "Global box-sizing: border-box required"
        )

    def test_responsive_table_wrapper_class(self):
        css = self._get_raw_css()
        assert "responsive-table" in css, (
            "CSS must define .responsive-table class with overflow-x: auto"
        )

    def test_bk_root_max_width(self):
        css = self._get_raw_css()
        assert "max-width" in css and "100%" in css, (
            "Containers must have max-width: 100%"
        )


class TestNoHorizontalOverflow:
    """Source code must not contain patterns that cause horizontal overflow."""

    def _read_source(self, module_path):
        import importlib
        import inspect
        mod = importlib.import_module(module_path)
        return inspect.getsource(mod)

    def test_gen_dash_no_unguarded_width_1200(self):
        """gen_dash.py must not have width=1200 in hvplot/opts calls."""
        source = self._read_source("aemo_dashboard.generation.gen_dash")
        # Find width=1200 that's in a plot context (not a widget/CSS)
        lines = source.split("\n")
        violations = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if "width=1200" in stripped and not stripped.startswith("#"):
                # Allow in CSS/HTML context, not in .opts() or hvplot
                if ".opts(" in stripped or "hvplot" in stripped:
                    violations.append(f"Line {i}: {stripped[:80]}")
        assert not violations, f"Hardcoded width=1200 in plot calls:\n" + "\n".join(violations)
