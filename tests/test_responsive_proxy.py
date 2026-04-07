"""
Phase 6: Reverse proxy tests.

Verify the dashboard is served directly (no iframe) with proper
viewport meta and WebSocket origin configuration.
"""
import pytest
import importlib
import inspect
import re
from pathlib import Path

pytestmark = pytest.mark.responsive

SRC_DIR = Path(__file__).parent.parent / "src" / "aemo_dashboard"


class TestViewportMeta:
    """Dashboard must include viewport meta tag for mobile responsiveness."""

    def test_viewport_meta_in_raw_css_or_config(self):
        """Panel config must enable responsive viewport (sizing_mode or meta tag)."""
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        has_viewport = (
            ("viewport" in content and "width=device-width" in content)
            or "pn.config.sizing_mode" in content
        )
        assert has_viewport, (
            "gen_dash.py must set pn.config.sizing_mode or include viewport meta tag"
        )

    def test_no_fixed_sizing_mode_default(self):
        """Panel should not default to fixed sizing mode."""
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        # Check that sizing_mode is not set to 'fixed' globally
        assert "pn.config.sizing_mode = 'fixed'" not in content


class TestWebSocketOrigin:
    """WebSocket origin must include the production domain."""

    def test_allow_websocket_origin_has_domain(self):
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        assert "nemgen.itkservices2.com" in content, (
            "allow_websocket_origin must include nemgen.itkservices2.com"
        )

    def test_no_wildcard_origin_in_production(self):
        """Wildcard '*' in allow_websocket_origin is a security risk."""
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        # Find the allow_websocket_origin line
        for line in content.split("\n"):
            if "allow_websocket_origin" in line and not line.strip().startswith("#"):
                assert '"*"' not in line, (
                    "allow_websocket_origin should not include wildcard '*' in production"
                )
                break


class TestNoIframe:
    """The Quarto page must not embed the dashboard in an iframe."""

    def test_quarto_file_no_iframe(self):
        """nemgen.qmd must redirect, not iframe."""
        # Check the local Quarto file
        qmd_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/itk_articles/nemgen.qmd")
        if not qmd_path.exists():
            pytest.skip("nemgen.qmd not found on this machine")
        content = qmd_path.read_text()
        assert "<iframe" not in content.lower(), (
            "nemgen.qmd still uses an iframe — should redirect to nemgen.itkservices2.com"
        )


class TestPanelTemplate:
    """Panel template must be self-contained (no iframe dependency)."""

    def test_template_has_title(self):
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        assert "title=" in content and ("NEM" in content or "ITK" in content or "Dashboard" in content)

    def test_template_has_header(self):
        """Dashboard must use a Panel template with header styling."""
        gen_dash = SRC_DIR / "generation" / "gen_dash.py"
        content = gen_dash.read_text()
        assert "header_background" in content or "template=" in content
