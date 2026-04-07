"""
Phase 7: Cleanup verification tests.

After all conversions, verify no HoloViews/Bokeh artifacts remain.
"""
import pytest
import importlib
import inspect
import os
from pathlib import Path

pytestmark = pytest.mark.responsive

SRC_DIR = Path(__file__).parent.parent / "src" / "aemo_dashboard"


def _all_py_files():
    """Yield all .py files in src/aemo_dashboard/, excluding backups."""
    excludes = {"_original", "_cached", "_debug", "_diagnostic", "_fast", "_fixed", "_optimized"}
    for root, dirs, files in os.walk(SRC_DIR):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            if f.endswith(".py") and not any(ex in f for ex in excludes) and not f.endswith(".bak"):
                yield Path(root) / f


class TestNoHoloViewsImports:
    """No source file should import holoviews or hvplot."""

    def test_no_import_holoviews(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "import holoviews" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"Files still importing holoviews:\n" + "\n".join(violations)

    def test_no_import_hvplot(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "import hvplot" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"Files still importing hvplot:\n" + "\n".join(violations)


class TestNoHoloViewsUsage:
    """No source file should use hv.*, hvplot.*, or pn.pane.HoloViews."""

    def test_no_pn_pane_holoviews(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "pn.pane.HoloViews" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"pn.pane.HoloViews still used:\n" + "\n".join(violations)

    def test_no_hv_text(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "hv.Text(" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"hv.Text() still used:\n" + "\n".join(violations)

    def test_no_hv_overlay(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "hv.Overlay(" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"hv.Overlay() still used:\n" + "\n".join(violations)

    def test_no_hv_hline(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "hv.HLine(" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}")
        assert not violations, f"hv.HLine() still used:\n" + "\n".join(violations)


class TestNoBokehImports:
    """No source file should import from bokeh (after full migration)."""

    def test_no_bokeh_models_import(self):
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            for i, line in enumerate(content.split("\n"), 1):
                if "from bokeh" in line and not line.strip().startswith("#"):
                    violations.append(f"{path.relative_to(SRC_DIR)}:{i}: {line.strip()[:60]}")
        assert not violations, f"Bokeh imports remain:\n" + "\n".join(violations)


class TestNoBackupFiles:
    """No backup or variant files should remain in src/."""

    def test_no_bak_files(self):
        bak_files = list(SRC_DIR.rglob("*.bak"))
        bak_files += list(SRC_DIR.rglob("*.bak_*"))
        assert not bak_files, f"Backup files found:\n" + "\n".join(str(f) for f in bak_files)

    def test_no_variant_files(self):
        variants = []
        patterns = ["_original.py", "_cached.py", "_debug.py", "_diagnostic.py",
                     "_fast.py", "_fixed.py", "_optimized.py"]
        for pattern in patterns:
            variants.extend(SRC_DIR.rglob(f"*{pattern}"))
        assert not variants, f"Variant files found:\n" + "\n".join(str(f) for f in variants)


class TestNoDeadCode:
    """Verify specific dead code blocks are removed."""

    def test_no_hv_extension_call(self):
        """hv.extension('bokeh') should be removed."""
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            if "hv.extension(" in content:
                violations.append(str(path.relative_to(SRC_DIR)))
        assert not violations, f"hv.extension() still called in:\n" + "\n".join(violations)

    def test_no_hv_opts_defaults(self):
        """hv.opts.defaults() block should be removed."""
        violations = []
        for path in _all_py_files():
            content = path.read_text()
            if "hv.opts.defaults(" in content:
                violations.append(str(path.relative_to(SRC_DIR)))
        assert not violations, f"hv.opts.defaults() still present in:\n" + "\n".join(violations)

    def test_no_bokeh_css_classes(self):
        """Bokeh-specific CSS classes should be removed from raw_css."""
        for path in _all_py_files():
            content = path.read_text()
            if ".bk-canvas-wrapper" in content or ".bk-Figure" in content:
                if "raw_css" in content:
                    pytest.fail(f"Bokeh CSS classes in raw_css: {path.relative_to(SRC_DIR)}")
