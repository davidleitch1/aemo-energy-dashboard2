#!/usr/bin/env python3
"""RETIRED 2026-07-03. The Panel dashboard is no longer used."""
import sys

sys.exit(
    "\n".join([
        "run_dashboard_duckdb.py (Panel) is RETIRED as of 2026-07-03.",
        "The live dashboard is the FastAPI/HTMX redesign on port 5008.",
        "",
        "Launch it via:",
        "  ~/tmux_files/start_services.sh       (on boot)",
        "  ~/tmux_files/restart_dashboards.sh   (nightly cron)",
        "or directly (one line):",
        "  cd /Users/davidleitch/aemo-redesign/src/aemo_dashboard/web && "
        "/Users/davidleitch/aemo_production/aemo-energy-dashboard2/.venv/bin/uvicorn "
        "app:app --host 0.0.0.0 --port 5008 --workers 4",
        "",
        "Original file preserved at ./_retired_panel/run_dashboard_duckdb.py",
    ])
)
