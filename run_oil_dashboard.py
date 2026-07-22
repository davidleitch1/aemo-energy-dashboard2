#!/usr/bin/env python3
"""
Standalone launcher for the WTI oil futures curve.

Runs the Oil tab as its own Panel app, independent of the main AEMO
dashboard — nothing is added to the live dashboard tabs.

Prerequisites
-------------
Seed the data once (needs outbound access to Yahoo Finance)::

    uv run aemo-oil-update --backfill

then keep it current with ``uv run aemo-oil-update`` (idempotent; cron-safe).

Run
---
    uv run python run_oil_dashboard.py        # serves http://localhost:5011
"""

import os
import sys
from pathlib import Path

# Make the src/ package importable when run from the repo root.
sys.path.insert(0, str(Path(__file__).parent / "src"))

import panel as pn

from aemo_dashboard.oil import create_oil_tab

PORT = int(os.getenv("OIL_DASHBOARD_PORT", "5011"))


def app():
    """Panel app factory: the Oil tab wrapped with a title."""
    pn.extension("plotly", sizing_mode="stretch_width")
    return pn.Column(
        pn.pane.Markdown("# Oil Futures — WTI Crude"),
        create_oil_tab(),
        sizing_mode="stretch_width",
    )


def main():
    pn.extension("plotly")
    pn.serve(
        app,
        port=PORT,
        show=False,
        title="Oil Futures (WTI)",
        websocket_origin="*",
    )


if __name__ == "__main__":
    main()
