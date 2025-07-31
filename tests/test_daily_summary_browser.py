#!/usr/bin/env python3
"""Test the daily summary component in browser to verify fixes"""

import sys
sys.path.insert(0, 'src')

import panel as pn
pn.extension()

from aemo_dashboard.nem_dash.daily_summary import create_daily_summary_component

# Create the component
print("Creating daily summary component...")
component = create_daily_summary_component()

# Create a simple app
app = pn.template.MaterialTemplate(
    title="Daily Summary Test - Fixed Version",
    main=[
        pn.Column(
            "## Daily Summary Component (Fixed)",
            pn.pane.Markdown("""
            ### Changes Made:
            - ✅ NEM average now shows correctly (simple average when no generation data)
            - ✅ Insights always show at least one comment
            - ✅ Volume-weighted note only shows when applicable
            - ✅ All values use 0 decimal places
            """),
            pn.Spacer(height=20),
            component,
            sizing_mode='stretch_width'
        )
    ]
)

print("Opening in browser on port 5009...")
print("Press Ctrl+C to stop")
app.show(port=5009)