#!/usr/bin/env python3
"""Test the final polished daily summary component"""

import sys
sys.path.insert(0, 'src')

import panel as pn
pn.extension()

from aemo_dashboard.nem_dash.daily_summary import create_daily_summary_component

# Create the component
print("Creating final daily summary component...")
component = create_daily_summary_component()

# Create a test app
app = pn.template.MaterialTemplate(
    title="Daily Summary - Final Version",
    main=[
        pn.Column(
            "## Daily Summary Component - Final Polish",
            pn.pane.Markdown("""
            ### Final Changes Implemented:
            - ✅ All numbers rounded to nearest whole number (no decimals)
            - ✅ High/Low prices displayed in white color like average
            - ✅ Renewable, Gas, Coal percentages keep their original colors
            - ✅ Generation year-over-year comparison insight added
            
            ### Features:
            - 24-hour price statistics for all regions + NEM
            - Total generation in GWh (first row after prices)
            - Fuel share percentages with color coding
            - Automated insights comparing today vs yesterday vs last year
            - Generation comparison between this year and last year
            """),
            pn.Spacer(height=20),
            component,
            sizing_mode='stretch_width'
        )
    ]
)

print("Opening in browser on port 5010...")
print("Press Ctrl+C to stop")
app.show(port=5010)