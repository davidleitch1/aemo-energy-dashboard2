#!/usr/bin/env python3
"""Visual test of the daily summary component"""

import sys
sys.path.insert(0, 'src')

import panel as pn
pn.extension()

from aemo_dashboard.nem_dash.daily_summary import create_daily_summary_component

# Create the component
component = create_daily_summary_component()

# Create a simple app to view it
app = pn.template.MaterialTemplate(
    title="Daily Summary Component Test",
    main=[
        pn.Column(
            "## Daily Summary Component Preview",
            pn.Spacer(height=20),
            component,
            sizing_mode='stretch_width'
        )
    ]
)

print("Opening daily summary component in browser...")
print("Press Ctrl+C to stop the server")
app.show(port=5008)