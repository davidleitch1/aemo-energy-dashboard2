#!/usr/bin/env python3
"""
Test the penetration tab fixes for Y-axis scaling and selector behavior.
"""
import os
import sys
import time
import panel as pn

# Set environment variables
os.environ['DASHBOARD_PORT'] = '5010'
os.environ['USE_DUCKDB'] = 'true'

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from aemo_dashboard.penetration import PenetrationTab

def test_penetration_tab():
    """Test the penetration tab with different selector combinations."""
    print("Testing Penetration Tab fixes...")
    
    # Initialize Panel
    pn.extension('tabulator')
    
    # Create the penetration tab
    penetration = PenetrationTab()
    layout = penetration.create_layout()
    
    # Create a simple test app
    template = pn.template.MaterialTemplate(
        title="Penetration Tab Test",
        sidebar=[
            pn.pane.Markdown("## Test Instructions\n\n" +
                           "1. Select 'Solar' from Fuel Type\n" +
                           "2. Select 'NSW1' from Region\n" +
                           "3. Y-axis should auto-scale to ~10 TWh\n" +
                           "4. Switch back to 'VRE' - chart should replace, not add series\n" +
                           "5. Switch regions - chart should update cleanly")
        ]
    )
    
    template.main.append(layout)
    
    print("\nStarting test server on http://localhost:5010")
    print("Check the following:")
    print("1. Y-axis auto-scales when selecting Solar + NSW1")
    print("2. Changing selectors replaces chart (no duplicate series)")
    print("3. Chart width is 600px")
    print("\nPress Ctrl+C to exit")
    
    template.show(port=5010, open=False)

if __name__ == "__main__":
    test_penetration_tab()