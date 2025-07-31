#!/usr/bin/env python3
"""Test the Insights tab comparison table showing all 5 regions by default"""

import pandas as pd
import panel as pn
from datetime import datetime, timedelta
import sys
sys.path.insert(0, 'src')

from aemo_dashboard.insights.insights_tab import InsightsTab
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

def test_insights_table():
    """Test that comparison table shows all 5 regions on load"""
    print("Testing Insights tab with all-regions comparison table...")
    
    # Create insights tab
    insights = InsightsTab()
    
    # Check that the table is being generated
    print("\nChecking initial table content...")
    
    # Manually call the initial table generation since we're not in a server context
    insights._generate_initial_table()
    
    # Get the table content
    table_html = insights.comparison_table_pane.object
    
    # Check that all regions are present
    regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
    missing_regions = []
    
    for region in regions:
        if region not in table_html:
            missing_regions.append(region)
        else:
            print(f"✓ Found {region} in table")
    
    if missing_regions:
        print(f"\n❌ Missing regions in table: {missing_regions}")
    else:
        print("\n✅ All 5 regions are displayed in the comparison table")
    
    # Check table structure
    if "2020" in table_html and "Last 12mo" in table_html:
        print("✅ Table shows both 2020 and Last 12 months data")
    else:
        print("❌ Table missing time period columns")
    
    if "VRE Share" in table_html:
        print("✅ Table includes VRE Share column")
    else:
        print("❌ Table missing VRE Share column")
    
    if "Variability*" in table_html:
        print("✅ Table shows Variability (CV%) column")
    else:
        print("❌ Table missing Variability column")
    
    # Test that updating with selected regions still works
    print("\n\nTesting update with selected regions...")
    insights.region_selector.value = ['NSW1', 'VIC1']
    insights._update_comparison_table()
    
    updated_html = insights.comparison_table_pane.object
    
    # Check only selected regions are shown
    selected_shown = all(region in updated_html for region in ['NSW1', 'VIC1'])
    unselected_shown = any(region in updated_html for region in ['QLD1', 'SA1', 'TAS1'])
    
    if selected_shown and not unselected_shown:
        print("✅ Update button correctly shows only selected regions")
    else:
        print("❌ Update button not working correctly")
    
    return insights

if __name__ == "__main__":
    pn.extension('tabulator', 'bokeh')
    
    print("=" * 60)
    print("INSIGHTS TAB ALL-REGIONS TABLE TEST")
    print("=" * 60)
    
    insights = test_insights_table()
    
    print("\n\nTest complete!")
    print("\nTo see the full dashboard with the updated Insights tab, run:")
    print(".venv/bin/python src/aemo_dashboard/generation/gen_dash.py")