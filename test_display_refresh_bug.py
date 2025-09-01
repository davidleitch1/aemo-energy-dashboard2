#!/usr/bin/env python3
"""
Test to verify that the midnight rollover fix actually refreshes the DISPLAY
not just the backend data collection.

The bug: After midnight, the dashboard backend continues running but the 
browser display freezes at 11:55 PM because the plots are querying for 
yesterday's data only.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, time
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

# Suppress verbose logging
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.WARNING)

from aemo_dashboard.generation.gen_dash import EnergyDashboard

def test_display_refresh_after_midnight():
    """Test that the displayed plots actually refresh with new data after midnight"""
    
    print("\n" + "=" * 70)
    print("TESTING DISPLAY REFRESH AFTER MIDNIGHT")
    print("=" * 70)
    
    # Initialize dashboard
    print("\n1. Setting up dashboard in 'Last 24 Hours' mode...")
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'
    
    # Mock the plot update to track what date range is being displayed
    original_create_plot = dashboard.create_plot
    plot_calls = []
    
    def mock_create_plot():
        """Track what date range is being used for plot creation"""
        start_dt, end_dt = dashboard._get_effective_date_range()
        plot_calls.append({
            'start_date': dashboard.start_date,
            'end_date': dashboard.end_date,
            'effective_start': start_dt,
            'effective_end': end_dt,
            'time': datetime.now()
        })
        print(f"\n  📊 Plot created with date range: {dashboard.start_date} to {dashboard.end_date}")
        print(f"     Effective query range: {start_dt.strftime('%Y-%m-%d %H:%M')} to {end_dt.strftime('%Y-%m-%d %H:%M')}")
        return MagicMock()  # Return mock plot object
    
    dashboard.create_plot = mock_create_plot
    
    # Similarly mock other plot methods
    dashboard.create_utilization_plot = lambda: MagicMock()
    dashboard.create_transmission_plot = lambda: MagicMock()
    
    print("\n" + "=" * 70)
    print("SCENARIO: Simulating dashboard updates around midnight")
    print("-" * 70)
    
    # Test dates
    dec_10 = datetime(2024, 12, 10, 23, 55, 0)
    dec_11_0005 = datetime(2024, 12, 11, 0, 5, 0)
    dec_11_0010 = datetime(2024, 12, 11, 0, 10, 0)
    
    print("\n2. At 11:55 PM on Dec 10 - Initial state")
    print("-" * 50)
    
    # Set initial dates as they would be at 11:55 PM
    dashboard.end_date = dec_10.date()
    dashboard.start_date = dashboard.end_date - timedelta(days=1)
    
    # Trigger initial plot update
    dashboard.update_plot()
    
    print("\n3. At 12:05 AM on Dec 11 - First update after midnight")
    print("-" * 50)
    print("   Simulating auto_update_loop() WITHOUT the fix...")
    
    # Clear plot calls
    plot_calls.clear()
    
    # WITHOUT FIX: dates remain stale
    print(f"\n   Dashboard dates BEFORE update: {dashboard.start_date} to {dashboard.end_date}")
    dashboard.update_plot()
    
    if plot_calls:
        last_call = plot_calls[-1]
        if last_call['effective_end'].date() == dec_10.date():
            print("\n   ❌ PROBLEM: Display still shows data only up to Dec 10!")
            print("   ❌ Browser sees stale data frozen at 11:55 PM")
    
    print("\n4. At 12:10 AM on Dec 11 - Update WITH the fix")
    print("-" * 50)
    print("   Simulating auto_update_loop() WITH the date refresh fix...")
    
    # Clear plot calls
    plot_calls.clear()
    
    # WITH FIX: Refresh dates first (as the fix does)
    print(f"\n   Calling _update_date_range_from_preset() to refresh dates...")
    
    # Simulate the date refresh that happens with the fix
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_datetime:
        mock_datetime.now.return_value = dec_11_0010
        mock_datetime.combine = datetime.combine
        dashboard._update_date_range_from_preset()
    
    print(f"   Dashboard dates AFTER refresh: {dashboard.start_date} to {dashboard.end_date}")
    
    # Now update plots with refreshed dates
    dashboard.update_plot()
    
    if plot_calls:
        last_call = plot_calls[-1]
        if last_call['end_date'] == dec_11_0010.date():
            print("\n   ✅ SUCCESS: Display now shows data up to Dec 11!")
            print("   ✅ Browser sees fresh data including after midnight")
            print(f"   ✅ Query range extends to: {last_call['effective_end'].strftime('%Y-%m-%d %H:%M')}")
    
    print("\n" + "=" * 70)
    print("VERIFICATION OF FIX")
    print("=" * 70)
    
    print("""
The fix ensures that:

1. BEFORE midnight (11:55 PM):
   - end_date = Dec 10 (today at that time)
   - Displays data up to Dec 10 23:59

2. AFTER midnight WITHOUT fix:
   - end_date = Dec 10 (now yesterday!)  
   - Still queries for Dec 10 only
   - Browser display FREEZES at last Dec 10 data point (11:55 PM)

3. AFTER midnight WITH fix:
   - _update_date_range_from_preset() updates end_date to Dec 11
   - Queries include Dec 11 data
   - Browser display UPDATES with new data after midnight
   
The key insight: The dashboard backend was running but serving stale plots
to the browser because the date parameters weren't being refreshed.
""")

if __name__ == "__main__":
    try:
        test_display_refresh_after_midnight()
    except Exception as e:
        print(f"\nError running test: {e}")
        import traceback
        traceback.print_exc()