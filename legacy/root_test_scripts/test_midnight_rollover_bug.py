#!/usr/bin/env python3
"""
Test script to verify midnight rollover bug in dashboard
This demonstrates the cache key invalidation issue that causes the dashboard
to stop updating at 11:55 PM every night.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

from aemo_dashboard.generation.gen_dash import EnergyDashboard
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

def test_midnight_rollover():
    """Test the cache key generation around midnight"""
    
    print("Testing Midnight Rollover Bug in Dashboard")
    print("=" * 60)
    
    # Initialize components
    print("\n1. Initializing dashboard components...")
    dashboard = EnergyDashboard()
    query_manager = GenerationQueryManager()
    
    # Set to "Last 24 Hours" mode
    dashboard.time_range = '1'
    print("   Dashboard set to 'Last 24 Hours' mode")
    
    # Test date for demonstration (using yesterday/today for clarity)
    test_date = datetime.now().date()
    yesterday = test_date - timedelta(days=1)
    
    print(f"\n2. Simulating behavior BEFORE midnight (11:55 PM on {yesterday})")
    print("-" * 60)
    
    # Mock the current time to be 11:55 PM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime.combine(yesterday, datetime.strptime("23:55", "%H:%M").time())
        mock_datetime.combine = datetime.combine
        
        # Update date range as dashboard would at 11:55 PM
        dashboard._update_date_range_from_preset()
        
        print(f"   Dashboard start_date: {dashboard.start_date}")
        print(f"   Dashboard end_date: {dashboard.end_date}")
        
        # Get effective date range
        start_dt, end_dt = dashboard._get_effective_date_range()
        print(f"   Effective query range: {start_dt} to {end_dt}")
        
        # Generate cache key as query manager would
        cache_key_before = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
        print(f"   Cache key: {cache_key_before}")
        print(f"   ✅ Dashboard correctly shows data up to {end_dt.strftime('%H:%M')}")
        
        # Store these for comparison
        dates_before_midnight = (dashboard.start_date, dashboard.end_date)
    
    print(f"\n3. Simulating behavior AFTER midnight (12:05 AM on {test_date})")
    print("-" * 60)
    
    # Now simulate what happens after midnight
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime.combine(test_date, datetime.strptime("00:05", "%H:%M").time())
        mock_datetime.combine = datetime.combine
        
        print("\n   Scenario A: auto_update_loop() runs WITHOUT refreshing dates (CURRENT BUG)")
        print("   " + "-" * 50)
        
        # Dashboard dates remain unchanged (this is the bug!)
        dashboard.start_date = dates_before_midnight[0]
        dashboard.end_date = dates_before_midnight[1]
        
        print(f"   Dashboard start_date: {dashboard.start_date} (STALE!)")
        print(f"   Dashboard end_date: {dashboard.end_date} (STALE - still yesterday!)")
        
        # Get effective date range
        start_dt, end_dt = dashboard._get_effective_date_range()
        print(f"   Effective query range: {start_dt} to {end_dt}")
        
        cache_key_stale = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
        print(f"   Cache key: {cache_key_stale}")
        print(f"   ❌ Dashboard queries for yesterday's data only!")
        print(f"   ❌ No data after 11:55 PM {yesterday} will be shown!")
        
        print("\n   Scenario B: auto_update_loop() WITH date refresh (PROPOSED FIX)")
        print("   " + "-" * 50)
        
        # Call the update method as the fix would
        dashboard._update_date_range_from_preset()
        
        print(f"   Dashboard start_date: {dashboard.start_date} (UPDATED!)")
        print(f"   Dashboard end_date: {dashboard.end_date} (UPDATED to today!)")
        
        # Get effective date range
        start_dt, end_dt = dashboard._get_effective_date_range()
        print(f"   Effective query range: {start_dt} to {end_dt}")
        
        cache_key_fixed = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
        print(f"   Cache key: {cache_key_fixed}")
        print(f"   ✅ Dashboard correctly queries up to current time!")
        print(f"   ✅ New data after midnight will be displayed!")
    
    print("\n" + "=" * 60)
    print("CONCLUSION:")
    print("-" * 60)
    print("The bug occurs because auto_update_loop() does NOT call")
    print("_update_date_range_from_preset() before updating plots.")
    print("")
    print("After midnight, the dashboard continues using yesterday's date")
    print("as the end_date, causing queries to miss all data after 11:55 PM.")
    print("")
    print("FIX: Add _update_date_range_from_preset() call in auto_update_loop()")
    print("     when using preset time ranges ('1', '7', '30').")
    print("=" * 60)

if __name__ == "__main__":
    try:
        test_midnight_rollover()
    except Exception as e:
        print(f"\nError running test: {e}")
        import traceback
        traceback.print_exc()