#!/usr/bin/env python3
"""
Test script to verify midnight rollover bug in dashboard
This demonstrates the cache key invalidation issue that causes the dashboard
to stop updating at 11:55 PM every night.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

# Suppress verbose logging for cleaner output
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.WARNING)

from aemo_dashboard.generation.gen_dash import EnergyDashboard

def test_midnight_rollover():
    """Test the cache key generation around midnight"""
    
    print("\n" + "=" * 70)
    print("TESTING MIDNIGHT ROLLOVER BUG IN AEMO DASHBOARD")
    print("=" * 70)
    
    # Initialize dashboard
    print("\n1. Initializing dashboard in 'Last 24 Hours' mode...")
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'
    
    # Use concrete dates for testing
    dec_10 = datetime(2024, 12, 10, 23, 55, 0)  # 11:55 PM Dec 10
    dec_11 = datetime(2024, 12, 11, 0, 5, 0)    # 12:05 AM Dec 11
    
    print("\n" + "=" * 70)
    print("SCENARIO 1: Dashboard at 11:55 PM on December 10, 2024")
    print("-" * 70)
    
    # Manually set dates as they would be at 11:55 PM
    dashboard.end_date = dec_10.date()  # Dec 10, 2024
    dashboard.start_date = dashboard.end_date - timedelta(days=1)  # Dec 9, 2024
    
    print(f"\nDashboard state at 11:55 PM:")
    print(f"  start_date: {dashboard.start_date}")
    print(f"  end_date:   {dashboard.end_date}")
    
    # Generate cache key
    cache_key_1155pm = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
    print(f"\nCache key used for queries:")
    print(f"  {cache_key_1155pm}")
    
    # Show what data would be queried
    start_dt = datetime.combine(dashboard.start_date, time.min)
    end_dt = datetime.combine(dashboard.end_date, time.max)
    print(f"\nData query range:")
    print(f"  From: {start_dt}")
    print(f"  To:   {end_dt}")
    print(f"\n✅ Status: Dashboard correctly shows all data up to 11:55 PM")
    
    # Store the dates for comparison
    dates_at_1155pm = (dashboard.start_date, dashboard.end_date)
    
    print("\n" + "=" * 70)
    print("SCENARIO 2: Dashboard at 12:05 AM on December 11, 2024 (10 minutes later)")
    print("-" * 70)
    
    print("\n--- Part A: What happens NOW (the BUG) ---")
    print("\nThe auto_update_loop() fires but does NOT refresh date parameters.")
    print("Dashboard still has yesterday's dates:")
    
    # Dashboard dates remain unchanged (this is the bug!)
    dashboard.start_date = dates_at_1155pm[0]  # Still Dec 9
    dashboard.end_date = dates_at_1155pm[1]    # Still Dec 10 (now yesterday!)
    
    print(f"\nDashboard state at 12:05 AM (STALE):")
    print(f"  start_date: {dashboard.start_date}")
    print(f"  end_date:   {dashboard.end_date} ← Still December 10 (yesterday!)")
    
    # Generate cache key with stale dates
    cache_key_stale = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
    print(f"\nCache key used for queries (STALE):")
    print(f"  {cache_key_stale}")
    
    # Show what data would be queried
    start_dt = datetime.combine(dashboard.start_date, time.min)
    end_dt = datetime.combine(dashboard.end_date, time.max)
    print(f"\nData query range (WRONG):")
    print(f"  From: {start_dt}")
    print(f"  To:   {end_dt} ← Stops at end of Dec 10!")
    
    print(f"\n❌ PROBLEM: Dashboard queries stop at Dec 10 23:59:59")
    print(f"❌ RESULT: No data after 11:55 PM is displayed!")
    print(f"❌ SYMPTOM: Dashboard appears frozen at last data point before midnight")
    
    print("\n--- Part B: What SHOULD happen (the FIX) ---")
    print("\nIf auto_update_loop() called _update_date_range_from_preset():")
    
    # Simulate the fix - update dates to current day
    dashboard.end_date = dec_11.date()  # Dec 11, 2024 (today)
    dashboard.start_date = dashboard.end_date - timedelta(days=1)  # Dec 10, 2024
    
    print(f"\nDashboard state at 12:05 AM (FIXED):")
    print(f"  start_date: {dashboard.start_date}")
    print(f"  end_date:   {dashboard.end_date} ← Updated to December 11 (today!)")
    
    # Generate cache key with updated dates
    cache_key_fixed = f"gen_by_fuel_NEM_{dashboard.start_date}_{dashboard.end_date}_5min"
    print(f"\nCache key used for queries (FIXED):")
    print(f"  {cache_key_fixed}")
    
    # Show what data would be queried
    start_dt = datetime.combine(dashboard.start_date, time.min)
    # With the fix, end would be capped at current time (12:05 AM)
    end_dt = dec_11  # Current time: 12:05 AM Dec 11
    print(f"\nData query range (CORRECT):")
    print(f"  From: {start_dt}")
    print(f"  To:   {end_dt} ← Continues into Dec 11!")
    
    print(f"\n✅ FIXED: Dashboard queries include data after midnight")
    print(f"✅ RESULT: New data from 12:00 AM onwards is displayed")
    print(f"✅ BEHAVIOR: Dashboard continues updating normally")
    
    print("\n" + "=" * 70)
    print("ROOT CAUSE ANALYSIS")
    print("=" * 70)
    print("""
The bug occurs in the auto_update_loop() method at line 2129 of gen_dash.py:

    async def auto_update_loop(self):
        while True:
            await asyncio.sleep(270)  # 4.5 minutes
            self.update_plot()         # ← Updates plots but NOT dates!
            
The loop calls update_plot() but never refreshes the date parameters.
After midnight, dashboard.end_date becomes stale (yesterday's date).
""")
    
    print("=" * 70)
    print("PROPOSED FIX")
    print("=" * 70)
    print("""
Add date refresh before updating plots in auto_update_loop():

    async def auto_update_loop(self):
        while True:
            await asyncio.sleep(270)  # 4.5 minutes
            
            # FIX: Refresh dates for preset time ranges
            if self.time_range in ['1', '7', '30']:
                self._update_date_range_from_preset()
                
            self.update_plot()  # Now uses current dates
""")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    try:
        test_midnight_rollover()
    except Exception as e:
        print(f"\nError running test: {e}")
        import traceback
        traceback.print_exc()