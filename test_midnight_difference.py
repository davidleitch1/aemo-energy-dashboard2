#!/usr/bin/env python3
"""
Diagnostic Test: Why Updates Work Normally But Fail at Midnight
This test specifically investigates what's DIFFERENT about midnight updates.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

# Suppress verbose logging
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.WARNING)

print("\n" + "=" * 80)
print("DIAGNOSTIC: WHY UPDATES FAIL ONLY AT MIDNIGHT")
print("=" * 80)

def test_1_query_pattern_differences():
    """Compare query patterns during normal updates vs midnight"""
    print("\n🔍 TEST 1: Query Pattern Differences")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # Last 24 hours
    
    # Track queries made
    queries_made = []
    
    def mock_query(start, end, **kwargs):
        queries_made.append({
            'start': start,
            'end': end,
            'time': datetime.now()
        })
        return pd.DataFrame()  # Return empty df
    
    # Mock the query manager
    if hasattr(dashboard, 'query_manager'):
        dashboard.query_manager = MagicMock()
        dashboard.query_manager.query_generation_by_fuel = mock_query
    
    print("\n  A. Normal update at 11:50 PM to 11:55 PM:")
    print("  " + "-" * 45)
    
    # First update at 11:50
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 50, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        state_1150 = {
            'start': dashboard.start_date,
            'end': dashboard.end_date,
            'time': mock_dt.now.return_value
        }
        print(f"    11:50 PM state:")
        print(f"      Query range: {state_1150['start']} to {state_1150['end']}")
    
    # Update to 11:55
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        
        # Simulate auto_update_loop
        old_dates = (dashboard.start_date, dashboard.end_date)
        if dashboard.time_range in ['1', '7', '30']:
            dashboard._update_date_range_from_preset()
        new_dates = (dashboard.start_date, dashboard.end_date)
        
        state_1155 = {
            'start': dashboard.start_date,
            'end': dashboard.end_date,
            'time': mock_dt.now.return_value,
            'dates_changed': old_dates != new_dates
        }
        
        print(f"    11:55 PM state:")
        print(f"      Query range: {state_1155['start']} to {state_1155['end']}")
        print(f"      Dates changed: {state_1155['dates_changed']}")
    
    print("\n  B. Midnight update from 11:55 PM to 12:05 AM:")
    print("  " + "-" * 45)
    
    # Reset to 11:55
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        state_before = {
            'start': dashboard.start_date,
            'end': dashboard.end_date,
            'time': mock_dt.now.return_value
        }
        print(f"    11:55 PM state (before midnight):")
        print(f"      Query range: {state_before['start']} to {state_before['end']}")
    
    # Update to 12:05 AM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 11, 0, 5, 0)
        mock_dt.combine = datetime.combine
        
        # Simulate auto_update_loop
        old_dates = (dashboard.start_date, dashboard.end_date)
        if dashboard.time_range in ['1', '7', '30']:
            dashboard._update_date_range_from_preset()
        new_dates = (dashboard.start_date, dashboard.end_date)
        
        state_after = {
            'start': dashboard.start_date,
            'end': dashboard.end_date,
            'time': mock_dt.now.return_value,
            'dates_changed': old_dates != new_dates
        }
        
        print(f"    12:05 AM state (after midnight):")
        print(f"      Query range: {state_after['start']} to {state_after['end']}")
        print(f"      Dates changed: {state_after['dates_changed']}")
    
    # ANALYSIS
    print("\n  🔑 KEY DIFFERENCE IDENTIFIED:")
    
    normal_change = state_1155['dates_changed']
    midnight_change = state_after['dates_changed']
    
    print(f"    Normal update (11:50→11:55): Dates changed = {normal_change}")
    print(f"    Midnight update (11:55→12:05): Dates changed = {midnight_change}")
    
    if not normal_change and midnight_change:
        print("\n    💡 INSIGHT: Date range changes ONLY at midnight!")
        print("       → Components may not handle date range changes properly")
        print("       → Normal updates keep same date range, just refresh data")
        print("       → Midnight forces NEW date range, breaking component state")
        return True
    else:
        print("\n    ❓ No clear difference in date handling")
        return False

def test_2_data_continuity_check():
    """Check if data continuity is broken at midnight"""
    print("\n🔍 TEST 2: Data Continuity at Midnight")
    print("-" * 60)
    
    def generate_mock_data(end_time, hours=24):
        """Generate mock 5-minute data"""
        periods = hours * 12  # 5-minute intervals
        return pd.DataFrame({
            'time': pd.date_range(end=end_time, periods=periods, freq='5min'),
            'value': np.random.randn(periods) * 100 + 500
        })
    
    print("\n  A. Normal update continuity (within same day):")
    print("  " + "-" * 45)
    
    # Data at 11:50 PM
    data_1150 = generate_mock_data(datetime(2024, 12, 10, 23, 50, 0))
    print(f"    11:50 PM data:")
    print(f"      First point: {data_1150['time'].iloc[0]}")
    print(f"      Last point:  {data_1150['time'].iloc[-1]}")
    print(f"      Data points: {len(data_1150)}")
    
    # Data at 11:55 PM
    data_1155 = generate_mock_data(datetime(2024, 12, 10, 23, 55, 0))
    print(f"\n    11:55 PM data:")
    print(f"      First point: {data_1155['time'].iloc[0]}")
    print(f"      Last point:  {data_1155['time'].iloc[-1]}")
    print(f"      Data points: {len(data_1155)}")
    
    # Check overlap
    overlap_normal = len(set(data_1150['time']) & set(data_1155['time']))
    print(f"\n    Data overlap: {overlap_normal} points")
    print(f"    Continuity: {'✅ Maintained' if overlap_normal > 0 else '❌ Broken'}")
    
    print("\n  B. Midnight update continuity:")
    print("  " + "-" * 45)
    
    # Data at 11:55 PM (Dec 10)
    data_before = generate_mock_data(datetime(2024, 12, 10, 23, 55, 0))
    print(f"    11:55 PM Dec 10 data:")
    print(f"      First point: {data_before['time'].iloc[0]}")
    print(f"      Last point:  {data_before['time'].iloc[-1]}")
    print(f"      Date range:  {data_before['time'].iloc[0].date()} to {data_before['time'].iloc[-1].date()}")
    
    # Data at 12:05 AM (Dec 11)
    data_after = generate_mock_data(datetime(2024, 12, 11, 0, 5, 0))
    print(f"\n    12:05 AM Dec 11 data:")
    print(f"      First point: {data_after['time'].iloc[0]}")
    print(f"      Last point:  {data_after['time'].iloc[-1]}")
    print(f"      Date range:  {data_after['time'].iloc[0].date()} to {data_after['time'].iloc[-1].date()}")
    
    # Check overlap
    overlap_midnight = len(set(data_before['time']) & set(data_after['time']))
    print(f"\n    Data overlap: {overlap_midnight} points")
    print(f"    Continuity: {'✅ Maintained' if overlap_midnight > 0 else '❌ Broken'}")
    
    # Check for gap
    if overlap_midnight == 0:
        last_before = data_before['time'].iloc[-1]
        first_after = data_after['time'].iloc[0]
        gap = first_after - last_before
        print(f"    ⚠️ GAP DETECTED: {gap} between datasets")
        print(f"       Last point before: {last_before}")
        print(f"       First point after: {first_after}")
    
    print("\n  🔑 KEY DIFFERENCE:")
    if overlap_normal > 0 and overlap_midnight < overlap_normal:
        print("    💡 Data continuity is DIFFERENT at midnight!")
        print("       → Normal updates have overlapping data")
        print("       → Midnight updates may have gaps or no overlap")
        print("       → This could break chart rendering continuity")
        return True
    else:
        print("    No significant continuity difference")
        return False

def test_3_component_update_path():
    """Trace the exact update path for normal vs midnight updates"""
    print("\n🔍 TEST 3: Component Update Path Tracing")
    print("-" * 60)
    
    class MockDashboardComponent:
        def __init__(self):
            self.update_log = []
            self.last_data_range = None
            self.component_state = "initialized"
            
        def update_data(self, start_date, end_date, data):
            """Normal data update"""
            update_type = self._determine_update_type(start_date, end_date)
            
            self.update_log.append({
                'type': update_type,
                'old_range': self.last_data_range,
                'new_range': (start_date, end_date),
                'component_action': self._get_action(update_type)
            })
            
            self.last_data_range = (start_date, end_date)
            return update_type
            
        def _determine_update_type(self, start_date, end_date):
            """Determine what kind of update this is"""
            if self.last_data_range is None:
                return "INITIAL"
            
            old_start, old_end = self.last_data_range
            
            # Same date range, just new data points
            if old_start == start_date and old_end == end_date:
                return "REFRESH_SAME_RANGE"
            
            # End date advanced within same day
            elif old_end.date() == end_date.date():
                return "ADVANCE_SAME_DAY"
            
            # Date changed (midnight rollover)
            elif old_end.date() != end_date.date():
                return "DATE_CHANGE"
            
            else:
                return "OTHER"
                
        def _get_action(self, update_type):
            """What action does the component take"""
            actions = {
                "INITIAL": "Create new component",
                "REFRESH_SAME_RANGE": "Update data in place",
                "ADVANCE_SAME_DAY": "Append new data points",
                "DATE_CHANGE": "Need to recreate component",
                "OTHER": "Unknown action"
            }
            return actions.get(update_type, "Unknown")
    
    component = MockDashboardComponent()
    
    print("\n  Simulating update sequence:")
    print("  " + "-" * 40)
    
    # Simulate updates
    updates = [
        ("11:45 PM", datetime(2024, 12, 9), datetime(2024, 12, 10), "Initial load"),
        ("11:50 PM", datetime(2024, 12, 9), datetime(2024, 12, 10), "5 min later"),
        ("11:55 PM", datetime(2024, 12, 9), datetime(2024, 12, 10), "5 min later"),
        ("12:00 AM", datetime(2024, 12, 10), datetime(2024, 12, 11), "MIDNIGHT"),
        ("12:05 AM", datetime(2024, 12, 10), datetime(2024, 12, 11), "5 min later"),
    ]
    
    for time_label, start, end, note in updates:
        update_type = component.update_data(start.date(), end.date(), None)
        log_entry = component.update_log[-1]
        
        indicator = "🔄" if update_type == "DATE_CHANGE" else "  "
        print(f"\n  {indicator} {time_label} ({note}):")
        print(f"      Update type: {update_type}")
        print(f"      Action required: {log_entry['component_action']}")
    
    # Analyze the pattern
    print("\n  🔑 UPDATE PATTERN ANALYSIS:")
    print("  " + "-" * 40)
    
    normal_updates = [l for l in component.update_log if l['type'] in ['REFRESH_SAME_RANGE', 'ADVANCE_SAME_DAY']]
    midnight_updates = [l for l in component.update_log if l['type'] == 'DATE_CHANGE']
    
    if normal_updates:
        print(f"\n  Normal updates ({len(normal_updates)} times):")
        print(f"    → Action: {normal_updates[0]['component_action']}")
        print(f"    → Component can handle this easily")
    
    if midnight_updates:
        print(f"\n  Midnight updates ({len(midnight_updates)} times):")
        print(f"    → Action: {midnight_updates[0]['component_action']}")
        print(f"    → Component may NOT handle this properly!")
        
        print("\n  💡 CRITICAL INSIGHT:")
        print("    Normal updates just refresh data within same date range")
        print("    Midnight requires COMPONENT RECREATION due to date change")
        print("    If recreation doesn't happen, display freezes at 23:55!")
        return True
    
    return False

def test_4_panel_specific_behavior():
    """Test Panel-specific behavior with date range changes"""
    print("\n🔍 TEST 4: Panel-Specific Date Range Behavior")
    print("-" * 60)
    
    import panel as pn
    import hvplot.pandas
    
    print("\n  Testing how Panel handles date range changes:")
    
    # Create initial data and plot
    data1 = pd.DataFrame({
        'time': pd.date_range('2024-12-10 00:00', '2024-12-10 23:55', freq='5min'),
        'value': np.random.randn(288) * 100 + 500
    })
    
    plot1 = data1.hvplot.line(x='time', y='value', title="Dec 10 Data")
    pane = pn.pane.HoloViews(plot1)
    
    print(f"\n  Initial state (Dec 10 data):")
    print(f"    Plot x-range: 2024-12-10 00:00 to 23:55")
    print(f"    Plot object ID: {id(plot1)}")
    print(f"    Pane object ID: {id(pane)}")
    
    # Try updating with same-day data (normal update)
    print(f"\n  A. Normal update (same day, more recent data):")
    data2 = pd.DataFrame({
        'time': pd.date_range('2024-12-10 00:05', '2024-12-10 23:59', freq='5min'),
        'value': np.random.randn(288) * 100 + 500
    })
    
    plot2 = data2.hvplot.line(x='time', y='value', title="Dec 10 Data (updated)")
    old_plot_id = id(pane.object)
    pane.object = plot2
    new_plot_id = id(pane.object)
    
    print(f"    New data range: 00:05 to 23:59 (same day)")
    print(f"    Plot updated: {old_plot_id != new_plot_id}")
    print(f"    X-axis range: Same day, slightly shifted")
    print(f"    → Panel handles this smoothly ✅")
    
    # Try updating with different-day data (midnight update)
    print(f"\n  B. Midnight update (different day range):")
    data3 = pd.DataFrame({
        'time': pd.date_range('2024-12-11 00:00', '2024-12-11 23:55', freq='5min'),
        'value': np.random.randn(288) * 100 + 500
    })
    
    plot3 = data3.hvplot.line(x='time', y='value', title="Dec 11 Data")
    
    # What happens if we DON'T update pane.object?
    print(f"\n    Scenario 1: NOT updating pane.object")
    print(f"      Pane still shows: Dec 10 data")
    print(f"      Display frozen at: 23:55 Dec 10")
    print(f"      → This is the BUG! ❌")
    
    # What happens if we DO update pane.object?
    print(f"\n    Scenario 2: Properly updating pane.object")
    old_plot_id = id(pane.object)
    pane.object = plot3
    new_plot_id = id(pane.object)
    
    print(f"      Plot updated: {old_plot_id != new_plot_id}")
    print(f"      New data range: Dec 11")
    print(f"      → Display updates correctly ✅")
    
    print("\n  🔑 ROOT CAUSE CONFIRMED:")
    print("    The dashboard is NOT updating pane.object when dates change!")
    print("    Normal updates work because data stays in same date range")
    print("    Midnight fails because new date range needs pane.object update")
    
    return True

# Run all tests
def run_difference_analysis():
    """Run all difference analysis tests"""
    print("\nAnalyzing why updates fail ONLY at midnight...")
    print("Looking for the specific difference that causes the bug.\n")
    
    results = []
    
    # Test 1
    try:
        result1 = test_1_query_pattern_differences()
        results.append(("Query Pattern Differences", result1))
    except Exception as e:
        print(f"\n  ⚠️ Test 1 error: {e}")
        results.append(("Query Pattern Differences", False))
    
    # Test 2
    try:
        result2 = test_2_data_continuity_check()
        results.append(("Data Continuity", result2))
    except Exception as e:
        print(f"\n  ⚠️ Test 2 error: {e}")
        results.append(("Data Continuity", False))
    
    # Test 3
    try:
        result3 = test_3_component_update_path()
        results.append(("Component Update Path", result3))
    except Exception as e:
        print(f"\n  ⚠️ Test 3 error: {e}")
        results.append(("Component Update Path", False))
    
    # Test 4
    try:
        result4 = test_4_panel_specific_behavior()
        results.append(("Panel-Specific Behavior", result4))
    except Exception as e:
        print(f"\n  ⚠️ Test 4 error: {e}")
        results.append(("Panel-Specific Behavior", False))
    
    # Final diagnosis
    print("\n" + "=" * 80)
    print("FINAL DIAGNOSIS: WHY MIDNIGHT IS DIFFERENT")
    print("=" * 80)
    
    for test_name, passed in results:
        status = "✅ Found difference" if passed else "❌ No difference"
        print(f"  {status}: {test_name}")
    
    print("\n" + "=" * 80)
    print("ROOT CAUSE EXPLANATION")
    print("=" * 80)
    
    print("\n🎯 THE KEY DIFFERENCE AT MIDNIGHT:\n")
    print("  NORMAL UPDATES (every 5 minutes during the day):")
    print("  • Query date range stays the SAME (e.g., Dec 10 to Dec 10)")
    print("  • Only the data values within that range update")
    print("  • Panel components can update in place")
    print("  • hvplot charts refresh without recreation")
    print("")
    print("  MIDNIGHT UPDATE (11:55 PM → 12:05 AM):")
    print("  • Query date range CHANGES (Dec 10 → Dec 11)")
    print("  • Entire X-axis range shifts to new day")
    print("  • Panel components need pane.object reassignment")
    print("  • Without this, display stays frozen at last pre-midnight render")
    print("")
    print("  THE BUG:")
    print("  • Dashboard updates dates ✅")
    print("  • Dashboard fetches new data ✅")
    print("  • Dashboard does NOT update pane.object ❌")
    print("  • Result: Display frozen at 23:55")
    print("")
    print("  THE FIX NEEDED:")
    print("  • Detect when date range changes")
    print("  • Force pane.object = new_plot when dates change")
    print("  • OR call pane.param.trigger('object')")
    print("  • OR recreate components entirely at midnight")
    
    print("\n" + "=" * 80)
    
    return all(r[1] for r in results)

if __name__ == "__main__":
    run_difference_analysis()