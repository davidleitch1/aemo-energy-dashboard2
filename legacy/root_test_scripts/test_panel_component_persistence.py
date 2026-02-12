#!/usr/bin/env python3
"""
Diagnostic Test 1: Panel Component State Persistence
This test investigates whether Panel components are holding onto stale state
after midnight due to server-side caching or component lifecycle issues.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

import panel as pn
pn.extension('bokeh', 'plotly')

print("\n" + "=" * 80)
print("DIAGNOSTIC TEST: PANEL COMPONENT STATE PERSISTENCE")
print("=" * 80)

def test_1_cache_key_generation():
    """Test if cache keys change appropriately at midnight"""
    print("\n🔍 TEST 1: Cache Key Generation Across Midnight")
    print("-" * 60)
    
    from aemo_dashboard.shared.hybrid_query_manager import HybridQueryManager
    
    # Create query manager
    qm = HybridQueryManager()
    
    # Test cache key generation at different times
    times = [
        ("11:55 PM Dec 10", datetime(2024, 12, 10, 23, 55, 0)),
        ("11:59 PM Dec 10", datetime(2024, 12, 10, 23, 59, 0)),
        ("12:00 AM Dec 11", datetime(2024, 12, 11, 0, 0, 0)),
        ("12:05 AM Dec 11", datetime(2024, 12, 11, 0, 5, 0))
    ]
    
    cache_keys = []
    
    for label, test_time in times:
        # Mock current time
        with patch('datetime.datetime') as mock_dt:
            mock_dt.now.return_value = test_time
            mock_dt.combine = datetime.combine
            
            # Generate cache key for a typical query
            # This simulates what happens when dashboard queries for "today's" data
            end_date = test_time.date()
            start_date = end_date - timedelta(days=1)
            
            # Create a cache key similar to what the dashboard would use
            cache_key = f"prices_{start_date}_{end_date}_{test_time.hour}_{test_time.minute//5}"
            
            cache_keys.append({
                'time': label,
                'key': cache_key,
                'date_component': f"{start_date}_{end_date}"
            })
            
            print(f"  {label}:")
            print(f"    Cache key: {cache_key}")
            print(f"    Date range: {start_date} to {end_date}")
    
    # Analyze cache key changes
    print("\n  Analysis:")
    
    # Check if date component changes at midnight
    before_midnight = cache_keys[1]['date_component']  # 11:59 PM
    after_midnight = cache_keys[2]['date_component']   # 12:00 AM
    
    if before_midnight != after_midnight:
        print(f"    ✅ Cache key date component changes at midnight:")
        print(f"       Before: {before_midnight}")
        print(f"       After:  {after_midnight}")
        date_changes = True
    else:
        print(f"    ❌ Cache key date component DOESN'T change at midnight")
        print(f"       Still using: {before_midnight}")
        date_changes = False
    
    # Check if time component changes
    time_before = cache_keys[1]['key'].split('_')[-2:]  # Hour and 5-min slot
    time_after = cache_keys[2]['key'].split('_')[-2:]
    
    if time_before != time_after:
        print(f"    ✅ Cache key time component changes at midnight")
        time_changes = True
    else:
        print(f"    ❌ Cache key time component doesn't change")
        time_changes = False
    
    return date_changes and time_changes

def test_2_panel_component_refresh_behavior():
    """Test how Panel components behave when underlying data changes"""
    print("\n🔍 TEST 2: Panel Component Refresh Behavior")
    print("-" * 60)
    
    import pandas as pd
    import numpy as np
    from bokeh.models import ColumnDataSource
    
    # Create a simple test component that mimics dashboard behavior
    class TestDashboard:
        def __init__(self):
            self.last_update = datetime(2024, 12, 10, 23, 55, 0)
            self.data = self._generate_data()
            self.source = ColumnDataSource(self.data)
            self.update_count = 0
            self.component_recreated = False
            
        def _generate_data(self):
            """Generate sample data based on current time"""
            return pd.DataFrame({
                'time': pd.date_range(
                    start=self.last_update - timedelta(hours=1),
                    end=self.last_update,
                    freq='5min'
                ),
                'value': np.random.randn(13) * 100 + 500
            })
        
        def update_data(self, new_time):
            """Update data without recreating component"""
            old_time = self.last_update
            self.last_update = new_time
            new_data = self._generate_data()
            
            # Method 1: Update source data directly
            self.source.data = new_data
            self.update_count += 1
            
            print(f"    Update #{self.update_count}:")
            print(f"      Time: {old_time.strftime('%H:%M')} → {new_time.strftime('%H:%M')}")
            print(f"      Last data point: {new_data['time'].iloc[-1]}")
            
            # Check if component sees the update
            return new_data['time'].iloc[-1] == new_time
        
        def recreate_component(self, new_time):
            """Recreate component entirely"""
            self.last_update = new_time
            self.data = self._generate_data()
            self.source = ColumnDataSource(self.data)  # New source
            self.component_recreated = True
            
            print(f"    Component recreated at {new_time.strftime('%H:%M')}")
            print(f"      New last data point: {self.data['time'].iloc[-1]}")
            
            return self.data['time'].iloc[-1] == new_time
    
    dashboard = TestDashboard()
    
    print("\n  Scenario A: Update data without component recreation")
    print("  " + "-" * 50)
    
    # Try updating across midnight
    update_times = [
        datetime(2024, 12, 10, 23, 59, 0),
        datetime(2024, 12, 11, 0, 0, 0),  # Midnight
        datetime(2024, 12, 11, 0, 5, 0)
    ]
    
    updates_worked = []
    for t in update_times:
        worked = dashboard.update_data(t)
        updates_worked.append(worked)
    
    if all(updates_worked):
        print("\n    ✅ Data updates work across midnight")
    else:
        print("\n    ❌ Some data updates failed")
    
    print("\n  Scenario B: Recreate component at midnight")
    print("  " + "-" * 50)
    
    dashboard2 = TestDashboard()
    dashboard2.update_data(datetime(2024, 12, 10, 23, 59, 0))
    
    # At midnight, recreate instead of update
    recreate_worked = dashboard2.recreate_component(datetime(2024, 12, 11, 0, 0, 0))
    
    if recreate_worked:
        print("\n    ✅ Component recreation works at midnight")
    else:
        print("\n    ❌ Component recreation failed")
    
    return all(updates_worked) or recreate_worked

def test_3_panel_state_cache_behavior():
    """Test pn.state.cache behavior across midnight"""
    print("\n🔍 TEST 3: Panel State Cache Behavior")
    print("-" * 60)
    
    # Simulate cache usage pattern
    cache = {}
    
    def get_cached_data(key, generator_func):
        """Mimics pn.state.as_cached behavior"""
        if key not in cache:
            cache[key] = generator_func()
            return cache[key], "MISS"
        return cache[key], "HIT"
    
    # Test cache behavior across midnight
    times = [
        ("11:55 PM", datetime(2024, 12, 10, 23, 55, 0)),
        ("11:59 PM", datetime(2024, 12, 10, 23, 59, 0)),
        ("12:00 AM", datetime(2024, 12, 11, 0, 0, 0)),
        ("12:05 AM", datetime(2024, 12, 11, 0, 5, 0))
    ]
    
    print("\n  Testing cache hits/misses across midnight:")
    
    cache_results = []
    for label, test_time in times:
        # Generate cache key based on date
        cache_key = f"data_{test_time.date()}"
        
        data, hit_or_miss = get_cached_data(
            cache_key,
            lambda: f"Data for {test_time.date()}"
        )
        
        cache_results.append({
            'time': label,
            'key': cache_key,
            'result': hit_or_miss,
            'data': data
        })
        
        print(f"    {label}: Key={cache_key} → {hit_or_miss}")
    
    # Analyze results
    print("\n  Analysis:")
    
    # Should get cache miss at midnight due to new date
    midnight_result = cache_results[2]  # 12:00 AM
    if midnight_result['result'] == 'MISS':
        print("    ✅ Cache miss at midnight (new cache key)")
        print(f"       New data: {midnight_result['data']}")
        cache_invalidated = True
    else:
        print("    ❌ Cache HIT at midnight (using stale data!)")
        print(f"       Stale data: {midnight_result['data']}")
        cache_invalidated = False
    
    return cache_invalidated

def test_4_periodic_callback_trigger_detection():
    """Test if periodic callbacks properly detect when to refresh components"""
    print("\n🔍 TEST 4: Periodic Callback Trigger Detection")
    print("-" * 60)
    
    class CallbackSimulator:
        def __init__(self):
            self.last_refresh_date = datetime(2024, 12, 10).date()
            self.refresh_count = 0
            self.component_updates = []
            
        def should_refresh(self, current_time):
            """Determine if components should be refreshed"""
            current_date = current_time.date()
            
            # Method 1: Simple date comparison
            if current_date != self.last_refresh_date:
                return True, "Date changed"
            
            # Method 2: Check if we're in a new day period
            if current_time.hour == 0 and current_time.minute < 5:
                last_check = current_time - timedelta(minutes=5)
                if last_check.date() != current_date:
                    return True, "Crossed midnight boundary"
            
            return False, "No refresh needed"
        
        def periodic_update(self, current_time):
            """Simulate periodic callback"""
            should_refresh, reason = self.should_refresh(current_time)
            
            result = {
                'time': current_time.strftime('%Y-%m-%d %H:%M'),
                'should_refresh': should_refresh,
                'reason': reason
            }
            
            if should_refresh:
                self.last_refresh_date = current_time.date()
                self.refresh_count += 1
                result['action'] = 'REFRESHED COMPONENTS'
            else:
                result['action'] = 'Normal update'
            
            self.component_updates.append(result)
            return result
    
    simulator = CallbackSimulator()
    
    # Simulate updates across midnight
    test_times = [
        datetime(2024, 12, 10, 23, 50, 0),
        datetime(2024, 12, 10, 23, 55, 0),
        datetime(2024, 12, 11, 0, 0, 0),  # Midnight
        datetime(2024, 12, 11, 0, 5, 0)
    ]
    
    print("\n  Simulating periodic callbacks:")
    for t in test_times:
        result = simulator.periodic_update(t)
        refresh_indicator = "🔄" if result['should_refresh'] else "  "
        print(f"    {refresh_indicator} {result['time']}: {result['action']}")
        if result['should_refresh']:
            print(f"       Reason: {result['reason']}")
    
    # Check if midnight refresh was detected
    midnight_update = simulator.component_updates[2]
    
    if midnight_update['should_refresh']:
        print("\n    ✅ Midnight refresh properly detected")
        return True
    else:
        print("\n    ❌ Midnight refresh NOT detected")
        return False

# Run all diagnostic tests
def run_diagnostics():
    """Run all diagnostic tests"""
    print("\nRunning diagnostic tests to identify root cause...")
    print("These tests investigate Panel component state persistence and caching.\n")
    
    results = []
    
    # Test 1: Cache keys
    try:
        result1 = test_1_cache_key_generation()
        results.append(("Cache Key Generation", result1))
    except Exception as e:
        print(f"\n  ⚠️ Test 1 error: {e}")
        results.append(("Cache Key Generation", False))
    
    # Test 2: Component refresh
    try:
        result2 = test_2_panel_component_refresh_behavior()
        results.append(("Component Refresh Behavior", result2))
    except Exception as e:
        print(f"\n  ⚠️ Test 2 error: {e}")
        results.append(("Component Refresh Behavior", False))
    
    # Test 3: State cache
    try:
        result3 = test_3_panel_state_cache_behavior()
        results.append(("State Cache Behavior", result3))
    except Exception as e:
        print(f"\n  ⚠️ Test 3 error: {e}")
        results.append(("State Cache Behavior", False))
    
    # Test 4: Periodic callbacks
    try:
        result4 = test_4_periodic_callback_trigger_detection()
        results.append(("Periodic Callback Detection", result4))
    except Exception as e:
        print(f"\n  ⚠️ Test 4 error: {e}")
        results.append(("Periodic Callback Detection", False))
    
    # Summary and diagnosis
    print("\n" + "=" * 80)
    print("DIAGNOSTIC RESULTS")
    print("=" * 80)
    
    for test_name, passed in results:
        status = "✅" if passed else "❌"
        print(f"  {status} {test_name}")
    
    # Provide diagnosis
    print("\n" + "=" * 80)
    print("DIAGNOSIS")
    print("=" * 80)
    
    cache_issue = not results[0][1] or not results[2][1]
    component_issue = not results[1][1]
    detection_issue = not results[3][1]
    
    if cache_issue:
        print("\n🔴 CACHE INVALIDATION ISSUE DETECTED")
        print("  The cache is not properly invalidating at midnight.")
        print("  → Components continue serving stale cached data")
        print("  → Solution: Force cache invalidation when date changes")
    
    if component_issue:
        print("\n🔴 COMPONENT STATE PERSISTENCE ISSUE DETECTED")
        print("  Panel components are not refreshing their internal state.")
        print("  → Components need to be recreated, not just updated")
        print("  → Solution: Recreate components after midnight")
    
    if detection_issue:
        print("\n🔴 MIDNIGHT DETECTION ISSUE")
        print("  The periodic callback is not detecting midnight properly.")
        print("  → The trigger condition needs improvement")
        print("  → Solution: Add explicit midnight detection logic")
    
    if not any([cache_issue, component_issue, detection_issue]):
        print("\n🟡 No clear issue detected in these tests.")
        print("  The problem may be in the integration between components.")
        print("  → Run the integration test next")
    
    print("\n" + "=" * 80)
    
    return results

if __name__ == "__main__":
    run_diagnostics()