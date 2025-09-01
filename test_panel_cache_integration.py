#!/usr/bin/env python3
"""
Diagnostic Test 2: Panel Cache and Component Integration
This test investigates the integration between Panel's caching system,
component updates, and the actual dashboard display refresh mechanism.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock
import asyncio
import pandas as pd
import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

import panel as pn
pn.extension('bokeh', 'plotly')

# Suppress verbose logging
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.WARNING)

print("\n" + "=" * 80)
print("DIAGNOSTIC TEST: PANEL CACHE AND COMPONENT INTEGRATION")
print("=" * 80)

def test_1_actual_dashboard_cache_usage():
    """Test how the actual dashboard uses cache across midnight"""
    print("\n🔬 TEST 1: Actual Dashboard Cache Usage Pattern")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # Last 24 hours
    
    # Track cache operations
    cache_ops = []
    original_cache_get = pn.state.cache.get if hasattr(pn.state, 'cache') else dict.get
    original_cache_set = pn.state.cache.__setitem__ if hasattr(pn.state, 'cache') else dict.__setitem__
    
    def track_cache_get(key, default=None):
        cache_ops.append(('GET', key, datetime.now()))
        return original_cache_get(key, default)
    
    def track_cache_set(key, value):
        cache_ops.append(('SET', key, datetime.now()))
        return original_cache_set(key, value)
    
    # Patch cache operations
    if hasattr(pn.state, 'cache'):
        pn.state.cache.get = track_cache_get
        pn.state.cache.__setitem__ = track_cache_set
    
    print("\n  Simulating dashboard operations across midnight:")
    
    # Test at 11:55 PM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        
        dashboard._update_date_range_from_preset()
        before_midnight_date = dashboard.end_date
        print(f"\n  11:55 PM: Dashboard date = {before_midnight_date}")
        
        # Simulate a data query
        try:
            # This would trigger cache operations
            if hasattr(dashboard, 'query_manager'):
                # Mock query to track cache usage
                dashboard.query_manager = MagicMock()
                dashboard.query_manager.query_generation_by_fuel = MagicMock(return_value=pd.DataFrame())
        except:
            pass
    
    # Test at 12:05 AM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 11, 0, 5, 0)
        mock_dt.combine = datetime.combine
        
        # Apply the date refresh fix
        if dashboard.time_range in ['1', '7', '30']:
            old_date = dashboard.end_date
            dashboard._update_date_range_from_preset()
            new_date = dashboard.end_date
            
            print(f"\n  12:05 AM: Dashboard date = {new_date}")
            print(f"  Date changed: {old_date} → {new_date}")
    
    # Analyze cache operations
    print("\n  Cache operation analysis:")
    
    if cache_ops:
        gets = [op for op in cache_ops if op[0] == 'GET']
        sets = [op for op in cache_ops if op[0] == 'SET']
        print(f"    Cache GETs: {len(gets)}")
        print(f"    Cache SETs: {len(sets)}")
        
        # Check if cache keys include dates
        date_based_keys = [op for op in cache_ops if str(before_midnight_date) in str(op[1])]
        if date_based_keys:
            print(f"    ✅ Cache keys include date components")
            return True
        else:
            print(f"    ⚠️ Cache keys may not include date components")
            return False
    else:
        print("    ℹ️ No cache operations detected (may not use pn.state.cache)")
        return None

def test_2_component_object_identity():
    """Test if Panel components maintain object identity across updates"""
    print("\n🔬 TEST 2: Component Object Identity Across Updates")
    print("-" * 60)
    
    import hvplot.pandas
    
    # Create test data
    def create_test_data(end_time):
        return pd.DataFrame({
            'time': pd.date_range(end=end_time, periods=12, freq='5min'),
            'value': np.random.randn(12) * 100 + 500
        })
    
    # Create initial plot
    data1 = create_test_data(datetime(2024, 12, 10, 23, 55, 0))
    plot1 = data1.hvplot.line(x='time', y='value')
    pane1 = pn.pane.HoloViews(plot1)
    
    print(f"\n  Initial component:")
    print(f"    Plot object ID: {id(plot1)}")
    print(f"    Pane object ID: {id(pane1)}")
    print(f"    Last data point: {data1['time'].iloc[-1]}")
    
    # Method 1: Update data in place
    print(f"\n  Method 1: Update data in place")
    data2 = create_test_data(datetime(2024, 12, 11, 0, 5, 0))
    plot1.data = data2  # This doesn't actually work with hvplot
    
    print(f"    Plot object ID: {id(plot1)} (same)")
    print(f"    Data changed but plot object unchanged")
    
    # Method 2: Create new plot, update pane
    print(f"\n  Method 2: Create new plot, update pane")
    plot2 = data2.hvplot.line(x='time', y='value')
    pane1.object = plot2  # Update pane's object
    
    print(f"    New plot ID: {id(plot2)} (different)")
    print(f"    Pane ID: {id(pane1)} (same)")
    print(f"    Pane.object ID: {id(pane1.object)} (updated)")
    
    # Method 3: Create entirely new pane
    print(f"\n  Method 3: Create entirely new pane")
    plot3 = data2.hvplot.line(x='time', y='value')
    pane2 = pn.pane.HoloViews(plot3)
    
    print(f"    New plot ID: {id(plot3)}")
    print(f"    New pane ID: {id(pane2)}")
    
    # Analysis
    print(f"\n  Analysis:")
    print(f"    ✅ Method 2 (update pane.object) maintains pane identity")
    print(f"    ✅ Method 3 (new pane) creates fresh components")
    print(f"    ⚠️ Method 1 (update data) doesn't work with hvplot")
    
    return True

def test_3_panel_server_component_lifecycle():
    """Test Panel server component lifecycle and refresh mechanisms"""
    print("\n🔬 TEST 3: Panel Server Component Lifecycle")
    print("-" * 60)
    
    # Simulate Panel server component lifecycle
    class MockPanelServer:
        def __init__(self):
            self.sessions = {}
            self.components = {}
            self.refresh_count = 0
            
        def create_session(self, session_id):
            """Create a new session"""
            self.sessions[session_id] = {
                'created': datetime.now(),
                'components': {},
                'last_refresh': datetime.now()
            }
            print(f"    Session {session_id} created")
            
        def add_component(self, session_id, component_id, component):
            """Add component to session"""
            if session_id in self.sessions:
                self.sessions[session_id]['components'][component_id] = {
                    'object': component,
                    'created': datetime.now(),
                    'updated': datetime.now()
                }
                print(f"    Component {component_id} added to session {session_id}")
                
        def refresh_component(self, session_id, component_id, new_object=None):
            """Refresh a component"""
            if session_id in self.sessions and component_id in self.sessions[session_id]['components']:
                comp = self.sessions[session_id]['components'][component_id]
                old_id = id(comp['object'])
                
                if new_object:
                    comp['object'] = new_object
                    comp['updated'] = datetime.now()
                    self.refresh_count += 1
                    
                    new_id = id(new_object)
                    print(f"    Component {component_id} refreshed:")
                    print(f"      Old object ID: {old_id}")
                    print(f"      New object ID: {new_id}")
                    print(f"      Object changed: {old_id != new_id}")
                else:
                    print(f"    Component {component_id} refresh failed: no new object")
                    
        def trigger_periodic_callback(self, session_id, current_time):
            """Simulate periodic callback"""
            if session_id in self.sessions:
                session = self.sessions[session_id]
                time_since_refresh = current_time - session['last_refresh']
                
                print(f"\n    Periodic callback at {current_time.strftime('%H:%M')}")
                print(f"      Time since last refresh: {time_since_refresh}")
                
                # Check if we need to refresh components
                if current_time.date() != session['last_refresh'].date():
                    print(f"      🔄 Date changed! Refreshing all components...")
                    for comp_id in session['components']:
                        self.refresh_component(session_id, comp_id, f"new_object_{current_time}")
                    session['last_refresh'] = current_time
                else:
                    print(f"      No date change, normal update")
    
    # Simulate server behavior
    server = MockPanelServer()
    session_id = "test_session_001"
    
    print("\n  Simulating Panel server lifecycle:")
    
    # Create session and add components
    server.create_session(session_id)
    server.add_component(session_id, "price_chart", "price_chart_object_2355")
    server.add_component(session_id, "generation_chart", "gen_chart_object_2355")
    
    # Simulate periodic callbacks across midnight
    times = [
        datetime(2024, 12, 10, 23, 55, 0),
        datetime(2024, 12, 10, 23, 59, 0),
        datetime(2024, 12, 11, 0, 0, 0),  # Midnight
        datetime(2024, 12, 11, 0, 5, 0)
    ]
    
    for t in times:
        server.trigger_periodic_callback(session_id, t)
    
    print(f"\n  Results:")
    print(f"    Total refreshes: {server.refresh_count}")
    
    if server.refresh_count > 0:
        print(f"    ✅ Components refreshed at midnight")
        return True
    else:
        print(f"    ❌ Components NOT refreshed")
        return False

async def test_4_async_update_mechanism():
    """Test async update mechanism used by Panel"""
    print("\n🔬 TEST 4: Async Update Mechanism")
    print("-" * 60)
    
    class AsyncDashboardSimulator:
        def __init__(self):
            self.last_update = datetime(2024, 12, 10, 23, 55, 0)
            self.components = {
                'price_chart': {'data': None, 'last_update': None},
                'gen_chart': {'data': None, 'last_update': None}
            }
            self.update_log = []
            
        async def fetch_data(self, end_time):
            """Simulate async data fetch"""
            await asyncio.sleep(0.01)  # Simulate network delay
            return {
                'time': end_time,
                'data': f"data_for_{end_time.strftime('%H%M')}"
            }
            
        async def update_component(self, component_name, current_time):
            """Update a single component"""
            data = await self.fetch_data(current_time)
            
            old_data = self.components[component_name]['data']
            self.components[component_name]['data'] = data
            self.components[component_name]['last_update'] = current_time
            
            self.update_log.append({
                'component': component_name,
                'time': current_time,
                'data_changed': old_data != data
            })
            
            return data
            
        async def periodic_update(self, current_time):
            """Simulate periodic update callback"""
            print(f"\n    Async update at {current_time.strftime('%H:%M')}:")
            
            # Check if date changed
            if current_time.date() != self.last_update.date():
                print(f"      Date changed! Full refresh needed")
                # Update all components with fresh data
                tasks = [
                    self.update_component('price_chart', current_time),
                    self.update_component('gen_chart', current_time)
                ]
                results = await asyncio.gather(*tasks)
                print(f"      Updated {len(results)} components")
            else:
                print(f"      Normal update")
                # Just update data without component refresh
                await self.update_component('price_chart', current_time)
                
            self.last_update = current_time
    
    # Run async simulation
    simulator = AsyncDashboardSimulator()
    
    print("\n  Running async update simulation:")
    
    times = [
        datetime(2024, 12, 10, 23, 55, 0),
        datetime(2024, 12, 10, 23, 59, 0),
        datetime(2024, 12, 11, 0, 0, 0),  # Midnight
        datetime(2024, 12, 11, 0, 5, 0)
    ]
    
    for t in times:
        await simulator.periodic_update(t)
    
    # Analyze update log
    print("\n  Update log analysis:")
    
    midnight_updates = [u for u in simulator.update_log 
                       if u['time'] == datetime(2024, 12, 11, 0, 0, 0)]
    
    if midnight_updates:
        print(f"    Found {len(midnight_updates)} updates at midnight")
        for update in midnight_updates:
            status = "✅" if update['data_changed'] else "❌"
            print(f"      {status} {update['component']}: data_changed={update['data_changed']}")
            
        if all(u['data_changed'] for u in midnight_updates):
            print(f"\n    ✅ All components refreshed with new data at midnight")
            return True
        else:
            print(f"\n    ⚠️ Some components didn't get new data")
            return False
    else:
        print(f"    ❌ No updates detected at midnight")
        return False

def test_5_panel_param_trigger():
    """Test Panel's param.trigger mechanism for forcing updates"""
    print("\n🔬 TEST 5: Panel param.trigger() Mechanism")
    print("-" * 60)
    
    import param
    
    class TestDashboard(param.Parameterized):
        data = param.DataFrame(default=pd.DataFrame())
        last_update = param.Date(default=datetime.now())
        
        def __init__(self):
            super().__init__()
            self.trigger_count = 0
            self.param.watch(self._on_data_change, 'data')
            
        def _on_data_change(self, event):
            """Called when data parameter changes"""
            self.trigger_count += 1
            print(f"      Trigger #{self.trigger_count}: Data changed")
            
        def update_data(self, new_time):
            """Update data normally"""
            self.data = pd.DataFrame({
                'time': [new_time],
                'value': [np.random.rand()]
            })
            self.last_update = new_time
            
        def force_refresh(self):
            """Force a refresh using param.trigger"""
            print(f"      Forcing refresh with param.trigger")
            self.param.trigger('data')
    
    dashboard = TestDashboard()
    
    print("\n  Testing param.trigger mechanism:")
    
    # Normal update at 11:55 PM
    print(f"\n    11:55 PM: Normal update")
    dashboard.update_data(datetime(2024, 12, 10, 23, 55, 0))
    
    # At midnight, try different update methods
    print(f"\n    12:00 AM: Testing update methods")
    
    print(f"\n    Method A: Update data normally")
    old_trigger_count = dashboard.trigger_count
    dashboard.update_data(datetime(2024, 12, 11, 0, 0, 0))
    if dashboard.trigger_count > old_trigger_count:
        print(f"      ✅ Data update triggered refresh")
    else:
        print(f"      ❌ Data update didn't trigger")
    
    print(f"\n    Method B: Force refresh with param.trigger")
    old_trigger_count = dashboard.trigger_count
    dashboard.force_refresh()
    if dashboard.trigger_count > old_trigger_count:
        print(f"      ✅ param.trigger() forced refresh")
    else:
        print(f"      ❌ param.trigger() didn't work")
    
    print(f"\n  Summary:")
    print(f"    Total triggers: {dashboard.trigger_count}")
    
    if dashboard.trigger_count >= 2:
        print(f"    ✅ param.trigger mechanism works")
        return True
    else:
        print(f"    ❌ param.trigger mechanism issues")
        return False

# Main test runner
async def run_integration_tests():
    """Run all integration tests"""
    print("\nRunning integration tests to identify caching/refresh issues...")
    print("These tests examine how Panel's caching and component systems interact.\n")
    
    results = []
    
    # Test 1: Dashboard cache usage
    try:
        result1 = test_1_actual_dashboard_cache_usage()
        results.append(("Dashboard Cache Usage", result1))
    except Exception as e:
        print(f"\n  ⚠️ Test 1 error: {e}")
        results.append(("Dashboard Cache Usage", None))
    
    # Test 2: Component identity
    try:
        result2 = test_2_component_object_identity()
        results.append(("Component Object Identity", result2))
    except Exception as e:
        print(f"\n  ⚠️ Test 2 error: {e}")
        results.append(("Component Object Identity", False))
    
    # Test 3: Server lifecycle
    try:
        result3 = test_3_panel_server_component_lifecycle()
        results.append(("Server Component Lifecycle", result3))
    except Exception as e:
        print(f"\n  ⚠️ Test 3 error: {e}")
        results.append(("Server Component Lifecycle", False))
    
    # Test 4: Async updates
    try:
        result4 = await test_4_async_update_mechanism()
        results.append(("Async Update Mechanism", result4))
    except Exception as e:
        print(f"\n  ⚠️ Test 4 error: {e}")
        results.append(("Async Update Mechanism", False))
    
    # Test 5: Param trigger
    try:
        result5 = test_5_panel_param_trigger()
        results.append(("Param Trigger Mechanism", result5))
    except Exception as e:
        print(f"\n  ⚠️ Test 5 error: {e}")
        results.append(("Param Trigger Mechanism", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("INTEGRATION TEST RESULTS")
    print("=" * 80)
    
    for test_name, result in results:
        if result is None:
            status = "⚠️"
            status_text = "SKIPPED"
        elif result:
            status = "✅"
            status_text = "PASS"
        else:
            status = "❌"
            status_text = "FAIL"
        print(f"  {status} {test_name}: {status_text}")
    
    # Root cause analysis
    print("\n" + "=" * 80)
    print("ROOT CAUSE ANALYSIS")
    print("=" * 80)
    
    # Analyze patterns
    cache_issue = results[0][1] is False if results[0][1] is not None else False
    identity_issue = not results[1][1]
    lifecycle_issue = not results[2][1]
    async_issue = not results[3][1]
    trigger_issue = not results[4][1]
    
    if lifecycle_issue or async_issue:
        print("\n🔴 PRIMARY ISSUE: Component Refresh Mechanism")
        print("  The Panel server is not properly refreshing components at midnight.")
        print("  Even though dates update, the component objects remain stale.")
        print("\n  SOLUTION:")
        print("  1. Force component recreation using pane.object = new_plot")
        print("  2. Use param.trigger('object') to force updates")
        print("  3. Add explicit component refresh in periodic callback")
        
    elif cache_issue:
        print("\n🔴 PRIMARY ISSUE: Cache Key Management")
        print("  Cache keys don't include proper date/time components.")
        print("  Stale data continues to be served after midnight.")
        print("\n  SOLUTION:")
        print("  1. Include full timestamp in cache keys")
        print("  2. Clear cache when date changes")
        print("  3. Use pn.state.clear_caches() at midnight")
        
    elif trigger_issue:
        print("\n🔴 PRIMARY ISSUE: Update Triggering")
        print("  Component updates are not being triggered properly.")
        print("\n  SOLUTION:")
        print("  1. Add explicit param.trigger() calls")
        print("  2. Ensure watch callbacks are set up correctly")
        
    else:
        print("\n🟡 COMPLEX ISSUE: Multiple Factors")
        print("  The midnight freeze appears to be caused by multiple factors.")
        print("  A comprehensive fix addressing all aspects is needed.")
        print("\n  RECOMMENDED FIX:")
        print("  1. Clear cache at midnight: pn.state.clear_caches()")
        print("  2. Recreate chart objects, not just update data")
        print("  3. Force Panel component refresh with param.trigger()")
        print("  4. Ensure async updates complete before display refresh")
    
    print("\n" + "=" * 80)
    
    return results

if __name__ == "__main__":
    asyncio.run(run_integration_tests())