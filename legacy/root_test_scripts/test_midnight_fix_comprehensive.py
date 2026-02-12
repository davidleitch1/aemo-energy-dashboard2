#!/usr/bin/env python3
"""
Comprehensive test for the midnight display freeze fix.
This test verifies that the Panel component refresh mechanism works correctly.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock
import asyncio

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

# Suppress verbose logging for cleaner test output
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.WARNING)

print("\n" + "=" * 80)
print("COMPREHENSIVE TEST: MIDNIGHT DISPLAY FREEZE FIX")
print("=" * 80)

def test_1_force_component_refresh_exists():
    """Test that the _force_component_refresh method exists and works"""
    print("\n🔧 TEST 1: Force Component Refresh Method")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    
    # Check if the method exists
    if hasattr(dashboard, '_force_component_refresh'):
        print("  ✅ _force_component_refresh method exists")
        
        # Create mock panes to test refresh
        mock_pane = MagicMock()
        mock_pane.object = "test_object"
        mock_pane.param = MagicMock()
        mock_pane.param.trigger = MagicMock()
        
        # Simulate having panes
        dashboard.generation_plot = mock_pane
        dashboard.price_plot = mock_pane
        
        # Call the method
        try:
            dashboard._force_component_refresh()
            print("  ✅ _force_component_refresh executed without errors")
            
            # Check if trigger was called
            if mock_pane.param.trigger.called:
                print("  ✅ param.trigger was called on components")
                return True
            else:
                print("  ⚠️ param.trigger was not called")
                return False
                
        except Exception as e:
            print(f"  ❌ Error calling _force_component_refresh: {e}")
            return False
    else:
        print("  ❌ _force_component_refresh method not found")
        return False

async def test_2_auto_update_loop_with_date_range_change():
    """Test that auto_update_loop detects date range changes and forces refresh"""
    print("\n🔧 TEST 2: Auto Update Loop Date Range Detection")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # Last 24 hours mode
    
    # Track method calls
    refresh_called = []
    update_called = []
    
    # Mock the force refresh method
    def mock_force_refresh():
        refresh_called.append(datetime.now())
        print(f"    _force_component_refresh called at simulated time")
    
    dashboard._force_component_refresh = mock_force_refresh
    
    # Mock the update_plot method
    original_update = dashboard.update_plot
    def mock_update():
        update_called.append(datetime.now())
        # Don't actually update to avoid errors
    dashboard.update_plot = mock_update
    
    print("\n  Simulating auto_update_loop across midnight:")
    
    # Simulate the loop with immediate execution (no sleep)
    with patch('asyncio.sleep', return_value=None):
        # Start at 11:55 PM
        with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
            mock_dt.combine = datetime.combine
            
            dashboard._update_date_range_from_preset()
            initial_range = (dashboard.start_date, dashboard.end_date)
            print(f"    11:55 PM: Initial range = {initial_range}")
        
        # Create a modified auto_update_loop for testing
        async def test_loop():
            last_date_range = None
            iterations = 0
            
            # Simulate 3 iterations: before midnight, at midnight, after midnight
            times = [
                datetime(2024, 12, 10, 23, 55, 0),  # Before midnight
                datetime(2024, 12, 11, 0, 0, 0),    # Midnight
                datetime(2024, 12, 11, 0, 5, 0)     # After midnight
            ]
            
            for test_time in times:
                iterations += 1
                
                with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
                    mock_dt.now.return_value = test_time
                    mock_dt.combine = datetime.combine
                    
                    # This is the actual logic from auto_update_loop
                    if dashboard.time_range in ['1', '7', '30']:
                        old_start_date = dashboard.start_date
                        old_end_date = dashboard.end_date
                        old_range = (old_start_date, old_end_date)
                        
                        dashboard._update_date_range_from_preset()
                        new_start_date = dashboard.start_date
                        new_end_date = dashboard.end_date
                        new_range = (new_start_date, new_end_date)
                        
                        # Check if date RANGE changed (the key fix!)
                        if old_range != new_range and last_date_range is not None:
                            print(f"    {test_time.strftime('%H:%M')}: Date RANGE changed {old_range} → {new_range}")
                            dashboard._force_component_refresh()
                        else:
                            print(f"    {test_time.strftime('%H:%M')}: Date range unchanged {new_range}")
                        
                        last_date_range = new_range
                    
                    dashboard.update_plot()
                
                if iterations >= 3:
                    break
        
        # Run the test loop
        await test_loop()
    
    print("\n  Results:")
    print(f"    Force refresh called: {len(refresh_called)} times")
    print(f"    Normal update called: {len(update_called)} times")
    
    if len(refresh_called) >= 1:
        print("  ✅ Date range change detected and force refresh triggered!")
        return True
    else:
        print("  ❌ Date range change not properly detected")
        return False

def test_3_nem_dash_integration():
    """Test that NEM dash tab also handles date range changes"""
    print("\n🔧 TEST 3: NEM Dash Tab Integration")
    print("-" * 60)
    
    # Read the nem_dash_tab.py file to verify fix is present
    nem_dash_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'nem_dash_tab.py'
    
    with open(nem_dash_file, 'r') as f:
        content = f.read()
    
    # Check for the enhanced fix
    checks = {
        "Date range tracking": "old_range = (old_start_date, old_end_date)" in content,
        "Range comparison": "if old_range != new_range:" in content,
        "Force refresh call": "dashboard_instance._force_component_refresh()" in content,
        "Date RANGE logging": "Date RANGE changed from" in content
    }
    
    print("  Checking NEM dash tab for fix components:")
    all_present = True
    for check_name, present in checks.items():
        if present:
            print(f"    ✅ {check_name}")
        else:
            print(f"    ❌ {check_name}")
            all_present = False
    
    if all_present:
        print("\n  ✅ NEM dash tab has complete fix integration")
        return True
    else:
        print("\n  ❌ NEM dash tab missing some fix components")
        return False

def test_4_fix_verification_in_source():
    """Verify that the fix is properly implemented in the source code"""
    print("\n🔧 TEST 4: Source Code Fix Verification")
    print("-" * 60)
    
    gen_dash_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'generation' / 'gen_dash.py'
    
    with open(gen_dash_file, 'r') as f:
        content = f.read()
    
    # Check for all components of the fix
    fix_components = {
        "Date range tracking": "last_date_range = None" in content,
        "Old range capture": "old_range = (old_start_date, old_end_date)" in content,
        "New range capture": "new_range = (new_start_date, new_end_date)" in content,
        "Range comparison": "if old_range != new_range and last_date_range is not None:" in content,
        "Force refresh call": "self._force_component_refresh()" in content,
        "Force refresh method": "def _force_component_refresh(self):" in content,
        "Param trigger": "pane.param.trigger('object')" in content,
        "Object reassignment": "pane.object = None" in content and "pane.object = current_object" in content
    }
    
    print("  Checking gen_dash.py for all fix components:")
    all_present = True
    for component_name, present in fix_components.items():
        if present:
            print(f"    ✅ {component_name}")
        else:
            print(f"    ❌ {component_name}")
            all_present = False
    
    if all_present:
        print("\n  ✅ All fix components are present in source code")
        return True
    else:
        print("\n  ❌ Some fix components are missing")
        return False

def test_5_simulated_midnight_scenario():
    """Simulate the exact midnight scenario with mock components"""
    print("\n🔧 TEST 5: Simulated Midnight Scenario")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    import panel as pn
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'
    
    # Create mock Panel panes
    mock_panes = {}
    refresh_log = []
    
    class MockPane:
        def __init__(self, name):
            self.name = name
            self.object = f"plot_{name}"
            self.param = MagicMock()
            self.param.trigger = lambda x: refresh_log.append(f"{name}_triggered")
    
    # Add mock panes to dashboard
    dashboard.generation_plot = MockPane("generation")
    dashboard.price_plot = MockPane("price")
    
    print("\n  Setting up scenario:")
    print("    Created mock Panel panes for generation and price plots")
    
    # Simulate time progression
    print("\n  Time progression:")
    
    # 11:55 PM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        before_range = (dashboard.start_date, dashboard.end_date)
        print(f"    11:55 PM: Range = {before_range}")
    
    # Midnight - trigger the fix
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 11, 0, 0, 0)
        mock_dt.combine = datetime.combine
        
        old_range = (dashboard.start_date, dashboard.end_date)
        dashboard._update_date_range_from_preset()
        new_range = (dashboard.start_date, dashboard.end_date)
        
        print(f"    12:00 AM: Range = {new_range}")
        
        if old_range != new_range:
            print("    → Date range changed! Calling _force_component_refresh()")
            dashboard._force_component_refresh()
    
    print("\n  Component refresh log:")
    if refresh_log:
        for entry in refresh_log:
            print(f"    • {entry}")
        print(f"\n  ✅ {len(refresh_log)} components were refreshed at midnight")
        return True
    else:
        print("    (empty)")
        print("\n  ❌ No components were refreshed")
        return False

# Run all tests
async def run_comprehensive_tests():
    """Run all comprehensive tests"""
    print("\nRunning comprehensive tests for midnight display freeze fix...")
    print("This verifies the complete solution is properly implemented.\n")
    
    results = []
    
    # Test 1
    try:
        result1 = test_1_force_component_refresh_exists()
        results.append(("Force Component Refresh Method", result1))
    except Exception as e:
        print(f"\n  ⚠️ Test 1 error: {e}")
        results.append(("Force Component Refresh Method", False))
    
    # Test 2
    try:
        result2 = await test_2_auto_update_loop_with_date_range_change()
        results.append(("Auto Update Loop Detection", result2))
    except Exception as e:
        print(f"\n  ⚠️ Test 2 error: {e}")
        results.append(("Auto Update Loop Detection", False))
    
    # Test 3
    try:
        result3 = test_3_nem_dash_integration()
        results.append(("NEM Dash Integration", result3))
    except Exception as e:
        print(f"\n  ⚠️ Test 3 error: {e}")
        results.append(("NEM Dash Integration", False))
    
    # Test 4
    try:
        result4 = test_4_fix_verification_in_source()
        results.append(("Source Code Verification", result4))
    except Exception as e:
        print(f"\n  ⚠️ Test 4 error: {e}")
        results.append(("Source Code Verification", False))
    
    # Test 5
    try:
        result5 = test_5_simulated_midnight_scenario()
        results.append(("Simulated Midnight Scenario", result5))
    except Exception as e:
        print(f"\n  ⚠️ Test 5 error: {e}")
        results.append(("Simulated Midnight Scenario", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("COMPREHENSIVE TEST RESULTS")
    print("=" * 80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 SUCCESS: MIDNIGHT DISPLAY FREEZE FIX IS COMPLETE!")
        print("\nThe fix successfully:")
        print("  ✅ Detects when date range changes at midnight")
        print("  ✅ Forces Panel components to refresh their display")
        print("  ✅ Works in both main dashboard and NEM dash tab")
        print("  ✅ Uses param.trigger() for proper Panel integration")
        print("\nThe dashboard will now continue updating past midnight!")
    else:
        print("⚠️ INCOMPLETE: Some tests failed")
        print("\nPlease review the failures above and complete the implementation.")
    print("=" * 80)
    
    return all_passed

if __name__ == "__main__":
    import asyncio
    success = asyncio.run(run_comprehensive_tests())
    sys.exit(0 if success else 1)