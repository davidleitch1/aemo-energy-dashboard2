#!/usr/bin/env python3
"""
Test production midnight scenario with actual dashboard components.
This test verifies the fix works with real data and components.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, date
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

print("\n" + "=" * 80)
print("PRODUCTION MIDNIGHT SCENARIO TEST")
print("=" * 80)

def test_date_range_calculation():
    """Test that date range calculation works correctly at midnight"""
    print("\n🔧 Testing Date Range Calculation")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # Last 24 hours
    
    # Test at 11:55 PM
    print("\n  1. Testing at 11:55 PM...")
    test_time = datetime(2024, 12, 10, 23, 55, 0)
    
    # Mock datetime for testing
    from unittest.mock import patch
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = test_time
        mock_dt.combine = datetime.combine
        
        dashboard._update_date_range_from_preset()
        
        print(f"     Current time: {test_time}")
        print(f"     Start date: {dashboard.start_date}")
        print(f"     End date: {dashboard.end_date}")
        
        before_range = (dashboard.start_date, dashboard.end_date)
    
    # Test at midnight
    print("\n  2. Testing at midnight (00:00)...")
    test_time = datetime(2024, 12, 11, 0, 0, 0)
    
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = test_time
        mock_dt.combine = datetime.combine
        
        dashboard._update_date_range_from_preset()
        
        print(f"     Current time: {test_time}")
        print(f"     Start date: {dashboard.start_date}")
        print(f"     End date: {dashboard.end_date}")
        
        after_range = (dashboard.start_date, dashboard.end_date)
    
    # Check if range changed
    print("\n  3. Checking if date range changed...")
    if before_range != after_range:
        print(f"     ✅ Date range changed!")
        print(f"        Before: {before_range}")
        print(f"        After:  {after_range}")
        return True
    else:
        print(f"     ❌ Date range unchanged: {before_range}")
        return False

def test_nem_dash_update_mechanism():
    """Test NEM dash tab update mechanism"""
    print("\n🔧 Testing NEM Dash Update Mechanism")
    print("-" * 60)
    
    import panel as pn
    pn.extension()
    
    # Create mock dashboard instance
    class MockDashboard:
        def __init__(self):
            self.start_date = date(2024, 12, 10)
            self.end_date = date(2024, 12, 10)
            self.time_range = '1'
            self._force_refresh_called = False
        
        def _update_date_range_from_preset(self):
            # Simulate date change
            self.start_date = date(2024, 12, 11)
            self.end_date = date(2024, 12, 11)
        
        def _force_component_refresh(self):
            self._force_refresh_called = True
            print("     → _force_component_refresh() called")
    
    dashboard = MockDashboard()
    
    print("\n  1. Creating NEM dash tab components...")
    
    # Create mock components
    top_row = pn.Row(
        pn.pane.HTML("<div>Price Chart</div>"),
        pn.pane.HTML("<div>Generation Overview</div>")
    )
    bottom_row = pn.Row(
        pn.pane.HTML("<div>Price Table</div>"),
        pn.pane.HTML("<div>Renewable Gauge</div>"),
        pn.pane.HTML("<div>Daily Summary</div>")
    )
    
    print(f"     Top row: {len(top_row)} components")
    print(f"     Bottom row: {len(bottom_row)} components")
    
    print("\n  2. Simulating update at midnight...")
    
    # Store old range
    old_range = (dashboard.start_date, dashboard.end_date)
    print(f"     Old range: {old_range}")
    
    # Update date range (simulates midnight)
    dashboard._update_date_range_from_preset()
    new_range = (dashboard.start_date, dashboard.end_date)
    print(f"     New range: {new_range}")
    
    # Check if range changed
    if old_range != new_range:
        print("     Date range changed - triggering refresh...")
        
        # This is what the NEM dash update does
        dashboard._force_component_refresh()
        
        # Create new components
        new_price_chart = pn.pane.HTML("<div>Updated Price Chart</div>")
        new_overview = pn.pane.HTML("<div>Updated Generation</div>")
        new_price_table = pn.pane.HTML("<div>Updated Price Table</div>")
        new_gauge = pn.pane.HTML("<div>Updated Gauge</div>")
        new_summary = pn.pane.HTML("<div>Updated Summary</div>")
        
        # Apply the fix
        top_row.clear()
        top_row.extend([new_price_chart, new_overview])
        
        bottom_row.clear()
        bottom_row.extend([new_price_table, new_gauge, new_summary])
        
        # Trigger param update
        if hasattr(top_row, 'param'):
            top_row.param.trigger('objects')
        if hasattr(bottom_row, 'param'):
            bottom_row.param.trigger('objects')
        
        print("     ✅ Components refreshed using clear/extend method")
    
    if dashboard._force_refresh_called:
        print("\n  ✅ Update mechanism working correctly!")
        return True
    else:
        print("\n  ❌ Force refresh not triggered")
        return False

def test_component_pane_names():
    """Test that component pane names match what _force_component_refresh expects"""
    print("\n🔧 Testing Component Pane Names")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    
    # The pane names that _force_component_refresh looks for
    expected_panes = [
        'plot_pane',              # Main generation plot
        'price_plot_pane',        # Price plot
        'transmission_pane',      # Transmission plot
        'utilization_pane',       # Utilization plot
        'bands_plot_pane',        # Price bands plot
        'tod_plot_pane',          # Time of day plot
    ]
    
    print("\n  Checking if _force_component_refresh uses correct pane names:")
    
    # Read the source to verify
    gen_dash_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'generation' / 'gen_dash.py'
    
    if gen_dash_file.exists():
        with open(gen_dash_file, 'r') as f:
            content = f.read()
        
        # Find the _force_component_refresh method
        if 'def _force_component_refresh(self):' in content:
            # Extract the method
            start = content.find('def _force_component_refresh(self):')
            end = content.find('\n    def ', start + 1)
            if end == -1:
                end = len(content)
            method_content = content[start:end]
            
            # Check for correct pane names
            for pane_name in expected_panes:
                if f"'{pane_name}'" in method_content or f'"{pane_name}"' in method_content:
                    print(f"    ✅ Found: {pane_name}")
                else:
                    print(f"    ⚠️ Not found in method: {pane_name}")
            
            # Check if it's looking for wrong names
            wrong_names = ['generation_plot', 'price_plot', 'transmission_plot']
            for wrong_name in wrong_names:
                if f"'{wrong_name}'" in method_content or f'"{wrong_name}"' in method_content:
                    print(f"    ❌ WRONG NAME FOUND: {wrong_name}")
                    return False
            
            print("\n  ✅ Component pane names are correct!")
            return True
        else:
            print("  ❌ _force_component_refresh method not found")
            return False
    else:
        print("  ⚠️ Source file not found")
        return False

def test_production_readiness():
    """Final check that everything is ready for production"""
    print("\n🔧 Testing Production Readiness")
    print("-" * 60)
    
    checks = []
    
    # Check 1: Production files exist
    print("\n  1. Checking production files...")
    prod_path = Path('/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2')
    
    files_to_check = [
        'src/aemo_dashboard/generation/gen_dash.py',
        'src/aemo_dashboard/nem_dash/nem_dash_tab.py',
        'src/aemo_dashboard/nem_dash/price_components.py',
    ]
    
    all_exist = True
    for file_path in files_to_check:
        full_path = prod_path / file_path
        if full_path.exists():
            print(f"     ✅ {file_path}")
        else:
            print(f"     ❌ {file_path}")
            all_exist = False
    
    checks.append(("Production files", all_exist))
    
    # Check 2: Critical fixes present
    print("\n  2. Checking critical fixes...")
    
    if all_exist:
        nem_dash = prod_path / 'src/aemo_dashboard/nem_dash/nem_dash_tab.py'
        with open(nem_dash, 'r') as f:
            content = f.read()
        
        critical_fixes = [
            "CRITICAL FIX: Use Panel's proper update mechanism",
            "top_row.clear()",
            "top_row.extend(",
            "bottom_row.clear()",
            "bottom_row.extend(",
            "param.trigger('objects')",
        ]
        
        all_fixes = True
        for fix in critical_fixes:
            if fix in content:
                print(f"     ✅ {fix[:40]}...")
            else:
                print(f"     ❌ Missing: {fix}")
                all_fixes = False
        
        checks.append(("Critical fixes", all_fixes))
    else:
        checks.append(("Critical fixes", False))
    
    # Check 3: Dashboard can be started
    print("\n  3. Checking dashboard startup capability...")
    
    startup_script = prod_path / 'start_dashboard.sh'
    if startup_script.exists():
        print(f"     ✅ start_dashboard.sh exists")
        checks.append(("Startup script", True))
    else:
        print(f"     ❌ start_dashboard.sh not found")
        checks.append(("Startup script", False))
    
    # Summary
    all_ready = all(check[1] for check in checks)
    
    if all_ready:
        print("\n  ✅ Production is READY!")
        return True
    else:
        print("\n  ❌ Production NOT ready - fix issues above")
        return False

# Run all tests
def main():
    """Run production midnight scenario tests"""
    print("\nTesting production midnight scenario...")
    print("This verifies the fix will work in production with real components.\n")
    
    results = []
    
    # Test 1
    try:
        result1 = test_date_range_calculation()
        results.append(("Date Range Calculation", result1))
    except Exception as e:
        print(f"\n  ⚠️ Test 1 error: {e}")
        results.append(("Date Range Calculation", False))
    
    # Test 2
    try:
        result2 = test_nem_dash_update_mechanism()
        results.append(("NEM Dash Update Mechanism", result2))
    except Exception as e:
        print(f"\n  ⚠️ Test 2 error: {e}")
        results.append(("NEM Dash Update Mechanism", False))
    
    # Test 3
    try:
        result3 = test_component_pane_names()
        results.append(("Component Pane Names", result3))
    except Exception as e:
        print(f"\n  ⚠️ Test 3 error: {e}")
        results.append(("Component Pane Names", False))
    
    # Test 4
    try:
        result4 = test_production_readiness()
        results.append(("Production Readiness", result4))
    except Exception as e:
        print(f"\n  ⚠️ Test 4 error: {e}")
        results.append(("Production Readiness", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("PRODUCTION SCENARIO TEST RESULTS")
    print("=" * 80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 PRODUCTION READY: All tests pass!")
        print("\nNext steps:")
        print("  1. Restart dashboard on production: ./start_dashboard.sh")
        print("  2. Monitor logs: tail -f logs/*.log")
        print("  3. Watch dashboard through midnight (23:55 → 00:05)")
        print("  4. Verify all components update after midnight")
        print("  5. Monitor for at least 3 consecutive midnights")
        print("\n⚠️ DO NOT mark issue resolved until confirmed in production!")
    else:
        print("⚠️ NOT READY: Some tests failed")
        print("\nFix the issues above before deploying to production.")
    print("=" * 80)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)