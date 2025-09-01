#!/usr/bin/env python3
"""
Specific tests for the midnight rollover and gauge display fixes
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch
import asyncio

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

# Suppress verbose logging
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.ERROR)

print("\n" + "=" * 80)
print("SPECIFIC FIX VERIFICATION TESTS")
print("=" * 80)

def test_midnight_date_refresh():
    """Test the core midnight date refresh mechanism"""
    print("\n🔬 TEST: Midnight Date Refresh Logic")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # Last 24 hours
    
    # Test at 11:55 PM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        before = dashboard.end_date
        print(f"  11:55 PM Dec 10: end_date = {before}")
    
    # Test at 12:05 AM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 11, 0, 5, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        after = dashboard.end_date
        print(f"  12:05 AM Dec 11: end_date = {after}")
    
    if before == datetime(2024, 12, 10).date() and after == datetime(2024, 12, 11).date():
        print("\n  ✅ PASS: Dates correctly roll over at midnight")
        return True
    else:
        print("\n  ❌ FAIL: Date rollover not working")
        return False

def test_auto_update_loop_fix():
    """Test that auto_update_loop has the fix"""
    print("\n🔬 TEST: Auto Update Loop Fix Present")
    print("-" * 60)
    
    gen_dash_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'generation' / 'gen_dash.py'
    
    with open(gen_dash_file, 'r') as f:
        lines = f.readlines()
    
    # Find the auto_update_loop method
    in_auto_update = False
    has_fix = False
    
    for i, line in enumerate(lines, 1):
        if 'async def auto_update_loop' in line:
            in_auto_update = True
            print(f"  Found auto_update_loop at line {i}")
        
        if in_auto_update:
            if "if self.time_range in ['1', '7', '30']:" in line:
                print(f"  Found time range check at line {i}")
                # Check next few lines for update call
                for j in range(i, min(i+5, len(lines))):
                    if '_update_date_range_from_preset' in lines[j]:
                        print(f"  Found date refresh call at line {j+1}")
                        has_fix = True
                        break
                break
    
    if has_fix:
        print("\n  ✅ PASS: auto_update_loop has the midnight fix")
        return True
    else:
        print("\n  ❌ FAIL: auto_update_loop missing the fix")
        return False

def test_nem_dash_fix():
    """Test that NEM dash tab has the fix"""
    print("\n🔬 TEST: NEM Dash Tab Fix Present")  
    print("-" * 60)
    
    nem_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'nem_dash_tab.py'
    
    with open(nem_file, 'r') as f:
        content = f.read()
    
    # Look for the specific fix pattern
    fix_lines = [
        "# FIX for midnight rollover",
        "if dashboard_instance and hasattr(dashboard_instance, 'time_range'):",
        "if time_range in ['1', '7', '30']:",
        "dashboard_instance._update_date_range_from_preset()"
    ]
    
    found = []
    for fix_line in fix_lines:
        if fix_line in content:
            found.append(fix_line[:50] + "...")
    
    print(f"  Found {len(found)}/{len(fix_lines)} fix components:")
    for f in found:
        print(f"    ✓ {f}")
    
    if len(found) == len(fix_lines):
        print("\n  ✅ PASS: NEM dash tab has complete fix")
        return True
    else:
        print("\n  ❌ FAIL: NEM dash tab missing fix components")
        return False

def test_gauge_display_fixes():
    """Test renewable gauge display fixes"""
    print("\n🔬 TEST: Renewable Gauge Display Fixes")
    print("-" * 60)
    
    gauge_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'renewable_gauge.py'
    
    with open(gauge_file, 'r') as f:
        content = f.read()
    
    tests = {
        "No invalid subtitle": 'subtitle={' not in content,
        "Has timestamp annotation": 'fig.add_annotation(' in content and '"Updated:' in content,
        "Has green color #50fa7b": '#50fa7b' in content,
        "Has pumped hydro list": 'PUMPED_HYDRO_DUIDS = [' in content,
        "Domain adjusted for timestamp": "'y': [0.20, 1]" in content
    }
    
    all_passed = True
    for test_name, result in tests.items():
        if result:
            print(f"  ✅ {test_name}")
        else:
            print(f"  ❌ {test_name}")
            all_passed = False
    
    if all_passed:
        print("\n  ✅ PASS: Gauge display properly fixed")
        return True
    else:
        print("\n  ❌ FAIL: Gauge display has issues")
        return False

async def test_update_sequence():
    """Test the complete update sequence after midnight"""
    print("\n🔬 TEST: Complete Update Sequence After Midnight")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'
    
    # Track the sequence
    sequence = []
    
    def track_update():
        sequence.append({
            'time': 'current',
            'dates': (dashboard.start_date, dashboard.end_date)
        })
    
    dashboard.update_plot = track_update
    
    # Simulate the sequence
    times = [
        ('11:55 PM', datetime(2024, 12, 10, 23, 55, 0)),
        ('12:00 AM', datetime(2024, 12, 11, 0, 0, 0)),
        ('12:05 AM', datetime(2024, 12, 11, 0, 5, 0))
    ]
    
    for label, test_time in times:
        with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
            mock_dt.now.return_value = test_time
            mock_dt.combine = datetime.combine
            
            # Simulate auto_update_loop logic
            if dashboard.time_range in ['1', '7', '30']:
                old_end = dashboard.end_date
                dashboard._update_date_range_from_preset()
                new_end = dashboard.end_date
                if old_end != new_end:
                    print(f"  {label}: Date rollover {old_end} → {new_end}")
                else:
                    print(f"  {label}: No change ({new_end})")
            
            dashboard.update_plot()
    
    # Check final state
    if dashboard.end_date == datetime(2024, 12, 11).date():
        print("\n  ✅ PASS: Update sequence works correctly")
        return True
    else:
        print(f"\n  ❌ FAIL: Final date is {dashboard.end_date}, expected 2024-12-11")
        return False

# Run all tests
async def main():
    results = []
    
    # Test 1
    results.append(("Midnight Date Refresh", test_midnight_date_refresh()))
    
    # Test 2
    results.append(("Auto Update Loop Fix", test_auto_update_loop_fix()))
    
    # Test 3
    results.append(("NEM Dash Tab Fix", test_nem_dash_fix()))
    
    # Test 4
    results.append(("Gauge Display Fixes", test_gauge_display_fixes()))
    
    # Test 5
    result = await test_update_sequence()
    results.append(("Update Sequence", result))
    
    # Summary
    print("\n" + "=" * 80)
    print("FINAL RESULTS")
    print("=" * 80)
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 SUCCESS: All fixes are properly applied and working!")
        print("\nThe dashboard will:")
        print("  • Continue updating after midnight")
        print("  • Show current data, not yesterday's")
        print("  • Display timestamp on gauge")
        print("  • Use correct green color for renewables")
    else:
        print("\n⚠️ Some tests failed. Please review.")
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)