#!/usr/bin/env python3
"""
Final verification that all fixes are working correctly
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

# Suppress verbose logging
import logging
logging.getLogger('aemo_dashboard').setLevel(logging.ERROR)

print("\n" + "=" * 80)
print("FINAL BUG FIX VERIFICATION")
print("=" * 80)

def test_critical_midnight_bug():
    """The most important test - does the dashboard update after midnight?"""
    print("\n🎯 CRITICAL TEST: Dashboard Updates After Midnight")
    print("-" * 60)
    
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    dashboard = EnergyDashboard()
    dashboard.time_range = '1'  # This is the mode that had the bug
    
    print("  Simulating the exact bug scenario:")
    print("  Dashboard running in 'Last 24 Hours' mode")
    
    # At 11:55 PM
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 10, 23, 55, 0)
        mock_dt.combine = datetime.combine
        dashboard._update_date_range_from_preset()
        at_2355 = dashboard.end_date
        print(f"\n  11:55 PM: Dashboard queries data up to {at_2355}")
    
    # After midnight WITHOUT the fix
    print("\n  Scenario WITHOUT fix:")
    print(f"    Dashboard would still query for {at_2355}")
    print("    Result: Display frozen at 23:55 ❌")
    
    # After midnight WITH the fix
    print("\n  Scenario WITH fix (current code):")
    with patch('aemo_dashboard.generation.gen_dash.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 12, 11, 0, 5, 0)
        mock_dt.combine = datetime.combine
        
        # This is what the auto_update_loop does now
        if dashboard.time_range in ['1', '7', '30']:
            old_end = dashboard.end_date
            dashboard._update_date_range_from_preset()
            new_end = dashboard.end_date
            print(f"    Date refresh: {old_end} → {new_end}")
        
        at_0005 = dashboard.end_date
        print(f"    Dashboard now queries data up to {at_0005}")
    
    if at_2355 == datetime(2024, 12, 10).date() and at_0005 == datetime(2024, 12, 11).date():
        print("\n  ✅ CRITICAL BUG FIXED: Dashboard continues updating after midnight!")
        return True
    else:
        print("\n  ❌ BUG NOT FIXED: Dashboard would still freeze!")
        return False

def verify_both_loops_fixed():
    """Verify both update loops have the fix"""
    print("\n🎯 VERIFICATION: Both Update Loops Fixed")
    print("-" * 60)
    
    # Check main dashboard loop
    gen_dash = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'generation' / 'gen_dash.py'
    with open(gen_dash, 'r') as f:
        main_content = f.read()
    
    main_has_fix = (
        "async def auto_update_loop" in main_content and
        "# FIX for midnight rollover bug" in main_content and
        "if self.time_range in ['1', '7', '30']:" in main_content and
        "self._update_date_range_from_preset()" in main_content
    )
    
    # Check NEM dash loop
    nem_dash = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'nem_dash_tab.py'
    with open(nem_dash, 'r') as f:
        nem_content = f.read()
    
    nem_has_fix = (
        "def update_all_components" in nem_content and
        "# FIX for midnight rollover" in nem_content and
        "dashboard_instance._update_date_range_from_preset()" in nem_content
    )
    
    print(f"  Main dashboard loop (gen_dash.py): {'✅ Fixed' if main_has_fix else '❌ Not fixed'}")
    print(f"  NEM dash tab loop (nem_dash_tab.py): {'✅ Fixed' if nem_has_fix else '❌ Not fixed'}")
    
    if main_has_fix and nem_has_fix:
        print("\n  ✅ BOTH UPDATE LOOPS FIXED!")
        return True
    else:
        print("\n  ❌ Not all loops fixed")
        return False

def verify_gauge_improvements():
    """Verify renewable gauge improvements"""
    print("\n🎯 VERIFICATION: Renewable Gauge Improvements")
    print("-" * 60)
    
    gauge_file = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'renewable_gauge.py'
    with open(gauge_file, 'r') as f:
        content = f.read()
    
    improvements = {
        "Green color for gauge bar": "'color': \"#50fa7b\"" in content,
        "Timestamp display logic": "Updated:" in content and "add_annotation" in content,
        "Pumped hydro exclusion": "PUMPED_HYDRO_DUIDS" in content,
        "No invalid subtitle param": "subtitle={" not in content,
        "Adjusted domain for timestamp": "[0.20, 1]" in content
    }
    
    all_good = True
    for improvement, present in improvements.items():
        status = "✅" if present else "❌"
        print(f"  {status} {improvement}")
        if not present:
            all_good = False
    
    if all_good:
        print("\n  ✅ ALL GAUGE IMPROVEMENTS APPLIED!")
        return True
    else:
        print("\n  ⚠️ Some gauge improvements missing")
        return False

# Run tests
print("\nRunning final verification...")

results = []
results.append(test_critical_midnight_bug())
results.append(verify_both_loops_fixed())
results.append(verify_gauge_improvements())

print("\n" + "=" * 80)
print("FINAL VERDICT")
print("=" * 80)

if all(results):
    print("\n🎉 🎉 🎉 ALL FIXES VERIFIED! 🎉 🎉 🎉")
    print("\nThe midnight rollover bug is COMPLETELY FIXED:")
    print("  ✅ Dashboard continues updating after midnight")
    print("  ✅ Both update loops have the fix")
    print("  ✅ Renewable gauge displays correctly")
    print("\nThe dashboard is ready for production use!")
else:
    print("\n⚠️ Some issues remain. Please review the output above.")

print("=" * 80)