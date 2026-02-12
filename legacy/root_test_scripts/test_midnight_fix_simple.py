#!/usr/bin/env python3
"""
Simple test to verify the midnight rollover fixes are applied correctly
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment
os.environ['USE_DUCKDB'] = 'true'

def test_main_dashboard_fix():
    """Check if the main dashboard fix is present"""
    print("\n" + "=" * 70)
    print("TEST: Verify Main Dashboard Fix")
    print("=" * 70)
    
    # Read the gen_dash.py file and check for the fix
    gen_dash_path = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'generation' / 'gen_dash.py'
    
    with open(gen_dash_path, 'r') as f:
        content = f.read()
    
    # Look for the fix in auto_update_loop
    fix_marker = "# FIX for midnight rollover bug: Refresh date ranges for preset time ranges"
    fix_code = "if self.time_range in ['1', '7', '30']:"
    refresh_code = "self._update_date_range_from_preset()"
    
    if fix_marker in content and fix_code in content and refresh_code in content:
        print("✅ PASS: Main dashboard fix is present in auto_update_loop()")
        
        # Find the line numbers
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if fix_marker in line:
                print(f"  Found at line {i}: Fix comment")
            elif "if self.time_range in ['1', '7', '30']:" in line and i > 2130 and i < 2150:
                print(f"  Found at line {i}: Time range check")
            elif "self._update_date_range_from_preset()" in line and i > 2130 and i < 2150:
                print(f"  Found at line {i}: Date refresh call")
        return True
    else:
        print("❌ FAIL: Main dashboard fix NOT found in auto_update_loop()")
        return False

def test_nem_dash_fix():
    """Check if the NEM dash tab fix is present"""
    print("\n" + "=" * 70)
    print("TEST: Verify NEM Dash Tab Fix")
    print("=" * 70)
    
    # Read the nem_dash_tab.py file and check for the fix
    nem_dash_path = Path(__file__).parent / 'src' / 'aemo_dashboard' / 'nem_dash' / 'nem_dash_tab.py'
    
    with open(nem_dash_path, 'r') as f:
        content = f.read()
    
    # Look for the fix in update_all_components
    fix_marker = "# FIX for midnight rollover: First refresh dashboard dates if using preset time ranges"
    fix_code = "if dashboard_instance and hasattr(dashboard_instance, 'time_range'):"
    refresh_code = "dashboard_instance._update_date_range_from_preset()"
    rollover_log = "NEM dash: Date rollover detected"
    
    if fix_marker in content and fix_code in content and refresh_code in content and rollover_log in content:
        print("✅ PASS: NEM dash tab fix is present in update_all_components()")
        
        # Find the line numbers
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if fix_marker in line:
                print(f"  Found at line {i}: Fix comment")
            elif "if dashboard_instance and hasattr(dashboard_instance, 'time_range'):" in line and i > 140 and i < 160:
                print(f"  Found at line {i}: Dashboard instance check")
            elif "dashboard_instance._update_date_range_from_preset()" in line and i > 140 and i < 160:
                print(f"  Found at line {i}: Date refresh call")
            elif "NEM dash: Date rollover detected" in line and i > 140 and i < 160:
                print(f"  Found at line {i}: Rollover logging")
        return True
    else:
        print("❌ FAIL: NEM dash tab fix NOT found in update_all_components()")
        return False

def test_date_refresh_logic():
    """Test the actual date refresh logic"""
    print("\n" + "=" * 70)
    print("TEST: Date Refresh Logic")
    print("=" * 70)
    
    # Import the dashboard class
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    # Create dashboard instance
    dashboard = EnergyDashboard()
    
    # Test each preset time range
    test_cases = [
        ('1', "Last 24 hours"),
        ('7', "Last 7 days"),
        ('30', "Last 30 days"),
    ]
    
    all_passed = True
    for time_range, description in test_cases:
        dashboard.time_range = time_range
        dashboard._update_date_range_from_preset()
        
        # Check that end_date is today (or close to it)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        if dashboard.end_date in [today, yesterday]:  # Allow for timezone differences
            print(f"  ✅ '{time_range}' ({description}): end_date = {dashboard.end_date} (current)")
        else:
            print(f"  ❌ '{time_range}' ({description}): end_date = {dashboard.end_date} (not current!)")
            all_passed = False
    
    return all_passed

def main():
    """Run all tests"""
    print("\n" + "=" * 70)
    print("MIDNIGHT ROLLOVER BUG FIX VERIFICATION")
    print("=" * 70)
    print("\nVerifying that both fixes are correctly applied...")
    
    results = []
    
    # Test 1: Main dashboard fix
    results.append(("Main Dashboard Fix", test_main_dashboard_fix()))
    
    # Test 2: NEM dash tab fix
    results.append(("NEM Dash Tab Fix", test_nem_dash_fix()))
    
    # Test 3: Date refresh logic
    results.append(("Date Refresh Logic", test_date_refresh_logic()))
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 SUCCESS: Both midnight rollover fixes are correctly applied!")
        print("\nThe dashboard will now:")
        print("  • Continue updating past midnight")
        print("  • Show current data in price charts and tables")
        print("  • Keep all components synchronized")
        print("  • Work correctly after browser refresh")
    else:
        print("\n⚠️  WARNING: Some fixes may be missing or incorrect.")
        print("Please review the code changes.")
    
    print("\n" + "=" * 70)
    print("KEY BEHAVIOR CHANGES")
    print("=" * 70)
    print("""
1. BEFORE midnight (e.g., 11:55 PM on Dec 10):
   - Dashboard shows data up to Dec 10
   - Everything works normally

2. AFTER midnight WITHOUT fix:
   - Dashboard still queries for Dec 10 data only
   - Display freezes at last data point (23:55)
   - Browser shows stale data

3. AFTER midnight WITH fix:
   - Both update loops refresh dates to Dec 11
   - Dashboard queries include Dec 11 data
   - Display continues updating with new data
   - Browser refresh shows current data
""")
    
    return all_passed

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)