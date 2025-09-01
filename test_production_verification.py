#!/usr/bin/env python3
"""
Final verification test for production deployment.
This test checks that all fixes are properly in place in production.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import subprocess

print("\n" + "=" * 80)
print("PRODUCTION VERIFICATION TEST")
print("=" * 80)

def check_production_files():
    """Verify all production files have the correct fixes"""
    print("\n🔧 Checking Production Files")
    print("-" * 60)
    
    prod_path = Path('/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2')
    
    if not prod_path.exists():
        print("  ❌ Production path not accessible")
        return False
    
    results = {}
    
    # Check 1: gen_dash.py has correct pane names
    print("\n  1. Checking gen_dash.py for correct pane names...")
    gen_dash = prod_path / 'src/aemo_dashboard/generation/gen_dash.py'
    
    with open(gen_dash, 'r') as f:
        content = f.read()
    
    # Find _force_component_refresh method
    if 'def _force_component_refresh(self):' in content:
        method_start = content.find('def _force_component_refresh(self):')
        method_end = content.find('\n    def ', method_start + 1)
        if method_end == -1:
            method_end = method_start + 2000  # Take next 2000 chars
        method_content = content[method_start:method_end]
        
        # Check for CORRECT pane names
        correct_names = ['plot_pane', 'price_plot_pane', 'transmission_pane', 
                        'utilization_pane', 'bands_plot_pane', 'tod_plot_pane']
        all_correct = True
        for name in correct_names:
            if f"'{name}'" in method_content:
                print(f"     ✅ Correct: {name}")
            else:
                print(f"     ❌ Missing: {name}")
                all_correct = False
        
        # Check for WRONG names (should not be present)
        wrong_names = ['generation_plot', 'price_plot', 'transmission_plot']
        for name in wrong_names:
            if f"'{name}'" in method_content:
                print(f"     ❌ WRONG NAME PRESENT: {name}")
                all_correct = False
        
        results['gen_dash_panes'] = all_correct
    else:
        print("     ❌ _force_component_refresh method not found")
        results['gen_dash_panes'] = False
    
    # Check 2: nem_dash_tab.py has Panel update fix
    print("\n  2. Checking nem_dash_tab.py for Panel update fix...")
    nem_dash = prod_path / 'src/aemo_dashboard/nem_dash/nem_dash_tab.py'
    
    with open(nem_dash, 'r') as f:
        content = f.read()
    
    required_fixes = [
        ("Clear/extend pattern", "top_row.clear()"),
        ("Extend with new components", "top_row.extend([new_price_chart"),
        ("Bottom row clear", "bottom_row.clear()"),
        ("Bottom row extend", "bottom_row.extend([new_price_table"),
        ("Param trigger backup", "param.trigger('objects')"),
        ("Critical fix comment", "CRITICAL FIX:")
    ]
    
    all_present = True
    for name, pattern in required_fixes:
        if pattern in content:
            print(f"     ✅ {name}")
        else:
            print(f"     ❌ Missing: {name}")
            all_present = False
    
    results['nem_dash_fix'] = all_present
    
    # Check 3: Date range change detection
    print("\n  3. Checking date range change detection...")
    
    if 'old_range != new_range' in content:
        print("     ✅ Date range comparison logic")
        results['date_range_check'] = True
    else:
        print("     ❌ Missing date range comparison")
        results['date_range_check'] = False
    
    # Overall result
    all_good = all(results.values())
    
    if all_good:
        print("\n  ✅ All production files have correct fixes!")
    else:
        print("\n  ❌ Some fixes missing or incorrect")
    
    return all_good

def check_dashboard_process():
    """Check if dashboard is currently running"""
    print("\n🔧 Checking Dashboard Process")
    print("-" * 60)
    
    try:
        # Check for running dashboard process
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        
        dashboard_processes = []
        for line in result.stdout.split('\n'):
            if 'run_dashboard' in line or 'gen_dash.py' in line:
                dashboard_processes.append(line)
        
        if dashboard_processes:
            print("  Dashboard processes found:")
            for proc in dashboard_processes[:3]:  # Show first 3
                # Extract just the command part
                parts = proc.split()
                if len(parts) > 10:
                    cmd = ' '.join(parts[10:])[:80]  # First 80 chars of command
                    print(f"    • {cmd}...")
            
            print(f"\n  ⚠️ Dashboard is running - needs restart to apply fixes")
            return False
        else:
            print("  ✅ No dashboard process running")
            return True
            
    except Exception as e:
        print(f"  ⚠️ Could not check processes: {e}")
        return False

def create_monitoring_script():
    """Create a script to monitor the dashboard through midnight"""
    print("\n🔧 Creating Monitoring Script")
    print("-" * 60)
    
    monitor_script = """#!/bin/bash
# Monitor dashboard through midnight for display freeze bug

echo "Starting midnight monitoring..."
echo "This script will check the dashboard every 5 minutes from 23:50 to 00:10"
echo ""

# Function to check dashboard
check_dashboard() {
    time=$(date +"%H:%M:%S")
    
    # Check if dashboard is responding
    curl_output=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5006 2>/dev/null)
    
    if [ "$curl_output" = "200" ]; then
        echo "[$time] ✅ Dashboard responding (HTTP 200)"
        
        # Try to get actual content to check if it's updating
        content=$(curl -s http://localhost:5006 | grep -o "Last Updated.*" | head -1)
        if [ ! -z "$content" ]; then
            echo "[$time]    $content"
        fi
    else
        echo "[$time] ❌ Dashboard not responding (HTTP $curl_output)"
    fi
    
    # Check logs for errors
    errors=$(tail -20 /Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/logs/*.log 2>/dev/null | grep -i error | wc -l)
    if [ "$errors" -gt "0" ]; then
        echo "[$time] ⚠️  Found $errors errors in recent logs"
    fi
}

# Main monitoring loop
echo "Starting at $(date)"
echo "---"

# Check every 5 minutes from 23:50 to 00:10
for i in {1..5}; do
    check_dashboard
    echo ""
    
    # Sleep 5 minutes (except on last iteration)
    if [ "$i" -lt "5" ]; then
        sleep 300
    fi
done

echo "---"
echo "Monitoring complete at $(date)"
echo ""
echo "To check if the fix worked:"
echo "1. Dashboard should still be updating after midnight"
echo "2. Charts should show current data (not frozen at 23:55)"
echo "3. No errors in logs about component refresh"
"""
    
    script_path = Path('/tmp/monitor_midnight.sh')
    
    try:
        with open(script_path, 'w') as f:
            f.write(monitor_script)
        
        # Make executable
        script_path.chmod(0o755)
        
        print(f"  ✅ Monitoring script created: {script_path}")
        print("\n  To use the monitoring script:")
        print("    1. Start it at 23:45: /tmp/monitor_midnight.sh")
        print("    2. It will check dashboard every 5 minutes")
        print("    3. Look for frozen timestamps after midnight")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Could not create script: {e}")
        return False

def final_checklist():
    """Display final checklist for production deployment"""
    print("\n🔧 Final Deployment Checklist")
    print("-" * 60)
    
    checklist = [
        "Stop current dashboard process",
        "Pull latest code from git (if needed)",
        "Start dashboard with: ./start_dashboard.sh",
        "Verify dashboard loads at http://localhost:5006",
        "Check all tabs load without errors",
        "Run monitoring script at 23:45",
        "Watch dashboard through midnight (23:55 → 00:05)",
        "Verify charts update after midnight",
        "Check logs for any errors",
        "Monitor for 3 consecutive nights"
    ]
    
    print("\n  Steps to complete:")
    for i, item in enumerate(checklist, 1):
        print(f"    {i:2}. {item}")
    
    print("\n  ⚠️ CRITICAL: Do not mark issue resolved until:")
    print("     • Dashboard updates correctly through 3 midnights")
    print("     • No manual intervention required")
    print("     • All components refresh properly")
    
    return True

# Run all checks
def main():
    """Run production verification checks"""
    print("\nVerifying production deployment readiness...")
    print("This test ensures all fixes are properly in place.\n")
    
    results = []
    
    # Check 1: Production files
    try:
        result1 = check_production_files()
        results.append(("Production Files", result1))
    except Exception as e:
        print(f"\n  ⚠️ Error checking files: {e}")
        results.append(("Production Files", False))
    
    # Check 2: Dashboard process
    try:
        result2 = check_dashboard_process()
        results.append(("Dashboard Process", result2))
    except Exception as e:
        print(f"\n  ⚠️ Error checking process: {e}")
        results.append(("Dashboard Process", False))
    
    # Check 3: Create monitoring script
    try:
        result3 = create_monitoring_script()
        results.append(("Monitoring Script", result3))
    except Exception as e:
        print(f"\n  ⚠️ Error creating script: {e}")
        results.append(("Monitoring Script", False))
    
    # Check 4: Final checklist
    try:
        result4 = final_checklist()
        results.append(("Deployment Checklist", result4))
    except Exception as e:
        print(f"\n  ⚠️ Error: {e}")
        results.append(("Deployment Checklist", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("VERIFICATION RESULTS")
    print("=" * 80)
    
    all_passed = True
    for check_name, passed in results:
        status = "✅ READY" if passed else "⚠️ CHECK"
        print(f"  {status}: {check_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("✅ READY FOR PRODUCTION DEPLOYMENT")
        print("\nThe midnight display freeze fix is properly implemented.")
        print("Follow the deployment checklist above to complete the fix.")
    else:
        print("⚠️ REVIEW ITEMS ABOVE BEFORE DEPLOYMENT")
        print("\nSome checks need attention. Review and fix before deploying.")
    print("=" * 80)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)