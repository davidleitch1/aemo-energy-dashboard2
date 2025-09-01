#!/usr/bin/env python3
"""
Simple test to verify auto-refresh configuration changes
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import and check the dashboard module
from aemo_dashboard.generation import gen_dash

# Check if the auto-refresh script contains our changes
dashboard = gen_dash.EnergyDashboard()

print("Testing auto-refresh configuration...")
print("=" * 50)

# Create a test dashboard to check the HTML
try:
    test_app = dashboard.create_dashboard()
    
    # The auto_refresh_script should be in the dashboard
    # Let's check if it's a Column and has our script
    if hasattr(test_app, '__len__'):
        for component in test_app:
            if hasattr(component, 'object') and isinstance(component.object, str):
                if '540000' in component.object and 'saveDashboardState' in component.object:
                    print("✓ Auto-refresh interval updated to 9 minutes (540000ms)")
                    print("✓ State preservation functions added")
                    
                    # Check for specific features
                    if 'Auto-refresh: 9min' in component.object:
                        print("✓ UI indicator shows '9min'")
                    if 'localStorage.setItem' in component.object:
                        print("✓ localStorage save functionality present")
                    if 'restoreDashboardState' in component.object:
                        print("✓ State restoration functionality present")
                    if '2 data collector cycles' in component.object:
                        print("✓ Comment about collector cycles added")
                    
                    print("\nConfiguration test PASSED!")
                    break
    else:
        print("✗ Could not verify dashboard structure")
        
except Exception as e:
    print(f"✗ Error testing dashboard: {e}")
    
print("\nChecking main() function output...")
# Check if the print statement was updated
import inspect
main_source = inspect.getsource(gen_dash.main)
if '9 minutes (2 data collector cycles)' in main_source:
    print("✓ Console output message updated correctly")
else:
    print("✗ Console output message not updated")

print("\nAll configuration tests complete.")