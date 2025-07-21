#!/usr/bin/env python3
"""
Test penetration tab in isolation.
"""
import os
import sys
import traceback

# Set environment variables
os.environ['USE_DUCKDB'] = 'true'

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    print("Importing PenetrationTab...")
    from aemo_dashboard.penetration import PenetrationTab
    
    print("Creating PenetrationTab instance...")
    penetration = PenetrationTab()
    
    print("Creating layout...")
    layout = penetration.create_layout()
    
    print("Success! Penetration tab created without errors.")
    
except Exception as e:
    print(f"ERROR: {e}")
    traceback.print_exc()