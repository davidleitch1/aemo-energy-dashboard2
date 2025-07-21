#!/usr/bin/env python3
"""
Debug version of dashboard runner to identify hang location.
"""
import os
import sys

# Set environment variables
os.environ['USE_DUCKDB'] = 'true'
os.environ['DASHBOARD_PORT'] = '5006'

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    print("Starting dashboard with debug output...")
    print("1. Importing gen_dash module...")
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    print("2. Creating EnergyDashboard instance...")
    dashboard = EnergyDashboard()
    
    print("3. Creating main layout...")
    main_layout = dashboard.create_main_layout()
    
    print("4. Starting server...")
    dashboard.start_server()
    
except Exception as e:
    print(f"ERROR during startup: {e}")
    import traceback
    traceback.print_exc()