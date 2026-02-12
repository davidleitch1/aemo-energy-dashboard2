#!/usr/bin/env python3
"""
Run the AEMO dashboard with initialization fix
This version fixes the loading screen hang issue
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Apply the initialization fix
from aemo_dashboard.generation.init_fix_patch import patch_dashboard_initialization
patch_dashboard_initialization()

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

if __name__ == "__main__":
    print("Starting AEMO Dashboard with initialization fix...")
    print("This version fixes the loading screen hang issue")
    print("\nDashboard will be available at http://localhost:5008")
    print("Press Ctrl+C to stop\n")
    
    try:
        main()
    except KeyboardInterrupt:
        print("\nDashboard stopped by user")
    except Exception as e:
        print(f"\nError starting dashboard: {e}")
        import traceback
        traceback.print_exc()