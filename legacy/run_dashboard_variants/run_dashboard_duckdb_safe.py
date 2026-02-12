#!/usr/bin/env python3
"""
Run the AEMO dashboard with safe DuckDB implementation that includes retry logic
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'
os.environ['USE_SAFE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Monkey patch the duckdb_data_service import to use safe version
import sys
original_import = __builtins__.__import__

def safe_import(name, *args, **kwargs):
    if name == 'data_service.shared_data_duckdb':
        # Import the safe version instead
        module = original_import('data_service.shared_data_duckdb_safe', *args, **kwargs)
        # Replace the service instance
        module.duckdb_data_service = module.safe_duckdb_service
        return module
    return original_import(name, *args, **kwargs)

__builtins__.__import__ = safe_import

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

if __name__ == "__main__":
    print("Starting AEMO Dashboard with Safe DuckDB implementation...")
    print("This version includes retry logic for handling concurrent file access")
    print("USE_DUCKDB environment variable:", os.getenv('USE_DUCKDB'))
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