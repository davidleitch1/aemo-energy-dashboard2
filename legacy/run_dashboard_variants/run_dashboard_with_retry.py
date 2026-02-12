#!/usr/bin/env python3
"""
Run the AEMO dashboard with retry logic for file access
This version patches the DuckDB initialization to handle concurrent file access
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Apply the retry patch before importing the dashboard
print("Applying retry logic patch for concurrent file access...")
from data_service.duckdb_init_patch import patch_duckdb_initialization
patch_duckdb_initialization()

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

if __name__ == "__main__":
    print("Starting AEMO Dashboard with file access retry logic...")
    print("This version handles concurrent file access gracefully")
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