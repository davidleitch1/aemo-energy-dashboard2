#!/usr/bin/env python3
"""
Run the AEMO dashboard with DuckDB adapters
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

if __name__ == "__main__":
    print("Starting AEMO Dashboard with DuckDB adapters...")
    print("USE_DUCKDB environment variable:", os.getenv('USE_DUCKDB'))
    print("\nDashboard will be available at http://localhost:5006")
    print("Press Ctrl+C to stop\n")
    
    main()