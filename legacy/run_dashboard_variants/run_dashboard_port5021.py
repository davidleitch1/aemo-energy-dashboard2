#!/usr/bin/env python3
"""
Run the AEMO dashboard with DuckDB adapters on port 5021
"""

import os
import sys
from pathlib import Path

# Set environment variables BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'
os.environ['DASHBOARD_PORT'] = '5021'  # Set custom port

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

if __name__ == "__main__":
    # Import and run the dashboard main function
    from aemo_dashboard.generation.gen_dash import main

    main()
