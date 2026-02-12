#!/usr/bin/env python3
"""
Run the optimized AEMO dashboard with lazy loading and performance improvements
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import and run the optimized dashboard
from aemo_dashboard.generation.gen_dash_optimized import main

if __name__ == "__main__":
    print("Starting Optimized AEMO Dashboard...")
    print("USE_DUCKDB environment variable:", os.getenv('USE_DUCKDB'))
    print("\nOptimizations enabled:")
    print("  ✓ Lazy tab loading")
    print("  ✓ Shared query managers") 
    print("  ✓ Minimal initial data loading")
    print("  ✓ Background tab initialization")
    print("\nDashboard will be available at http://localhost:5006")
    print("Press Ctrl+C to stop\n")
    
    main()