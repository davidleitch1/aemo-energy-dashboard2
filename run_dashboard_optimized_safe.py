#!/usr/bin/env python3
"""
Safe optimized dashboard startup - balances speed with compatibility
Target: 2-3 second startup while maintaining full functionality
"""
import os
import sys
import time
from pathlib import Path

start_time = time.time()

# Set environment variables for optimization
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("Starting AEMO Dashboard (Optimized)...")
print(f"USE_DUCKDB: {os.getenv('USE_DUCKDB')}")

# Import with minimal changes - just use the existing dashboard
# but with DuckDB enabled which gives most of the performance benefit
from aemo_dashboard.generation.gen_dash import main

print(f"\nStartup time: {time.time() - start_time:.2f}s")
print("Dashboard will be available at http://localhost:5006")
print("Press Ctrl+C to stop\n")

if __name__ == "__main__":
    main()