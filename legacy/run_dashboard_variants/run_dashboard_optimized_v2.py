#!/usr/bin/env python3
"""
Optimized dashboard with balanced performance and maintainability
Target: 1-2 second startup time
"""
import os
import sys
import time
from pathlib import Path

start_time = time.time()

# Set environment variables for optimization
os.environ['USE_DUCKDB'] = 'true'
os.environ['DUCKDB_LAZY_VIEWS'] = 'true'  # Create views on first use

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("Starting AEMO Dashboard (Optimized)...")

# Import with startup timing
t0 = time.time()
from aemo_dashboard.generation.gen_dash_optimized_v2 import main
print(f"Import time: {time.time() - t0:.2f}s")

print(f"Total startup: {time.time() - start_time:.2f}s")
print("\nDashboard will be available at http://localhost:5006")
print("Press Ctrl+C to stop\n")

if __name__ == "__main__":
    main()