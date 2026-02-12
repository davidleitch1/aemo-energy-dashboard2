#!/usr/bin/env python3
"""
Optimized dashboard startup - targets 3 second startup time
"""
import os
import sys
import time
from pathlib import Path

start_time = time.time()

# Set environment BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'
os.environ['PANEL_EAGER_LOAD'] = 'false'  # Defer panel extension loading
os.environ['DUCKDB_LAZY_VIEWS'] = 'true'  # Create views on-demand

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("Starting AEMO Dashboard (Fast Mode)...")
print(f"USE_DUCKDB: {os.getenv('USE_DUCKDB')}")

# Import only essentials first
import panel as pn

# Configure panel with minimal extensions first
pn.extension('tabulator', defer_load=True, loading_indicator=False)

# Now do the main import
from aemo_dashboard.generation.gen_dash_fast import FastEnergyDashboard, main

print(f"\nStartup time: {time.time() - start_time:.2f}s")
print("Dashboard will be available at http://localhost:5006")
print("Press Ctrl+C to stop\n")

if __name__ == "__main__":
    main()