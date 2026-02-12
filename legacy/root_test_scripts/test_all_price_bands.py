#!/usr/bin/env python3
"""
Test the updated Price Band Details table to verify it shows all price bands including $0-$300
"""

import os
import sys
from pathlib import Path

# Use mounted production data
os.environ['USE_DUCKDB'] = 'true'
os.environ['DASHBOARD_PORT'] = '5559'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("=" * 80)
print("Testing Price Band Details Table with ALL Price Bands")
print("=" * 80)
print("\nVerifying that the table now includes:")
print("1. Below $0 (negative prices)")
print("2. $0-$300 (normal prices) - NEWLY ADDED")
print("3. $301-$1000 (high prices)")
print("4. Above $1000 (extreme prices)")
print("\nThe table should show all bands regardless of their frequency.")
print("=" * 80)

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

print("\nStarting dashboard on port 5559...")
print("Navigate to http://localhost:5559")
print("\nTo test:")
print("1. Go to the 'Prices' tab")
print("2. Click on 'Price Bands' sub-tab")
print("3. Check the 'Price Band Details' table below")
print("4. Verify all 4 price bands are shown for each selected region")
print("\nPress Ctrl+C to stop")

# Override the port
import sys
sys.argv = ['gen_dash.py', '--port', '5559']

# Run the dashboard
main()