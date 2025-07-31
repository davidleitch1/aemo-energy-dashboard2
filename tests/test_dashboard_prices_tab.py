#!/usr/bin/env python3
"""Quick test to verify the prices tab loads data correctly"""

import os
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set environment variables
os.environ['ENABLE_PN_CACHE'] = 'false'
os.environ['DASHBOARD_PORT'] = '5559'

from aemo_dashboard.generation.gen_dash import EnergyGenerationDashboard

# Create dashboard instance
print("Creating dashboard...")
dashboard = EnergyGenerationDashboard()

# Create the tabs
print("Creating dashboard layout...")
tabs = dashboard.create_dashboard_layout()

# Manually trigger the prices tab creation
print("Creating prices tab...")
prices_tab = dashboard._create_prices_tab()

print("\nPrices tab created successfully!")
print("Check if the price chart is loading data...")

# Save to HTML to verify
template = dashboard.template
template.save('test_dashboard_prices_output.html')
print("Saved test output to test_dashboard_prices_output.html")

# Try to get the server
try:
    server = dashboard.show(port=5559, open=False)
    print(f"\nDashboard running at http://localhost:5559")
    print("Press Ctrl+C to stop")
    server.join()
except KeyboardInterrupt:
    print("\nStopping dashboard...")
except Exception as e:
    print(f"Could not start server: {e}")