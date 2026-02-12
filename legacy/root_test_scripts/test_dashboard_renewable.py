#!/usr/bin/env python3
"""
Test dashboard renewable calculation with rooftop solar
"""

import sys
import os
from datetime import datetime

# Add the src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager

# Mock datetime to use 12:00 on Sept 26
import aemo_dashboard.nem_dash.nem_dash_query_manager as nem_module
from datetime import timedelta  # Import timedelta separately
original_datetime = nem_module.datetime

class MockDatetime:
    @staticmethod
    def now():
        return datetime(2025, 9, 26, 12, 0, 0)

    def __getattr__(self, name):
        return getattr(original_datetime, name)

    def __call__(self, *args, **kwargs):
        return original_datetime(*args, **kwargs)

# Apply the mock
nem_module.datetime = MockDatetime()
nem_module.timedelta = timedelta  # Use the imported timedelta

print("Testing Dashboard Renewable Calculation with Rooftop Solar")
print("=" * 60)

try:
    # Initialize and call
    manager = NEMDashQueryManager()
    result = manager.get_renewable_data()

    print(f"\nDashboard Results (at 12:00 on Sept 26, 2025):")
    print(f"  Renewable MW: {result['renewable_mw']:.1f}")
    print(f"  Total MW: {result['total_mw']:.1f}")
    print(f"  Renewable %: {result['renewable_pct']:.1f}%")

    # Compare with records
    import json
    from pathlib import Path
    records_file = Path("/Volumes/davidleitch/aemo_production/data/renewable_records.json")
    if records_file.exists():
        with open(records_file, 'r') as f:
            records = json.load(f)
        print(f"\nRecords from file:")
        print(f"  All-time: {records['all_time']['value']:.1f}%")
        print(f"  Hour 12: {records['hourly']['12']['value']:.1f}%")

finally:
    # Restore
    nem_module.datetime = original_datetime

print("\n" + "=" * 60)