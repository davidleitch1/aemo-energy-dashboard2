#!/usr/bin/env python3
"""
Test script to verify the renewable energy calculation in the dashboard
"""

import sys
import os
from datetime import datetime, timedelta

# Add the src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Now we can import the dashboard modules
from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager

def test_renewable_calculation():
    """Test the renewable calculation with and without rooftop solar"""

    print("Testing Dashboard Renewable Calculation")
    print("=" * 50)

    # Initialize the query manager
    manager = NEMDashQueryManager()

    # Override the get_renewable_data to use a specific time that has data
    # Use 11:55 to 12:00 as the time range
    from datetime import datetime, timedelta
    import pandas as pd

    # Check latest available data
    gen_path = '/Volumes/davidleitch/aemo_production/data/scada5.parquet'
    df = pd.read_parquet(gen_path)
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    latest_time = df['settlementdate'].max()
    print(f"\nLatest data available: {latest_time}")

    # Temporarily patch the method to use the latest available time
    original_method = manager.get_renewable_data
    def patched_get_renewable_data():
        # Use a time range that has data
        end_date = latest_time
        start_date = end_date - timedelta(minutes=5)

        # Call the original method but with our time range
        from unittest.mock import patch
        with patch('aemo_dashboard.nem_dash.nem_dash_query_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value = end_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            return original_method()

    manager.get_renewable_data = patched_get_renewable_data

    # Get renewable data
    print("\nGetting renewable data for latest available time...")
    renewable_data = manager.get_renewable_data()

    print(f"\nResults:")
    print(f"  Renewable MW: {renewable_data['renewable_mw']:.1f}")
    print(f"  Total MW: {renewable_data['total_mw']:.1f}")
    print(f"  Renewable %: {renewable_data['renewable_pct']:.1f}%")

    # Compare with records file
    import json
    from pathlib import Path

    records_file = Path("/Volumes/davidleitch/aemo_production/data/renewable_records.json")
    if records_file.exists():
        with open(records_file, 'r') as f:
            records = json.load(f)
        print(f"\nRecords from file:")
        print(f"  All-time record: {records['all_time']['value']:.1f}%")
        print(f"  Current hour ({datetime.now().hour}): {records['hourly'][str(datetime.now().hour)]['value']:.1f}%")

    print("\n" + "=" * 50)
    print("Test complete!")

if __name__ == "__main__":
    test_renewable_calculation()