#!/usr/bin/env python3
"""
Simple test to check renewable calculation
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# Add the src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Check what time has data
gen_path = '/Volumes/davidleitch/aemo_production/data/scada5.parquet'
df = pd.read_parquet(gen_path)
df['settlementdate'] = pd.to_datetime(df['settlementdate'])
latest_time = df['settlementdate'].max()
print(f"Latest data available: {latest_time}")

# Now test with that time
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

manager = GenerationQueryManager()
end_date = latest_time
start_date = end_date - timedelta(minutes=15)

print(f"\nQuerying generation from {start_date} to {end_date}")
data = manager.query_generation_by_fuel(
    start_date=start_date,
    end_date=end_date,
    region='NEM',
    resolution='5min'
)

if not data.empty:
    print(f"Data shape: {data.shape}")
    latest = data.groupby('fuel_type')['total_generation_mw'].last()

    # Calculate renewable without rooftop
    renewable_fuels = ['Wind', 'Solar', 'Water', 'Hydro', 'Biomass']
    generation_fuels = ['Coal', 'CCGT', 'OCGT', 'Gas other', 'Other',
                       'Wind', 'Solar', 'Water', 'Hydro', 'Biomass']

    renewable_mw = latest[latest.index.isin(renewable_fuels)].sum()
    total_mw = latest[latest.index.isin(generation_fuels)].sum()

    print(f"\nWithout Rooftop Solar:")
    print(f"  Renewable: {renewable_mw:.1f} MW")
    print(f"  Total: {total_mw:.1f} MW")
    print(f"  Renewable %: {(renewable_mw/total_mw*100):.1f}%")

    # Now add rooftop solar
    rooftop_path = '/Volumes/davidleitch/aemo_production/data/rooftop30.parquet'
    rooftop_df = pd.read_parquet(rooftop_path)
    rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])

    # Get latest rooftop data at or before our time
    rooftop_latest = rooftop_df[rooftop_df['settlementdate'] <= latest_time].copy()
    if not rooftop_latest.empty:
        rooftop_latest = rooftop_latest.sort_values('settlementdate').iloc[-1]
        rooftop_mw = rooftop_latest['value'] if 'value' in rooftop_latest else 0
        rooftop_time = rooftop_latest['settlementdate']

        print(f"\nRooftop Solar:")
        print(f"  Time: {rooftop_time}")
        print(f"  Value: {rooftop_mw:.1f} MW")

        print(f"\nWith Rooftop Solar:")
        print(f"  Renewable: {renewable_mw + rooftop_mw:.1f} MW")
        print(f"  Total: {total_mw + rooftop_mw:.1f} MW")
        print(f"  Renewable %: {((renewable_mw + rooftop_mw)/(total_mw + rooftop_mw)*100):.1f}%")
else:
    print("No data found!")

print("\n" + "=" * 50)

# Now test the actual dashboard method
print("\nTesting Dashboard Query Manager:")
from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager

nem_manager = NEMDashQueryManager()

# Monkey patch datetime to use our known good time
import aemo_dashboard.nem_dash.nem_dash_query_manager as nem_module
original_datetime = nem_module.datetime

class MockDatetime:
    @staticmethod
    def now():
        return latest_time

    def __getattr__(self, name):
        return getattr(original_datetime, name)

nem_module.datetime = MockDatetime()

# Now call the method
result = nem_manager.get_renewable_data()

print(f"Dashboard results:")
print(f"  Renewable: {result['renewable_mw']:.1f} MW")
print(f"  Total: {result['total_mw']:.1f} MW")
print(f"  Renewable %: {result['renewable_pct']:.1f}%")

# Restore
nem_module.datetime = original_datetime