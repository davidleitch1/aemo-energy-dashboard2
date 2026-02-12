#!/usr/bin/env python3
"""
Debug wind generation calculation
"""

import os
import sys
import pandas as pd
import pickle
from datetime import datetime, timedelta

# Add the src directory to path
sys.path.insert(0, 'src')

from dotenv import load_dotenv
load_dotenv()

# Get file paths
gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN')
if not gen_5min_path:
    gen_5min_path = os.getenv('GEN_OUTPUT_FILE').replace('scada30.parquet', 'scada5.parquet')

gen_info_path = os.getenv('GEN_INFO_FILE')

print("="*60)
print("Wind Generation Debug")
print("="*60)

# Load DUID mapping
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)

# Check wind DUIDs
wind_duids = gen_info[gen_info['Fuel'] == 'Wind']
print(f"\nTotal Wind DUIDs: {len(wind_duids)}")
print(f"Total Wind Capacity: {wind_duids['Capacity(MW)'].sum():,.0f} MW")

# Get recent generation data
end_time = datetime.now()
start_time = end_time - timedelta(minutes=15)

gen_df = pd.read_parquet(gen_5min_path)
gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
gen_df = gen_df[(gen_df['settlementdate'] >= start_time) & 
                (gen_df['settlementdate'] <= end_time)]

# Get latest timestamp
latest_time = gen_df['settlementdate'].max()
print(f"\nChecking generation at: {latest_time}")

# Filter for wind DUIDs only
wind_duid_list = wind_duids['DUID'].tolist()
wind_gen = gen_df[(gen_df['settlementdate'] == latest_time) & 
                  (gen_df['duid'].isin(wind_duid_list))]

print(f"\nWind DUIDs generating: {len(wind_gen)}")
print(f"Total wind generation: {wind_gen['scadavalue'].sum():,.1f} MW")

# Check for any suspicious values
print("\nTop 10 wind generators:")
top_wind = wind_gen.nlargest(10, 'scadavalue')[['duid', 'scadavalue']]
for _, row in top_wind.iterrows():
    duid_info = wind_duids[wind_duids['DUID'] == row['duid']].iloc[0]
    capacity = duid_info['Capacity(MW)']
    utilization = (row['scadavalue'] / capacity * 100) if capacity > 0 else 0
    print(f"  {row['duid']}: {row['scadavalue']:>7.1f} MW (Capacity: {capacity:>6.1f} MW, {utilization:>5.1f}%)")

# Check if any DUIDs are over capacity
print("\nChecking for over-capacity generation:")
over_capacity = []
for _, row in wind_gen.iterrows():
    duid_info = wind_duids[wind_duids['DUID'] == row['duid']]
    if not duid_info.empty:
        capacity = duid_info.iloc[0]['Capacity(MW)']
        if row['scadavalue'] > capacity * 1.05:  # Allow 5% over
            over_capacity.append({
                'duid': row['duid'],
                'generation': row['scadavalue'],
                'capacity': capacity,
                'percent': row['scadavalue'] / capacity * 100
            })

if over_capacity:
    print("WARNING: DUIDs generating over capacity:")
    for item in over_capacity:
        print(f"  {item['duid']}: {item['generation']:.1f} MW / {item['capacity']:.1f} MW = {item['percent']:.1f}%")
else:
    print("✓ All wind generation within capacity limits")

# Double-check by mapping fuel types
print("\n" + "-"*40)
print("Double-checking with fuel type mapping:")
duid_to_fuel = gen_info.set_index('DUID')['Fuel'].to_dict()
latest_gen = gen_df[gen_df['settlementdate'] == latest_time].copy()
latest_gen['fuel_type'] = latest_gen['duid'].map(duid_to_fuel)

# Group by fuel type
fuel_totals = latest_gen.groupby('fuel_type')['scadavalue'].sum()
print(f"\nWind generation by fuel mapping: {fuel_totals.get('Wind', 0):,.1f} MW")

# Check for any mismatched DUIDs
wind_gen_mapped = latest_gen[latest_gen['fuel_type'] == 'Wind']
print(f"DUIDs mapped as Wind: {len(wind_gen_mapped)}")

# Compare the two methods
diff = abs(wind_gen['scadavalue'].sum() - fuel_totals.get('Wind', 0))
if diff > 0.1:
    print(f"\nWARNING: Difference between methods: {diff:.1f} MW")
else:
    print(f"\n✓ Both methods match")

# Historical comparison
print("\n" + "-"*40)
print("Historical wind generation (last 24 hours):")
day_ago = end_time - timedelta(days=1)
day_gen = gen_df[gen_df['settlementdate'] >= day_ago].copy()
day_gen['fuel_type'] = day_gen['duid'].map(duid_to_fuel)
wind_day = day_gen[day_gen['fuel_type'] == 'Wind']

# Group by hour
wind_day['hour'] = wind_day['settlementdate'].dt.floor('H')
hourly_wind = wind_day.groupby('hour')['scadavalue'].sum() / 12  # Convert to MW from MWh

print("Hourly averages:")
for hour, mw in hourly_wind.tail(12).items():
    print(f"  {hour}: {mw:,.0f} MW")

print(f"\n24-hour max: {hourly_wind.max():,.0f} MW")
print(f"24-hour average: {hourly_wind.mean():,.0f} MW")