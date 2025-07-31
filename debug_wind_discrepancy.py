#!/usr/bin/env python3
"""
Debug discrepancy between calculated wind and dashboard display
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
print("Debugging Wind Calculation vs Dashboard Display")
print("="*60)

# Load DUID mapping
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)

duid_to_fuel = gen_info.set_index('DUID')['Fuel'].to_dict()

# Get time around 20:00-21:00 based on screenshot
check_time = datetime(2025, 7, 25, 20, 30)
start_time = check_time - timedelta(minutes=30)
end_time = check_time + timedelta(minutes=30)

print(f"\nChecking period: {start_time} to {end_time}")

# Load generation data
gen_df = pd.read_parquet(gen_5min_path)
gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
gen_df = gen_df[(gen_df['settlementdate'] >= start_time) & 
                (gen_df['settlementdate'] <= end_time)]

# Map fuel types
gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)

# Check all fuel types at 20:30
target_time = datetime(2025, 7, 25, 20, 30)
at_target = gen_df[gen_df['settlementdate'] == target_time]

print(f"\nGeneration at {target_time}:")
fuel_totals = at_target.groupby('fuel_type')['scadavalue'].sum().sort_values(ascending=False)

total_gen = 0
for fuel, mw in fuel_totals.items():
    if fuel not in ['Battery Storage', 'Transmission Flow'] and pd.notna(fuel):
        print(f"  {fuel}: {mw:,.0f} MW")
        total_gen += mw

print(f"\nTotal generation (excl battery): {total_gen:,.0f} MW")

# Look for any anomalies
print("\n" + "-"*40)
print("Checking for duplicate DUIDs or mapping issues...")

# Check if any DUIDs appear multiple times
duid_counts = at_target['duid'].value_counts()
duplicates = duid_counts[duid_counts > 1]
if len(duplicates) > 0:
    print(f"\nWARNING: Found duplicate DUIDs:")
    for duid, count in duplicates.items():
        print(f"  {duid}: appears {count} times")
else:
    print("✓ No duplicate DUIDs")

# Check for unmapped DUIDs
unmapped = at_target[at_target['fuel_type'].isna()]
if len(unmapped) > 0:
    print(f"\nWARNING: {len(unmapped)} DUIDs without fuel mapping")
    print("Top unmapped by generation:")
    for _, row in unmapped.nlargest(5, 'scadavalue').iterrows():
        print(f"  {row['duid']}: {row['scadavalue']:.1f} MW")

# Check if we're using wrong data file
print("\n" + "-"*40)
print("Data file verification:")
print(f"Reading from: {gen_5min_path}")

# Check file timestamp
file_stats = os.stat(gen_5min_path)
file_modified = datetime.fromtimestamp(file_stats.st_mtime)
print(f"File last modified: {file_modified}")

# Sample some data to verify it's 5-minute intervals
sample_times = gen_df['settlementdate'].head(20).tolist()
intervals = [(sample_times[i+1] - sample_times[i]).total_seconds() / 60 
             for i in range(len(sample_times)-1)]
avg_interval = sum(intervals) / len(intervals) if intervals else 0
print(f"Average interval: {avg_interval:.1f} minutes")

# Double-check wind specifically
print("\n" + "-"*40)
print("Wind generation breakdown:")
wind_at_target = at_target[at_target['fuel_type'] == 'Wind']
print(f"Wind DUIDs generating: {len(wind_at_target)}")
print(f"Total wind: {wind_at_target['scadavalue'].sum():,.1f} MW")

# Show top wind generators
print("\nTop 5 wind generators at this time:")
for _, row in wind_at_target.nlargest(5, 'scadavalue').iterrows():
    print(f"  {row['duid']}: {row['scadavalue']:.1f} MW")